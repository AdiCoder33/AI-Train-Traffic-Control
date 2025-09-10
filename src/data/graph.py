"""Graph construction utilities for corridor sections.

This module derives a simplified graph representation from scheduled
train events. Each edge between consecutive stations stores the median
scheduled run time and the 90th percentile headway between departures in
that direction.  Nodes simply capture the available platform count per
station.
"""

from __future__ import annotations

from pathlib import Path
from typing import Tuple

import pandas as pd

__all__ = ["build", "save"]


def build(
    df_slice: pd.DataFrame, stations_dict: dict[str, int]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build edge and node tables for a corridor.

    Parameters
    ----------
    df_slice:
        DataFrame containing events for trains operating within the
        corridor. Must include ``train_id``, ``station_id``, ``sched_dep``
        and ``sched_arr`` columns with timezone-aware timestamps.
    stations_dict:
        Mapping from station identifier to its ordered position along the
        corridor.

    Returns
    -------
    edges_df, nodes_df:
        ``edges_df`` contains columns ``u``, ``v``, ``min_run_time`` (in
        minutes), ``headway`` (90th percentile in minutes), ``block_id``,
        ``capacity`` and ``platform_cap``. ``nodes_df`` lists each
        ``station_id`` with its default number of ``platforms``.
    """

    # Nodes: every station has default single platform
    nodes_df = (
        pd.DataFrame({"station_id": list(stations_dict.keys())})
        .assign(platforms=1)
        .sort_values("station_id")
        .reset_index(drop=True)
    )

    if df_slice.empty:
        edges_cols = ["u", "v", "min_run_time", "headway", "block_id", "capacity", "platform_cap"]
        return pd.DataFrame(columns=edges_cols), nodes_df

    df = df_slice.copy()
    # Build reference departure and arrival times using available columns
    t_dep = pd.Series(pd.NaT, dtype="datetime64[ns, UTC]", index=df.index)
    for c in ("act_dep", "sched_dep", "act_arr", "sched_arr"):
        if c in df.columns:
            t_dep = t_dep.fillna(df[c])
    t_arr = pd.Series(pd.NaT, dtype="datetime64[ns, UTC]", index=df.index)
    for c in ("act_arr", "sched_arr", "act_dep", "sched_dep"):
        if c in df.columns:
            t_arr = t_arr.fillna(df[c])

    df["__t_dep__"] = t_dep
    df["__t_arr__"] = t_arr
    # Sort by temporal order per train to support both directions
    df = df.sort_values(["train_id", "__t_dep__", "__t_arr__"])  

    # Identify consecutive station visits for each train in time order
    df["next_station"] = df.groupby("train_id")["station_id"].shift(-1)
    df["next_arr"] = df.groupby("train_id")["__t_arr__"].shift(-1)
    df["run_time_min"] = (df["next_arr"] - df["__t_dep__"]).dt.total_seconds() / 60

    edges = df[df["next_station"].notna()].copy()
    edges["u"] = edges["station_id"]
    edges["v"] = edges["next_station"]

    # Median scheduled run time per direction
    run_times = (
        edges.groupby(["u", "v"])["run_time_min"].median().reset_index(name="min_run_time")
    )

    # 90th percentile headway based on temporal departure reference
    dep_times = edges[["u", "v", "__t_dep__"]].rename(columns={"__t_dep__": "dep_time"})
    dep_times = dep_times.sort_values("dep_time")
    dep_times["headway"] = (
        dep_times.groupby(["u", "v"])["dep_time"].diff().dt.total_seconds() / 60
    )
    headways = (
        dep_times.groupby(["u", "v"])["headway"].quantile(0.9).reset_index()
    )

    edges_df = run_times.merge(headways, on=["u", "v"], how="left")
    edges_df["headway"] = edges_df["headway"].fillna(0)

    # Optional seasonality: peak vs off-peak headway p90 (7-10 and 17-20 local time)
    try:
        if not dep_times.empty:
            dt = dep_times.copy()
            dt["hour"] = pd.to_datetime(dt["dep_time"]).dt.hour
            peak = dt[dt["hour"].isin([7,8,9,17,18,19,20])]
            offp = dt[~dt["hour"].isin([7,8,9,17,18,19,20])]
            h_peak = peak.groupby(["u","v"]).apply(lambda g: (g["dep_time"].diff().dt.total_seconds()/60).quantile(0.9)).reset_index(name="headway_peak") if not peak.empty else pd.DataFrame(columns=["u","v","headway_peak"])
            h_offp = offp.groupby(["u","v"]).apply(lambda g: (g["dep_time"].diff().dt.total_seconds()/60).quantile(0.9)).reset_index(name="headway_offpeak") if not offp.empty else pd.DataFrame(columns=["u","v","headway_offpeak"])
            edges_df = edges_df.merge(h_peak, on=["u","v"], how="left").merge(h_offp, on=["u","v"], how="left")
    except Exception:
        pass

    # Deterministic block ids and default capacities
    edges_df = edges_df.sort_values(["u", "v"]).reset_index(drop=True)
    edges_df["block_id"] = [f"B{i:04d}" for i in range(len(edges_df))]
    edges_df["capacity"] = 1
    edges_df["platform_cap"] = 1
    # Optional realism tags (placeholders)
    edges_df["gradient"] = 0.0
    edges_df["speed_profile"] = "normal"

    return edges_df, nodes_df


def save(
    edges_df: pd.DataFrame,
    nodes_df: pd.DataFrame,
    corridor: str,
    date: str | pd.Timestamp,
    base_dir: str | Path = "artifacts",
) -> None:
    """Persist graph tables under the artifact hierarchy.

    Parameters
    ----------
    edges_df, nodes_df:
        DataFrames as returned by :func:`build`.
    corridor:
        Name of the corridor used to create the subdirectory.
    date:
        Service date; will be formatted as ``YYYY-MM-DD``.
    base_dir:
        Base artifacts directory. Defaults to ``"artifacts"``.
    """

    date_str = pd.to_datetime(date).date().isoformat()
    out_dir = Path(base_dir) / corridor / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    edges_df.to_parquet(out_dir / "section_edges.parquet", index=False)
    nodes_df.to_parquet(out_dir / "section_nodes.parquet", index=False)
