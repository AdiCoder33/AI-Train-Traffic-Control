"""Baseline replay and KPI computation utilities.

This module replays train movements using available actual timestamps and
falls back to scheduled times combined with median run-times when necessary.
It exposes helpers to compute simple key performance indicators (KPIs) and
persist a basic Gantt chart for visualization.
"""

from __future__ import annotations

from pathlib import Path
import json
import pandas as pd
import matplotlib.pyplot as plt

__all__ = ["replay_and_kpis", "save"]


def replay_and_kpis(
    df_slice: pd.DataFrame, edges_df: pd.DataFrame
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Replay trains through the corridor and compute KPIs.

    Parameters
    ----------
    df_slice:
        DataFrame with events for trains within the corridor. Expected
        columns include ``train_id``, ``station_id``, ``sched_arr``,
        ``sched_dep``, ``act_arr`` and ``act_dep``.
    edges_df:
        Edge table containing at least ``u``, ``v`` and ``min_run_time``
        (minutes) for consecutive stations.

    Returns
    -------
    df_replay, metrics:
        ``df_replay`` is a copy of ``df_slice`` augmented with ``arr_time``
        and ``dep_time`` columns representing the replayed timestamps. The
        ``metrics`` dictionary stores simple KPIs such as average and 90th
        percentile arrival delay.
    """

    df = df_slice.copy()

    if df.empty:
        metrics = {
            "trains": 0,
            "avg_arrival_delay_min": 0.0,
            "p90_arrival_delay_min": 0.0,
        }
        df["arr_time"] = pd.NaT
        df["dep_time"] = pd.NaT
        df["arr_delay_min"] = []
        df["dep_delay_min"] = []
        return df, metrics

    run_lookup = (
        edges_df.set_index(["u", "v"])["min_run_time"].to_dict()
        if not edges_df.empty
        else {}
    )

    # Pre-create tz-aware datetime columns to avoid dtype mismatches on assignment
    df["arr_time"] = pd.Series(pd.NaT, dtype="datetime64[ns, UTC]", index=df.index)
    df["dep_time"] = pd.Series(pd.NaT, dtype="datetime64[ns, UTC]", index=df.index)

    for train_id, grp in df.groupby("train_id"):
        grp = grp.sort_values("sched_dep", na_position="first")
        prev_dep = None
        prev_station = None
        for idx, row in grp.iterrows():
            arr = row.get("act_arr")
            dep = row.get("act_dep")

            if pd.isna(arr):
                if prev_dep is not None and prev_station is not None:
                    rt = run_lookup.get((prev_station, row["station_id"]))
                    if rt is None:
                        rt = run_lookup.get((row["station_id"], prev_station), 0)
                    arr = prev_dep + pd.Timedelta(minutes=float(rt or 0))
                else:
                    arr = row.get("sched_arr") or row.get("sched_dep")

            if pd.isna(dep):
                if pd.notna(row.get("sched_arr")) and pd.notna(row.get("sched_dep")):
                    dwell = row["sched_dep"] - row["sched_arr"]
                    dep = arr + dwell
                else:
                    dep = row.get("sched_dep") or arr

            # Ensure assigned values are tz-aware timestamps compatible with column dtype
            df.at[idx, "arr_time"] = pd.to_datetime(arr, utc=True) if pd.notna(arr) else pd.NaT
            df.at[idx, "dep_time"] = pd.to_datetime(dep, utc=True) if pd.notna(dep) else pd.NaT
            prev_dep = dep
            prev_station = row["station_id"]

    if "sched_arr" in df.columns:
        df["arr_delay_min"] = (
            (df["arr_time"] - df["sched_arr"]).dt.total_seconds() / 60
        )
    else:
        df["arr_delay_min"] = pd.Series(dtype=float)

    if "sched_dep" in df.columns:
        df["dep_delay_min"] = (
            (df["dep_time"] - df["sched_dep"]).dt.total_seconds() / 60
        )
    else:
        df["dep_delay_min"] = pd.Series(dtype=float)

    metrics = {
        "trains": int(df["train_id"].nunique()),
        "avg_arrival_delay_min": float(df["arr_delay_min"].mean(skipna=True)),
        "p90_arrival_delay_min": float(
            df["arr_delay_min"].quantile(0.9) if not df["arr_delay_min"].isna().all() else 0.0
        ),
    }

    return df, metrics


def save(
    df_slice: pd.DataFrame,
    edges_df: pd.DataFrame,
    corridor: str,
    date: str | pd.Timestamp,
    base_dir: str | Path = "artifacts",
) -> dict[str, float]:
    """Replay baseline, compute KPIs and persist artifacts.

    Parameters
    ----------
    df_slice, edges_df:
        Inputs as for :func:`replay_and_kpis`.
    corridor:
        Name of the corridor used to structure the artifact directory.
    date:
        Service date; will be formatted as ``YYYY-MM-DD``.
    base_dir:
        Root directory to place artifacts under. Defaults to ``"artifacts"``.

    Returns
    -------
    dict[str, float]
        KPI metrics as returned by :func:`replay_and_kpis`.
    """

    df_replay, metrics = replay_and_kpis(df_slice, edges_df)

    date_str = pd.to_datetime(date).date().isoformat()
    out_dir = Path(base_dir) / corridor / date_str
    out_dir.mkdir(parents=True, exist_ok=True)

    with (out_dir / "kpis.json").open("w") as f:
        json.dump(metrics, f, indent=2)

    fig, ax = plt.subplots(
        figsize=(8, max(2, 0.3 * df_replay["train_id"].nunique() + 2))
    )

    if not df_replay.empty:
        stations = list(dict.fromkeys(df_replay["station_id"].tolist()))
        station_idx = {sid: i for i, sid in enumerate(stations)}
        for train_id, grp in df_replay.groupby("train_id"):
            grp = grp.sort_values("arr_time")
            grp_plot = grp.dropna(subset=["arr_time"])  # Matplotlib cannot plot NaT
            if grp_plot.empty:
                continue
            ax.plot(
                grp_plot["arr_time"],
                grp_plot["station_id"].map(station_idx),
                marker="o",
                label=train_id,
            )
        ax.set_yticks(range(len(stations)))
        ax.set_yticklabels(stations)
        ax.legend(loc="best", fontsize="small")

    ax.set_xlabel("Time")
    ax.set_ylabel("Station")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_dir / "baseline_gantt.png")
    plt.close(fig)

    return metrics
