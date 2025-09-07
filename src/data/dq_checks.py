"""Data quality checks for train event slices.

This module exposes :func:`run_all` which performs a small set of data
quality validations on a slice of train event data.  The checks include
negative dwell times, backward time travel, unknown stations, missing
edges and headway range violations.  A summary report is written to
``dq_report.md``.  The function raises a :class:`ValueError` when fatal
issues are detected.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import pandas as pd

__all__ = ["run_all"]


def _ensure_series(df: pd.DataFrame, primary: str, fallback: str) -> pd.Series:
    """Return a tz-aware datetime Series combining primary and fallback.

    Ensures a consistent dtype (datetime64[ns, UTC]) to avoid pandas
    FutureWarning about downcasting on ``fillna``.
    """

    if primary in df.columns:
        ser = pd.to_datetime(df[primary], utc=True, errors="coerce")
    else:
        ser = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")

    if fallback in df.columns:
        fb = pd.to_datetime(df[fallback], utc=True, errors="coerce")
        ser = ser.fillna(fb)

    # Guarantee dtype
    if str(ser.dtype) != "datetime64[ns, UTC]":
        ser = ser.astype("datetime64[ns, UTC]")
    return ser


def run_all(
    df_slice: pd.DataFrame,
    edges_df: pd.DataFrame,
    stations_dict: Dict[str, int],
    report_path: str | Path = Path("reports") / "dq_report.md",
) -> dict[str, List[str]]:
    """Run data quality checks on *df_slice*.

    Parameters
    ----------
    df_slice:
        Train event records filtered to the corridor under analysis.
        Expected columns include ``train_id``, ``station_id``, ``sched_arr``,
        ``sched_dep``, ``act_arr`` and ``act_dep``.
    edges_df:
        DataFrame containing edges of the corridor with columns ``u`` and
        ``v`` describing consecutive stations.
    stations_dict:
        Mapping of ``station_id`` to its ordered position along the corridor.
    report_path:
        Location where the markdown report will be written.  Defaults to
        ``reports/dq_report.md`` relative to the repository root.

    Returns
    -------
    dict
        Dictionary containing ``"warnings"`` and ``"errors"`` lists.

    Raises
    ------
    ValueError
        If any errors are encountered during the checks.
    """

    warnings: List[str] = []
    errors: List[str] = []

    if df_slice.empty:
        # Still create an empty report
        Path(report_path).parent.mkdir(parents=True, exist_ok=True)
        Path(report_path).write_text("# Data Quality Report\n\nNo data provided.")
        return {"warnings": warnings, "errors": errors}

    arr = _ensure_series(df_slice, "act_arr", "sched_arr")
    dep = _ensure_series(df_slice, "act_dep", "sched_dep")

    # ------------------------------------------------------------------
    # Negative dwell check
    dwell = (dep - arr).dt.total_seconds() / 60
    neg_dwell_mask = dwell < 0
    for idx in df_slice[neg_dwell_mask].index:
        row = df_slice.loc[idx]
        errors.append(
            f"Negative dwell for train {row.get('train_id')} at station {row.get('station_id')}"
        )

    # ------------------------------------------------------------------
    # Backward time check within each train's temporal sequence
    # Sort events by ascending reference time (arr if available else dep) to handle both directions
    ref_time = arr.fillna(dep)
    for train_id, grp in df_slice.groupby("train_id"):
        grp_ref = ref_time.loc[grp.index]
        # Drop rows with no usable time to avoid false positives
        valid_idx = grp_ref.dropna().sort_values().index
        grp_sorted = grp.loc[valid_idx]
        grp_arr = arr.loc[valid_idx]
        grp_dep = dep.loc[valid_idx]
        prev_dep = None
        for idx in grp_sorted.index:
            if prev_dep is not None and pd.notna(grp_arr.loc[idx]) and grp_arr.loc[idx] < prev_dep:
                errors.append(
                    f"Backward time for train {train_id} arriving at station {grp_sorted.loc[idx, 'station_id']}"
                )
            if pd.notna(grp_dep.loc[idx]):
                prev_dep = grp_dep.loc[idx]
    # ------------------------------------------------------------------
    # Unknown station check
    unknown_mask = ~df_slice["station_id"].isin(stations_dict.keys())
    for station in df_slice.loc[unknown_mask, "station_id"].unique():
        errors.append(f"Unknown station id {station}")

    # ------------------------------------------------------------------
    # Missing edge check based on observed temporal order per train
    edge_set = {(row["u"], row["v"]) for _, row in edges_df.iterrows()}
    for train_id, grp in df_slice.groupby("train_id"):
        grp_ref = ref_time.loc[grp.index]
        valid_idx = grp_ref.dropna().sort_values().index
        grp_sorted = grp.loc[valid_idx]
        prev_station = None
        for station in grp_sorted["station_id"]:
            if prev_station is not None:
                if (prev_station, station) not in edge_set and (station, prev_station) not in edge_set:
                    errors.append(
                        f"Missing edge between {prev_station} and {station} for train {train_id}"
                    )
            prev_station = station

    # ------------------------------------------------------------------
    # Headway range check (uses departures)
    dep_times = dep.copy()
    df_head = df_slice.assign(dep_time=dep_times).dropna(subset=["dep_time"])
    df_head = df_head.sort_values("dep_time")
    headways = (
        df_head.groupby("station_id")["dep_time"].diff().dt.total_seconds() / 60
    )
    neg_headway = headways < 0
    large_headway = headways > 180  # minutes
    for idx in headways[neg_headway].index:
        row = df_head.loc[idx]
        errors.append(
            f"Negative headway at station {row['station_id']} for train {row['train_id']}"
        )
    for idx, hw in headways[large_headway].items():
        row = df_head.loc[idx]
        warnings.append(
            f"Large headway {hw:.1f} min at station {row['station_id']} before train {row['train_id']}"
        )

    # ------------------------------------------------------------------
    # Write report
    report_lines = ["# Data Quality Report", "", "## Errors"]
    if errors:
        report_lines.extend([f"- {msg}" for msg in errors])
    else:
        report_lines.append("None")
    report_lines.extend(["", "## Warnings"])
    if warnings:
        report_lines.extend([f"- {msg}" for msg in warnings])
    else:
        report_lines.append("None")

    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(report_lines))

    if errors:
        raise ValueError("Data quality checks failed")

    return {"warnings": warnings, "errors": errors}
