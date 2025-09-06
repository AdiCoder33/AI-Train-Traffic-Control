"""Normalization utilities for train event datasets.

This module provides :func:`to_train_events` which converts raw CSV
records into a canonical format ready for downstream processing.  It
handles column renaming, timestamp parsing, delay computation and
station identifier mapping.

Usage
-----
Run the module as a script to quickly normalize the CSV files placed in
``data/raw``::

    python -m src.data.normalize

The CLI loads all CSVs using :func:`src.data.loader.load_raw`, applies the
normalization and prints the first few rows.
"""

from __future__ import annotations

from pathlib import Path
import argparse
import logging
from typing import Mapping

import pandas as pd

from .loader import load_raw

__all__ = ["to_train_events"]

logger = logging.getLogger(__name__)

# Column mapping from potential raw names to canonical ones.  The mapping
# purposefully contains a variety of common alternatives so that the
# function remains robust to slight variations across datasets.
_COLUMN_MAP: Mapping[str, str] = {
    "train": "train_id",
    "train_id": "train_id",
    "trainno": "train_id",
    "train_no": "train_id",
    "station": "station_name",
    "station_name": "station_name",
    "sched_arr": "sched_arr",
    "scheduled_arrival": "sched_arr",
    "planned_arrival": "sched_arr",
    "sched_dep": "sched_dep",
    "scheduled_departure": "sched_dep",
    "planned_departure": "sched_dep",
    "act_arr": "act_arr",
    "actual_arrival": "act_arr",
    "real_arrival": "act_arr",
    "act_dep": "act_dep",
    "actual_departure": "act_dep",
    "real_departure": "act_dep",
    "service_day": "day",
    "day": "day",
    "priority": "priority",
}


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of *df* with columns renamed to canonical names."""
    rename_map = {c: _COLUMN_MAP[c] for c in df.columns if c in _COLUMN_MAP}
    return df.rename(columns=rename_map).copy()


def _parse_times(df: pd.DataFrame, cols: list[str]) -> None:
    """Parse columns in *cols* to UTC timestamps in-place."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")


def _ensure_station_map(df: pd.DataFrame, path: Path) -> dict[str, str]:
    """Ensure a station mapping exists and return mapping ``name -> id``.

    If ``path`` does not exist it is created based on the unique station
    names in *df*.  New station names encountered are appended to the
    existing mapping.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        station_map = pd.read_csv(path)
    else:
        station_map = pd.DataFrame(columns=["station_id", "name"])

    # Build lookup dictionary for existing entries
    name_to_id: dict[str, str] = dict(zip(station_map["name"], station_map["station_id"]))

    unique_names = sorted({n for n in df["station_name"].dropna().unique() if n not in name_to_id})
    if unique_names:
        start_idx = len(station_map)
        new_ids = [f"S{start_idx + i:04d}" for i in range(len(unique_names))]
        new_entries = pd.DataFrame({"station_id": new_ids, "name": unique_names})
        station_map = pd.concat([station_map, new_entries], ignore_index=True)
        station_map.to_csv(path, index=False)
        name_to_id.update(dict(zip(unique_names, new_ids)))

    return name_to_id


def to_train_events(
    df_raw: pd.DataFrame, station_map_path: str | Path | None = None
) -> pd.DataFrame:
    """Normalize raw train event records.

    Parameters
    ----------
    df_raw:
        Raw DataFrame containing train event information.
    station_map_path:
        Location of ``station_map.csv``.  If ``None`` the file residing next
        to this module is used.

    Returns
    -------
    pandas.DataFrame
        Normalized DataFrame with canonical columns ``train_id``,
        ``station_id``, ``sched_arr``, ``sched_dep``, ``act_arr``,
        ``act_dep``, ``day``, ``priority``, ``arr_delay_min`` and
        ``dep_delay_min``.
    """
    df = _rename_columns(df_raw)

    time_cols = ["sched_arr", "sched_dep", "act_arr", "act_dep"]
    _parse_times(df, time_cols)

    # Compute delays
    if "act_arr" in df.columns and "sched_arr" in df.columns:
        df["arr_delay_min"] = (df["act_arr"] - df["sched_arr"]).dt.total_seconds() / 60
    else:
        df["arr_delay_min"] = pd.NA
    if "act_dep" in df.columns and "sched_dep" in df.columns:
        df["dep_delay_min"] = (df["act_dep"] - df["sched_dep"]).dt.total_seconds() / 60
    else:
        df["dep_delay_min"] = pd.NA

    # Map station names to ids
    if station_map_path is None:
        station_map_path = Path(__file__).with_name("station_map.csv")
    else:
        station_map_path = Path(station_map_path)

    if "station_name" in df.columns:
        name_to_id = _ensure_station_map(df, station_map_path)
        df["station_id"] = df["station_name"].map(name_to_id)

    # Arrange columns
    cols = [
        "train_id",
        "station_id",
        "sched_arr",
        "sched_dep",
        "act_arr",
        "act_dep",
        "day",
        "priority",
        "arr_delay_min",
        "dep_delay_min",
    ]

    # Only include columns that exist
    existing_cols = [c for c in cols if c in df.columns]
    return df[existing_cols]


def _main() -> None:  # pragma: no cover - convenience utility
    parser = argparse.ArgumentParser(description="Normalize raw train events")
    parser.add_argument(
        "--data-dir",
        default=Path("data/raw"),
        type=Path,
        help="Directory containing raw CSV files",
    )
    parser.add_argument(
        "--head",
        type=int,
        default=5,
        help="Number of normalized rows to display",
    )
    args = parser.parse_args()

    df_raw = load_raw(args.data_dir)
    df_norm = to_train_events(df_raw)
    print(df_norm.head(args.head).to_string(index=False))


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    _main()
