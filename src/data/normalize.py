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

Or use the small sample dataset::

    python -m src.data.normalize --sample

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
_COLUMN_MAP_RAW: Mapping[str, str] = {
    "train": "train_id",
    "train_id": "train_id",
    "trainno": "train_id",
    "train_no": "train_id",
    "Train No": "train_id",
    "Train No ": "train_id",
    "Train Number": "train_id",
    "station": "station_name",
    "station_name": "station_name",
    "Station Code": "station_code",
    "Station Name": "station_name",
    "sched_arr": "sched_arr",
    "scheduled_arrival": "sched_arr",
    "planned_arrival": "sched_arr",
    "Arrival time": "sched_arr",
    "ARRIVAL time": "sched_arr",
    "sched_dep": "sched_dep",
    "scheduled_departure": "sched_dep",
    "planned_departure": "sched_dep",
    "Departure Time": "sched_dep",
    "DEPARTURE TIME": "sched_dep",
    "act_arr": "act_arr",
    "actual_arrival": "act_arr",
    "Actual Arrival": "act_arr",
    "real_arrival": "act_arr",
    "act_dep": "act_dep",
    "actual_departure": "act_dep",
    "Actual Departure": "act_dep",
    "real_departure": "act_dep",
    # Date indicators
    "service_date": "service_date",
    "Service Date": "service_date",
    "date": "service_date",
    "service_day": "day",
    "day": "day",
    "priority": "priority",
}

# Normalize keys to allow case-insensitive and whitespace tolerant lookups
_COLUMN_MAP: Mapping[str, str] = {k.strip().lower(): v for k, v in _COLUMN_MAP_RAW.items()}


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of *df* with columns renamed to canonical names."""
    rename_map = {}
    for c in df.columns:
        key = c.strip().lower()
        if key in _COLUMN_MAP:
            rename_map[c] = _COLUMN_MAP[key]
    return df.rename(columns=rename_map).copy()


def _parse_times_with_service_date(df: pd.DataFrame, cols: list[str]) -> None:
    """Parse time or datetime columns using ``service_date`` when needed.

    If a column contains time-of-day strings (e.g. ``11:06:00``) without a
    date, combine with ``service_date`` to form full timestamps. Results are
    timezone-aware (UTC).
    """
    if "service_date" not in df.columns:
        # Without service_date, fall back to best-effort parsing.
        for col in cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
        return

    # Ensure service_date is a date string per row
    svc = pd.to_datetime(df["service_date"], errors="coerce").dt.date.astype(str)

    for col in cols:
        if col not in df.columns:
            continue
        ser = df[col].astype("string")
        mask_time_only = ser.str.match(r"^\s*\d{1,2}:\d{2}(:\d{2})?\s*$", na=False)
        # Parse anything that already looks like a full datetime without warnings
        parsed = pd.to_datetime(
            ser.where(~mask_time_only), utc=True, errors="coerce", format="mixed"
        )
        # Combine service_date with time-of-day entries and parse with explicit format
        if mask_time_only.any():
            combined = (svc + " " + ser.where(mask_time_only, "")).where(mask_time_only)
            parsed_time = pd.to_datetime(
                combined, utc=True, errors="coerce", format="%Y-%m-%d %H:%M:%S"
            )
            # Fallback for values without seconds
            missing = parsed_time.isna()
            if missing.any():
                parsed_time.loc[missing] = pd.to_datetime(
                    combined[missing], utc=True, errors="coerce", format="%Y-%m-%d %H:%M"
                )
            parsed = parsed.fillna(parsed_time)
        df[col] = parsed


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

    # Handle potential duplicate 'station_name' columns (e.g., both code and name
    # mapped to the same canonical name). If duplicates exist, take the first.
    col = df["station_name"]
    if isinstance(col, pd.DataFrame):
        ser = col.iloc[:, 0]
    else:
        ser = col

    unique_names = sorted({n for n in ser.dropna().unique() if n not in name_to_id})
    if unique_names:
        start_idx = len(station_map)
        new_ids = [f"S{start_idx + i:04d}" for i in range(len(unique_names))]
        new_entries = pd.DataFrame({"station_id": new_ids, "name": unique_names})
        station_map = pd.concat([station_map, new_entries], ignore_index=True)
        station_map.to_csv(path, index=False)
        name_to_id.update(dict(zip(unique_names, new_ids)))

    return name_to_id


def to_train_events(
    df_raw: pd.DataFrame,
    station_map_path: str | Path | None = None,
    *,
    default_service_date: str | pd.Timestamp | None = None,
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

    # Establish service_date
    if "service_date" not in df.columns:
        if default_service_date is None:
            # Try deriving from any existing datetime-like column; if none,
            # we cannot construct proper timestamps for time-of-day fields.
            candidate = None
            for col in ("sched_arr", "sched_dep", "act_arr", "act_dep"):
                if col in df.columns:
                    candidate = pd.to_datetime(df[col], errors="coerce")
                    if candidate.notna().any():
                        break
            if candidate is not None and candidate.notna().any():
                df["service_date"] = candidate.dt.date
            else:
                if len(df) > 0:
                    raise ValueError(
                        "service_date missing and cannot be derived; provide default_service_date"
                    )
                else:
                    df["service_date"] = pd.NaT
        else:
            df["service_date"] = pd.to_datetime(default_service_date).date()

    # Parse times with awareness of service_date
    time_cols = ["sched_arr", "sched_dep", "act_arr", "act_dep"]
    _parse_times_with_service_date(df, time_cols)

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

    # Determine a single station naming series (prefer human-readable name)
    station_series = None
    if "station_name" in df.columns:
        col = df["station_name"]
        station_series = col.iloc[:, 0] if isinstance(col, pd.DataFrame) else col
    elif "station_code" in df.columns:
        station_series = df["station_code"]

    if station_series is not None:
        # Build/update station_map from the resolved series
        tmp_df = pd.DataFrame({"station_name": station_series})
        name_to_id = _ensure_station_map(tmp_df, station_map_path)
        df["station_id"] = station_series.map(name_to_id)

    # Arrange columns
    cols = [
        "train_id",
        "station_id",
        "service_date",
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
        "--sample",
        action="store_true",
        help="Use sample data from data/sample instead of full raw dataset",
    )
    parser.add_argument(
        "--head",
        type=int,
        default=5,
        help="Number of normalized rows to display",
    )
    args = parser.parse_args()

    data_dir = Path("data/sample") if args.sample else args.data_dir
    df_raw = load_raw(data_dir)
    df_norm = to_train_events(df_raw)
    print(df_norm.head(args.head).to_string(index=False))


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    _main()
