"""Utilities to load raw CSV datasets.

This module provides a :func:`load_raw` helper that discovers all CSV
files within the ``data/raw`` directory, logs the columns present in each,
and returns a concatenated :class:`pandas.DataFrame` containing the data.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import pandas as pd

__all__ = ["load_raw"]

logger = logging.getLogger(__name__)


def _iter_csv_files(data_dir: Path) -> Iterable[Path]:
    """Yield CSV files within ``data_dir`` sorted by name."""
    return sorted(data_dir.glob("*.csv"))


def load_raw(data_dir: str | Path = Path("data/raw")) -> pd.DataFrame:
    """Load and concatenate all raw CSV files.

    Parameters
    ----------
    data_dir:
        Directory containing the ``.csv`` files to load.

    Returns
    -------
    pandas.DataFrame
        Concatenated DataFrame of all discovered CSV files. An empty
        DataFrame is returned if no files are found.
    """
    data_dir = Path(data_dir)
    frames: list[pd.DataFrame] = []

    for csv_path in _iter_csv_files(data_dir):
        df = pd.read_csv(csv_path)
        logger.info("Columns in %s: %s", csv_path.name, list(df.columns))
        frames.append(df)

    if frames:
        return pd.concat(frames, ignore_index=True)

    logger.warning("No CSV files found in %s", data_dir)
    return pd.DataFrame()


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    df = load_raw()
    print(f"Total rows: {len(df)}")
