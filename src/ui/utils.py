from __future__ import annotations

"""Formatting helpers and small utilities."""

from typing import Any, Dict, List
import pandas as pd


def as_table(records: List[Dict[str, Any]] | Dict[str, Any], *, columns: List[str] | None = None) -> pd.DataFrame:
    if isinstance(records, dict):
        try:
            df = pd.json_normalize(records)
        except Exception:
            df = pd.DataFrame([records])
    else:
        df = pd.DataFrame(records)
    if columns:
        cols = [c for c in columns if c in df.columns]
        if cols:
            df = df[cols]
    return df


def kv_table(d: Dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame({"key": list(d.keys()), "value": [d[k] for k in d.keys()]})

