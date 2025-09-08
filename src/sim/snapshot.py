"""Snapshot utilities for nationwide replay outputs.

Computes per-train virtual GPS at a given time from block occupancy.
"""

from __future__ import annotations

import pandas as pd

__all__ = ["snapshot_positions"]


def snapshot_positions(block_occupancy: pd.DataFrame, t: str | pd.Timestamp) -> pd.DataFrame:
    if block_occupancy.empty:
        return pd.DataFrame(columns=["train_id", "block_id", "u", "v", "progress_pct", "ETA_next"])  # noqa: E501

    t = pd.to_datetime(t, utc=True)
    occ = block_occupancy.copy()
    dur = (occ["exit_time"] - occ["entry_time"]).dt.total_seconds() / 60
    safe = dur.where(dur > 0, other=1.0)
    prog = (t - occ["entry_time"]).dt.total_seconds() / 60 / safe
    prog = prog.clip(lower=0.0, upper=1.0)

    out = occ[["train_id", "block_id", "u", "v"]].copy()
    out["progress_pct"] = prog
    out["ETA_next"] = occ["exit_time"]
    return out

