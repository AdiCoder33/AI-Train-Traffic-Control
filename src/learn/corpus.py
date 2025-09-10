from __future__ import annotations

"""Aggregate training corpus across all artifact runs.

Discovers artifacts/<scope>/<date> that contain Phase 3/4 outputs and
builds per-risk examples using expert + feedback where available.

Persists combined parquet at artifacts/global_models/il_training.parquet.
"""

from pathlib import Path
from typing import List, Tuple
import pandas as pd

from .state_builder import build_examples


def _runs(base_dir: str | Path = "artifacts") -> List[Tuple[str, str]]:
    base = Path(base_dir)
    out: List[Tuple[str, str]] = []
    if not base.exists():
        return out
    for scope_dir in base.iterdir():
        if not scope_dir.is_dir():
            continue
        for date_dir in scope_dir.iterdir():
            if not date_dir.is_dir():
                continue
            # Heuristic: include if risk or occupancy exists
            if any((date_dir / f).exists() for f in ["conflict_radar.json", "national_block_occupancy.parquet", "block_occupancy.parquet"]):
                out.append((scope_dir.name, date_dir.name))
    return sorted(out)


def build_corpus(base_dir: str | Path = "artifacts", *, persist: bool = True) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    for scope, date in _runs(base_dir):
        try:
            df = build_examples(scope, date, persist=False, prefer_expert=True)
            if not df.empty:
                df = df.assign(origin_scope=scope, origin_date=date)
                rows.append(df)
        except Exception:
            continue
    if not rows:
        return pd.DataFrame()
    full = pd.concat(rows, ignore_index=True)
    if persist:
        out = Path(base_dir) / "global_models"
        out.mkdir(parents=True, exist_ok=True)
        full.to_parquet(out / "il_training.parquet", index=False)
    return full


if __name__ == "__main__":  # pragma: no cover
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="artifacts")
    args = ap.parse_args()
    df = build_corpus(args.base, persist=True)
    print(df.head().to_string(index=False) if not df.empty else "<empty>")

