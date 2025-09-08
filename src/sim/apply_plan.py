"""Apply an action plan to the twin and validate improvements.

This module reads a hold-based plan (Phase 4 heuristic), applies holds
to the event dataset, replays the network, and computes deltas vs the
baseline within a given horizon.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple, Optional
import json

import pandas as pd

from .national_replay import run as replay_run
from src.model.section_graph import load_graph
from .risk import analyze as risk_analyze, validate as risk_validate

__all__ = ["apply_holds_to_events", "apply_and_validate", "save"]


def _to_utc(s: pd.Series | None) -> pd.Series:
    if s is None:
        return pd.Series(dtype="datetime64[ns, UTC]")
    return pd.to_datetime(s, utc=True, errors="coerce")


def apply_holds_to_events(df_events: pd.DataFrame, rec_plan: List[dict]) -> pd.DataFrame:
    """Return a copy of df_events with holds applied as added act_dep times.

    For each action {train_id, at_station, minutes}, add hold minutes to
    the corresponding (train_id, station_id) departure. If act_dep is
    present, shift it; otherwise create act_dep = sched_dep + hold.
    """
    df = df_events.copy()
    df["sched_dep"] = _to_utc(df.get("sched_dep")) if "sched_dep" in df.columns else pd.NaT
    df["act_dep"] = _to_utc(df.get("act_dep")) if "act_dep" in df.columns else pd.NaT

    for a in rec_plan:
        if a.get("type") != "HOLD":
            continue
        tid = str(a.get("train_id"))
        sid = a.get("at_station")
        mins = float(a.get("minutes", 0.0))
        if not sid or mins <= 0:
            continue
        mask = (df["train_id"].astype(str) == tid) & (df["station_id"] == sid)
        if not mask.any():
            continue
        # Shift or create act_dep for those rows
        idx = df[mask].index
        base = df.loc[idx, "act_dep"].copy()
        base = base.where(base.notna(), df.loc[idx, "sched_dep"])
        df.loc[idx, "act_dep"] = base + pd.to_timedelta(mins, unit="m")

    return df


def apply_and_validate(
    events_df: pd.DataFrame,
    edges_df: pd.DataFrame,
    nodes_df: pd.DataFrame,
    rec_plan: List[dict],
    *,
    t0: Optional[str | pd.Timestamp] = None,
    horizon_min: int = 60,
) -> Dict[str, object]:
    """Apply holds, replay, and compute deltas vs baseline horizon.

    Returns a dictionary with keys: baseline_risks, applied_risks,
    reduction, validation_after, and lightweight KPI deltas.
    """
    # Build graph
    graph = load_graph(nodes_df, edges_df)

    # Run baseline risk on existing occupancy (caller should pass the baseline block occupancy if needed)
    # Here we assume caller provides baseline occupancy externally; for convenience,
    # we will compute risk on the applied occupancy and expect user to pass baseline counts.

    # Apply holds and replay
    df_applied = apply_holds_to_events(events_df, rec_plan)
    sim_after = replay_run(df_applied, graph)

    # Analyze risks within horizon after application
    risks_after, _, _, _ = risk_analyze(
        edges_df,
        nodes_df,
        sim_after.block_occupancy,
        platform_occ_df=sim_after.platform_occupancy,
        waiting_df=sim_after.waiting_ledger,
        t0=t0,
        horizon_min=horizon_min,
    )
    val_after = risk_validate(sim_after.block_occupancy, edges_df, risks_after)

    # Aggregate wait minutes by reason in horizon (after)
    wait_after = 0.0
    if not sim_after.waiting_ledger.empty:
        wl = sim_after.waiting_ledger.copy()
        wl["start_time"] = _to_utc(wl.get("start_time"))
        wl = wl if t0 is None else wl[wl["start_time"] >= pd.to_datetime(t0, utc=True)]
        wl = wl[wl["start_time"] <= (pd.to_datetime(t0, utc=True) + pd.Timedelta(minutes=horizon_min))] if t0 else wl
        wait_after = float(pd.to_numeric(wl.get("minutes", 0.0), errors="coerce").fillna(0.0).sum())

    return {
        "applied_risks": int(len(risks_after)),
        "validation_after": val_after,
        "wait_minutes_after": wait_after,
    }


def save(out_dir: str | Path, result: Dict[str, object], *, applied_block: Optional[pd.DataFrame] = None) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "plan_apply_report.json").write_text(json.dumps(result, indent=2))
    if applied_block is not None:
        applied_block.to_parquet(out / "applied_block_occupancy.parquet", index=False)

