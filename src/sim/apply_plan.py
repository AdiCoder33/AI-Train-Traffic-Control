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
    df["sched_dep"] = _to_utc(df.get("sched_dep")) if "sched_dep" in df.columns else pd.Series(pd.NaT, dtype="datetime64[ns, UTC]", index=df.index)
    # Ensure act_dep column exists and is tz-aware to avoid dtype warnings
    if "act_dep" in df.columns:
        df["act_dep"] = _to_utc(df["act_dep"]).astype("datetime64[ns, UTC]")
    else:
        df["act_dep"] = pd.Series(pd.NaT, dtype="datetime64[ns, UTC]", index=df.index)

    for a in rec_plan:
        if a.get("type") not in ("HOLD", "OVERTAKE"):
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

    # Run baseline replay for KPI baselines
    sim_before = replay_run(events_df, graph)

    # Apply holds and replay
    df_applied = apply_holds_to_events(events_df, rec_plan)
    # Extract speed tuning from rec_plan
    speed_map: Dict[tuple, float] = {}
    for a in rec_plan:
        if a.get("type") == "SPEED_TUNE":
            tid = str(a.get("train_id"))
            bid = str(a.get("block_id")) if a.get("block_id") is not None else None
            fac = float(a.get("speed_factor", 1.0))
            if bid:
                speed_map[(tid, bid)] = fac
    # Extract platform reassignment mapping from rec_plan
    plat_override: Dict[tuple, int] = {}
    for a in rec_plan:
        if a.get("type") == "PLATFORM_REASSIGN":
            tid = str(a.get("train_id"))
            sid = a.get("station_id")
            plat = a.get("platform")
            if sid is None or plat in (None, "any"):
                continue
            try:
                idx = int(plat)
                plat_override[(tid, str(sid))] = idx
            except Exception:
                pass

    sim_after = replay_run(df_applied, graph, per_train_speed=speed_map if speed_map else None, platform_override=plat_override if plat_override else None)

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

    # Analyze risks within horizon before application
    risks_before, _, _, _ = risk_analyze(
        edges_df,
        nodes_df,
        sim_before.block_occupancy,
        platform_occ_df=sim_before.platform_occupancy,
        waiting_df=sim_before.waiting_ledger,
        t0=t0,
        horizon_min=horizon_min,
    )

    # Aggregate wait minutes by reason in horizon (before/after)
    wait_after = 0.0
    wait_before = 0.0
    if not sim_after.waiting_ledger.empty:
        wl = sim_after.waiting_ledger.copy()
        wl["start_time"] = _to_utc(wl.get("start_time"))
        wl = wl if t0 is None else wl[wl["start_time"] >= pd.to_datetime(t0, utc=True)]
        wl = wl[wl["start_time"] <= (pd.to_datetime(t0, utc=True) + pd.Timedelta(minutes=horizon_min))] if t0 else wl
        wait_after = float(pd.to_numeric(wl.get("minutes", 0.0), errors="coerce").fillna(0.0).sum())
    if not sim_before.waiting_ledger.empty:
        wl = sim_before.waiting_ledger.copy()
        wl["start_time"] = _to_utc(wl.get("start_time"))
        wl = wl if t0 is None else wl[wl["start_time"] >= pd.to_datetime(t0, utc=True)]
        wl = wl[wl["start_time"] <= (pd.to_datetime(t0, utc=True) + pd.Timedelta(minutes=horizon_min))] if t0 else wl
        wait_before = float(pd.to_numeric(wl.get("minutes", 0.0), errors="coerce").fillna(0.0).sum())

    # KPI deltas (OTP/avg delay) at horizon exit
    def _kpi_from_sim(sim) -> Dict[str, float]:
        if sim.platform_occupancy.empty:
            return {"otp_exit_pct": 0.0, "avg_exit_delay_min": 0.0}
        last_dep = sim.platform_occupancy.sort_values(["train_id", "dep_platform"]).groupby("train_id").tail(1)
        if t0 is not None:
            t0_ts = pd.to_datetime(t0, utc=True)
            t1_ts = t0_ts + pd.Timedelta(minutes=horizon_min)
            last_dep = last_dep[(last_dep["dep_platform"] >= t0_ts) & (last_dep["dep_platform"] <= t1_ts)]
        if last_dep.empty:
            return {"otp_exit_pct": 0.0, "avg_exit_delay_min": 0.0}
        # Scheduled arrival lookup for last station
        dfu = events_df.drop_duplicates(subset=["train_id", "station_id"], keep="first").copy()
        dfu["sched_arr"] = _to_utc(dfu.get("sched_arr"))
        idx = pd.MultiIndex.from_arrays([last_dep["train_id"].values, last_dep["station_id"].values], names=["train_id", "station_id"])  # type: ignore
        sched_map = dfu.set_index(["train_id", "station_id"])  # type: ignore
        sched_arr = sched_map.reindex(idx)["sched_arr"] if "sched_arr" in sched_map.columns else None
        if sched_arr is None:
            return {"otp_exit_pct": 0.0, "avg_exit_delay_min": 0.0}
        delay = (last_dep.set_index("train_id")["dep_platform"] - sched_arr).dt.total_seconds() / 60
        return {
            "otp_exit_pct": float((delay.le(5).mean() * 100.0) if len(delay) else 0.0),
            "avg_exit_delay_min": float(delay.mean(skipna=True) if len(delay) else 0.0),
        }

    kpi_before = _kpi_from_sim(sim_before)
    kpi_after = _kpi_from_sim(sim_after)

    # Risk breakdowns by type
    def _breakdown(rs: List[dict]) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for r in rs:
            t = str(r.get("type"))
            out[t] = out.get(t, 0) + 1
        return out
    breakdown_before = _breakdown(risks_before)
    breakdown_after = _breakdown(risks_after)

    return {
        "applied_risks": int(len(risks_after)),
        "baseline_risks": int(len(risks_before)),
        "risk_reduction": int(len(risks_before)) - int(len(risks_after)),
        "risk_reduction_headway_block": int(breakdown_before.get("headway", 0) + breakdown_before.get("block_capacity", 0)) - int(breakdown_after.get("headway", 0) + breakdown_after.get("block_capacity", 0)),
        "risk_breakdown_before": breakdown_before,
        "risk_breakdown_after": breakdown_after,
        "validation_after": val_after,
        "wait_minutes_before": wait_before,
        "wait_minutes_after": wait_after,
        "kpi_before": kpi_before,
        "kpi_after": kpi_after,
    }


def save(out_dir: str | Path, result: Dict[str, object], *, applied_block: Optional[pd.DataFrame] = None) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "plan_apply_report.json").write_text(json.dumps(result, indent=2))
    if applied_block is not None:
        applied_block.to_parquet(out / "applied_block_occupancy.parquet", index=False)
