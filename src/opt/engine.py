"""Phase 4: Real-time optimization engine (heuristic baseline).

This module proposes safe, explainable controller actions over a
rolling-horizon using the digital twin state and the Phase-3 risk radar.

Primary focus in this baseline is conflict resolution via holds with
deterministic, priority-aware tie-breaking. MILP/CP-SAT hooks can be
added later; this file stays dependency-light.

Inputs
------
- edges_df: blocks with [u,v,block_id,headway,capacity,min_run_time]
- nodes_df: stations with [station_id,platforms,min_dwell_min]
- block_occ_df: current post-enforcement plan with [train_id,block_id,u,v,entry_time,exit_time]
- risks: list of dicts from conflict_radar.json
- priorities (optional): mapping train_id -> priority (higher=worse to delay)

Outputs
-------
- rec_plan: ordered actions (HOLD/PLATFORM_REASSIGN placeholders)
- alt_options: per risk, alternatives with scores and tradeoffs
- plan_metrics: deltas vs baseline (approx) and counts
- audit_log: metadata about run (runtime, strategy, constraints hit)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
import time
import json

import pandas as pd

__all__ = ["propose", "save"]


def _to_utc(s: pd.Series | None) -> pd.Series:
    if s is None:
        return pd.Series(dtype="datetime64[ns, UTC]")
    return pd.to_datetime(s, utc=True, errors="coerce")


def _priority(train_id: str, prio_map: Dict[str, int] | None) -> int:
    if not prio_map:
        return 0
    return int(prio_map.get(str(train_id), 0))


def _headway_ok(entry: pd.Timestamp, prev_exit: pd.Timestamp, headway_min: float) -> bool:
    return entry >= prev_exit + pd.Timedelta(minutes=float(headway_min))


def propose(
    edges_df: pd.DataFrame,
    nodes_df: pd.DataFrame,
    block_occ_df: pd.DataFrame,
    risks: List[dict],
    *,
    t0: Optional[str | pd.Timestamp] = None,
    horizon_min: int = 60,
    priorities: Optional[Dict[str, int]] = None,
    max_hold_min: int = 5,
) -> tuple[List[dict], List[dict], Dict[str, float], Dict[str, object]]:
    t_start = time.time()
    edges = edges_df.set_index("block_id") if not edges_df.empty else pd.DataFrame()

    bo = block_occ_df.copy()
    if bo.empty:
        return [], [], {"actions": 0, "conflicts_targeted": 0}, {"strategy": "heuristic", "runtime_sec": 0.0}
    bo["entry_time"] = _to_utc(bo["entry_time"]) if "entry_time" in bo.columns else pd.NaT
    bo["exit_time"] = _to_utc(bo["exit_time"]) if "exit_time" in bo.columns else pd.NaT

    if t0 is None:
        t0 = min(bo["entry_time"].min(), pd.to_datetime(risks[0]["time_window"][0], utc=True) if risks else pd.Timestamp.utcnow().tz_localize("UTC"))
    t0 = pd.to_datetime(t0, utc=True)
    t1 = t0 + pd.Timedelta(minutes=horizon_min)

    # Filter risks within horizon and sort by severity/lead time, then by priority
    def _risk_key(r: dict) -> tuple:
        sev_rank = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}.get(r.get("severity"), 9)
        lead = float(r.get("lead_min", 1e9))
        trains = r.get("train_ids") or []
        worst_prio = max((_priority(str(t), priorities) for t in trains), default=0)
        return (sev_rank, lead, -worst_prio)

    risks_h = []
    for r in risks:
        ts = pd.to_datetime(r.get("time_window")[0], utc=True, errors="coerce") if r.get("time_window") else None
        if ts is not None and t0 <= ts <= t1:
            risks_h.append(r)
    risks_h.sort(key=_risk_key)

    # Build quick lookups for current plan per block and train
    by_block = {bid: g.sort_values("entry_time").copy() for bid, g in bo.groupby("block_id")}
    by_train = {tid: g.sort_values("entry_time").copy() for tid, g in bo.groupby("train_id")}

    rec_plan: List[dict] = []
    alt_options: List[dict] = []
    targeted = 0

    for r in risks_h:
        rtype = r.get("type")
        trains = [str(t) for t in (r.get("train_ids") or [])]
        ts = pd.to_datetime(r.get("time_window")[0], utc=True, errors="coerce") if r.get("time_window") else None
        if rtype in ("headway", "block_capacity"):
            # Choose follower to hold: lower priority gets held first
            if not trains:
                continue
            if len(trains) == 1:
                follower = trains[0]
            else:
                follower = sorted(trains, key=lambda t: (_priority(t, priorities), t))[-1]

            need = float(r.get("required_hold_min", 0.0)) if rtype == "headway" else 2.0
            hold_min = min(max_hold_min, max(2.0, need))

            # Construct action at the upstream station u for the conflicting block
            block_id = r.get("block_id")
            u = r.get("u")
            action = {
                "train_id": follower,
                "type": "HOLD",
                "at_station": u,
                "minutes": round(hold_min, 1),
                "reason": rtype,
                "block_id": block_id,
                "why": f"Resolve {rtype} on {block_id} vs {', '.join(t for t in trains if t!=follower)}",
            }
            # Verify headway feasibility post-hold on that block if data available
            g = by_block.get(block_id)
            if g is not None and not g.empty and ts is not None:
                # Find follower row with entry at/after ts
                idx = g.index[(g["train_id"].astype(str) == follower) & (g["entry_time"] >= ts)]
                if len(idx) > 0:
                    i = idx[0]
                    row = g.loc[i]
                    prevs = g[g["entry_time"] < row["entry_time"]]
                    if not prevs.empty:
                        prev_exit = prevs["exit_time"].max()
                        headway_min = float(edges.loc[block_id, "headway"]) if block_id in edges.index and "headway" in edges.columns else 0.0
                        entry_new = row["entry_time"] + pd.Timedelta(minutes=hold_min)
                        if not _headway_ok(entry_new, prev_exit, headway_min):
                            # Increase hold to required
                            gap = (prev_exit + pd.Timedelta(minutes=headway_min) - row["entry_time"]).total_seconds() / 60.0
                            action["minutes"] = round(min(max_hold_min, max(2.0, gap)), 1)
            rec_plan.append(action)
            targeted += 1

            # Alternatives (2 vs 5 minutes)
            alt = {
                "risk_ref": r,
                "options": [
                    {"type": "HOLD", "train_id": follower, "at_station": u, "minutes": 2.0, "score": 0.0},
                    {"type": "HOLD", "train_id": follower, "at_station": u, "minutes": min(5.0, max_hold_min), "score": -0.1},
                ],
                "tradeoffs": "Short hold vs safer longer hold; impact estimated via ETA deltas.",
            }
            alt_options.append(alt)
        elif rtype == "platform_overflow":
            sid = r.get("station_id")
            tr = trains[0] if trains else None
            if tr is None:
                continue
            action = {
                "train_id": tr,
                "type": "HOLD",
                "at_station": sid,
                "minutes": min(3.0, max_hold_min),
                "reason": "platform_overflow",
                "why": f"Reduce concurrent dwells at {sid}",
            }
            rec_plan.append(action)
            targeted += 1
            alt_options.append({
                "risk_ref": r,
                "options": [
                    {"type": "HOLD", "train_id": tr, "at_station": sid, "minutes": 2.0, "score": 0.0},
                    {"type": "HOLD", "train_id": tr, "at_station": sid, "minutes": min(5.0, max_hold_min), "score": -0.1},
                ],
                "tradeoffs": "Hold to avoid platform overflow; reassignment possible if multiple platforms.",
            })

    # Metrics (approximate)
    plan_metrics = {
        "actions": float(len(rec_plan)),
        "conflicts_targeted": float(targeted),
        "expected_conflict_reduction": float(targeted),
    }
    audit_log = {
        "strategy": "heuristic",
        "runtime_sec": round(time.time() - t_start, 3),
        "max_hold_min": max_hold_min,
        "horizon_min": horizon_min,
        "t0": str(t0),
    }
    return rec_plan, alt_options, plan_metrics, audit_log


def save(
    rec_plan: List[dict],
    alt_options: List[dict],
    plan_metrics: Dict[str, float],
    audit_log: Dict[str, object],
    out_dir: str | "PathLike[str]",
) -> None:
    from pathlib import Path
    p = Path(out_dir)
    p.mkdir(parents=True, exist_ok=True)
    (p / "rec_plan.json").write_text(json.dumps(rec_plan, indent=2))
    (p / "alt_options.json").write_text(json.dumps(alt_options, indent=2))
    (p / "plan_metrics.json").write_text(json.dumps(plan_metrics, indent=2))
    (p / "audit_log.json").write_text(json.dumps(audit_log, indent=2))

