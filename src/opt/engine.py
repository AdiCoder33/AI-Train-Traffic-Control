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

try:
    from src.opt.milp import solve_local, SOLVER_AVAILABLE  # type: ignore
except Exception:
    SOLVER_AVAILABLE = False
    def solve_local(**kwargs):
        return None


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
    max_holds_per_train: int = 2,
    use_ga: bool = False,
    risk_heat: Optional[Dict[str, float]] = None,
    precedence_pins: Optional[List[Dict[str, str]]] = None,
    locked_stations: Optional[List[str]] = None,
    epsilon: float = 0.2,
) -> tuple[List[dict], List[dict], Dict[str, float], Dict[str, object]]:
    t_start = time.time()
    edges = edges_df.set_index("block_id") if not edges_df.empty else pd.DataFrame()
    # Station platform counts (for slot selection)
    plat_count: Dict[str, int] = {}
    if not nodes_df.empty and "station_id" in nodes_df.columns:
        try:
            tmp = nodes_df.set_index("station_id")["platforms"] if "platforms" in nodes_df.columns else None
            if tmp is not None:
                plat_count = {str(k): int(v) for k, v in tmp.fillna(1).to_dict().items()}
        except Exception:
            plat_count = {}

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

    # Precompute earliest-free platform slot assignment within horizon (smart platform selection)
    assigned_slot: Dict[tuple, int] = {}
    try:
        if not bo.empty and plat_count:
            # Build arrivals to stations with times in horizon
            arr = bo[["train_id", "v", "exit_time"]].rename(columns={"v": "station_id", "exit_time": "arr_time"}).copy()
            arr["arr_time"] = pd.to_datetime(arr["arr_time"], utc=True, errors="coerce")
            if t0 is None:
                t0_ts = arr["arr_time"].min()
            else:
                t0_ts = pd.to_datetime(t0, utc=True)
            t1_ts = t0_ts + pd.Timedelta(minutes=horizon_min)
            arr = arr[(arr["arr_time"] >= t0_ts) & (arr["arr_time"] <= t1_ts)]
            # Dwell per station
            dwell_map: Dict[str, float] = {}
            if not nodes_df.empty and "station_id" in nodes_df.columns:
                if "min_dwell_min" in nodes_df.columns:
                    dwell_map = {str(k): float(v) for k, v in nodes_df.set_index("station_id")["min_dwell_min"].fillna(2.0).to_dict().items()}
            # Initialize per-station slot availability
            slot_avail: Dict[str, List[pd.Timestamp]] = {}
            for sid, nplat in plat_count.items():
                n = max(1, int(nplat))
                slot_avail[sid] = [pd.Timestamp.min.tz_localize("UTC")] * n
            # Assign slots greedily by earliest available
            for _, row in arr.sort_values("arr_time").iterrows():
                sid = str(row["station_id"])
                tid = str(row["train_id"])
                at = row["arr_time"]
                if sid not in slot_avail:
                    continue
                slots = slot_avail[sid]
                # choose slot with earliest availability
                idx = min(range(len(slots)), key=lambda i: slots[i])
                start = max(at, slots[idx])
                dwell = float(dwell_map.get(sid, 2.0))
                dep = start + pd.Timedelta(minutes=dwell)
                slots[idx] = dep
                slot_avail[sid] = slots
                assigned_slot[(tid, sid)] = idx
    except Exception:
        assigned_slot = {}

    rec_plan: List[dict] = []
    alt_options: List[dict] = []
    targeted = 0

    holds_count: Dict[str, int] = {}

    pins = precedence_pins or []
    locked_stations = [str(s) for s in (locked_stations or [])]
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
                # If precedence pin exists for this block, enforce it
                follower = None
                block_id = r.get("block_id")
                if block_id is not None:
                    for pin in pins:
                        if str(pin.get("block_id")) == str(block_id):
                            leader = str(pin.get("leader"))
                            foll = str(pin.get("follower"))
                            if leader in trains and foll in trains:
                                follower = foll
                                break
                if follower is None:
                    # Prefer holding the train with lower priority and fewer holds so far
                    follower = sorted(
                        trains,
                        key=lambda t: (
                            _priority(t, priorities),
                            holds_count.get(t, 0),
                            t,
                        ),
                    )[-1]
                # Fairness: if chosen exceeds max_holds_per_train, try the other candidate
                if holds_count.get(follower, 0) >= max_holds_per_train and len(trains) == 2:
                    other = [t for t in trains if t != follower][0]
                    if holds_count.get(other, 0) < max_holds_per_train:
                        follower = other

            need = float(r.get("required_hold_min", 0.0)) if rtype == "headway" else 2.0
            hold_min = min(max_hold_min, max(2.0, need))
            # Risk-aware slack: if incident risk is high on this block, add 1–2 min buffer
            if risk_heat and r.get("block_id") is not None:
                try:
                    prob = float(risk_heat.get(str(r.get("block_id")), 0.0))
                    # Dynamic thresholds from epsilon (chance constraint P(conflict) < epsilon)
                    th_hi = max(0.5, 1.0 - float(max(0.01, min(0.5, epsilon))))
                    th_lo = max(0.3, th_hi - 0.2)
                    if prob >= th_hi:
                        hold_min = min(max_hold_min, hold_min + 2.0)
                    elif prob >= th_lo:
                        hold_min = min(max_hold_min, hold_min + 1.0)
                except Exception:
                    pass

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
                "binding_constraints": ["headway"] if rtype == "headway" else (["block_capacity"] if rtype == "block_capacity" else []),
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
            holds_count[follower] = holds_count.get(follower, 0) + 1
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

            # If follower has higher priority than leader, propose OVERTAKE as alternative (leader hold)
            if len(trains) >= 2:
                leader = [t for t in trains if t != follower][0]
                # Optionally query small local solver for a decision
                decision = None
                if SOLVER_AVAILABLE:
                    decision = solve_local(
                        headway_min=float(edges_df.set_index("block_id").loc[block_id, "headway"]) if "headway" in edges_df.columns else 0.0,
                        follower_hold_min=float(action["minutes"]),
                        leader_hold_min=float(action["minutes"]),
                        follower_priority=_priority(follower, priorities),
                        leader_priority=_priority(leader, priorities),
                    )
                if decision and decision.get("action") == "HOLD_LEADER" or _priority(follower, priorities) > _priority(leader, priorities):
                    alt_options.append({
                        "risk_ref": r,
                        "options": [
                            {"type": "OVERTAKE", "train_id": leader, "at_station": u, "minutes": action["minutes"], "score": -0.05}
                        ],
                        "tradeoffs": "Hold leader to allow higher-priority follower to pass at station.",
                    })

            # SPEED_TUNE alternative: small run-time reduction on the conflicting block
            alt_options.append({
                "risk_ref": r,
                "options": [
                    {"type": "SPEED_TUNE", "train_id": follower, "block_id": block_id, "speed_factor": 0.95, "score": -0.02}
                ],
                "tradeoffs": "Within policy, reduce run-time by 5% on this block.",
            })
        elif rtype == "platform_overflow":
            sid = r.get("station_id")
            tr = trains[0] if trains else None
            if tr is None or not sid:
                continue
            # Skip platform reassignment suggestions for locked stations
            sid_str = str(sid)
            # Prefer holding upstream before entering the station to smooth arrivals
            # Find the corresponding incoming block (u->sid) for this train near the risk time
            ts = pd.to_datetime(r.get("time_window")[0], utc=True, errors="coerce") if r.get("time_window") else None
            u_choice = None
            g_tr = by_train.get(tr)
            if g_tr is not None and not g_tr.empty and ts is not None:
                cand = g_tr[(g_tr["v"] == sid)].copy()
                if not cand.empty:
                    cand["arr_gap"] = (cand["exit_time"] - ts).abs()
                    row = cand.sort_values("arr_gap").iloc[0]
                    u_choice = row.get("u")
            at_station = u_choice if u_choice else sid
            # Fairness-aware selection if multiple trains present (rare in our risk record)
            pick = tr
            if len(trains) > 1:
                pick = sorted(
                    trains,
                    key=lambda t: (
                        _priority(t, priorities),
                        holds_count.get(t, 0),
                        t,
                    ),
                )[-1]
            # Propose upstream hold and a platform reassignment advisory
            action = {
                "train_id": tr,
                "type": "HOLD",
                "at_station": at_station,
                "minutes": min(3.0, max_hold_min),
                "reason": "platform_overflow_upstream" if at_station != sid else "platform_overflow",
                "why": f"Smooth arrival into {sid} by holding at {at_station}",
            }
            rec_plan.append(action)
            holds_count[pick] = holds_count.get(pick, 0) + 1
            targeted += 1
            # Advisory: PLATFORM_REASSIGN (non-operative without per-platform IDs)
            # Choose a concrete platform slot index: prefer earliest-free precomputed slot
            slot_idx = assigned_slot.get((str(tr), str(sid)))
            if slot_idx is None:
                try:
                    nplat = int(plat_count.get(str(sid), 1))
                    if nplat > 1:
                        slot_idx = abs(hash(f"{sid}-{tr}")) % nplat
                except Exception:
                    slot_idx = None
            if sid_str not in locked_stations:
                rec_plan.append({
                    "train_id": tr,
                    "type": "PLATFORM_REASSIGN",
                    "station_id": sid,
                    "platform": (int(slot_idx) if slot_idx is not None else "any"),
                    "reason": "spread_load",
                    "why": f"Use alternate platform at {sid} if available",
                    "binding_constraints": ["platform_capacity"],
                })
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
        "max_holds_per_train": max_holds_per_train,
        "horizon_min": horizon_min,
        "t0": str(t0),
    }
    # Optional GA fallback/alternative
    if (use_ga or len(rec_plan) == 0) and not block_occ_df.empty:
        try:
            from src.opt.ga import propose_ga
            ga_actions, ga_metrics = propose_ga(edges_df, nodes_df, block_occ_df, risks_h, max_hold_min=max_hold_min)
            if use_ga or (len(rec_plan) == 0 and ga_actions):
                rec_plan = ga_actions
                plan_metrics.update({"actions": float(len(rec_plan)), "ga_score": float(ga_metrics.get("score", 0.0))})
                audit_log["strategy"] = "ga"
        except Exception:
            pass
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
