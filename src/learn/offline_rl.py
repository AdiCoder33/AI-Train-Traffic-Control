from __future__ import annotations

"""Build offline RL dataset from artifacts across runs.

Each line is a transition-like record containing:
  - state: features from state_builder
  - action: discrete hold_class in {2,3,5}
  - minutes: hold minutes
  - reward: scalar using simple shaping (conflict_resolved - alpha * minutes)
  - info: scope/date/run ids and risk metadata

Saved as JSONL under artifacts/global_models/offline_rl.jsonl.
"""

from pathlib import Path
from typing import Dict, List, Tuple
import json

import pandas as pd

from .corpus import _runs
from .state_builder import build_examples, SEV_RANK


def _read_json(p: Path):
    try:
        if not p.exists():
            return None
        return json.loads(p.read_text())
    except Exception:
        return None


def _to_utc(s: pd.Series | None) -> pd.Series:
    if s is None:
        return pd.Series(dtype="datetime64[ns, UTC]")
    return pd.to_datetime(s, utc=True, errors="coerce")


def _target_train_for_risk(r: dict) -> str:
    t = r.get("train_ids") or []
    if r.get("type") == "headway" and len(t) >= 2:
        return str(t[1])
    return str(t[0]) if t else ""


def _resolve_reward(r: dict, minutes: float, preview: dict | None, alpha: float, *, priority_weight: float = 0.0, recent_holds: int = 0, beta: float = 0.1, gamma: float = 0.05) -> tuple[float, bool, float]:
    """Return (reward, resolves_flag). Use preview where possible."""
    resolves = False
    if preview:
        # Use required_hold_min when present
        need = float(preview.get("required_hold_min", r.get("required_hold_min", 0.0)) or 0.0)
        if minutes >= need and need > 0:
            resolves = True
        else:
            # Fall back to binary flags
            if minutes <= 2.5 and bool(preview.get("hold_2min_resolves", False)):
                resolves = True
            elif minutes >= 4.0 and bool(preview.get("hold_5min_resolves", False)):
                resolves = True
    else:
        # Heuristic: compare with risk required_hold_min
        need = float(r.get("required_hold_min", 0.0) or 0.0)
        resolves = minutes >= need and need > 0
    base = (1.0 if resolves else 0.0)
    # Priority penalty (delay high-priority trains less) and unfairness penalty for repeat holds
    penalty = float(alpha) * float(minutes) + float(beta) * float(priority_weight) * float(minutes) + float(gamma) * float(recent_holds)
    reward = base - penalty
    return reward, resolves, base


def build_offline_rl(base_dir: str | Path = "artifacts", *, alpha: float = 0.2, beta: float = 0.1, gamma: float = 0.05, out_path: str | Path | None = None) -> Path:
    runs = _runs(base_dir)
    out_dir = Path(base_dir) / "global_models"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_p = Path(out_path) if out_path else (out_dir / "offline_rl.jsonl")

    # Truncate file
    out_p.write_text("")

    for scope, date in runs:
        base = Path(base_dir) / scope / date
        radar = _read_json(base / "conflict_radar.json") or []
        preview = _read_json(base / "mitigation_preview.json") or []
        rec_plan = _read_json(base / "rec_plan.json") or []
        # Build examples and index by (type, block_id, station_id, train)
        ex = build_examples(scope, date, persist=False, prefer_expert=True)
        if ex.empty or not radar:
            continue
        # Build preview index by risk_index if present else by signature
        prev_by_idx: Dict[int, dict] = {}
        for p in (preview or []):
            idx = p.get("risk_index")
            if isinstance(idx, int):
                prev_by_idx[idx] = p

        # Build expert lookup: (train, block_id/station) -> minutes
        expert: Dict[tuple, float] = {}
        for a in rec_plan or []:
            if a.get("type") != "HOLD":
                continue
            tid = str(a.get("train_id"))
            loc = a.get("block_id") or a.get("station_id") or a.get("at_station")
            if loc is None:
                continue
            expert[(tid, str(loc))] = float(a.get("minutes", 0.0) or 0.0)

        # Iterate risks
        for i, r in enumerate(radar):
            rtype = str(r.get("type"))
            bid = r.get("block_id")
            sid = r.get("station_id")
            tid = _target_train_for_risk(r)
            # match example row
            cand = ex[(ex["risk_type"] == rtype) & (ex["train_id"].astype(str) == str(tid))]
            if bid:
                cand = cand[(cand["block_id"].astype(str) == str(bid))]
            if sid and cand.empty:
                cand = ex[(ex["risk_type"] == rtype) & (ex["train_id"].astype(str) == str(tid)) & (ex["station_id"].astype(str) == str(sid))]
            if cand.empty:
                continue
            row = cand.iloc[0]
            # Determine chosen minutes from expert or from label
            mins = None
            if bid and (tid, str(bid)) in expert:
                mins = expert[(tid, str(bid))]
            elif sid and (tid, str(sid)) in expert:
                mins = expert[(tid, str(sid))]
            if mins is None:
                # use label class
                c = int(row.get("hold_class", 2))
                mins = 2.0 if c <= 2 else (3.0 if c == 3 else 5.0)

            p = prev_by_idx.get(i)
            # Look up features for fairness/priority
            prio_w = 0.0
            recent_holds = 0
            try:
                prio_w = float(row.get("priority_weight", 0.0))
                recent_holds = int(row.get("recent_holds", 0))
            except Exception:
                pass
            reward, resolves, base_ok = _resolve_reward(r, float(mins), p, alpha, priority_weight=prio_w, recent_holds=recent_holds, beta=beta, gamma=gamma)

            state = {
                "severity_rank": int(row.get("severity_rank", 3)),
                "lead_min": float(row.get("lead_min", 0.0)),
                "headway_min": float(row.get("headway_min", 0.0)),
                "capacity": int(row.get("capacity", 1)),
                "block_len_trains": int(row.get("block_len_trains", 0)),
                "platforms": int(row.get("platforms", 1)),
            }
            action = {"type": "HOLD", "hold_class": int(2 if mins <= 2.5 else 3 if mins <= 4.0 else 5), "minutes": float(mins)}
            info = {
                "scope": scope,
                "date": date,
                "risk_index": i,
                "risk_type": rtype,
                "block_id": bid,
                "station_id": sid,
                "train_id": tid,
                "resolved": bool(resolves),
            }
            info.update({"base_resolve": bool(base_ok >= 1.0), "priority_weight": prio_w, "recent_holds": int(recent_holds)})
            line = json.dumps({"state": state, "action": action, "reward": reward, "info": info})
            with out_p.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
    return out_p


if __name__ == "__main__":  # pragma: no cover
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="artifacts")
    ap.add_argument("--alpha", type=float, default=0.2)
    args = ap.parse_args()
    p = build_offline_rl(args.base, alpha=args.alpha)
    print(str(p))
