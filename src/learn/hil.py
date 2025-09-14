from __future__ import annotations

"""Human-in-the-loop RL helpers.

Append feedback-derived transitions to the global offline RL log so that
periodic training can include controller overrides.
"""

from pathlib import Path
from typing import Dict, Any
import json
import pandas as pd

from .state_builder import build_examples, SEV_RANK


def _base(scope: str, date: str) -> Path:
    return Path("artifacts") / scope / date


def _read_json(p: Path):
    try:
        if not p.exists():
            return None
        return json.loads(p.read_text())
    except Exception:
        return None


def append_feedback(scope: str, date: str, action: Dict[str, Any], decision: str, *, alpha: float = 0.2) -> None:
    # Only consider APPLY/MODIFY/ACK decisions with HOLD actions
    dec = (decision or "").upper()
    if action.get("type") != "HOLD" or dec not in ("APPLY", "MODIFY", "ACK"):
        return
    base = _base(scope, date)
    radar = _read_json(base / "conflict_radar.json") or []
    # Find matching risk best-effort
    bid = action.get("block_id")
    sid = action.get("station_id") or action.get("at_station")
    tid = str(action.get("train_id"))
    rpick = None
    for r in radar:
        trains = [str(t) for t in (r.get("train_ids") or [])]
        if tid and tid not in trains:
            continue
        if bid and str(r.get("block_id")) == str(bid):
            rpick = r
            break
        if sid and (str(r.get("station_id")) == str(sid) or str(r.get("u")) == str(sid) or str(r.get("v")) == str(sid)):
            rpick = r
            break
    if rpick is None:
        return

    # Build minimal state row via state_builder
    df = build_examples(scope, date, persist=False, prefer_expert=False)
    row = None
    if not df.empty:
        sub = df[df["train_id"].astype(str) == tid]
        if bid is not None:
            sub = sub[sub["block_id"].astype(str) == str(bid)]
        if sid is not None and (sub is None or sub.empty):
            sub = df[(df["train_id"].astype(str) == tid) & (df["station_id"].astype(str) == str(sid))]
        if sub is not None and not sub.empty:
            row = sub.iloc[0].to_dict()
    if row is None:
        # fallback generic
        row = {
            "severity_rank": SEV_RANK.get(str(rpick.get("severity")), 2),
            "lead_min": float(rpick.get("lead_min", 0.0) or 0.0),
            "headway_min": 0.0,
            "capacity": 1,
            "block_len_trains": 0,
            "platforms": 1,
        }

    minutes = float(action.get("minutes", 0.0) or 0.0)
    reward = (1.0 if minutes >= float(rpick.get("required_hold_min", 0.0) or 0.0) else 0.0) - float(alpha) * minutes

    entry = {
        "state": {k: float(row.get(k, 0.0)) for k in ["severity_rank","lead_min","headway_min","capacity","block_len_trains","platforms"]},
        "action": {"type": "HOLD", "hold_class": int(2 if minutes <= 2.5 else 3 if minutes <= 4.0 else 5), "minutes": minutes},
        "reward": float(reward),
        "info": {
            "scope": scope,
            "date": date,
            "risk_type": rpick.get("type"),
            "block_id": rpick.get("block_id"),
            "station_id": rpick.get("station_id"),
            "train_id": tid,
            "decision": dec,
        },
    }

    out_dir = Path("artifacts") / "global_models"
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "offline_rl.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")

