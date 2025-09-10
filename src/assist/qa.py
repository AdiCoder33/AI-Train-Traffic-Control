from __future__ import annotations

"""Simple role-based Q&A assistant over artifacts.

Supports: KPIs, risk summaries, ETAs for a train, and action suggestions
by delegating to ``src.policy.infer.suggest``.
"""

from pathlib import Path
from typing import Dict, Optional, List
import json
import re

import pandas as pd

from src.policy.infer import suggest as suggest_actions


def _base(scope: str, date: str) -> Path:
    return Path("artifacts") / scope / date


def _read_json(p: Path) -> object | None:
    try:
        if not p.exists():
            return None
        return json.loads(p.read_text())
    except Exception:
        return None


def _load_df(p: Path) -> pd.DataFrame:
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)


def answer(scope: str, date: str, query: str, *, role: str = "AN", train_id: Optional[str] = None, station_id: Optional[str] = None) -> Dict[str, object]:
    base = _base(scope, date)
    q = (query or "").strip().lower()
    sim = _read_json(base / "national_sim_kpis.json") or {}
    radar = _read_json(base / "conflict_radar.json") or []
    df_plat = _load_df(base / "national_platform_occupancy.parquet")
    if df_plat.empty:
        df_plat = _load_df(base / "platform_occupancy.parquet")

    # OTP / delay queries
    if re.search(r"\b(otp|on[-\s]?time)\b", q) or re.search(r"\bdelay\b", q):
        otp = float(sim.get("otp_exit_pct", sim.get("otp", 0.0)))
        avg_delay = float(sim.get("avg_exit_delay_min", sim.get("avg_delay_min", 0.0)))
        return {
            "answer": f"OTP {otp:.1f}% | Avg exit delay {avg_delay:.1f} min",
            "details": sim,
            "role_view": "summary",
        }

    # Risks summary
    if "risk" in q or "conflict" in q:
        # optional station filter for SC
        rs: List[dict] = radar
        if station_id:
            sid = str(station_id)
            rs = [r for r in radar if str(r.get("station_id", "")) == sid or str(r.get("u", "")) == sid or str(r.get("v", "")) == sid]
        crit = sum(1 for r in rs if r.get("severity") == "Critical")
        high = sum(1 for r in rs if r.get("severity") == "High")
        total = len(rs)
        top = []
        for r in rs[:5]:
            loc = r.get("block_id") or r.get("station_id")
            top.append(f"{r.get('type')} at {loc} in {r.get('lead_min', 0)} min")
        return {
            "answer": f"Risks: total {total}, Critical {crit}, High {high}",
            "details": {"top": top},
            "role_view": "summary",
        }

    # Train ETA
    m = re.search(r"train\s+(\w+)", q)
    tid = train_id or (m.group(1) if m else None)
    if tid and ("eta" in q or "next" in q or "where" in q):
        try:
            dfc = df_plat.copy()
            if not dfc.empty:
                dfc["arr_platform"] = pd.to_datetime(dfc["arr_platform"], utc=True)
                dfc["dep_platform"] = pd.to_datetime(dfc["dep_platform"], utc=True)
                now = pd.Timestamp.utcnow().tz_localize("UTC")
                nxt = dfc[(dfc["train_id"].astype(str) == str(tid)) & (dfc["dep_platform"] >= now)].sort_values("arr_platform").head(2)
                if not nxt.empty:
                    recs = nxt[["station_id", "arr_platform", "dep_platform"]].to_dict(orient="records")
                    return {"answer": f"Next stops for {tid}", "details": recs, "role_view": "crew"}
        except Exception:
            pass
        return {"answer": f"No future stops found for {tid}", "role_view": "crew"}

    # Suggestions intent
    if "suggest" in q or "what should" in q or "hold" in q:
        res = suggest_actions(scope, date, role=role, train_id=train_id, station_id=station_id)
        sug = res.get("suggestions", []) if isinstance(res, dict) else []
        if role == "CREW" and train_id:
            view = [f"Hold at {s.get('at_station')} for {s.get('minutes')} min" for s in sug]
            return {"answer": f"Suggestions for {train_id}", "details": view, "role_view": "crew"}
        return {"answer": f"Top {min(5,len(sug))} suggestions", "details": sug[:5], "role_view": "ops"}

    # Default help
    return {
        "answer": "Ask about OTP, risks, ETA for a train, or say 'suggest'.",
        "role_view": "help",
    }
