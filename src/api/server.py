from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import json
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Train Control Decision Support API")


def _art_dir(scope: str, date: str) -> Path:
    return Path("artifacts") / scope / date


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text())


def _read_parquet(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    return pd.read_parquet(path)


@app.get("/state")
def get_state(scope: str, date: str) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    plats = _read_parquet(base / "national_platform_occupancy.parquet") or _read_parquet(
        base / "platform_occupancy.parquet"
    )
    waits = _read_parquet(base / "national_waiting_ledger.parquet") or _read_parquet(
        base / "waiting_ledger.parquet"
    )
    kpis = _read_json(base / "national_sim_kpis.json")
    return {
        "platform_occupancy": (plats.head(1000).to_dict(orient="records") if plats is not None else []),
        "waiting_ledger": (waits.head(1000).to_dict(orient="records") if waits is not None else []),
        "sim_kpis": (kpis or {}),
    }


@app.get("/radar")
def get_radar(scope: str, date: str) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    radar = _read_json(base / "conflict_radar.json") or []
    risk_kpis = _read_json(base / "risk_kpis.json") or {}
    return {"radar": radar, "risk_kpis": risk_kpis}


@app.get("/recommendations")
def get_recommendations(scope: str, date: str) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    rec_plan = _read_json(base / "rec_plan.json") or []
    alt_options = _read_json(base / "alt_options.json") or []
    plan_metrics = _read_json(base / "plan_metrics.json") or {}
    audit_log = _read_json(base / "audit_log.json") or {}
    return {
        "rec_plan": rec_plan,
        "alt_options": alt_options,
        "plan_metrics": plan_metrics,
        "audit_log": audit_log,
    }


class Feedback(BaseModel):
    scope: str
    date: str
    action: Dict[str, Any]
    decision: str  # APPLY | DISMISS | MODIFY
    reason: Optional[str] = None
    modified: Optional[Dict[str, Any]] = None


@app.post("/feedback")
def post_feedback(fb: Feedback) -> Dict[str, Any]:
    base = _art_dir(fb.scope, fb.date)
    base.mkdir(parents=True, exist_ok=True)

    # Append to audit_trail.json
    trail_path = base / "audit_trail.json"
    trail = _read_json(trail_path) or []
    entry = {
        "action": fb.action,
        "decision": fb.decision,
        "reason": fb.reason,
        "modified": fb.modified,
    }
    trail.append(entry)
    trail_path.write_text(json.dumps(trail, indent=2))

    # Append to feedback.parquet
    df_new = pd.DataFrame(
        [
            {
                "decision": fb.decision,
                "reason": fb.reason,
                "modified": json.dumps(fb.modified) if fb.modified else None,
                "action": json.dumps(fb.action),
            }
        ]
    )
    fb_path = base / "feedback.parquet"
    if fb_path.exists():
        df_all = pd.read_parquet(fb_path)
        df_all = pd.concat([df_all, df_new], ignore_index=True)
    else:
        df_all = df_new
    df_all.to_parquet(fb_path, index=False)

    return {"status": "ok"}

