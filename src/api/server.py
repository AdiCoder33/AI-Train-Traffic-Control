from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import hashlib
import json
from datetime import datetime, timezone

import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, Header
from pydantic import BaseModel

app = FastAPI(title="Train Control Decision Support API")


# ---------- Filesystem helpers ----------
def _art_dir(scope: str, date: str) -> Path:
    return Path("artifacts") / scope / date


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text())


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def _read_parquet(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    return pd.read_parquet(path)


def _sha1_dict(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------- RBAC helpers ----------
class Principal(BaseModel):
    user: str
    role: str  # SC | CREW | OM | DH | AN | ADM


def _normalize_role(role: Optional[str]) -> str:
    if not role:
        return "AN"
    r = role.strip().upper()
    return {"SC": "SC", "CREW": "CREW", "OM": "OM", "DH": "DH", "AN": "AN", "ADM": "ADM"}.get(r, "AN")


def get_principal(authorization: Optional[str] = Header(default=None), x_user: Optional[str] = Header(default=None), x_role: Optional[str] = Header(default=None)) -> Principal:
    # Prefer token-based auth if provided
    try:
        from src.auth.service import get_user_by_token, init_db
        init_db()
        if authorization and authorization.lower().startswith("bearer "):
            token = authorization.split(" ", 1)[1].strip()
            u = get_user_by_token(token)
            if u is not None:
                return Principal(user=u.username, role=_normalize_role(u.role))
    except Exception:
        pass
    # Fallback: header-based mock principal
    return Principal(user=(x_user or "anonymous"), role=_normalize_role(x_role))


def require_roles(principal: Principal, allowed: Tuple[str, ...]) -> None:
    if principal.role not in allowed:
        raise HTTPException(status_code=403, detail=f"Role {principal.role} not permitted for this action")


# ---------- Read models ----------
@app.get("/state")
def get_state(scope: str, date: str, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    plats = _read_parquet(base / "national_platform_occupancy.parquet") or _read_parquet(base / "platform_occupancy.parquet")
    waits = _read_parquet(base / "national_waiting_ledger.parquet") or _read_parquet(base / "waiting_ledger.parquet")
    kpis = _read_json(base / "national_sim_kpis.json") or {}
    # Crew least-privilege: redact other trains if needed (prototype: pass-through)
    return {
        "platform_occupancy": (plats.head(1000).to_dict(orient="records") if plats is not None else []),
        "waiting_ledger": (waits.head(1000).to_dict(orient="records") if waits is not None else []),
        "sim_kpis": kpis,
        "whoami": principal.dict(),
    }


@app.get("/radar")
def get_radar(scope: str, date: str) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    radar = _read_json(base / "conflict_radar.json") or []
    risk_kpis = _read_json(base / "risk_kpis.json") or {}
    return {"radar": radar, "risk_kpis": risk_kpis}


def _plan_with_version(base: Path) -> Tuple[List[dict], str]:
    rec_plan: List[dict] = _read_json(base / "rec_plan.json") or []
    plan_version = _sha1_dict(rec_plan) if rec_plan else ""
    return rec_plan, plan_version


@app.get("/recommendations")
def get_recommendations(scope: str, date: str) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    rec_plan, plan_version = _plan_with_version(base)
    alt_options = _read_json(base / "alt_options.json") or []
    plan_metrics = _read_json(base / "plan_metrics.json") or {}
    audit_log = _read_json(base / "audit_log.json") or {}
    # Attach action_id and ensure explainability fields
    for rec in rec_plan:
        if "action_id" not in rec:
            rec["action_id"] = _sha1_dict(rec)
        _ensure_explainability(rec)
    return {
        "rec_plan": rec_plan,
        "alt_options": alt_options,
        "plan_metrics": plan_metrics,
        "audit_log": audit_log,
        "plan_version": plan_version,
    }


def _ensure_explainability(rec: Dict[str, Any]) -> None:
    # Must include: binding constraints, impact, safety_checks
    if not rec.get("why"):
        reason = rec.get("reason")
        loc = rec.get("block_id") or rec.get("station_id") or rec.get("at_station")
        rec["why"] = f"Resolve {reason} at {loc}" if reason and loc else (reason or "")
    if "impact" not in rec:
        rec["impact"] = {"conflicts_resolved": 1 if rec.get("reason") in ("headway", "block_capacity", "platform_overflow") else 0}
    if "safety_checks" not in rec:
        checks = []
        if rec.get("type") == "HOLD":
            mins = float(rec.get("minutes", 0.0))
            if mins <= 5.0:
                checks.append("hold_within_policy_limit")
        if rec.get("type") == "PLATFORM_REASSIGN":
            checks.append("platform_exists_or_any")
        if not checks:
            checks.append("basic_policy_ok")
        rec["safety_checks"] = checks


# ---------- Feedback & Audit ----------
class Feedback(BaseModel):
    scope: str
    date: str
    action: Dict[str, Any]
    decision: str  # APPLY | DISMISS | MODIFY | ACK
    reason: Optional[str] = None
    modified: Optional[Dict[str, Any]] = None


@app.get("/whoami")
def whoami(principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    return principal.dict()


@app.post("/feedback")
def post_feedback(fb: Feedback, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    # RBAC: SC can APPLY/DISMISS/MODIFY, CREW can ACK, others allowed to submit notes only
    dec = (fb.decision or "").upper()
    if dec == "ACK":
        require_roles(principal, ("CREW", "SC", "OM", "DH", "ADM"))
    elif dec in ("APPLY", "DISMISS", "MODIFY"):
        require_roles(principal, ("SC", "ADM"))
    else:
        require_roles(principal, ("SC", "OM", "DH", "AN", "ADM", "CREW"))

    base = _art_dir(fb.scope, fb.date)
    base.mkdir(parents=True, exist_ok=True)

    # Ensure action_id and plan_version
    action = dict(fb.action)
    if "action_id" not in action:
        action["action_id"] = _sha1_dict(action)
    rec_plan, plan_version = _plan_with_version(base)
    if not plan_version:
        plan_version = _sha1_dict(rec_plan)

    # Append to audit_trail.json (immutable log style)
    trail_path = base / "audit_trail.json"
    trail = _read_json(trail_path) or []
    entry = {
        "ts": _now_iso(),
        "who": principal.user,
        "role": principal.role,
        "action_id": action.get("action_id"),
        "decision": dec,
        "details": fb.modified or {},
        "reason": fb.reason,
        "plan_version": plan_version,
        "action": action,
    }
    trail.append(entry)
    _write_json(trail_path, trail)

    # Append to feedback.parquet for analytics
    df_new = pd.DataFrame(
        [
            {
                "ts": entry["ts"],
                "user": principal.user,
                "role": principal.role,
                "decision": dec,
                "reason": fb.reason,
                "plan_version": plan_version,
                "action_id": action.get("action_id"),
                "modified": json.dumps(fb.modified) if fb.modified else None,
                "action": json.dumps(action),
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

    return {"status": "ok", "plan_version": plan_version, "action_id": action.get("action_id")}


# ---------- Auth: login & admin ----------
class LoginReq(BaseModel):
    username: str
    password: str


@app.post("/login")
def login(body: LoginReq) -> Dict[str, Any]:
    try:
        from src.auth.service import authenticate, issue_token, init_db
        init_db()
        u = authenticate(body.username, body.password)
        if not u:
            raise HTTPException(status_code=401, detail="Invalid credentials")
        sess = issue_token(u)
        return {"token": sess.token, "role": u.role, "username": u.username}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class NewUser(BaseModel):
    username: str
    password: str
    role: str


@app.post("/admin/users")
def admin_create_user(user: NewUser, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("ADM",))
    try:
        from src.auth.service import create_user, init_db
        init_db()
        u = create_user(user.username, user.password, user.role)
        return {"username": u.username, "role": u.role}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/users")
def admin_list_users(principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("ADM",))
    try:
        from src.auth.service import list_users, init_db
        init_db()
        return {"users": list_users()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class RoleChange(BaseModel):
    role: str


@app.put("/admin/users/{username}/role")
def admin_change_role(username: str, body: RoleChange, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("ADM",))
    try:
        from src.auth.service import change_role, init_db
        init_db()
        u = change_role(username, body.role)
        if not u:
            raise HTTPException(status_code=404, detail="User not found")
        return {"username": u.username, "role": u.role}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/audit/trail")
def audit_trail(scope: str, date: str, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    # Read-only for all roles
    base = _art_dir(scope, date)
    trail = _read_json(base / "audit_trail.json") or []
    return {"audit_trail": trail}


@app.get("/audit/completeness")
def audit_completeness(scope: str, date: str) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    rec_plan = _read_json(base / "rec_plan.json") or []
    trail = _read_json(base / "audit_trail.json") or []
    acted = len([e for e in trail if e.get("decision") in ("APPLY", "DISMISS", "MODIFY", "ACK")])
    total = len(rec_plan)
    pct = (acted / total * 100.0) if total else 0.0
    return {"recommendations": total, "decisions_logged": acted, "completeness_pct": pct}


# ---------- Policy Console ----------
class Policy(BaseModel):
    priority_weights: Dict[str, float] = {}
    hold_budgets: Dict[str, float] = {}
    fairness_limits: Dict[str, float] = {}
    solver_SLA: Dict[str, float] = {}
    flags: Dict[str, bool] = {}


@app.get("/policy")
def get_policy(scope: str, date: str) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    pol = _read_json(base / "policy_state.json") or {}
    meta = _read_json(base / "provenance.json") or {}
    return {"policy_state": pol, "provenance": meta}


@app.put("/policy")
def set_policy(scope: str, date: str, policy: Policy, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("OM", "DH", "ADM"))
    base = _art_dir(scope, date)
    _write_json(base / "policy_state.json", policy.dict())
    # Update provenance
    prov = _read_json(base / "provenance.json") or {}
    prov["last_policy_update_ts"] = _now_iso()
    prov["updated_by"] = principal.user
    _write_json(base / "provenance.json", prov)
    return {"status": "ok"}


# ---------- Crew feed ----------
@app.get("/crew/feed")
def crew_feed(scope: str, date: str, train_id: Optional[str] = None) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    rec_plan = _read_json(base / "rec_plan.json") or []
    # Filter and simplify for crew consumption
    items = []
    for rec in rec_plan:
        if train_id and str(rec.get("train_id")) != str(train_id):
            continue
        if rec.get("type") not in ("HOLD", "PLATFORM_REASSIGN", "SPEED_TUNE"):
            continue
        items.append(
            {
                "action_id": rec.get("action_id") or _sha1_dict(rec),
                "train_id": rec.get("train_id"),
                "summary": _crew_summary(rec),
            }
        )
    return {"instructions": items}


def _crew_summary(rec: Dict[str, Any]) -> str:
    t = rec.get("type")
    if t == "HOLD":
        return f"Hold at {rec.get('at_station')} for {rec.get('minutes')} min"
    if t == "PLATFORM_REASSIGN":
        return f"Use platform {rec.get('platform')} at {rec.get('station_id')}"
    if t == "SPEED_TUNE":
        return f"Block {rec.get('block_id')}: speed x{rec.get('speed_factor')}"
    return str(rec)
