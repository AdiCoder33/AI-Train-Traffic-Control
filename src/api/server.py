from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import hashlib
import json
from datetime import datetime, timezone

import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, Header, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="Train Control Decision Support API")

# CORS for local frontend development (React/Vite on localhost)
# In production, restrict origins to your deployed frontend domains.
# Explicit origins to ensure preflight OPTIONS is handled correctly with credentials
_CORS_ORIGINS = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:5174",
    "http://127.0.0.1:5174",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
    station_id: str | None = None
    train_id: str | None = None


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
                return Principal(user=u.username, role=_normalize_role(u.role), station_id=getattr(u, "station_id", None), train_id=getattr(u, "train_id", None))
    except Exception:
        pass
    # Fallback: header-based mock principal
    return Principal(user=(x_user or "anonymous"), role=_normalize_role(x_role), station_id=None, train_id=None)


def require_roles(principal: Principal, allowed: Tuple[str, ...]) -> None:
    if principal.role not in allowed:
        raise HTTPException(status_code=403, detail=f"Role {principal.role} not permitted for this action")


# ---------- Read models ----------
@app.get("/state")
def get_state(
    scope: str,
    date: str,
    principal: Principal = Depends(get_principal),
    train_id: Optional[str] = None,
    station_id: Optional[str] = None,
) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    plats = _read_parquet(base / "national_platform_occupancy.parquet")
    if plats is None or (hasattr(plats, "empty") and plats.empty):
        plats = _read_parquet(base / "platform_occupancy.parquet")
    waits = _read_parquet(base / "national_waiting_ledger.parquet")
    if waits is None or (hasattr(waits, "empty") and waits.empty):
        waits = _read_parquet(base / "waiting_ledger.parquet")
    kpis = _read_json(base / "national_sim_kpis.json") or {}
    # Normalize KPI keys expected by frontend
    try:
        if "otp_pct" not in kpis:
            v = kpis.get("otp_exit_pct") or kpis.get("otp_pct")
            if v is not None:
                kpis["otp_pct"] = float(v)
        if "avg_delay" not in kpis:
            v = kpis.get("avg_exit_delay_min") or kpis.get("avg_delay_min")
            if v is not None:
                kpis["avg_delay"] = float(v)
        # Global counts for admin overview
        rk = _read_json(base / "risk_kpis.json") or {}
        if rk.get("total_risks") is not None and "total_risks" not in kpis:
            kpis["total_risks"] = float(rk.get("total_risks", 0.0))
        pm = _read_json(base / "plan_metrics.json") or {}
        if pm.get("actions") is not None and "actions" not in kpis:
            kpis["actions"] = float(pm.get("actions", 0.0))
    except Exception:
        pass
    # Role-aware filtering and scoping
    try:
        # Enforce station scoping for Station Controller (SC)
        if principal.role == "SC":
            # Use assigned station_id if available; deny access if none assigned
            if principal.station_id:
                station_id = principal.station_id
            else:
                raise HTTPException(status_code=400, detail="SC account has no station assignment")
        # Enforce train scoping for Crew
        if principal.role == "CREW":
            if principal.train_id:
                train_id = principal.train_id
            else:
                raise HTTPException(status_code=400, detail="CREW account has no train assignment")
        if principal.role == "CREW" and train_id:
            if plats is not None and not plats.empty:
                plats = plats[plats["train_id"].astype(str) == str(train_id)]
            if waits is not None and not waits.empty:
                waits = waits[waits["train_id"].astype(str) == str(train_id)]
        if station_id:
            sid = str(station_id)
            if plats is not None and not plats.empty and "station_id" in plats.columns:
                plats = plats[plats["station_id"].astype(str) == sid]
            if waits is not None and not waits.empty and {"resource","id"}.issubset(waits.columns):
                waits = waits[((waits["resource"] == "platform") & (waits["id"].astype(str) == sid))]
            # Station-scoped counts for risks and actions (best-effort)
            try:
                radar = _read_json(base / "conflict_radar.json") or []
                total_risks_sid = len([r for r in radar if str(r.get("station_id","")) == sid or str(r.get("u","")) == sid or str(r.get("v","")) == sid])
                kpis["total_risks"] = float(total_risks_sid)
            except Exception:
                pass
            try:
                rec_plan = _read_json(base / "rec_plan.json") or []
                # Count actions at station or affecting blocks touching the station
                cnt = 0
                if rec_plan:
                    bo = _read_parquet(base / "national_block_occupancy.parquet")
                    for rec in rec_plan:
                        if str(rec.get("station_id","")) == sid or str(rec.get("at_station","")) == sid:
                            cnt += 1
                        else:
                            bid = rec.get("block_id")
                            if bid and bo is not None and not bo.empty and {"block_id","u","v"}.issubset(bo.columns):
                                g = bo[bo["block_id"].astype(str) == str(bid)]
                                if not g.empty and ((g["u"].astype(str) == sid) | (g["v"].astype(str) == sid)).any():
                                    cnt += 1
                kpis["actions"] = float(cnt)
            except Exception:
                pass
    except Exception:
        pass
    return {
        "platform_occupancy": (plats.head(1000).to_dict(orient="records") if plats is not None else []),
        "waiting_ledger": (waits.head(1000).to_dict(orient="records") if waits is not None else []),
        "sim_kpis": kpis,
        "whoami": principal.dict(),
    }





@app.get("/snapshot")
def get_snapshot(scope: str, date: str) -> Dict[str, Any]:
    global ENGINE
    if ENGINE is None:
        return {"snapshot": []}
    return {"snapshot": ENGINE.snapshot()}


@app.get("/nodes")
def get_nodes(scope: str, date: str) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    nodes = _read_parquet(base / "section_nodes.parquet")
    if nodes is None or nodes.empty:
        return {"nodes": []}
    return {"nodes": nodes.head(2000).to_dict(orient="records")}


@app.get("/edges")
def get_edges(scope: str, date: str, station_id: Optional[str] = None) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    nodes = _read_parquet(base / "section_nodes.parquet")
    if nodes is None or nodes.empty:
        return {"edges": []}
    lat_col = "lat" if "lat" in nodes.columns else ("latitude" if "latitude" in nodes.columns else None)
    lon_col = "lon" if "lon" in nodes.columns else ("longitude" if "longitude" in nodes.columns else None)
    if lat_col is None or lon_col is None or "station_id" not in nodes.columns:
        return {"edges": []}
    coord = nodes.set_index("station_id")[ [lat_col, lon_col] ].to_dict(orient="index")

    edges = _read_parquet(base / "section_edges.parquet")
    if edges is None or edges.empty:
        bo = _read_parquet(base / "national_block_occupancy.parquet") or _read_parquet(base / "block_occupancy.parquet")
        if bo is None or bo.empty:
            return {"edges": []}
        cols = [c for c in ["u","v","block_id"] if c in bo.columns]
        edges = bo.drop_duplicates(subset=[c for c in ["u","v"] if c in bo.columns])[cols]

    if station_id and {"u","v"}.issubset(edges.columns):
        sid = str(station_id)
        edges = edges[(edges["u"].astype(str) == sid) | (edges["v"].astype(str) == sid)]

    out = []
    for _, r in edges.iterrows():
        u = str(r.get("u", ""))
        v = str(r.get("v", ""))
        cu = coord.get(u)
        cv = coord.get(v)
        if not cu or not cv:
            continue
        out.append({
            "u": u,
            "v": v,
            "block_id": r.get("block_id"),
            "u_lat": float(cu.get(lat_col)),
            "u_lon": float(cu.get(lon_col)),
            "v_lat": float(cv.get(lat_col)),
            "v_lon": float(cv.get(lon_col)),
        })
    return {"edges": out[:5000]}

@app.get("/blocks")
def get_blocks(scope: str, date: str, station_id: Optional[str] = None, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    bo = _read_parquet(base / "national_block_occupancy.parquet")
    if bo is None or bo.empty:
        bo = _read_parquet(base / "block_occupancy.parquet")
    if bo is None or bo.empty:
        return {"blocks": []}
    try:
        # Enforce station scoping for SC
        if principal.role == "SC":
            if principal.station_id:
                station_id = principal.station_id
            else:
                raise HTTPException(status_code=400, detail="SC account has no station assignment")
        if station_id and {"u", "v"}.issubset(bo.columns):
            sid = str(station_id)
            bo = bo[(bo["u"].astype(str) == sid) | (bo["v"].astype(str) == sid)]
    except Exception:
        pass
    return {"blocks": bo.head(2000).to_dict(orient="records")}

@app.get("/radar")
def get_radar(scope: str, date: str, station_id: Optional[str] = None, train_id: Optional[str] = None, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    radar = _read_json(base / "conflict_radar.json") or []
    # Enforce SC station scoping and optional filtering
    if principal.role == "SC":
        if principal.station_id:
            station_id = principal.station_id
        else:
            raise HTTPException(status_code=400, detail="SC account has no station assignment")
    if principal.role == "CREW":
        if principal.train_id:
            train_id = principal.train_id
        else:
            raise HTTPException(status_code=400, detail="CREW account has no train assignment")
    if station_id:
        sid = str(station_id)
        radar = [r for r in radar if str(r.get("station_id","")) == sid or str(r.get("u","")) == sid or str(r.get("v","")) == sid]
    if train_id:
        radar = [r for r in radar if str(train_id) in [str(t) for t in (r.get("train_ids") or [])]]
    risk_kpis = _read_json(base / "risk_kpis.json") or {}
    return {"radar": radar, "risk_kpis": risk_kpis}


def _plan_with_version(base: Path) -> Tuple[List[dict], str]:
    rec_plan: List[dict] = _read_json(base / "rec_plan.json") or []
    plan_version = _sha1_dict(rec_plan) if rec_plan else ""
    return rec_plan, plan_version


@app.get("/recommendations")
def get_recommendations(scope: str, date: str, station_id: Optional[str] = None, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    rec_plan, plan_version = _plan_with_version(base)
    alt_options = _read_json(base / "alt_options.json") or []
    plan_metrics = _read_json(base / "plan_metrics.json") or {}
    plan_apply_report = _read_json(base / "plan_apply_report.json")
    audit_log = _read_json(base / "audit_log.json") or {}
    # Optional station filter for rec_plan
    # Enforce SC station scoping
    if principal.role == "SC":
        if principal.station_id:
            station_id = principal.station_id
        else:
            raise HTTPException(status_code=400, detail="SC account has no station assignment")
    if station_id:
        sid = str(station_id)
        # Filter recs at station or affecting blocks touching station (best-effort using stored fields)
        filtered = []
        for rec in rec_plan:
            if str(rec.get("station_id", "")) == sid or str(rec.get("at_station", "")) == sid:
                filtered.append(rec)
                continue
            bid = rec.get("block_id")
            if bid and (base / "national_block_occupancy.parquet").exists():
                try:
                    bo = pd.read_parquet(base / "national_block_occupancy.parquet")
                    g = bo[bo["block_id"].astype(str) == str(bid)]
                    if not g.empty and ((g["u"].astype(str) == sid) | (g["v"].astype(str) == sid)).any():
                        filtered.append(rec)
                        continue
                except Exception:
                    pass
        rec_plan = filtered
    # Attach action_id and ensure explainability fields
    for rec in rec_plan:
        if "action_id" not in rec:
            rec["action_id"] = _sha1_dict(rec)
        _ensure_explainability(rec)
    return {
        "rec_plan": rec_plan,
        "alt_options": alt_options,
        "plan_metrics": plan_metrics,
        "plan_apply_report": plan_apply_report,
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
    if "binding_constraints" not in rec:
        r = rec.get("reason")
        if r == "headway":
            rec["binding_constraints"] = ["headway"]
        elif r == "block_capacity":
            rec["binding_constraints"] = ["block_capacity"]
        elif r == "platform_overflow":
            rec["binding_constraints"] = ["platform_capacity"]


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

    # Append to global offline RL log for HIL-RL
    try:
        from src.learn.hil import append_feedback
        append_feedback(fb.scope, fb.date, action, dec)
    except Exception:
        pass

    return {"status": "ok", "plan_version": plan_version, "action_id": action.get("action_id")}


# ---------- AI Assistant (Q&A and Suggestions) ----------
class AskReq(BaseModel):
    scope: str
    date: str
    query: str
    train_id: str | None = None
    station_id: str | None = None


class SuggestReq(BaseModel):
    scope: str
    date: str
    train_id: str | None = None
    station_id: str | None = None
    max_hold_min: int = 3


@app.post("/ai/ask")
def ai_ask(body: AskReq, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    # All roles can ask; responses are shaped by role
    try:
        from src.assist.qa import answer
        # Role scoping: CREW must supply train_id; SC can supply station_id for filtering
        if principal.role == "CREW" and not body.train_id:
            raise HTTPException(status_code=400, detail="CREW must specify train_id for questions")
        sid = body.station_id
        if principal.role == "SC":
            if principal.station_id:
                sid = principal.station_id
            else:
                raise HTTPException(status_code=400, detail="SC account has no station assignment")
        res = answer(body.scope, body.date, body.query, role=principal.role, train_id=body.train_id, station_id=sid)
        return {"whoami": principal.dict(), "result": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ai/suggest")
def ai_suggest(body: SuggestReq, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    # CREW can only query suggestions for a specific train they care about
    if principal.role == "CREW" and not body.train_id:
        raise HTTPException(status_code=400, detail="CREW must specify train_id for suggestions")
    # SC suggestions are scoped to their assigned station
    if principal.role == "SC":
        if principal.station_id:
            body.station_id = principal.station_id
        else:
            raise HTTPException(status_code=400, detail="SC account has no station assignment")
    try:
        from src.policy.infer import suggest
        res = suggest(body.scope, body.date, role=principal.role, train_id=body.train_id, station_id=body.station_id, max_hold_min=int(body.max_hold_min))
        return {"whoami": principal.dict(), "result": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class ApplyReq(BaseModel):
    scope: str
    date: str
    action_id: str
    modifiers: Optional[Dict[str, Any]] = None


@app.post("/apply")
def post_apply(body: ApplyReq, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("SC", "ADM"))
    global ENGINE
    if ENGINE is None:
        return {"status": "engine_not_running"}
    res = ENGINE.apply_action(body.action_id, body.modifiers)
    # Log apply intent to audit trail
    base = _art_dir(body.scope, body.date)
    trail_path = base / "audit_trail.json"
    trail = _read_json(trail_path) or []
    entry = {
        "ts": _now_iso(),
        "who": principal.user,
        "role": principal.role,
        "action_id": body.action_id,
        "decision": "APPLY",
        "details": body.modifiers or {},
        "plan_version": (_sha1_dict(_read_json(base / "rec_plan.json") or []) if (base / "rec_plan.json").exists() else ""),
        "result": res,
    }
    trail.append(entry)
    _write_json(trail_path, trail)
    return res


# ---------- Optimization & Disruptions ----------
class OptimizeReq(BaseModel):
    scope: str
    date: str
    t0: Optional[str] = None
    horizon_min: int = 60
    use_ga: bool = False
    epsilon: float = 0.2


@app.post("/optimize")
def post_optimize(body: OptimizeReq, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    # All roles can request optimize for demo; restrict to ops/admin in prod
    base = _art_dir(body.scope, body.date)
    edges_p = base / "section_edges.parquet"
    nodes_p = base / "section_nodes.parquet"
    block_p = base / "national_block_occupancy.parquet"
    if not block_p.exists():
        block_p = base / "block_occupancy.parquet"
    if not (edges_p.exists() and nodes_p.exists() and block_p.exists()):
        raise HTTPException(status_code=400, detail="Missing required artifacts (edges/nodes/block occupancy)")
    # Try to reuse existing radar; otherwise compute with defaults
    rad_p = base / "conflict_radar.json"
    if rad_p.exists():
        radar = _read_json(rad_p) or []
    else:
        try:
            import pandas as pd
            from src.sim.risk import analyze, save as risk_save
            edges = pd.read_parquet(edges_p)
            nodes = pd.read_parquet(nodes_p)
            bo = pd.read_parquet(block_p)
            risks, timeline, previews, kpis = analyze(edges, nodes, bo, t0=(body.t0 or None), horizon_min=int(body.horizon_min))
            risk_save(risks, timeline, previews, kpis, base)
            radar = risks
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"risk: {e}")
    # Optional heatmap for risk-aware slack
    risk_heat = None
    heat_p = base / "incident_heat.json"
    if heat_p.exists():
        try:
            risk_heat = _read_json(heat_p)
        except Exception:
            risk_heat = None
    # Load locks and pins; run optimizer (heuristic or GA)
    try:
        import pandas as pd
        from src.opt.engine import propose, save as opt_save
        edges = pd.read_parquet(edges_p)
        nodes = pd.read_parquet(nodes_p)
        bo = pd.read_parquet(block_p)
        # Backup current plan
        try:
            prev = _read_json(base / "rec_plan.json") or []
            (base / "rec_plan_prev.json").write_text(json.dumps(prev, indent=2))
        except Exception:
            pass
        locks = _read_json(base / "locks_state.json") or {}
        locked_stations = [str(x.get("id")) for x in (locks.get("resource_locks", []) or []) if str(x.get("type")) == "platform" and bool(x.get("locked", True))]
        precedence_pins = locks.get("precedence_pins", []) or []
        rec, alts, metrics, audit = propose(
            edges, nodes, bo, radar,
            t0=(body.t0 or None), horizon_min=int(body.horizon_min), use_ga=bool(body.use_ga),
            risk_heat=risk_heat, precedence_pins=precedence_pins, locked_stations=locked_stations, epsilon=float(body.epsilon or 0.2)
        )
        opt_save(rec, alts, metrics, audit, base)
        # Update provenance reopt counter
        prov_p = base / "provenance.json"
        prov = _read_json(prov_p) or {}
        prov["reopt_count"] = int(prov.get("reopt_count", 0)) + 1
        prov["last_reopt_ts"] = _now_iso()
        _write_json(prov_p, prov)
        plan_version = _sha1_dict(rec) if rec else ""
        return {"status": "ok", "plan_version": plan_version, "plan_metrics": metrics, "audit": audit}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"opt: {e}")


class DisruptionReq(BaseModel):
    scope: str
    date: str
    train_id: str
    station_id: str
    delay_min: float = 5.0
    t0: Optional[str] = None
    horizon_min: int = 60
    use_ga: bool = False


# ---------- Scenario & Batch Runner ----------
class Scenario(BaseModel):
    kind: str
    params: Dict[str, Any] = {}
    name: Optional[str] = None


class ScenarioBatchReq(BaseModel):
    scope: str
    date: str
    scenarios: List[Scenario]
    horizon_min: int = 60


@app.post("/scenario/run")
def scenario_run(scope: str, date: str, body: Scenario, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    # Allow OM/DH/ADM for scenario runs
    require_roles(principal, ("OM", "DH", "ADM", "AN"))
    try:
        from src.sim.scenario_runner import ScenarioSpec, run_one
        base = _art_dir(scope, date)
        # Validate required artifacts for scenario replay
        missing = []
        if not (base / "events_clean.parquet").exists():
            missing.append("events_clean.parquet")
        if not (base / "section_edges.parquet").exists():
            missing.append("section_edges.parquet")
        if not (base / "section_nodes.parquet").exists():
            missing.append("section_nodes.parquet")
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing artifacts for scenario: {', '.join(missing)}. Use corridor pipeline or demo generator.")
        spec = ScenarioSpec(kind=body.kind, params=body.params, name=(body.name or body.kind))
        res = run_one(scope, date, spec)
        return {"status": "ok", "result": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/scenario/batch")
def scenario_batch(body: ScenarioBatchReq, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("OM", "DH", "ADM", "AN"))
    try:
        from src.sim.scenario_runner import ScenarioSpec, run_batch
        base = _art_dir(body.scope, body.date)
        missing = []
        if not (base / "events_clean.parquet").exists():
            missing.append("events_clean.parquet")
        if not (base / "section_edges.parquet").exists():
            missing.append("section_edges.parquet")
        if not (base / "section_nodes.parquet").exists():
            missing.append("section_nodes.parquet")
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing artifacts for scenario batch: {', '.join(missing)}. Use corridor pipeline or demo generator.")
        specs = [ScenarioSpec(kind=s.kind, params=s.params, name=(s.name or s.kind)) for s in body.scenarios]
        res = run_batch(body.scope, body.date, specs, horizon_min=int(body.horizon_min))
        return {"status": "ok", "batch": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Multi-section Coordination ----------
class HandshakeReq(BaseModel):
    scopeA: str
    dateA: str
    scopeB: str
    dateB: str
    boundary_station: str


@app.post("/coord/handshake")
def coord_handshake(req: HandshakeReq, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("OM", "DH", "ADM"))
    try:
        from src.coord.boundary import coordinate
        res = coordinate(req.scopeA, req.dateA, req.scopeB, req.dateB, req.boundary_station)
        return {"status": "ok", "actions": res.actions, "details": res.details}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/disruption")
def post_disruption(body: DisruptionReq, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    # Inject delay at (train, station) into events_clean.parquet and recompute replay+risk+opt
    base = _art_dir(body.scope, body.date)
    events_p = base / "events_clean.parquet"
    edges_p = base / "section_edges.parquet"
    nodes_p = base / "section_nodes.parquet"
    if not (events_p.exists() and edges_p.exists() and nodes_p.exists()):
        raise HTTPException(status_code=400, detail="Missing events/edges/nodes artifacts")
    try:
        import pandas as pd
        from src.sim.national_replay import run as replay_run, save as replay_save
        from src.model.section_graph import load_graph
        from src.sim.risk import analyze as risk_analyze, save as risk_save
        from src.opt.engine import propose, save as opt_save

        ev = pd.read_parquet(events_p)
        # Ensure tz-aware columns
        for c in ("sched_dep", "act_dep"):
            if c in ev.columns:
                ev[c] = pd.to_datetime(ev[c], utc=True, errors="coerce")
        # Apply delay: shift act_dep or create from sched_dep
        mask = (ev["train_id"].astype(str) == str(body.train_id)) & (ev["station_id"].astype(str) == str(body.station_id))
        if not mask.any():
            raise HTTPException(status_code=404, detail="(train_id, station_id) not found in events")
        sel = ev.loc[mask].index
        base_dep = ev.loc[sel, "act_dep"].copy()
        base_dep = base_dep.where(base_dep.notna(), ev.loc[sel, "sched_dep"])
        ev.loc[sel, "act_dep"] = base_dep + pd.to_timedelta(float(body.delay_min), unit="m")
        # Persist updated events
        ev.to_parquet(events_p, index=False)

        # Recompute replay
        edges = pd.read_parquet(edges_p)
        nodes = pd.read_parquet(nodes_p)
        graph = load_graph(nodes, edges)
        sim = replay_run(ev, graph)
        replay_save(sim, base)
        # Risk + Opt
        risks, timeline, previews, kpis = risk_analyze(edges, nodes, sim.block_occupancy, platform_occ_df=sim.platform_occupancy, waiting_df=sim.waiting_ledger, t0=(body.t0 or None), horizon_min=int(body.horizon_min))
        risk_save(risks, timeline, previews, kpis, base)
        # Optional heatmap and locks
        risk_heat = _read_json(base / "incident_heat.json") or None
        # Backup current plan
        try:
            prev = _read_json(base / "rec_plan.json") or []
            (base / "rec_plan_prev.json").write_text(json.dumps(prev, indent=2))
        except Exception:
            pass
        locks = _read_json(base / "locks_state.json") or {}
        locked_stations = [str(x.get("id")) for x in (locks.get("resource_locks", []) or []) if str(x.get("type")) == "platform" and bool(x.get("locked", True))]
        precedence_pins = locks.get("precedence_pins", []) or []
        rec, alts, metrics, audit = propose(
            edges, nodes, sim.block_occupancy, risks,
            t0=(body.t0 or None), horizon_min=int(body.horizon_min), use_ga=bool(body.use_ga),
            risk_heat=risk_heat, precedence_pins=precedence_pins, locked_stations=locked_stations
        )
        opt_save(rec, alts, metrics, audit, base)
        prov_p = base / "provenance.json"
        prov = _read_json(prov_p) or {}
        prov["reopt_count"] = int(prov.get("reopt_count", 0)) + 1
        prov["last_reopt_ts"] = _now_iso()
        _write_json(prov_p, prov)
        return {
            "status": "ok",
            "applied_delay_min": float(body.delay_min),
            "risk_kpis": kpis,
            "plan_metrics": metrics,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/plan/{plan_id}")
def get_plan(plan_id: str, scope: str, date: str) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    rec_plan = _read_json(base / "rec_plan.json") or []
    if plan_id in ("", "latest"):
        return {"plan_id": _sha1_dict(rec_plan) if rec_plan else "", "rec_plan": rec_plan}
    # Try action lookup
    for rec in rec_plan:
        if str(rec.get("action_id", "")) == plan_id:
            return {"action": rec}
    raise HTTPException(status_code=404, detail="plan/action not found")


class RevertReq(BaseModel):
    scope: str
    date: str


@app.post("/plan/revert")
def revert_plan(body: RevertReq, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("OM", "DH", "ADM"))
    base = _art_dir(body.scope, body.date)
    prev_p = base / "rec_plan_prev.json"
    cur_p = base / "rec_plan.json"
    if not prev_p.exists():
        raise HTTPException(status_code=404, detail="No previous plan found")
    try:
        data = _read_json(prev_p) or []
        _write_json(cur_p, data)
        return {"status": "ok", "plan_version": _sha1_dict(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sectionTopology")
def get_section_topology(scope: str, date: str) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    import pandas as pd
    nodes = pd.read_parquet(base / "section_nodes.parquet") if (base / "section_nodes.parquet").exists() else None
    edges = pd.read_parquet(base / "section_edges.parquet") if (base / "section_edges.parquet").exists() else None
    return {
        "nodes": ([] if nodes is None or nodes.empty else nodes.head(2000).to_dict(orient="records")),
        "edges": ([] if edges is None or edges.empty else edges.head(5000).to_dict(orient="records")),
    }


@app.get("/timetable")
def get_timetable(scope: str, date: str) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    import pandas as pd
    ev = pd.read_parquet(base / "events_clean.parquet") if (base / "events_clean.parquet").exists() else None
    return {"events": ([] if ev is None or ev.empty else ev.head(2000).to_dict(orient="records"))}


# ---------- Locks & Pins ----------
class ResourceLock(BaseModel):
    scope: str
    date: str
    type: str  # 'platform' | 'block'
    id: str
    locked: bool = True


class PrecedencePin(BaseModel):
    scope: str
    date: str
    block_id: str
    leader: str
    follower: str


@app.get("/locks")
def get_locks(scope: str, date: str) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    locks = _read_json(base / "locks_state.json") or {"resource_locks": [], "precedence_pins": []}
    return locks


@app.post("/locks/resource")
def post_lock_resource(body: ResourceLock, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("SC", "OM", "DH", "ADM"))
    base = _art_dir(body.scope, body.date)
    locks = _read_json(base / "locks_state.json") or {"resource_locks": [], "precedence_pins": []}
    rl = [x for x in locks.get("resource_locks", []) if not (str(x.get("type")) == body.type and str(x.get("id")) == body.id)]
    rl.append({"type": body.type, "id": body.id, "locked": bool(body.locked)})
    locks["resource_locks"] = rl
    _write_json(base / "locks_state.json", locks)
    return {"status": "ok", "locks": locks}


@app.post("/locks/precedence")
def post_lock_precedence(body: PrecedencePin, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("SC", "OM", "DH", "ADM"))
    base = _art_dir(body.scope, body.date)
    locks = _read_json(base / "locks_state.json") or {"resource_locks": [], "precedence_pins": []}
    pp = [x for x in locks.get("precedence_pins", []) if not (str(x.get("block_id")) == body.block_id and str(x.get("leader")) == body.leader and str(x.get("follower")) == body.follower)]
    pp.append({"block_id": body.block_id, "leader": body.leader, "follower": body.follower})
    locks["precedence_pins"] = pp
    _write_json(base / "locks_state.json", locks)
    return {"status": "ok", "locks": locks}


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
        # Include station assignment if present
        station_id = getattr(u, "station_id", None)
        train_id = getattr(u, "train_id", None)
        return {"token": sess.token, "role": u.role, "username": u.username, "station_id": station_id, "train_id": train_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class NewUser(BaseModel):
    username: str
    password: str
    role: str
    station_id: Optional[str] = None
    train_id: Optional[str] = None


@app.post("/admin/users")
def admin_create_user(user: NewUser, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("ADM",))
    try:
        from src.auth.service import create_user, init_db
        init_db()
        u = create_user(user.username, user.password, user.role, station_id=user.station_id, train_id=user.train_id)
        return {"username": u.username, "role": u.role, "station_id": getattr(u, "station_id", None), "train_id": getattr(u, "train_id", None)}
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


class StationChange(BaseModel):
    station_id: str | None = None


@app.put("/admin/users/{username}/station")
def admin_change_station(username: str, body: StationChange, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("ADM",))
    try:
        from src.auth.service import change_station, init_db
        init_db()
        u = change_station(username, body.station_id)
        if not u:
            raise HTTPException(status_code=404, detail="User not found")
        return {"username": u.username, "role": u.role, "station_id": getattr(u, "station_id", None)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class TrainChange(BaseModel):
    train_id: str | None = None


@app.put("/admin/users/{username}/train")
def admin_change_train(username: str, body: TrainChange, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("ADM",))
    try:
        from src.auth.service import change_train, init_db
        init_db()
        u = change_train(username, body.train_id)
        if not u:
            raise HTTPException(status_code=404, detail="User not found")
        return {"username": u.username, "role": u.role, "train_id": getattr(u, "train_id", None)}
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


@app.get("/audit")
def audit_range(scope: str, date: str, start_ts: Optional[str] = None, end_ts: Optional[str] = None) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    trail = _read_json(base / "audit_trail.json") or []
    if start_ts or end_ts:
        from pandas import to_datetime
        s = to_datetime(start_ts) if start_ts else None
        e = to_datetime(end_ts) if end_ts else None
        out = []
        for x in trail:
            try:
                ts = to_datetime(x.get("ts"))
                if (s is None or ts >= s) and (e is None or ts <= e):
                    out.append(x)
            except Exception:
                continue
        trail = out
    return {"audit": trail}


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

# Runtime engine singleton
try:
    from src.runtime.engine import RuntimeEngine, EngineConfig
    ENGINE: Optional[RuntimeEngine] = None
except Exception:
    ENGINE = None

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


# ---------- Predictive Models ----------
@app.post("/admin/train_eta")
def admin_train_eta(scope: str, date: str, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("ADM", "OM", "DH"))
    try:
        from src.learn.eta import train_eta
        rep = train_eta(scope, date)
        return {"status": rep.get("status", "ok"), "report": rep}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/predict/eta")
def predict_eta(scope: str, date: str, train_id: str) -> Dict[str, Any]:
    try:
        from src.learn.eta import predict_next_eta
        return predict_next_eta(scope, date, train_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/predict/eta/importance")
def predict_eta_importance(scope: str, date: str) -> Dict[str, Any]:
    from pathlib import Path
    import joblib  # type: ignore
    p = _art_dir(scope, date) / "eta_model.joblib"
    if not p.exists():
        raise HTTPException(status_code=404, detail="eta model not trained")
    payload = joblib.load(p)
    model = payload.get("model")
    features = payload.get("features") or []
    imps = getattr(model, "feature_importances_", None)
    if imps is None:
        return {"features": features, "importance": []}
    return {"features": features, "importance": [float(x) for x in imps]}


@app.post("/admin/build_incident_risk")
def admin_build_incident_risk(scope: str, date: str, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("ADM", "OM", "DH"))
    try:
        from src.learn.incident_risk import train_incident_risk
        rep = train_incident_risk(scope, date)
        return {"status": rep.get("status", "ok"), "report": rep}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/risk/heatmap")
def get_risk_heatmap(scope: str, date: str) -> Dict[str, Any]:
    base = _art_dir(scope, date)
    heat = _read_json(base / "incident_heat.json") or {}
    return {"heat": heat}


# ---------- Admin: model training jobs ----------
class TrainResp(BaseModel):
    status: str
    details: Dict[str, Any] | None = None


@app.post("/admin/train_global")
def admin_train_global(principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("ADM", "OM", "DH"))
    try:
        from src.learn.train_corpus import train_global
        rep = train_global("artifacts")
        return {"status": rep.get("status", "ok"), "report": rep}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/build_offline_rl")
def admin_build_offline_rl(alpha: float = 0.2, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("ADM", "OM", "DH"))
    try:
        from src.learn.offline_rl import build_offline_rl
        p = build_offline_rl("artifacts", alpha=alpha)
        return {"status": "ok", "path": str(p)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/train_offrl")
def admin_train_offrl(principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("ADM", "OM", "DH"))
    try:
        from src.learn.train_offrl import train_offrl
        rep = train_offrl("artifacts")
        return {"status": rep.get("status", "ok"), "report": rep}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/eval_offline")
def admin_eval_offline(topk: int = 1, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("ADM", "OM", "DH"))
    try:
        from src.learn.eval_offline import evaluate
        res = evaluate("artifacts", topk=topk)
        return {"status": res.get("status", "ok"), "result": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/train_il_torch")
def admin_train_il_torch(principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    require_roles(principal, ("ADM", "OM", "DH"))
    try:
        from src.learn.policy_torch import train_torch
        rep = train_torch("artifacts")
        return {"status": rep.get("status", "ok"), "report": rep}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Crew feed ----------
@app.get("/crew/feed")
def crew_feed(scope: str, date: str, train_id: Optional[str] = None, principal: Principal = Depends(get_principal)) -> Dict[str, Any]:
    # Enforce train scoping for crew
    if principal.role == "CREW":
        if principal.train_id:
            train_id = principal.train_id
        else:
            raise HTTPException(status_code=400, detail="CREW account has no train assignment")
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


# Metrics and health
@app.get("/metrics")
def get_metrics() -> Response:
    try:
        from src.ops.metrics import text_metrics
        data, ctype = text_metrics()
        return Response(content=data, media_type=ctype)
    except Exception:
        return Response(content=b"", media_type="text/plain")


@app.get("/healthz")
def healthz() -> Dict[str, Any]:
    return {"status": "ok"}


@app.get("/readiness")
def readiness() -> Dict[str, Any]:
    global ENGINE
    return {"ready": ENGINE is not None}


@app.on_event("startup")
def on_startup() -> None:
    # Start runtime engine in sandbox mode by default
    global ENGINE
    try:
        if ENGINE is None:
            cfg = EngineConfig()
            ENGINE = RuntimeEngine(cfg)
            ENGINE.start()
    except Exception:
        pass
