from __future__ import annotations

"""Runtime inference for micro-actions using IL model + safety checks.

Prefers HOLD actions for first deployment phases. Falls back to
heuristic optimizer if model is missing.
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple
import json
import math

import joblib  # type: ignore
import pandas as pd

from src.learn.state_builder import build_examples, feature_label, SEV_RANK


def _base(scope: str, date: str) -> Path:
    return Path("artifacts") / scope / date


def _read_json(p: Path) -> object | None:
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


def _safety_adjust_minutes(
    minutes: float,
    *,
    bid: Optional[str],
    follower: Optional[str],
    ts: Optional[pd.Timestamp],
    edges_df: pd.DataFrame,
    block_occ_df: pd.DataFrame,
    max_hold_min: float,
) -> float:
    """Ensure headway will be satisfied on the target block if possible.

    If insufficient, increase hold up to ``max_hold_min``. Conservative
    check using current plan windows.
    """
    if not bid or not follower or ts is None:
        return max(0.0, min(float(minutes), float(max_hold_min)))
    if block_occ_df.empty or edges_df.empty:
        return max(0.0, min(float(minutes), float(max_hold_min)))
    try:
        g = block_occ_df.copy()
        g = g[g["block_id"].astype(str) == str(bid)]
        if g.empty:
            return max(0.0, min(float(minutes), float(max_hold_min)))
        g = g.sort_values("entry_time")
        headway = 0.0
        try:
            headway = float(edges_df.set_index("block_id").loc[str(bid), "headway"]) if "headway" in edges_df.columns else 0.0
        except Exception:
            headway = 0.0
        # Find follower row at/after ts
        g_f = g[(g["train_id"].astype(str) == str(follower)) & (g["entry_time"] >= ts)]
        if g_f.empty:
            return max(0.0, min(float(minutes), float(max_hold_min)))
        row = g_f.iloc[0]
        prevs = g[g["entry_time"] < row["entry_time"]]
        if prevs.empty:
            return max(0.0, min(float(minutes), float(max_hold_min)))
        prev_exit = prevs["exit_time"].max()
        required = (prev_exit + pd.Timedelta(minutes=headway) - row["entry_time"]).total_seconds() / 60.0
        if required <= 0:
            return max(0.0, min(float(minutes), float(max_hold_min)))
        need = max(required, float(minutes))
        return max(0.0, min(float(need), float(max_hold_min)))
    except Exception:
        return max(0.0, min(float(minutes), float(max_hold_min)))


def _minutes_from_class(c: int) -> float:
    c = int(c)
    if c <= 2:
        return 2.0
    if c == 3:
        return 3.0
    return 5.0


def suggest(
    scope: str,
    date: str,
    *,
    role: str = "AN",
    train_id: Optional[str] = None,
    station_id: Optional[str] = None,
    max_hold_min: int = 3,
) -> Dict[str, object]:
    base = _base(scope, date)
    # Prefer global model if available
    global_dir = Path("artifacts") / "global_models"
    rl_p = global_dir / "policy_rl.joblib"
    torch_p = global_dir / "policy_torch.pt"
    il_p = global_dir / "policy_il.joblib"
    # Priority: RL -> Torch IL -> Global IL -> Run IL
    model_path_kind = None
    model_p = None
    if rl_p.exists():
        model_p = rl_p
        model_path_kind = "rl"
    elif torch_p.exists():
        model_p = torch_p
        model_path_kind = "torch"
    elif il_p.exists():
        model_p = il_p
        model_path_kind = "il"
    else:
        model_p = base / "policy_il.joblib"
        model_path_kind = "il_run"
    radar_p = base / "conflict_radar.json"
    edges_p = base / "section_edges.parquet"
    block_p = base / "national_block_occupancy.parquet"
    nodes_p = base / "section_nodes.parquet"

    edges = pd.read_parquet(edges_p) if edges_p.exists() else pd.DataFrame()
    nodes = pd.read_parquet(nodes_p) if nodes_p.exists() else pd.DataFrame()
    bo = pd.read_parquet(block_p) if block_p.exists() else pd.DataFrame()
    radar = _read_json(radar_p) or []

    if bo is not None and not bo.empty:
        bo = bo.copy()
        bo["entry_time"] = _to_utc(bo.get("entry_time"))
        bo["exit_time"] = _to_utc(bo.get("exit_time"))

    # Build current examples for inference
    df = build_examples(scope, date, persist=False)
    if df.empty and not model_p.exists():
        # Nothing to do
        return {"suggestions": [], "source": "empty"}

    # Filter to a specific train for crew if provided
    if train_id:
        df = df[df["train_id"].astype(str) == str(train_id)]
    # Station controller filter: keep risks/actions at the station (station_id) or blocks touching the station
    if station_id:
        sid = str(station_id)
        blocks_touching: set[str] = set()
        try:
            if not bo.empty and {"u","v","block_id"}.issubset(bo.columns):
                sub = bo[(bo["u"].astype(str) == sid) | (bo["v"].astype(str) == sid)]
                blocks_touching = set(sub["block_id"].astype(str).unique().tolist())
        except Exception:
            blocks_touching = set()
        keep = []
        for _, row in df.iterrows():
            bid = row.get("block_id")
            stid = row.get("station_id")
            if stid is not None and str(stid) == sid:
                keep.append(True)
            elif bid is not None and str(bid) in blocks_touching:
                keep.append(True)
            else:
                keep.append(False)
        if len(keep) == len(df):
            df = df[pd.Series(keep, index=df.index)]

    # If no model, fall back to heuristics via optimizer
    if not model_p.exists():
        try:
            from src.opt.engine import propose
            rec, _, metrics, audit = propose(edges, nodes, bo, radar, max_hold_min=int(max_hold_min))
            if train_id:
                rec = [r for r in rec if str(r.get("train_id")) == str(train_id)]
            return {"suggestions": rec, "source": "heuristic"}
        except Exception:
            return {"suggestions": [], "source": "unavailable"}

    pred_cls = None
    if model_path_kind == "rl":
    payload = joblib.load(model_p)
        model = payload.get("model")
        features = payload.get("features") or []
        actions = payload.get("actions") or [2, 3, 5]
        X_base = df[features].copy()
        pred_cls = []
        for _, row in X_base.iterrows():
            q_vals = []
            for a in actions:
                vec = row.to_dict()
                for aa in actions:
                    vec[f"a_{aa}"] = 1.0 if aa == a else 0.0
                q = float(model.predict(pd.DataFrame([vec]))[0])
                q_vals.append((a, q))
            pred_cls.append(int(max(q_vals, key=lambda t: t[1])[0]))
    elif model_path_kind == "torch":
        try:
            import torch  # type: ignore
        except Exception:
            # Fallback to IL if torch not installed
            model_path_kind = "il"
        if model_path_kind == "torch":
            payload = torch.load(model_p, map_location="cpu")  # type: ignore
            feats = payload.get("features") or []
            mean = payload.get("mean") or {}
            std = payload.get("std") or {}
            classes = payload.get("classes") or [2, 3, 5]
            Xb = df[feats].copy()
            # Normalize
            for c in feats:
                mu = float(mean.get(c, 0.0))
                sd = float(std.get(c, 1.0)) or 1.0
                Xb[c] = (Xb[c].astype(float) - mu) / sd
            X_tensor = torch.tensor(Xb.values, dtype=torch.float32)  # type: ignore
            # Recreate model
            hidden = payload.get("hidden") or [64, 64]
            in_dim = X_tensor.shape[1]
            import torch.nn as nn  # type: ignore

            class _MLP(nn.Module):  # type: ignore[misc]
                def __init__(self, in_dim: int, hidden: list[int], out_dim: int) -> None:
                    super().__init__()
                    layers = []
                    prev = in_dim
                    for h in hidden:
                        layers += [nn.Linear(prev, h), nn.ReLU()]
                        prev = h
                    layers += [nn.Linear(prev, len(classes))]
                    self.net = nn.Sequential(*layers)

                def forward(self, x):  # type: ignore[override]
                    return self.net(x)

            model = _MLP(in_dim, hidden, len(classes))
            state = payload.get("state_dict") or {}
            model.load_state_dict(state, strict=False)
            model.eval()
            with torch.no_grad():
                logits = model(X_tensor)
                idx = logits.argmax(dim=1).cpu().numpy().tolist()
            pred_cls = [int(classes[i]) for i in idx]
    if pred_cls is None:
        # Fallback to IL
        payload = joblib.load(model_p)
        model = payload.get("model")
        features = payload.get("features") or []
        X_base = df[features].copy() if features else df.drop(columns=["hold_class"], errors="ignore")
        pred_cls = model.predict(X_base)

    out: List[dict] = []
    for i, row in df.reset_index(drop=True).iterrows():
        c = int(pred_cls[i]) if i < len(pred_cls) else 2
        mins = _minutes_from_class(c)
        rtype = str(row.get("risk_type"))
        bid = row.get("block_id")
        sid = row.get("station_id")
        tid = str(row.get("train_id"))
        # Risk timestamp
        ts = None
        try:
            # reconstruct ts from radar for this train/block/station
            ts = None
            for rr in radar:
                if rr.get("type") == rtype and (rr.get("block_id") == bid or rr.get("station_id") == sid) and (tid in [str(t) for t in (rr.get("train_ids") or [])]):
                    ts = pd.to_datetime(rr.get("time_window")[0], utc=True)
                    need = float(rr.get("required_hold_min", 0.0) or 0.0)
                    if need > 0:
                        mins = max(mins, need)
                    break
        except Exception:
            ts = None
        # Safety adjust for headway if applicable
        mins_adj = _safety_adjust_minutes(mins, bid=bid, follower=tid, ts=ts, edges_df=edges, block_occ_df=bo if isinstance(bo, pd.DataFrame) else pd.DataFrame(), max_hold_min=max_hold_min)
        mins_adj = max(0.0, min(float(mins_adj), float(max_hold_min)))
        if mins_adj <= 0:
            # No-op for this item
            continue
        # Build action at upstream station if block risk, else at station
        at_station = sid
        if rtype in ("headway", "block_capacity") and bid and not bo.empty:
            try:
                g_b = bo[bo["block_id"].astype(str) == str(bid)]
                g_b = g_b[g_b["train_id"].astype(str) == tid]
                if not g_b.empty:
                    at_station = str(g_b.iloc[0].get("u"))
            except Exception:
                pass
        why = "Resolve {t} on {loc}".format(t=rtype, loc=(bid or sid or "unknown"))
        action = {
            "train_id": tid,
            "type": "HOLD",
            "at_station": at_station,
            "minutes": round(float(mins_adj), 1),
            "reason": rtype,
            "block_id": bid,
            "station_id": sid,
            "why": why,
            "impact": {"conflicts_resolved": 1 if rtype in ("headway", "block_capacity", "platform_overflow") else 0},
            "safety_checks": [
                "hold_within_policy_limit" if float(mins_adj) <= float(max_hold_min) else "",
            ],
        }
        out.append(action)

    # Rate limiting per station (basic): max 20 suggestions/minute per station; cooldown if recent DISMISS
    rate_meta = {}
    try:
        if station_id:
            rate_p = base / "rate_limit.json"
            now = pd.Timestamp.utcnow().tz_localize("UTC")
            if rate_p.exists():
                rate_meta = json.loads(rate_p.read_text())
            key = f"{station_id}"
            times = [pd.to_datetime(t) for t in rate_meta.get(key, [])]
            times = [t for t in times if (now - t).total_seconds() < 60]
            max_per_min = 20
            if len(times) >= max_per_min:
                return {"suggestions": [], "source": model_path_kind or "policy_il", "rate_limited": True}
            times.append(now)
            rate_meta[key] = [str(t) for t in times]
            rate_p.write_text(json.dumps(rate_meta, indent=2))
            # Cooldown: if last DISMISS in past 5 minutes for this station, suppress
            audit = _read_json(base / "audit_trail.json") or []
            last_dismiss = None
            for e in reversed(audit):
                if str(e.get("decision","")) == "DISMISS":
                    rec = e.get("action") or {}
                    if str(rec.get("station_id","")) == str(station_id) or str(rec.get("at_station","")) == str(station_id):
                        last_dismiss = pd.to_datetime(e.get("ts"))
                        break
            if last_dismiss is not None and (now - last_dismiss).total_seconds() < 300:
                return {"suggestions": [], "source": model_path_kind or "policy_il", "cooldown": True}
    except Exception:
        pass

    # Role-based view shaping (responses formatted by API/UI)
    return {"suggestions": out, "source": model_path_kind or "policy_il"}
