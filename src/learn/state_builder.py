from __future__ import annotations

"""State/label builder for imitation learning (micro-holds).

Reads Phase-3/4 artifacts for a given ``scope/date`` and produces
feature rows per identified risk with target hold minutes class in
{2,3,5}. This keeps scope tight and opinionated for a first IL model.

Outputs an in-memory DataFrame and optionally persists a parquet at:
  artifacts/<scope>/<date>/il_training.parquet
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import json
import pandas as pd

SEV_RANK = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}

# Simple train class heuristics based on train name keywords
_TRAIN_CLASS_KEYWORDS = [
    ("SUPERFAST", "Superfast"),
    ("EXPRESS", "Express"),
    ("EMU", "EMU"),
    ("LOCAL", "EMU"),
    ("FREIGHT", "Freight"),
    ("GOODS", "Freight"),
]
_CLASS_PRIORITY = {  # higher number = higher priority cost to delay
    "Superfast": 3,
    "Express": 2,
    "EMU": 1,
    "Passenger": 1,
    "Freight": 0,
}


def _base(scope: str, date: str) -> Path:
    return Path("artifacts") / scope / date


def _to_utc(s: pd.Series | None) -> pd.Series:
    if s is None:
        return pd.Series(dtype="datetime64[ns, UTC]")
    return pd.to_datetime(s, utc=True, errors="coerce")


def _discretize_hold(mins: float) -> int:
    """Map minutes to nearest of {2,3,5}. Values <1 map to 2 by default."""
    if mins is None or pd.isna(mins):
        return 2
    x = float(mins)
    if x <= 2.5:
        return 2
    if x <= 4.0:
        return 3
    return 5


def _read_json(p: Path) -> object | None:
    try:
        if not p.exists():
            return None
        return json.loads(p.read_text())
    except Exception:
        return None


@dataclass
class Row:
    # Minimal features (keep stable across versions)
    risk_type: str
    severity_rank: int
    lead_min: float
    headway_min: float
    capacity: int
    # Optional context
    block_len_trains: int
    platforms: int
    # Priority / fairness
    train_class: str
    priority_weight: int
    recent_holds: int
    # Target
    hold_class: int  # in {2,3,5}
    # IDs for traceability
    train_id: str
    block_id: str | None
    station_id: str | None


def build_examples(scope: str, date: str, *, persist: bool = True, prefer_expert: bool = True) -> pd.DataFrame:
    base = _base(scope, date)
    edges_p = base / "section_edges.parquet"
    nodes_p = base / "section_nodes.parquet"
    radar_p = base / "conflict_radar.json"
    block_p = base / "national_block_occupancy.parquet"

    if not (radar_p.exists() and edges_p.exists() and block_p.exists()):
        # Try corridor artifacts names as fallback
        if not edges_p.exists():
            edges_p = base / "section_edges.parquet"
        if not block_p.exists():
            block_p = base / "block_occupancy.parquet"
        if not nodes_p.exists():
            nodes_p = base / "section_nodes.parquet"

    edges = pd.read_parquet(edges_p) if edges_p.exists() else pd.DataFrame()
    nodes = pd.read_parquet(nodes_p) if nodes_p.exists() else pd.DataFrame()
    bo = pd.read_parquet(block_p) if block_p.exists() else pd.DataFrame()
    radar = _read_json(radar_p) or []
    waits_p = base / "national_waiting_ledger.parquet"
    if not waits_p.exists():
        waits_p = base / "waiting_ledger.parquet"
    waits = pd.read_parquet(waits_p) if waits_p.exists() else pd.DataFrame()
    events_p = base / "events_clean.parquet"
    events = pd.read_parquet(events_p) if events_p.exists() else pd.DataFrame()
    rec_plan = _read_json(base / "rec_plan.json") or []
    # Build accepted action lookup from feedback (prefer APPLY/MODIFY/ACK)
    feedback_lookup: Dict[str, float] = {}
    fb_pq = base / "feedback.parquet"
    try:
        if fb_pq.exists():
            fb = pd.read_parquet(fb_pq)
            if not fb.empty:
                for _, r in fb.iterrows():
                    dec = str(r.get("decision", "")).upper()
                    if dec not in ("APPLY", "MODIFY", "ACK"):
                        continue
                    # action JSON contains minutes
                    act = r.get("action")
                    if isinstance(act, str):
                        try:
                            obj = json.loads(act)
                        except Exception:
                            obj = None
                    elif isinstance(act, dict):
                        obj = act
                    else:
                        obj = None
                    if obj and obj.get("type") == "HOLD":
                        key = (str(obj.get("train_id")), str(obj.get("block_id") or obj.get("station_id") or obj.get("at_station")))
                        mins = obj.get("minutes")
                        if mins is not None:
                            feedback_lookup[str(key)] = float(mins)
    except Exception:
        pass

    edges_idx = edges.set_index("block_id") if not edges.empty and "block_id" in edges.columns else pd.DataFrame()
    nodes_idx = nodes.set_index("station_id") if not nodes.empty and "station_id" in nodes.columns else pd.DataFrame()

    bo = bo.copy()
    if not bo.empty:
        bo["entry_time"] = _to_utc(bo.get("entry_time"))
        bo["exit_time"] = _to_utc(bo.get("exit_time"))

    # Train name -> train class mapping
    name_map: dict[str, str] = {}
    if not events.empty and "train_id" in events.columns:
        name_col = None
        for c in ("train_name", "Train Name", "name"):
            if c in events.columns:
                name_col = c
                break
        if name_col:
            sub = events.dropna(subset=["train_id"]).drop_duplicates(subset=["train_id"]) [["train_id", name_col]]
            for _, rr in sub.iterrows():
                name_map[str(rr["train_id"])]= str(rr[name_col])
    # Heuristic classifier
    def _train_class(tid: str) -> tuple[str, int]:
        nm = (name_map.get(str(tid)) or "").upper()
        for kw, cls in _TRAIN_CLASS_KEYWORDS:
            if kw in nm:
                return cls, _CLASS_PRIORITY.get(cls, 1)
        # default
        cls = "Passenger"
        return cls, _CLASS_PRIORITY.get(cls, 1)

    rows: List[Row] = []

    # Quick helper to count local density at risk start
    def _block_density(bid: str, start_ts: pd.Timestamp) -> int:
        if bo.empty or bid not in set(bo.get("block_id", [])):
            return 0
        g = bo[bo["block_id"] == bid]
        if g.empty:
            return 0
        g = g.sort_values("entry_time")
        active = g[(g["entry_time"] <= start_ts) & (g["exit_time"] >= start_ts)]
        return int(len(active))

    for r in radar:
        rtype = str(r.get("type"))
        sev = SEV_RANK.get(str(r.get("severity")), 3)
        lead = float(r.get("lead_min", 0.0))
        bid = r.get("block_id")
        sid = r.get("station_id")
        trains = [str(t) for t in (r.get("train_ids") or [])]
        # choose target train: follower or first if unknown
        if rtype == "headway" and len(trains) >= 2:
            target_train = trains[1]
        else:
            target_train = trains[0] if trains else ""
        ts0 = None
        try:
            ts0 = pd.to_datetime(r.get("time_window")[0], utc=True) if r.get("time_window") else None
        except Exception:
            ts0 = None

        headway = 0.0
        capacity = 1
        platforms = 1
        if bid and not edges_idx is None and len(edges_idx) and bid in edges_idx.index:
            headway = float(edges_idx.loc[bid].get("headway", 0.0))
            capacity = int(edges_idx.loc[bid].get("capacity", 1))
        if sid and not nodes_idx is None and len(nodes_idx) and sid in nodes_idx.index:
            platforms = int(nodes_idx.loc[sid].get("platforms", 1))

        blk_density = _block_density(str(bid), ts0) if (bid and ts0 is not None) else 0

        # Priority & fairness features
        tr_class, prio_w = _train_class(target_train)
        recent_holds = 0
        try:
            if not waits.empty and "train_id" in waits.columns:
                recent_holds = int(waits[waits["train_id"].astype(str) == str(target_train)].shape[0])
        except Exception:
            recent_holds = 0

        # target minutes: prefer expert (rec_plan + feedback) if available
        need = float(r.get("required_hold_min", 2.0 if rtype == "block_capacity" else 0.0))
        target_min = None
        if prefer_expert and rec_plan:
            # Match by train and location
            best = None
            for a in rec_plan:
                if a.get("type") != "HOLD":
                    continue
                if target_train and str(a.get("train_id")) != str(target_train):
                    continue
                loc_match = False
                if bid and str(a.get("block_id")) == str(bid):
                    loc_match = True
                elif sid and (str(a.get("station_id")) == str(sid) or str(a.get("at_station")) == str(sid)):
                    loc_match = True
                elif bid and a.get("at_station") and not sid and not str(a.get("block_id")):
                    # upstream station often stored as at_station for block risks
                    g_b = bo[(bo.get("block_id") == bid) & (bo.get("train_id").astype(str) == str(target_train))]
                    if not g_b.empty and str(a.get("at_station")) == str(g_b.iloc[0].get("u")):
                        loc_match = True
                if loc_match:
                    best = a
                    break
            if best is not None:
                target_min = float(best.get("minutes", need or 2.0))
        # Overwrite with feedback-applied minute if present
        if prefer_expert and target_train:
            k = str((str(target_train), str(bid or sid or "")))
            if k in feedback_lookup:
                target_min = float(feedback_lookup[k])
        if target_min is None:
            target_min = need if need > 0 else 2.0
        hold_cls = _discretize_hold(target_min)

        rows.append(
            Row(
                risk_type=rtype,
                severity_rank=int(sev),
                lead_min=float(lead),
                headway_min=float(headway),
                capacity=int(capacity),
                block_len_trains=int(blk_density),
                platforms=int(platforms),
                train_class=str(tr_class),
                priority_weight=int(prio_w),
                recent_holds=int(recent_holds),
                hold_class=int(hold_cls),
                train_id=str(target_train),
                block_id=str(bid) if bid else None,
                station_id=str(sid) if sid else None,
            )
        )

    df = pd.DataFrame([r.__dict__ for r in rows])
    if persist:
        out_p = base / "il_training.parquet"
        df.to_parquet(out_p, index=False)
    return df


def feature_label(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Split feature columns and label series with stable order."""
    feats = [
        "severity_rank",
        "lead_min",
        "headway_min",
        "capacity",
        "block_len_trains",
        "platforms",
        "priority_weight",
        "recent_holds",
    ]
    X = df[feats].copy()
    y = df["hold_class"].astype(int)
    return X, y


if __name__ == "__main__":  # pragma: no cover
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--scope", default="all_india")
    ap.add_argument("--date", default="2024-01-01")
    args = ap.parse_args()
    d = build_examples(args.scope, args.date, persist=True)
    print(d.head().to_string(index=False))
