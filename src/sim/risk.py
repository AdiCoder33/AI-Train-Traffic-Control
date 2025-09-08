"""Conflict detection and risk prediction layer (Phase 3).

Scans upcoming block and platform allocations, predicts potential
violations (capacity/headway/platform), and produces alerts and simple
mitigation previews (hold suggestions).

Inputs
------
- edges_df: blocks with columns ["block_id", "u", "v", "headway", "capacity"]
- nodes_df: stations with ["station_id", "platforms"]
- block_occ_df: post-safety occupancy with ["train_id","block_id","u","v",
  "entry_time","exit_time","headway_applied_min"]
- platform_occ_df (optional): post-safety platform windows with
  ["train_id","station_id","arr_platform","dep_platform"]
- waiting_df (optional): holds with ["train_id","resource","id","start_time",
  "end_time","minutes","reason"]

Outputs
-------
- radar: list of risk dicts
- timeline_df: risk density per time bucket
- preview: simple mitigation recommendations per risk
- kpis: summary metrics
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import json
import math
import pandas as pd

__all__ = ["analyze", "validate", "save"]


def _to_utc(s: pd.Series | None) -> pd.Series:
    if s is None:
        return pd.Series(dtype="datetime64[ns, UTC]")
    return pd.to_datetime(s, utc=True, errors="coerce")


def _severity(minutes_to: float) -> str:
    if minutes_to <= 5:
        return "Critical"
    if minutes_to <= 30:
        return "High"
    if minutes_to <= 120:
        return "Medium"
    return "Low"


def _bucket(ts: pd.Series, bucket_min: int) -> pd.Series:
    # floor timestamps to bucket_min
    return (ts.dt.floor(f"{bucket_min}min"))


def analyze(
    edges_df: pd.DataFrame,
    nodes_df: pd.DataFrame,
    block_occ_df: pd.DataFrame,
    *,
    platform_occ_df: Optional[pd.DataFrame] = None,
    waiting_df: Optional[pd.DataFrame] = None,
    t0: Optional[str | pd.Timestamp] = None,
    horizon_min: int = 60,
    bucket_min: int = 5,
) -> tuple[list[dict], pd.DataFrame, list[dict], dict]:
    # Normalize inputs
    edges = edges_df.copy()
    edges["headway"] = pd.to_numeric(edges.get("headway", 0), errors="coerce").fillna(0.0)
    edges["capacity"] = pd.to_numeric(edges.get("capacity", 1), errors="coerce").fillna(1).astype(int)
    nodes = nodes_df.copy()
    nodes["platforms"] = pd.to_numeric(nodes.get("platforms", 1), errors="coerce").fillna(1).astype(int)

    bo = block_occ_df.copy()
    if bo.empty:
        return [], pd.DataFrame(columns=["ts_bucket","resource_type","resource_id","risk_count"]), [], {"total_risks": 0}

    bo["entry_time"] = _to_utc(bo["entry_time"])
    bo["exit_time"] = _to_utc(bo["exit_time"])
    bo["headway_applied_min"] = pd.to_numeric(bo.get("headway_applied_min", 0.0), errors="coerce").fillna(0.0)

    # Join edge attributes
    bo = bo.merge(edges[["block_id","headway","capacity"]], on="block_id", how="left")
    # Approximate pre-headway windows
    run_min = (bo["exit_time"] - bo["entry_time"]).dt.total_seconds() / 60.0
    pre_entry = bo["entry_time"] - pd.to_timedelta(bo["headway_applied_min"], unit="m")
    pre_exit = pre_entry + pd.to_timedelta(run_min, unit="m")
    bo["pre_entry"] = pre_entry
    bo["pre_exit"] = pre_exit

    # Horizon window
    if t0 is None:
        t0 = bo["entry_time"].min()
    t0 = pd.to_datetime(t0, utc=True)
    t1 = t0 + pd.Timedelta(minutes=horizon_min)

    risks: List[dict] = []
    # Detect block capacity and headway risks using pre-headway windows
    for bid, grp in bo.sort_values("pre_entry").groupby("block_id"):
        cap = int(grp["capacity"].iloc[0]) if "capacity" in grp else 1
        headway_min = float(grp["headway"].iloc[0]) if "headway" in grp else 0.0
        rows = grp.sort_values("pre_entry").reset_index(drop=True)
        # capacity check via sweep line of pre windows
        active: List[Tuple[pd.Timestamp, int]] = []  # (pre_exit, idx)
        for i, row in rows.iterrows():
            entry_i = row["pre_entry"]
            exit_i = row["pre_exit"]
            if pd.isna(entry_i) or pd.isna(exit_i):
                continue
            # prune
            active = [(ex, j) for (ex, j) in active if ex > entry_i]
            # capacity breach would occur if adding this exceeds cap
            if len(active) >= cap:
                start = entry_i
                if t0 <= start <= t1:
                    minutes_to = max(0.0, (start - t0).total_seconds() / 60.0)
                    risks.append({
                        "type": "block_capacity",
                        "block_id": bid,
                        "u": row.get("u"),
                        "v": row.get("v"),
                        "train_ids": [rows.loc[j, "train_id"] for (_, j) in active] + [row["train_id"]],
                        "time_window": [str(entry_i), str(exit_i)],
                        "severity": _severity(minutes_to),
                        "lead_min": minutes_to,
                    })
            # headway breach vs the last active with latest exit
            if active:
                prev_j = max(active, key=lambda t: t[0])[1]
                prev_exit = rows.loc[prev_j, "pre_exit"]
                required = prev_exit + pd.Timedelta(minutes=headway_min)
                if entry_i < required:
                    start = entry_i
                    if t0 <= start <= t1:
                        minutes_to = max(0.0, (start - t0).total_seconds() / 60.0)
                        req_gap = (required - entry_i).total_seconds() / 60.0
                        risks.append({
                            "type": "headway",
                            "block_id": bid,
                            "u": row.get("u"),
                            "v": row.get("v"),
                            "train_ids": [rows.loc[prev_j, "train_id"], row["train_id"]],
                            "time_window": [str(entry_i), str(prev_exit)],
                            "severity": _severity(minutes_to),
                            "lead_min": minutes_to,
                            "required_hold_min": max(0.0, req_gap),
                        })
            active.append((exit_i, i))

    # Platform risks: derive from waiting ledger or platform occupancy if available
    if waiting_df is not None and not getattr(waiting_df, "empty", True):
        wd = waiting_df.copy()
        wd["start_time"] = _to_utc(wd["start_time"]) if "start_time" in wd.columns else pd.NaT
        wd["end_time"] = _to_utc(wd["end_time"]) if "end_time" in wd.columns else pd.NaT
        wd = wd[(wd["reason"] == "platform_busy") & wd["start_time"].notna()]
        wd = wd[(wd["start_time"] >= t0) & (wd["start_time"] <= t1)]
        for _, row in wd.iterrows():
            minutes_to = max(0.0, (row["start_time"] - t0).total_seconds() / 60.0)
            risks.append({
                "type": "platform_overflow",
                "station_id": row.get("id"),
                "train_ids": [row.get("train_id")],
                "time_window": [str(row.get("start_time")), str(row.get("end_time"))],
                "severity": _severity(minutes_to),
                "lead_min": minutes_to,
                "required_hold_min": float(row.get("minutes", 0.0)),
            })
    elif platform_occ_df is not None and not platform_occ_df.empty:
        # Detect overflows from provided platform occupancy
        po = platform_occ_df.copy()
        po["arr_platform"] = _to_utc(po["arr_platform"]) if "arr_platform" in po.columns else pd.NaT
        po["dep_platform"] = _to_utc(po["dep_platform"]) if "dep_platform" in po.columns else pd.NaT
        po = po[(po["arr_platform"] >= t0) & (po["arr_platform"] <= t1)]
        plat_map = nodes.set_index("station_id")["platforms"].to_dict()
        for sid, grp in po.groupby("station_id"):
            cap = int(plat_map.get(sid, 1))
            rows = grp.sort_values("arr_platform").reset_index(drop=True)
            active: List[pd.Timestamp] = []
            for _, row in rows.iterrows():
                a, d = row["arr_platform"], row["dep_platform"]
                active = [ex for ex in active if ex > a]
                if len(active) >= cap:
                    minutes_to = max(0.0, (a - t0).total_seconds() / 60.0)
                    risks.append({
                        "type": "platform_overflow",
                        "station_id": sid,
                        "train_ids": [row.get("train_id")],
                        "time_window": [str(a), str(d)],
                        "severity": _severity(minutes_to),
                        "lead_min": minutes_to,
                        "required_hold_min": 2.0,
                    })
                active.append(d)
    else:
        # Fallback: approximate platform windows from block arrivals + min_dwell
        dwell_map = nodes.set_index("station_id").get("min_dwell_min")
        if dwell_map is None:
            dwell_map = pd.Series(2.0, index=nodes["station_id"])  # default dwell
        else:
            dwell_map = pd.to_numeric(dwell_map, errors="coerce").fillna(2.0)
        arrivals = bo[["train_id", "v", "exit_time"]].rename(columns={"v": "station_id", "exit_time": "arr_platform"})
        arrivals["dep_platform"] = arrivals.apply(lambda r: r["arr_platform"] + pd.Timedelta(minutes=float(dwell_map.get(r["station_id"], 2.0))), axis=1)
        po = arrivals[(arrivals["arr_platform"] >= t0) & (arrivals["arr_platform"] <= t1)]
        plat_map = nodes.set_index("station_id")["platforms"].to_dict()
        for sid, grp in po.groupby("station_id"):
            cap = int(plat_map.get(sid, 1))
            rows = grp.sort_values("arr_platform").reset_index(drop=True)
            active: List[pd.Timestamp] = []
            for _, row in rows.iterrows():
                a, d = row["arr_platform"], row["dep_platform"]
                active = [ex for ex in active if ex > a]
                if len(active) >= cap:
                    minutes_to = max(0.0, (a - t0).total_seconds() / 60.0)
                    risks.append({
                        "type": "platform_overflow",
                        "station_id": sid,
                        "train_ids": [row.get("train_id")],
                        "time_window": [str(a), str(d)],
                        "severity": _severity(minutes_to),
                        "lead_min": minutes_to,
                        "required_hold_min": 2.0,
                    })
                active.append(d)

    # Timeline aggregation
    if risks:
        ts = pd.to_datetime([r["time_window"][0] for r in risks], utc=True, errors="coerce")
        res_type = ["block" if r["type"] in ("block_capacity","headway") else "platform" for r in risks]
        res_id = [r.get("block_id", r.get("station_id", "")) for r in risks]
        df_tl = pd.DataFrame({"ts": ts, "resource_type": res_type, "resource_id": res_id})
        df_tl = df_tl.dropna(subset=["ts"]).assign(ts_bucket=_bucket(df_tl["ts"], bucket_min))
        timeline = (
            df_tl.groupby(["ts_bucket","resource_type","resource_id"]).size().reset_index(name="risk_count")
        )
    else:
        timeline = pd.DataFrame(columns=["ts_bucket","resource_type","resource_id","risk_count"])

    # Mitigation preview: simple hold suggestions + ETA deltas (lightweight)
    previews: List[dict] = []
    # Precompute per-train downstream chains to approximate ETA deltas
    bo_sorted = bo.sort_values("entry_time")
    by_train: Dict[str, pd.DataFrame] = {tid: g.copy() for tid, g in bo_sorted.groupby("train_id")}

    def _eta_delta(train_id: str, hold_min: float, start_ts: pd.Timestamp) -> float:
        g = by_train.get(train_id)
        if g is None or g.empty:
            return 0.0
        g = g.sort_values("entry_time").copy()
        # cumulative shift applied to segments at/after start_ts
        shift = 0.0
        last_exit = None
        for i, row in g.iterrows():
            entry = row["entry_time"]
            exit_ = row["exit_time"]
            run = (exit_ - entry).total_seconds() / 60.0
            if entry >= start_ts:
                if shift == 0.0:
                    shift = float(hold_min)
                entry = entry + pd.Timedelta(minutes=shift)
                exit_ = entry + pd.Timedelta(minutes=run)
            last_exit = exit_
        if last_exit is None:
            return 0.0
        # Compare to original last exit
        base_last = by_train[train_id]["exit_time"].max()
        return max(0.0, (last_exit - base_last).total_seconds() / 60.0)
    for i, r in enumerate(risks):
        suggestion = None
        if r["type"] == "headway":
            need = float(r.get("required_hold_min", 0.0))
            hold2 = need <= 2.0
            hold5 = need <= 5.0
            # Approximate ETA delta by shifting follower from risk time
            follower = r["train_ids"][1] if len(r["train_ids"]) > 1 else r["train_ids"][0]
            start_ts = pd.to_datetime(r["time_window"][0], utc=True, errors="coerce")
            eta2 = _eta_delta(follower, 2.0, start_ts)
            eta5 = _eta_delta(follower, 5.0, start_ts)
            suggestion = {
                "risk_index": i,
                "type": r["type"],
                "block_id": r["block_id"],
                "train_ids": r["train_ids"],
                "hold_2min_resolves": bool(hold2),
                "hold_5min_resolves": bool(hold5),
                "required_hold_min": need,
                "eta_delta_min_2": float(eta2),
                "eta_delta_min_5": float(eta5),
            }
        elif r["type"] == "platform_overflow":
            need = float(r.get("required_hold_min", 0.0))
            tr = r["train_ids"][0] if r.get("train_ids") else None
            start_ts = pd.to_datetime(r["time_window"][0], utc=True, errors="coerce") if tr else None
            eta2 = _eta_delta(tr, 2.0, start_ts) if tr and start_ts is not None else 0.0
            eta5 = _eta_delta(tr, 5.0, start_ts) if tr and start_ts is not None else 0.0
            suggestion = {
                "risk_index": i,
                "type": r["type"],
                "station_id": r.get("station_id"),
                "train_ids": r["train_ids"],
                "hold_2min_resolves": bool(need <= 2.0),
                "hold_5min_resolves": bool(need <= 5.0),
                "required_hold_min": need,
                "eta_delta_min_2": float(eta2),
                "eta_delta_min_5": float(eta5),
            }
        elif r["type"] == "block_capacity":
            # approximate: spread by 2–5 min could ease overlap
            tr = r.get("train_ids", [])[-1] if r.get("train_ids") else None
            start_ts = pd.to_datetime(r["time_window"][0], utc=True, errors="coerce") if tr else None
            eta2 = _eta_delta(tr, 2.0, start_ts) if tr and start_ts is not None else 0.0
            eta5 = _eta_delta(tr, 5.0, start_ts) if tr and start_ts is not None else 0.0
            suggestion = {
                "risk_index": i,
                "type": r["type"],
                "block_id": r.get("block_id"),
                "train_ids": r.get("train_ids", []),
                "hold_2min_resolves": True,
                "hold_5min_resolves": True,
                "required_hold_min": 2.0,
                "eta_delta_min_2": float(eta2),
                "eta_delta_min_5": float(eta5),
            }
        if suggestion:
            previews.append(suggestion)

    # KPIs
    kpis: Dict[str, float] = {
        "total_risks": float(len(risks)),
        "critical": float(sum(1 for r in risks if r["severity"] == "Critical")),
        "high": float(sum(1 for r in risks if r["severity"] == "High")),
        "medium": float(sum(1 for r in risks if r["severity"] == "Medium")),
        "low": float(sum(1 for r in risks if r["severity"] == "Low")),
    }
    if risks:
        kpis["avg_lead_min"] = float(pd.Series([r["lead_min"] for r in risks]).mean())
        kpis["pct_with_preview"] = float(len(previews) / len(risks) * 100.0)
    else:
        kpis["avg_lead_min"] = 0.0
        kpis["pct_with_preview"] = 0.0

    return risks, timeline, previews, kpis


def validate(
    block_occ_df: pd.DataFrame,
    edges_df: pd.DataFrame,
    risks: List[dict],
) -> Dict[str, object]:
    # Overlaps in post-enforcement occupancy beyond capacity
    edges = edges_df[["block_id", "headway", "capacity"]].copy()
    edges["capacity"] = pd.to_numeric(edges.get("capacity", 1), errors="coerce").fillna(1).astype(int)
    bo = block_occ_df.copy()
    bo["entry_time"] = _to_utc(bo["entry_time"]) if "entry_time" in bo.columns else pd.NaT
    bo["exit_time"] = _to_utc(bo["exit_time"]) if "exit_time" in bo.columns else pd.NaT
    bo = bo.merge(edges, on="block_id", how="left")
    post_overlap = 0
    headway_viol = 0
    for bid, grp in bo.groupby("block_id"):
        cap = int(grp["capacity"].iloc[0]) if "capacity" in grp else 1
        rows = grp.sort_values("entry_time").reset_index(drop=True)
        # capacity check
        active: List[pd.Timestamp] = []
        for _, row in rows.iterrows():
            et, xt = row["entry_time"], row["exit_time"]
            active = [ex for ex in active if ex > et]
            if len(active) >= cap:
                post_overlap += 1
            active.append(xt)
        # headway check
        headway_min = float(rows.get("headway", 0.0).iloc[0]) if "headway" in rows.columns and not rows.empty else 0.0
        prev_exit = None
        for _, row in rows.iterrows():
            et, xt = row["entry_time"], row["exit_time"]
            if prev_exit is not None and et < prev_exit + pd.Timedelta(minutes=headway_min):
                headway_viol += 1
            prev_exit = xt

    # Critical lead times
    critical_leads = [float(r.get("lead_min", 0.0)) for r in risks if r.get("severity") == "Critical"]
    crit_min = min(critical_leads) if critical_leads else None
    return {
        "post_overlap_violations": int(post_overlap),
        "headway_violations": int(headway_viol),
        "critical_min_lead_min": (float(crit_min) if crit_min is not None else None),
        "ok_post_no_overlap": post_overlap == 0,
        "ok_headway_enforced": headway_viol == 0,
    }


def save(
    risks: List[dict],
    timeline: pd.DataFrame,
    previews: List[dict],
    kpis: Dict[str, float],
    out_dir: str | "PathLike[str]",
    validation: Optional[Dict[str, object]] = None,
) -> None:
    from pathlib import Path
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "conflict_radar.json").write_text(json.dumps(risks, indent=2))
    timeline.to_parquet(out / "risk_timeline.parquet", index=False)
    (out / "mitigation_preview.json").write_text(json.dumps(previews, indent=2))
    (out / "risk_kpis.json").write_text(json.dumps(kpis, indent=2))
    if validation is not None:
        (out / "risk_validation.json").write_text(json.dumps(validation, indent=2))
