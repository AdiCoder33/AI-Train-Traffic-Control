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

__all__ = ["analyze", "save"]


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

    # Mitigation preview: simple hold suggestions
    previews: List[dict] = []
    for i, r in enumerate(risks):
        suggestion = None
        if r["type"] == "headway":
            need = float(r.get("required_hold_min", 0.0))
            hold2 = need <= 2.0
            hold5 = need <= 5.0
            suggestion = {
                "risk_index": i,
                "type": r["type"],
                "block_id": r["block_id"],
                "train_ids": r["train_ids"],
                "hold_2min_resolves": bool(hold2),
                "hold_5min_resolves": bool(hold5),
                "required_hold_min": need,
            }
        elif r["type"] == "platform_overflow":
            need = float(r.get("required_hold_min", 0.0))
            suggestion = {
                "risk_index": i,
                "type": r["type"],
                "station_id": r.get("station_id"),
                "train_ids": r["train_ids"],
                "hold_2min_resolves": bool(need <= 2.0),
                "hold_5min_resolves": bool(need <= 5.0),
                "required_hold_min": need,
            }
        elif r["type"] == "block_capacity":
            # approximate: spread by 2–5 min could ease overlap
            suggestion = {
                "risk_index": i,
                "type": r["type"],
                "block_id": r.get("block_id"),
                "train_ids": r.get("train_ids", []),
                "hold_2min_resolves": True,
                "hold_5min_resolves": True,
                "required_hold_min": 2.0,
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


def save(
    risks: List[dict],
    timeline: pd.DataFrame,
    previews: List[dict],
    kpis: Dict[str, float],
    out_dir: str | "PathLike[str]",
) -> None:
    from pathlib import Path
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "conflict_radar.json").write_text(json.dumps(risks, indent=2))
    timeline.to_parquet(out / "risk_timeline.parquet", index=False)
    (out / "mitigation_preview.json").write_text(json.dumps(previews, indent=2))
    (out / "risk_kpis.json").write_text(json.dumps(kpis, indent=2))

