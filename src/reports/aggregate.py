from __future__ import annotations

from pathlib import Path
import json
import pandas as pd
import numpy as np


def _read_json(p: Path):
    return json.loads(p.read_text()) if p.exists() else None


def main(scope: str, date: str) -> None:
    base = Path("artifacts") / scope / date
    kpis = base / "national_sim_kpis.json"
    risk = base / "risk_kpis.json"
    plan = base / "plan_metrics.json"
    plan_apply = base / "plan_apply_report.json"
    risk_val = base / "risk_validation.json"
    rec_path = base / "rec_plan.json"
    trail_path = base / "audit_trail.json"
    fb_path = base / "feedback.parquet"
    block_p = base / "national_block_occupancy.parquet"
    plat_p = base / "national_platform_occupancy.parquet"
    waits_p = base / "national_waiting_ledger.parquet"
    if not waits_p.exists():
        waits_p = base / "waiting_ledger.parquet"
    events_p = base / "events_clean.parquet"

    out: dict = {}
    if kpis.exists():
        out["sim_kpis"] = _read_json(kpis)
    if risk.exists():
        out["risk_kpis"] = _read_json(risk)
    if plan.exists():
        out["plan_metrics"] = _read_json(plan)
    if plan_apply.exists():
        out["plan_apply_report"] = _read_json(plan_apply)
    if risk_val.exists():
        out["risk_validation"] = _read_json(risk_val)

    # Feedback completeness
    rec = _read_json(rec_path) or []
    trail = _read_json(trail_path) or []
    acted = len([e for e in trail if str(e.get("decision")).upper() in ("APPLY", "DISMISS", "MODIFY", "ACK")])
    total = len(rec)
    out["feedback_completeness"] = {
        "recommendations": total,
        "decisions_logged": acted,
        "completeness_pct": (acted / total * 100.0) if total else 0.0,
    }

    # Override insights (counts by action type and decision)
    override = {}
    if fb_path.exists():
        df = pd.read_parquet(fb_path)
        def _type_of(row):
            try:
                a = json.loads(row["action"]) if isinstance(row["action"], str) else {}
                return a.get("type", "UNKNOWN")
            except Exception:
                return "UNKNOWN"
        if not df.empty:
            df = df.copy()
            df["action_type"] = df.apply(_type_of, axis=1)
            grp = df.groupby(["action_type", "decision"]).size().reset_index(name="count")
            for _, r in grp.iterrows():
                t = str(r["action_type"])  # type: ignore
                d = str(r["decision"])  # type: ignore
                override.setdefault(t, {})[d] = int(r["count"])  # type: ignore
    out["override_insights"] = override

    # Primary KPIs
    prim: dict = {}
    # Throughput: blocks cleared/hour and trains/hour
    try:
        if block_p.exists():
            bo = pd.read_parquet(block_p)
            if not bo.empty:
                bo = bo.copy()
                bo["exit_time"] = pd.to_datetime(bo["exit_time"], utc=True)
                per_h = bo.set_index("exit_time").resample("1H").size()
                prim["blocks_cleared_per_hour_mean"] = float(per_h.mean())
                prim["blocks_cleared_per_hour_peak"] = float(per_h.max())
        if plat_p.exists():
            po = pd.read_parquet(plat_p)
            if not po.empty:
                po = po.copy()
                po["dep_platform"] = pd.to_datetime(po["dep_platform"], utc=True)
                last = po.sort_values(["train_id", "dep_platform"]).groupby("train_id").tail(1)
                per_h_t = last.set_index("dep_platform").resample("1H").size()
                prim["trains_per_hour_mean"] = float(per_h_t.mean())
                prim["trains_per_hour_peak"] = float(per_h_t.max())
    except Exception:
        pass
    # OTP and Avg delay from sim_kpis
    try:
        sk = out.get("sim_kpis", {}) or {}
        prim["otp_exit_pct"] = float(sk.get("otp_exit_pct", sk.get("otp", 0.0)))
        prim["avg_exit_delay_min"] = float(sk.get("avg_exit_delay_min", sk.get("avg_delay_min", 0.0)))
    except Exception:
        pass
    # Conflicts resolved (if apply report exists)
    try:
        if "plan_apply_report" in out:
            pr = out["plan_apply_report"] or {}
            prim["conflicts_resolved_total"] = int(pr.get("risk_reduction", 0))
            prim["conflicts_resolved_headway_block"] = int(pr.get("risk_reduction_headway_block", 0))
    except Exception:
        pass
    # Action rate from feedback
    try:
        rec = _read_json(rec_path) or []
        total_rec = len(rec)
        accepted = 0
        if fb_path.exists():
            df = pd.read_parquet(fb_path)
            if not df.empty and "decision" in df.columns:
                accepted = int((df["decision"].str.upper() == "APPLY").sum())
        prim["action_rate_apply_pct"] = float((accepted / total_rec * 100.0) if total_rec else 0.0)
        prim["actions_total"] = total_rec
        prim["actions_accepted"] = accepted
    except Exception:
        pass
    out["primary_kpis"] = prim

    # Safety KPIs
    safety: dict = {}
    try:
        rv = out.get("risk_validation", {}) or {}
        safety["ok_post_no_overlap"] = bool(rv.get("ok_post_no_overlap", True))
        safety["ok_headway_enforced"] = bool(rv.get("ok_headway_enforced", True))
        safety["post_overlap_violations"] = int(rv.get("post_overlap_violations", 0))
        safety["headway_violations"] = int(rv.get("headway_violations", 0))
    except Exception:
        pass
    # Fairness deltas by train class
    def _train_class_map(df_events: pd.DataFrame) -> dict[str, str]:
        name_col = None
        for c in ("train_name","Train Name","name"):
            if c in df_events.columns:
                name_col = c
                break
        mapping: dict[str, str] = {}
        if name_col:
            sub = df_events.dropna(subset=["train_id"]).drop_duplicates(subset=["train_id"]) [["train_id", name_col]]
            for _, r in sub.iterrows():
                nm = str(r[name_col]).upper()
                cls = "Passenger"
                if "SUPERFAST" in nm:
                    cls = "Superfast"
                elif "EXPRESS" in nm:
                    cls = "Express"
                elif "EMU" in nm or "LOCAL" in nm:
                    cls = "EMU"
                elif "GOODS" in nm or "FREIGHT" in nm:
                    cls = "Freight"
                mapping[str(r["train_id"])]= cls
        return mapping
    try:
        if waits_p.exists() and events_p.exists():
            wl = pd.read_parquet(waits_p)
            ev = pd.read_parquet(events_p)
            if not wl.empty and not ev.empty and "train_id" in wl.columns:
                cls_map = _train_class_map(ev)
                wl["cls"] = wl["train_id"].astype(str).map(lambda x: cls_map.get(x, "Passenger"))
                by = wl.groupby("cls")["minutes"].mean().to_dict()
                # simple fairness KPI: ratio of mean hold in class vs overall mean
                overall = float(pd.to_numeric(wl["minutes"], errors="coerce").mean())
                fairness = {k: (float(v) / overall if overall else 0.0) for k, v in by.items()}
                safety["fairness_hold_ratio_by_class"] = fairness
                safety["mean_hold_min_by_class"] = {k: float(v) for k, v in by.items()}
    except Exception:
        pass
    out["safety_kpis"] = safety

    # Operational KPIs
    ops: dict = {}
    try:
        # Latency from optimizer audit
        aud = _read_json(base / "audit_log.json") or {}
        if isinstance(aud, dict) and "runtime_sec" in aud:
            ops["opt_runtime_sec"] = float(aud.get("runtime_sec", 0.0))
        # Controller workload from feedback per hour
        if fb_path.exists():
            df = pd.read_parquet(fb_path)
            if not df.empty and "ts" in df.columns:
                df = df.copy()
                ts = pd.to_datetime(df["ts"], errors="coerce")
                per_h = ts.dt.floor("H").value_counts().sort_index()
                ops["decisions_per_hour"] = {str(k): int(v) for k, v in per_h.to_dict().items()}
                decs = df["decision"].str.upper()
                tot = int(len(decs))
                ops["dismiss_apply_ratio"] = float((decs.eq("DISMISS").sum() / decs.eq("APPLY").sum()) if decs.eq("APPLY").sum() else 0.0)
                ops["decisions_total"] = tot
    except Exception:
        pass
    out["ops_kpis"] = ops

    (base / "kpi_reports.json").write_text(json.dumps(out, indent=2, default=lambda o: float(o) if isinstance(o, (np.floating,)) else o))


if __name__ == "__main__":  # pragma: no cover
    import sys

    main(sys.argv[1], sys.argv[2])
