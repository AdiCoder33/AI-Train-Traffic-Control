from __future__ import annotations

from pathlib import Path
import json
import pandas as pd


def _read_json(p: Path):
    return json.loads(p.read_text()) if p.exists() else None


def main(scope: str, date: str) -> None:
    base = Path("artifacts") / scope / date
    kpis = base / "national_sim_kpis.json"
    risk = base / "risk_kpis.json"
    plan = base / "plan_metrics.json"
    rec_path = base / "rec_plan.json"
    trail_path = base / "audit_trail.json"
    fb_path = base / "feedback.parquet"

    out: dict = {}
    if kpis.exists():
        out["sim_kpis"] = _read_json(kpis)
    if risk.exists():
        out["risk_kpis"] = _read_json(risk)
    if plan.exists():
        out["plan_metrics"] = _read_json(plan)

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

    (base / "kpi_reports.json").write_text(json.dumps(out, indent=2))


if __name__ == "__main__":  # pragma: no cover
    import sys

    main(sys.argv[1], sys.argv[2])
