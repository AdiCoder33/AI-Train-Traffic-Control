from __future__ import annotations

from pathlib import Path
import json
import pandas as pd


def main(scope: str, date: str) -> None:
    base = Path("artifacts") / scope / date
    kpis = base / "national_sim_kpis.json"
    risk = base / "risk_kpis.json"
    plan = base / "plan_metrics.json"
    out = {}
    if kpis.exists():
        out["sim_kpis"] = json.loads(kpis.read_text())
    if risk.exists():
        out["risk_kpis"] = json.loads(risk.read_text())
    if plan.exists():
        out["plan_metrics"] = json.loads(plan.read_text())
    (base / "kpi_reports.json").write_text(json.dumps(out, indent=2))


if __name__ == "__main__":  # pragma: no cover
    import sys

    main(sys.argv[1], sys.argv[2])

