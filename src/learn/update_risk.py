from __future__ import annotations

from pathlib import Path
import json
import pandas as pd


def main(scope: str, date: str) -> None:
    base = Path("artifacts") / scope / date
    fb = base / "feedback.parquet"
    if not fb.exists():
        (base / "risk_update_report.md").write_text("No feedback available.")
        return
    df = pd.read_parquet(fb)
    by_type = {}
    for _, row in df.iterrows():
        try:
            action = json.loads(row["action"]) if isinstance(row["action"], str) else {}
        except Exception:
            action = {}
        t = action.get("type", "UNKNOWN")
        by_type.setdefault(t, {"APPLY": 0, "DISMISS": 0, "MODIFY": 0})
        dec = str(row["decision"]).upper()
        if dec in by_type[t]:
            by_type[t][dec] += 1
    (base / "risk_update_report.md").write_text(json.dumps(by_type, indent=2))


if __name__ == "__main__":  # pragma: no cover
    import sys

    main(sys.argv[1], sys.argv[2])

