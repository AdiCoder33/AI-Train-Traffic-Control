from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
import json
import pandas as pd


def append_feedback(scope: str, date: str, entry: Dict[str, Any]) -> None:
    base = Path("artifacts") / scope / date
    base.mkdir(parents=True, exist_ok=True)
    trail_path = base / "audit_trail.json"
    trail = []
    if trail_path.exists():
        trail = json.loads(trail_path.read_text())
    trail.append(entry)
    trail_path.write_text(json.dumps(trail, indent=2))

    df_new = pd.DataFrame(
        [
            {
                "decision": entry.get("decision"),
                "reason": entry.get("reason"),
                "modified": json.dumps(entry.get("modified")) if entry.get("modified") else None,
                "action": json.dumps(entry.get("action")),
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

