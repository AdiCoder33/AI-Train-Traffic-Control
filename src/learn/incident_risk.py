from __future__ import annotations

"""Train a coarse incident likelihood classifier per block (risk heatmap).

Label a block as risky if any block-or-headway wait is observed; features
are basic block attributes (headway, capacity) and historical wait totals.
Saves:
  - incident_risk.joblib (model, features)
  - incident_heat.json (mapping block_id -> probability)
"""

from pathlib import Path
from typing import Dict

import json
import joblib  # type: ignore
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier


def _base(scope: str, date: str) -> Path:
    return Path("artifacts") / scope / date


def train_incident_risk(scope: str, date: str) -> Dict[str, float]:
    base = _base(scope, date)
    edges_p = base / "section_edges.parquet"
    block_p = base / "national_block_occupancy.parquet"
    wait_p = base / "national_waiting_ledger.parquet"
    if not edges_p.exists() or not block_p.exists():
        return {"status": "missing_artifacts"}
    edges = pd.read_parquet(edges_p)
    bo = pd.read_parquet(block_p)
    waits = pd.read_parquet(wait_p) if wait_p.exists() else pd.DataFrame(columns=["id","minutes","reason"])
    # Aggregate wait minutes by block
    w = waits.copy()
    if not w.empty:
        w = w[w["resource"].astype(str) == "block"]
    agg = w.groupby("id")["minutes"].sum().reset_index().rename(columns={"id": "block_id", "minutes": "wait_min_total"}) if not w.empty else pd.DataFrame(columns=["block_id","wait_min_total"])
    df = edges[["block_id","headway","capacity"]].copy()
    df = df.merge(agg, on="block_id", how="left").fillna({"wait_min_total": 0.0})
    df["label_risky"] = (df["wait_min_total"] > 0.1).astype(int)
    if df.empty:
        return {"status": "no_data"}
    X = df[["headway","capacity","wait_min_total"]].astype(float)
    y = df["label_risky"].astype(int)
    # If all labels same, fallback to constant probability
    if y.nunique() <= 1:
        probs = {str(b): float(y.iloc[0]) for b in df["block_id"].astype(str)}
        (base / "incident_heat.json").write_text(json.dumps(probs, indent=2))
        return {"status": "ok", "note": "constant_labels"}
    model = GradientBoostingClassifier(random_state=42)
    model.fit(X, y)
    payload = {"model": model, "features": ["headway","capacity","wait_min_total"]}
    joblib.dump(payload, base / "incident_risk.joblib")
    p = model.predict_proba(X)[:, 1]
    probs = {str(b): float(pp) for b, pp in zip(df["block_id"].astype(str), p)}
    (base / "incident_heat.json").write_text(json.dumps(probs, indent=2))
    return {"status": "ok", "blocks": int(len(probs))}

