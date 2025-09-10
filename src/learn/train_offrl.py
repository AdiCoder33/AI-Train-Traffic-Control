from __future__ import annotations

"""Train a contextual bandit offline RL model from offline_rl.jsonl.

Approach: fit a regressor Q(s, a) that predicts reward for discrete
actions (hold_class in {2,3,5}). Build the design matrix by concatenating
state features with one-hot action indicators. This is a conservative
approximation suitable when only immediate rewards are logged.

Saves model to artifacts/global_models/policy_rl.joblib.
"""

from pathlib import Path
from typing import Dict, List, Tuple
import json

import joblib  # type: ignore
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split


FEATURES = [
    "severity_rank",
    "lead_min",
    "headway_min",
    "capacity",
    "block_len_trains",
    "platforms",
]
ACTION_CLASSES = [2, 3, 5]


def _load_jsonl(p: Path) -> List[dict]:
    if not p.exists():
        return []
    out: List[dict] = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                out.append(json.loads(s))
            except Exception:
                continue
    return out


def _design(entries: List[dict]) -> Tuple[pd.DataFrame, pd.Series]:
    rows: List[dict] = []
    for e in entries:
        st = e.get("state", {})
        act = e.get("action", {})
        rew = float(e.get("reward", 0.0))
        hold_c = int(act.get("hold_class", 2))
        row = {k: float(st.get(k, 0.0)) for k in FEATURES}
        for a in ACTION_CLASSES:
            row[f"a_{a}"] = 1.0 if hold_c == a else 0.0
        row["reward"] = rew
        rows.append(row)
    if not rows:
        return pd.DataFrame(columns=FEATURES + [f"a_{a}" for a in ACTION_CLASSES]), pd.Series(dtype=float)
    df = pd.DataFrame(rows)
    X = df[FEATURES + [f"a_{a}" for a in ACTION_CLASSES]].copy()
    y = df["reward"].astype(float)
    return X, y


def train_offrl(base_dir: str | Path = "artifacts") -> Dict[str, object]:
    out_dir = Path(base_dir) / "global_models"
    out_dir.mkdir(parents=True, exist_ok=True)
    data_p = out_dir / "offline_rl.jsonl"
    entries = _load_jsonl(data_p)
    if not entries:
        rep = {"status": "no_data"}
        (out_dir / "policy_rl_report.json").write_text(json.dumps(rep, indent=2))
        return rep
    X, y = _design(entries)
    if X.empty:
        rep = {"status": "no_data"}
        (out_dir / "policy_rl_report.json").write_text(json.dumps(rep, indent=2))
        return rep
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42)
    model = GradientBoostingRegressor(random_state=42)
    model.fit(X_tr, y_tr)
    pred = model.predict(X_te) if len(X_te) else np.array([])
    mae = float(mean_absolute_error(y_te, pred)) if len(pred) else None
    joblib.dump({"model": model, "features": FEATURES, "actions": ACTION_CLASSES}, out_dir / "policy_rl.joblib")
    rep = {"status": "ok", "rows": int(len(X)), "mae": mae, "features": FEATURES, "actions": ACTION_CLASSES}
    (out_dir / "policy_rl_report.json").write_text(json.dumps(rep, indent=2))
    return rep


if __name__ == "__main__":  # pragma: no cover
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="artifacts")
    args = ap.parse_args()
    print(json.dumps(train_offrl(args.base), indent=2))

