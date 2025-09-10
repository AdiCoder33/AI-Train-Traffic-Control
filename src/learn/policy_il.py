from __future__ import annotations

"""Train a simple IL classifier for micro-hold minutes {2,3,5}.

Uses features built by ``src.learn.state_builder`` and fits a logistic
regression (multinomial) wrapped in a pipeline with scaling. Saves model
and metadata under artifacts/<scope>/<date>/.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict
import json
import hashlib

import joblib  # type: ignore
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score

from .state_builder import build_examples, feature_label


def _base(scope: str, date: str) -> Path:
    return Path("artifacts") / scope / date


def _sha1_df(df: pd.DataFrame) -> str:
    # Stable hash on CSV bytes
    csv = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha1(csv).hexdigest()


def train(scope: str, date: str) -> Dict[str, object]:
    base = _base(scope, date)
    df = build_examples(scope, date, persist=True)
    if df.empty:
        meta = {"status": "no_data"}
        (base / "policy_il_report.json").write_text(json.dumps(meta, indent=2))
        return meta

    X, y = feature_label(df)
    pipe = Pipeline([
        ("scaler", StandardScaler()),
        (
            "clf",
            LogisticRegression(
                multi_class="multinomial",
                max_iter=500,
                class_weight="balanced",
                solver="lbfgs",
                random_state=42,
            ),
        ),
    ])
    pipe.fit(X, y)
    pred = pipe.predict(X)
    acc = float(accuracy_score(y, pred))

    # Persist
    model_p = base / "policy_il.joblib"
    joblib.dump({"model": pipe, "features": list(X.columns)}, model_p)
    meta = {
        "status": "ok",
        "train_rows": int(len(df)),
        "train_acc": acc,
        "features": list(X.columns),
        "data_hash": _sha1_df(df),
    }
    (base / "policy_il_report.json").write_text(json.dumps(meta, indent=2))
    return meta


if __name__ == "__main__":  # pragma: no cover
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--scope", default="all_india")
    ap.add_argument("--date", default="2024-01-01")
    args = ap.parse_args()
    res = train(args.scope, args.date)
    print(json.dumps(res, indent=2))

