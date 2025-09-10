from __future__ import annotations

"""Train global IL model on all discovered artifact runs.

Saves model under artifacts/global_models/policy_il.joblib and report
JSON with dataset stats.
"""

from pathlib import Path
from typing import Dict
import json
import hashlib

import joblib  # type: ignore
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, confusion_matrix

from .corpus import build_corpus
from .state_builder import feature_label


def _sha1_df(df: pd.DataFrame) -> str:
    return hashlib.sha1(df.to_csv(index=False).encode("utf-8")).hexdigest()


def train_global(base_dir: str | Path = "artifacts") -> Dict[str, object]:
    df = build_corpus(base_dir, persist=True)
    out_dir = Path(base_dir) / "global_models"
    out_dir.mkdir(parents=True, exist_ok=True)
    if df.empty:
        rep = {"status": "no_data"}
        (out_dir / "policy_il_report.json").write_text(json.dumps(rep, indent=2))
        return rep
    X, y = feature_label(df)
    pipe = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(multi_class="multinomial", max_iter=600, class_weight="balanced", solver="lbfgs", random_state=42)),
    ])
    # Backup existing model to compare before/after
    prev_model_p = out_dir / "policy_il_prev.joblib"
    curr_model_p = out_dir / "policy_il.joblib"
    if curr_model_p.exists():
        try:
            import shutil
            shutil.copyfile(curr_model_p, prev_model_p)
        except Exception:
            pass

    pipe.fit(X, y)
    acc = float(accuracy_score(y, pipe.predict(X)))
    joblib.dump({"model": pipe, "features": list(X.columns)}, curr_model_p)
    rep = {
        "status": "ok",
        "rows": int(len(df)),
        "scopes": sorted(set(df["origin_scope"])) if "origin_scope" in df.columns else [],
        "dates": sorted(set(df["origin_date"])) if "origin_date" in df.columns else [],
        "train_acc": acc,
        "data_hash": _sha1_df(df),
        "features": list(X.columns),
    }
    (out_dir / "policy_il_report.json").write_text(json.dumps(rep, indent=2))

    # Human-in-the-loop: confusion matrix shift vs previous model if present
    try:
        if prev_model_p.exists():
            prev = joblib.load(prev_model_p)
            prev_model = prev.get("model")
            prev_feats = prev.get("features") or list(X.columns)
            X_prev = X[prev_feats].copy() if set(prev_feats).issubset(set(X.columns)) else X.copy()
            labels = sorted(pd.unique(y).tolist())
            cm_prev = confusion_matrix(y, prev_model.predict(X_prev), labels=labels).tolist()
            cm_curr = confusion_matrix(y, pipe.predict(X), labels=labels).tolist()
            delta = [[int(cm_curr[i][j] - cm_prev[i][j]) for j in range(len(cm_curr[0]))] for i in range(len(cm_curr))]
            (out_dir / "policy_il_confusion_shift.json").write_text(json.dumps({"labels": labels, "prev": cm_prev, "curr": cm_curr, "delta": delta}, indent=2))
    except Exception:
        pass
    return rep


if __name__ == "__main__":  # pragma: no cover
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="artifacts")
    args = ap.parse_args()
    res = train_global(args.base)
    print(json.dumps(res, indent=2))
