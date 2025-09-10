from __future__ import annotations

"""Cross-section generalization evaluation (train on A, test on B).

Splits the global corpus by scope and reports IL accuracy on held-out
scopes and (optionally) RL Q-mean if RL model is present.
"""

from pathlib import Path
from typing import Dict, List
import json

import joblib  # type: ignore
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score

from .corpus import build_corpus
from .state_builder import feature_label


def evaluate_generalization(base_dir: str | Path = "artifacts", train_scopes: List[str] | None = None, test_scopes: List[str] | None = None) -> Dict[str, object]:
    df = build_corpus(base_dir, persist=False)
    if df.empty or "origin_scope" not in df.columns:
        return {"status": "no_data"}
    train_scopes = train_scopes or sorted(set(df["origin_scope"]))[:1]
    test_scopes = test_scopes or [s for s in sorted(set(df["origin_scope"])) if s not in train_scopes]
    if not test_scopes:
        return {"status": "no_test_scopes"}

    df_train = df[df["origin_scope"].isin(train_scopes)].copy()
    df_test = df[df["origin_scope"].isin(test_scopes)].copy()
    if df_train.empty or df_test.empty:
        return {"status": "insufficient_data"}
    Xtr, ytr = feature_label(df_train)
    Xte, yte = feature_label(df_test)
    pipe = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(multi_class="multinomial", max_iter=600, class_weight="balanced", solver="lbfgs", random_state=42)),
    ])
    pipe.fit(Xtr, ytr)
    acc_tr = float(accuracy_score(ytr, pipe.predict(Xtr)))
    acc_te = float(accuracy_score(yte, pipe.predict(Xte)))

    # Optional: score IL predictions with RL Q(s,a) if RL policy present
    out_dir = Path(base_dir) / "global_models"
    rl_p = out_dir / "policy_rl.joblib"
    mean_q = None
    if rl_p.exists():
        rl = joblib.load(rl_p)
        feats = rl.get("features") or []
        actions = rl.get("actions") or [2, 3, 5]
        model = rl.get("model")
        Xb = df_test[feats].copy() if feats else Xte.copy()
        # estimate Q for IL-chosen actions
        preds = pipe.predict(Xte)
        qvals = []
        for i, (_, row) in enumerate(Xb.iterrows()):
            a = int(preds[i])
            vec = row.to_dict()
            for aa in actions:
                vec[f"a_{aa}"] = 1.0 if aa == a else 0.0
            q = float(model.predict(pd.DataFrame([vec]))[0])
            qvals.append(q)
        mean_q = float(pd.Series(qvals).mean()) if qvals else None

    return {"status": "ok", "train_scopes": train_scopes, "test_scopes": test_scopes, "acc_train": acc_tr, "acc_test": acc_te, "mean_q_test": mean_q}


if __name__ == "__main__":  # pragma: no cover
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="artifacts")
    ap.add_argument("--train_scopes", type=str, default="")
    ap.add_argument("--test_scopes", type=str, default="")
    args = ap.parse_args()
    tr = args.train_scopes.split(",") if args.train_scopes else None
    te = args.test_scopes.split(",") if args.test_scopes else None
    res = evaluate_generalization(args.base, train_scopes=tr, test_scopes=te)
    print(json.dumps(res, indent=2))

