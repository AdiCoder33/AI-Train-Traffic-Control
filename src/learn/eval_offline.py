from __future__ import annotations

"""Offline evaluation comparing IL vs RL and heuristics on offline dataset.

Primary quality metric: mean estimated reward using RL Q(s,a) as proxy.
Secondary: match rate to logged action and mean logged reward on matches.

Usage:
  python -m src.learn.eval_offline --base artifacts --topk 1
"""

from pathlib import Path
from typing import Dict, List, Tuple
import argparse
import json

import joblib  # type: ignore
import pandas as pd


FEATURES_DEFAULT = [
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


def _df_from_offline(entries: List[dict]) -> pd.DataFrame:
    rows = []
    for e in entries:
        st = e.get("state", {})
        a = e.get("action", {})
        r = float(e.get("reward", 0.0))
        cls = int(a.get("hold_class", 2))
        row = {k: st.get(k) for k in FEATURES_DEFAULT}
        row.update({"logged_class": cls, "logged_reward": r})
        rows.append(row)
    return pd.DataFrame(rows)


def _rl_score(df: pd.DataFrame, rl_payload: dict, action: int) -> pd.Series:
    model = rl_payload.get("model")
    feats = rl_payload.get("features") or FEATURES_DEFAULT
    actions = rl_payload.get("actions") or ACTION_CLASSES
    X = df[feats].copy()
    # Add one-hots for actions
    for a in actions:
        X[f"a_{a}"] = 1.0 if a == action else 0.0
    # Predict
    return pd.Series(model.predict(X), index=df.index)


def evaluate(base_dir: str | Path = "artifacts", topk: int = 1) -> Dict[str, object]:
    base = Path(base_dir) / "global_models"
    offline_p = base / "offline_rl.jsonl"
    entries = _load_jsonl(offline_p)
    if not entries:
        return {"status": "no_data"}
    df = _df_from_offline(entries)
    if df.empty:
        return {"status": "no_data"}

    # Load models
    il_p = base / "policy_il.joblib"
    rl_p = base / "policy_rl.joblib"
    il_payload = joblib.load(il_p) if il_p.exists() else None
    rl_payload = joblib.load(rl_p) if rl_p.exists() else None

    # RL Q estimates per action
    if rl_payload:
        q2 = _rl_score(df, rl_payload, 2)
        q3 = _rl_score(df, rl_payload, 3)
        q5 = _rl_score(df, rl_payload, 5)
        q_map = {2: q2, 3: q3, 5: q5}
    else:
        # fall back to logged reward as proxy (only for matches)
        q_map = {a: pd.Series(0.0, index=df.index) for a in ACTION_CLASSES}

    # Heuristic policies
    heur = {}
    for a in ACTION_CLASSES:
        mean_q = float(q_map[a].mean()) if rl_payload else None
        match = float((df["logged_class"] == a).mean())
        mean_logged_on_match = float(df.loc[df["logged_class"] == a, "logged_reward"].mean()) if (df["logged_class"] == a).any() else None
        heur[f"heur_{a}"] = {"mean_q": mean_q, "match_rate": match, "mean_logged_reward_on_match": mean_logged_on_match}

    # IL policy
    il = None
    if il_payload is not None:
        il_model = il_payload.get("model")
        feats = il_payload.get("features") or FEATURES_DEFAULT
        X_il = df[feats].copy()
        # predict class
        il_pred = il_model.predict(X_il)
        # evaluate
        if rl_payload:
            q_il = pd.Series([float(q_map[int(c)].iloc[i]) for i, c in enumerate(il_pred)], index=df.index)
            mean_q_il = float(q_il.mean())
        else:
            mean_q_il = None
        match_il = float((df["logged_class"].values == il_pred).mean())
        mean_logged_il = float(df.loc[df["logged_class"].values == il_pred, "logged_reward"].mean()) if match_il > 0 else None
        il = {"mean_q": mean_q_il, "match_rate": match_il, "mean_logged_reward_on_match": mean_logged_il}

    # RL greedy policy (top-1)
    rl = None
    if rl_payload is not None:
        # choose best action per row
        qdf = pd.DataFrame({2: q_map[2], 3: q_map[3], 5: q_map[5]})
        best_cls = qdf.idxmax(axis=1)
        mean_q_rl = float(qdf.max(axis=1).mean())
        match_rl = float((df["logged_class"].values == best_cls.values).mean())
        mean_logged_rl = float(df.loc[df["logged_class"].values == best_cls.values, "logged_reward"].mean()) if match_rl > 0 else None
        rl = {"mean_q": mean_q_rl, "match_rate": match_rl, "mean_logged_reward_on_match": mean_logged_rl}

    # Build leaderboard sorted by mean_q where available
    board = []
    for name, stats in heur.items():
        board.append({"policy": name, **stats})
    if il is not None:
        board.append({"policy": "IL_top1", **il})
    if rl is not None:
        board.append({"policy": "RL_top1", **rl})
    # sort by mean_q (None last)
    board_sorted = sorted(board, key=lambda x: (x.get("mean_q") is None, -(x.get("mean_q") or 0.0)))

    return {
        "status": "ok",
        "n": int(len(df)),
        "leaderboard": board_sorted,
        "models": {"il": bool(il_payload), "rl": bool(rl_payload)},
    }


def _main() -> None:  # pragma: no cover
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="artifacts")
    ap.add_argument("--topk", type=int, default=1)
    args = ap.parse_args()
    res = evaluate(args.base, topk=args.topk)
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    _main()

