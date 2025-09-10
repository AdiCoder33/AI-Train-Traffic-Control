from __future__ import annotations

"""Global IL policy training using a small PyTorch MLP.

Predicts HOLD minutes class in {2,3,5} from features built by
state_builder across all artifact runs. Saves model weights and
normalization stats to artifacts/global_models/policy_torch.pt.
"""

from pathlib import Path
from typing import Dict, List
import json
import math
import random

import pandas as pd

try:  # optional dependency
    import torch
    from torch import nn
    from torch.utils.data import DataLoader, TensorDataset
except Exception as e:  # pragma: no cover - provide clear error later
    torch = None  # type: ignore
    nn = None  # type: ignore
    DataLoader = None  # type: ignore
    TensorDataset = None  # type: ignore

from .corpus import build_corpus
from .state_builder import feature_label


FEATURES_DEFAULT = [
    "severity_rank",
    "lead_min",
    "headway_min",
    "capacity",
    "block_len_trains",
    "platforms",
]
CLASSES = [2, 3, 5]


def _seed_all(seed: int) -> None:
    random.seed(seed)
    try:
        import numpy as np

        np.random.seed(seed)
    except Exception:
        pass
    if torch is not None:
        torch.manual_seed(seed)


class MLP(nn.Module):  # type: ignore[misc]
    def __init__(self, in_dim: int, hidden: List[int], out_dim: int) -> None:
        super().__init__()
        layers: List[nn.Module] = []
        prev = in_dim
        for h in hidden:
            layers.append(nn.Linear(prev, h))
            layers.append(nn.ReLU())
            layers.append(nn.Dropout(0.1))
            prev = h
        layers.append(nn.Linear(prev, out_dim))
        self.net = nn.Sequential(*layers)

    def forward(self, x):  # type: ignore[override]
        return self.net(x)


def train_torch(
    base_dir: str | Path = "artifacts",
    *,
    hidden: List[int] | None = None,
    epochs: int = 60,
    lr: float = 1e-3,
    weight_decay: float = 1e-4,
    batch_size: int = 256,
    seed: int = 42,
) -> Dict[str, object]:
    if torch is None:
        raise RuntimeError("torch is not installed. Please install PyTorch to use policy_torch.")

    _seed_all(seed)
    out_dir = Path(base_dir) / "global_models"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = build_corpus(base_dir, persist=False)
    if df.empty:
        rep = {"status": "no_data"}
        (out_dir / "policy_torch_report.json").write_text(json.dumps(rep, indent=2))
        return rep

    X, y = feature_label(df)
    feats = [c for c in FEATURES_DEFAULT if c in X.columns]
    X = X[feats].astype(float).copy()
    y = y.astype(int)

    # Train/valid split
    from sklearn.model_selection import train_test_split  # lightweight

    X_tr, X_va, y_tr, y_va = train_test_split(X, y, test_size=0.2, random_state=seed, stratify=y)

    # Standardize
    mean = X_tr.mean(axis=0)
    std = X_tr.std(axis=0).replace(0.0, 1.0)
    X_trn = (X_tr - mean) / std
    X_val = (X_va - mean) / std

    # Map labels to indices 0..K-1 in CLASSES order
    cls_to_idx = {c: i for i, c in enumerate(CLASSES)}
    y_tr_idx = y_tr.map(cls_to_idx).astype(int)
    y_va_idx = y_va.map(cls_to_idx).astype(int)

    # Tensors
    Xtr = torch.tensor(X_trn.values, dtype=torch.float32)
    ytr = torch.tensor(y_tr_idx.values, dtype=torch.long)
    Xva = torch.tensor(X_val.values, dtype=torch.float32)
    yva = torch.tensor(y_va_idx.values, dtype=torch.long)

    train_loader = DataLoader(TensorDataset(Xtr, ytr), batch_size=batch_size, shuffle=True)

    # Model
    hidden = hidden or [64, 64]
    model = MLP(in_dim=Xtr.shape[1], hidden=hidden, out_dim=len(CLASSES))
    opt = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=weight_decay)
    crit = nn.CrossEntropyLoss()

    best_acc = -1.0
    best_state = None

    for epoch in range(1, epochs + 1):
        model.train()
        total_loss = 0.0
        for xb, yb in train_loader:
            opt.zero_grad(set_to_none=True)
            logits = model(xb)
            loss = crit(logits, yb)
            loss.backward()
            opt.step()
            total_loss += float(loss.item()) * len(xb)
        # Validate
        model.eval()
        with torch.no_grad():
            logits = model(Xva)
            pred = torch.argmax(logits, dim=1)
            acc = float((pred == yva).float().mean().item())
        if acc > best_acc:
            best_acc = acc
            best_state = {k: v.cpu() for k, v in model.state_dict().items()}

    # Persist best
    payload = {
        "state_dict": best_state if best_state is not None else model.state_dict(),
        "features": feats,
        "classes": CLASSES,
        "mean": mean.to_dict(),
        "std": std.to_dict(),
        "hidden": hidden,
    }
    torch.save(payload, out_dir / "policy_torch.pt")
    rep = {"status": "ok", "rows": int(len(X)), "val_acc": round(best_acc, 4), "features": feats, "classes": CLASSES}
    (out_dir / "policy_torch_report.json").write_text(json.dumps(rep, indent=2))
    return rep


if __name__ == "__main__":  # pragma: no cover
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="artifacts")
    ap.add_argument("--epochs", type=int, default=60)
    ap.add_argument("--lr", type=float, default=1e-3)
    ap.add_argument("--hidden", type=str, default="64,64")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    hidden = [int(x) for x in (args.hidden.split(",") if args.hidden else []) if x]
    res = train_torch(args.base, epochs=args.epochs, lr=args.lr, hidden=hidden, seed=args.seed)
    print(json.dumps(res, indent=2))

