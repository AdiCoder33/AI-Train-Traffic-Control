from __future__ import annotations

"""Simple policy inference benchmark.

Measures latency to produce suggestions for a given scope/date and
optional station/train filters.
"""

from time import perf_counter
from typing import Optional

import pandas as pd

from src.policy.infer import suggest


def bench(scope: str, date: str, *, station_id: Optional[str] = None, train_id: Optional[str] = None, rounds: int = 10) -> dict:
    times = []
    for _ in range(rounds):
        t0 = perf_counter()
        res = suggest(scope, date, role="SC" if station_id else "AN", station_id=station_id, train_id=train_id)
        dt = perf_counter() - t0
        times.append(dt)
    s = pd.Series(times)
    return {"rounds": rounds, "mean_sec": float(s.mean()), "p95_sec": float(s.quantile(0.95)), "min_sec": float(s.min()), "max_sec": float(s.max())}


if __name__ == "__main__":
    import argparse, json
    ap = argparse.ArgumentParser()
    ap.add_argument("--scope", default="all_india")
    ap.add_argument("--date", default="2024-01-01")
    ap.add_argument("--station", default=None)
    ap.add_argument("--train", default=None)
    ap.add_argument("--rounds", type=int, default=10)
    args = ap.parse_args()
    res = bench(args.scope, args.date, station_id=args.station, train_id=args.train, rounds=args.rounds)
    print(json.dumps(res, indent=2))

