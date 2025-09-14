"""Genetic algorithm fallback for micro hold decisions.

This is a lightweight GA that assigns a small hold (0/2/3/5 min)
to each risk item within horizon to minimize a proxy objective:
  score = conflicts_remaining + 0.02 * total_hold_minutes

Inputs mirror those passed to the heuristic `propose` so outputs are
compatible with the UI and apply-and-validate path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple
import math

import pandas as pd


Action = Dict[str, object]


def _to_utc(s: pd.Series | None) -> pd.Series:
    if s is None:
        return pd.Series(dtype="datetime64[ns, UTC]")
    return pd.to_datetime(s, utc=True, errors="coerce")


def _headway_ok(entry: pd.Timestamp, prev_exit: pd.Timestamp, headway_min: float) -> bool:
    return entry >= prev_exit + pd.Timedelta(minutes=float(headway_min))


@dataclass
class GAConfig:
    pop_size: int = 40
    iters: int = 40
    elite_frac: float = 0.2
    mut_rate: float = 0.15
    choices: Tuple[float, ...] = (0.0, 2.0, 3.0, 5.0)


def _risk_key(r: dict) -> tuple:
    sev_rank = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}.get(r.get("severity"), 9)
    lead = float(r.get("lead_min", 1e9))
    return (sev_rank, lead)


def _score(chrom: Sequence[int], risks: List[dict], edges: pd.DataFrame, by_block: Dict[str, pd.DataFrame]) -> float:
    # Estimate conflicts remaining: for each risk, if we choose a hold on follower, test headway feasibility
    penalties = 0.0
    total_hold = 0.0
    for gidx, risk in enumerate(risks):
        choice = chrom[gidx]
        mins = (0.0, 2.0, 3.0, 5.0)[choice]
        total_hold += mins
        rtype = risk.get("type")
        if rtype in ("headway", "block_capacity"):
            block_id = str(risk.get("block_id")) if risk.get("block_id") is not None else None
            trains = [str(t) for t in (risk.get("train_ids") or [])]
            if not block_id or len(trains) == 0:
                penalties += 1.0
                continue
            follower = trains[-1]
            ts = pd.to_datetime(risk.get("time_window")[0], utc=True, errors="coerce") if risk.get("time_window") else None
            g = by_block.get(block_id)
            if g is None or ts is None or g.empty:
                penalties += 1.0
            else:
                idx = g.index[(g["train_id"].astype(str) == follower) & (g["entry_time"] >= ts)]
                if len(idx) == 0:
                    penalties += 1.0
                else:
                    i = idx[0]
                    row = g.loc[i]
                    prevs = g[g["entry_time"] < row["entry_time"]]
                    if prevs.empty:
                        penalties += 0.5  # uncertain
                    else:
                        prev_exit = prevs["exit_time"].max()
                        headway_min = float(edges.loc[block_id, "headway"]) if block_id in edges.index and "headway" in edges.columns else 0.0
                        entry_new = row["entry_time"] + pd.Timedelta(minutes=mins)
                        if not _headway_ok(entry_new, prev_exit, headway_min):
                            penalties += 1.0
        elif rtype == "platform_overflow":
            # Holding at station reduces overlap likelihood; treat any positive hold as resolving
            if mins <= 0.0:
                penalties += 1.0
        else:
            penalties += 0.0
    return penalties + 0.02 * total_hold


def _tournament(pop: List[Tuple[List[int], float]] , k: int = 3) -> List[int]:
    import random
    cand = random.sample(pop, k=min(k, len(pop)))
    cand.sort(key=lambda t: t[1])
    return cand[0][0][:]


def propose_ga(
    edges_df: pd.DataFrame,
    nodes_df: pd.DataFrame,
    block_occ_df: pd.DataFrame,
    risks: List[dict],
    *,
    cfg: Optional[GAConfig] = None,
    max_hold_min: int = 5,
) -> Tuple[List[Action], Dict[str, float]]:
    if cfg is None:
        cfg = GAConfig()
    if not risks or block_occ_df.empty:
        return [], {"actions": 0.0, "score": 0.0}
    edges = edges_df.set_index("block_id") if not edges_df.empty else pd.DataFrame()
    bo = block_occ_df.copy()
    bo["entry_time"] = _to_utc(bo.get("entry_time"))
    bo["exit_time"] = _to_utc(bo.get("exit_time"))
    by_block = {bid: g.sort_values("entry_time").copy() for bid, g in bo.groupby("block_id")}

    # Focus on top-N risks
    R = sorted(risks, key=_risk_key)[: min(20, len(risks))]
    import random
    # Population of chromosomes (len R), each gene ∈ {0,1,2,3}
    pop: List[List[int]] = [[random.randint(0, 3) for _ in range(len(R))] for _ in range(cfg.pop_size)]
    scored: List[Tuple[List[int], float]] = []
    for chrom in pop:
        s = _score(chrom, R, edges, by_block)
        scored.append((chrom, s))
    elite_k = max(1, int(cfg.elite_frac * cfg.pop_size))

    for _ in range(cfg.iters):
        scored.sort(key=lambda t: t[1])
        next_pop: List[List[int]] = [c for c, _ in scored[:elite_k]]
        # Reproduce
        while len(next_pop) < cfg.pop_size:
            p1 = _tournament(scored, 3)
            p2 = _tournament(scored, 3)
            cx = random.randint(1, len(R) - 1) if len(R) > 1 else 0
            child = p1[:cx] + p2[cx:]
            # Mutate
            for i in range(len(child)):
                if random.random() < cfg.mut_rate:
                    child[i] = random.randint(0, 3)
            next_pop.append(child)
        scored = [(c, _score(c, R, edges, by_block)) for c in next_pop]

    best = min(scored, key=lambda t: t[1])[0]
    # Convert to actions
    actions: List[Action] = []
    for gene, risk in zip(best, R):
        hold = (0.0, 2.0, 3.0, float(max_hold_min))[gene]
        if hold <= 0.0:
            continue
        rtype = risk.get("type")
        trains = [str(t) for t in (risk.get("train_ids") or [])]
        follower = trains[-1] if trains else None
        u = str(risk.get("u")) if rtype in ("headway", "block_capacity") else risk.get("station_id")
        actions.append(
            {
                "train_id": follower or trains[0] if trains else None,
                "type": "HOLD",
                "at_station": u,
                "minutes": round(hold, 1),
                "reason": rtype,
                "block_id": risk.get("block_id"),
                "why": f"GA resolve {rtype} via short hold",
            }
        )
    return actions, {"actions": float(len(actions)), "score": float(_score(best, R, edges, by_block))}

