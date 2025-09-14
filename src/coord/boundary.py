from __future__ import annotations

"""Multi-section boundary coordination (simplified handshake).

Given two sections (scopeA, scopeB) that share a boundary station_id,
compute a set of holds in A to align arrival into B within B's earliest
available departure slot at that station.
"""

from dataclasses import dataclass
from typing import Dict, List, Tuple

import pandas as pd


@dataclass
class HandshakeResult:
    actions: List[dict]
    details: Dict[str, object]


def _load_art(scope: str, date: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    from pathlib import Path
    base = Path("artifacts") / scope / date
    edges = pd.read_parquet(base / "section_edges.parquet")
    nodes = pd.read_parquet(base / "section_nodes.parquet")
    bo = pd.read_parquet(base / "national_block_occupancy.parquet") if (base / "national_block_occupancy.parquet").exists() else pd.read_parquet(base / "block_occupancy.parquet")
    return edges, nodes, bo


def coordinate(scopeA: str, dateA: str, scopeB: str, dateB: str, boundary_station: str) -> HandshakeResult:
    eA, nA, boA = _load_art(scopeA, dateA)
    eB, nB, boB = _load_art(scopeB, dateB)

    sid = str(boundary_station)
    # Last block into sid in A
    arrA = boA[boA["v"].astype(str) == sid].copy()
    arrA = arrA.sort_values(["train_id", "exit_time"]).groupby("train_id").tail(1)
    # First block out of sid in B
    depB = boB[boB["u"].astype(str) == sid].copy()
    depB = depB.sort_values(["train_id", "entry_time"]).groupby("train_id").head(1)

    # Earliest available departure window at sid in B per minute
    dep_times = depB["entry_time"].sort_values()
    if dep_times.empty:
        return HandshakeResult(actions=[], details={"note": "no departures in B"})
    t_earliest = dep_times.iloc[0]

    actions: List[dict] = []
    for _, row in arrA.iterrows():
        tid = str(row["train_id"])
        arr_t = row["exit_time"]
        if arr_t <= t_earliest:
            # already aligned
            continue
        # Hold at upstream station u in A to delay arrival to boundary
        u = str(row["u"]) if "u" in row else None
        hold_min = float((arr_t - t_earliest).total_seconds() / 60.0)
        if hold_min <= 0:
            continue
        actions.append({
            "train_id": tid,
            "type": "HOLD",
            "at_station": u or sid,
            "minutes": round(hold_min, 1),
            "reason": "boundary_handshake",
            "station_id": sid,
            "why": f"Align arrival into {scopeB} boundary {sid}",
        })

    return HandshakeResult(actions=actions, details={"earliest_dep_B": str(t_earliest), "candidates": int(len(arrA))})

