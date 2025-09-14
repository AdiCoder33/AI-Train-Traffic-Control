from __future__ import annotations

"""Scenario runner and Pareto selection utilities.

Supports templates:
- late_start: {train_id, station_id, delay_min}
- platform_outage: {station_id, platforms}
- speed_restriction: {u, v, speed_factor}
- single_line_working: {capacity: 1}
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd

from src.model.section_graph import load_graph
from src.sim.national_replay import run as replay_run
from src.sim.risk import analyze as risk_analyze
from src.opt.engine import propose


@dataclass
class ScenarioSpec:
    kind: str
    params: Dict[str, object]
    name: str = ""


def apply_template(events: pd.DataFrame, nodes: pd.DataFrame, edges: pd.DataFrame, spec: ScenarioSpec) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_e = events.copy()
    df_n = nodes.copy()
    df_ed = edges.copy()
    k = spec.kind
    p = spec.params or {}
    if k == "late_start":
        tid = str(p.get("train_id"))
        sid = str(p.get("station_id"))
        d = float(p.get("delay_min", 5.0))
        m = (df_e["train_id"].astype(str) == tid) & (df_e["station_id"].astype(str) == sid)
        if m.any():
            for c in ("sched_dep", "act_dep"):
                if c in df_e.columns:
                    df_e.loc[m, c] = pd.to_datetime(df_e.loc[m, c], utc=True, errors="coerce") + pd.to_timedelta(d, unit="m")
    elif k == "platform_outage":
        sid = str(p.get("station_id"))
        plats = int(p.get("platforms", 1))
        if "station_id" in df_n.columns and "platforms" in df_n.columns:
            df_n.loc[df_n["station_id"].astype(str) == sid, "platforms"] = max(1, plats)
    elif k == "speed_restriction":
        u = str(p.get("u"))
        v = str(p.get("v"))
        fac = float(p.get("speed_factor", 1.2))
        fac = max(1.0, fac)
        m = (df_ed["u"].astype(str) == u) & (df_ed["v"].astype(str) == v)
        if m.any():
            df_ed.loc[m, "min_run_time"] = df_ed.loc[m, "min_run_time"].astype(float) * fac
    elif k == "single_line_working":
        # Reduce all capacities to 1 (already default) to mimic temporary restriction
        if "capacity" in df_ed.columns:
            df_ed["capacity"] = 1
    return df_e, df_n, df_ed


def run_one(scope: str, date: str, spec: ScenarioSpec, *, horizon_min: int = 60) -> Dict[str, object]:
    from pathlib import Path
    base = Path("artifacts") / scope / date
    events = pd.read_parquet(base / "events_clean.parquet")
    nodes = pd.read_parquet(base / "section_nodes.parquet")
    edges = pd.read_parquet(base / "section_edges.parquet")

    ev2, n2, e2 = apply_template(events, nodes, edges, spec)
    graph = load_graph(n2, e2)
    sim = replay_run(ev2, graph)
    risks, _, _, kpis = risk_analyze(e2, n2, sim.block_occupancy, platform_occ_df=sim.platform_occupancy, waiting_df=sim.waiting_ledger, t0=None, horizon_min=horizon_min)
    rec, alts, metrics, audit = propose(e2, n2, sim.block_occupancy, risks, horizon_min=horizon_min)
    return {
        "name": spec.name or spec.kind,
        "kpis": kpis,
        "plan_metrics": metrics,
        "rec_count": int(len(rec)),
    }


def pareto_front(results: List[Dict[str, object]]) -> List[int]:
    # Indices of non-dominated results by (avg_exit_delay_min ASC, trains_served DESC)
    pts = []
    for i, r in enumerate(results):
        k = r.get("kpis", {}) or {}
        delay = float(k.get("avg_exit_delay_min", 0.0))
        served = float(k.get("trains_served", 0.0))
        pts.append((i, delay, -served))
    front: List[int] = []
    for i, di, si in pts:
        dominated = False
        for j, dj, sj in pts:
            if j == i:
                continue
            if (dj <= di and sj <= si) and (dj < di or sj < si):
                dominated = True
                break
        if not dominated:
            front.append(i)
    return front


def run_batch(scope: str, date: str, specs: List[ScenarioSpec], *, horizon_min: int = 60) -> Dict[str, object]:
    res = [run_one(scope, date, s, horizon_min=horizon_min) for s in specs]
    front = pareto_front(res)
    return {"results": res, "pareto_indices": front}

