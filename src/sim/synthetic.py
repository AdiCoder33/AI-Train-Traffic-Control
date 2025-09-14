"""Synthetic corridor generator for quick demos (2–3 stations, 6–10 trains).

Produces a plausible timetable with scheduled arrivals/departures and
optional jitter for actual times. Saves normalized events and graph
artifacts under artifacts/<scope>/<date>/.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from src.data.graph import build as build_graph, save as save_graph
from src.data.baseline import save as save_baseline


@dataclass
class CorridorSpec:
    scope: str = "demo_section"
    date: str = "2024-01-01"
    stations: Tuple[str, ...] = ("STN-A", "STN-B", "STN-C")
    trains: int = 8  # 6–10
    dwell_min: float = 2.0
    headway_min: float = 5.0


def _mk_events(spec: CorridorSpec) -> pd.DataFrame:
    tz = "UTC"
    base = pd.Timestamp(f"{spec.date} 08:00:00", tz=tz)
    rows: List[dict] = []
    # Simple alternating directions
    for i in range(spec.trains):
        tid = f"T{i+1:05d}"
        forward = (i % 2 == 0)
        stops = list(spec.stations) if forward else list(reversed(spec.stations))
        t = base + pd.Timedelta(minutes=3 * i)
        for j, sid in enumerate(stops):
            arr = t
            dep = arr + pd.Timedelta(minutes=spec.dwell_min)
            # Simple run time between stations ~ 6–10 minutes
            run = 8 if j < len(stops) - 1 else 0
            rows.append(
                {
                    "train_id": tid,
                    "station_id": sid,
                    "service_date": pd.to_datetime(spec.date).date(),
                    "sched_arr": arr,
                    "sched_dep": dep,
                    "act_arr": pd.NaT,
                    "act_dep": pd.NaT,
                    "stop_seq": j + 1,
                    "priority": 0,
                }
            )
            t = dep + pd.Timedelta(minutes=run)
    df = pd.DataFrame(rows)
    # Ensure tz-aware
    for c in ("sched_arr", "sched_dep", "act_arr", "act_dep"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True)
    return df


def generate(spec: CorridorSpec) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return (events_df, edges_df, nodes_df) for the synthetic corridor."""
    events = _mk_events(spec)
    stations_dict = {sid: i for i, sid in enumerate(spec.stations)}
    edges, nodes = build_graph(events, stations_dict)
    # Override headway for demo stability
    if not edges.empty:
        edges["headway"] = spec.headway_min
        edges["capacity"] = 1
    if not nodes.empty:
        nodes["platforms"] = 1
        nodes["min_dwell_min"] = spec.dwell_min
    return events, edges, nodes


def save_artifacts(events: pd.DataFrame, edges: pd.DataFrame, nodes: pd.DataFrame, scope: str, date: str) -> Path:
    out_dir = Path("artifacts") / scope / pd.to_datetime(date).date().isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    # Persist core
    events.to_parquet(out_dir / "events_clean.parquet", index=False)
    edges.to_parquet(out_dir / "section_edges.parquet", index=False)
    nodes.to_parquet(out_dir / "section_nodes.parquet", index=False)
    # Baseline replay + KPIs
    save_baseline(events, edges, scope, date)
    return out_dir


def build_and_save(spec: CorridorSpec) -> Path:
    ev, e, n = generate(spec)
    return save_artifacts(ev, e, n, spec.scope, spec.date)

