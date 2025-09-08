"""Nationwide section graph utilities.

This module loads and validates station (nodes) and block (edges)
tables for nationwide simulation. It provides a light wrapper with
lookups used by the simulator, without taking a hard dependency on
networkx.

Expected columns
----------------
Nodes (stations):
  - station_id:str (unique)
  - platforms:int (default 1 if missing)
  - min_dwell_min:float (default 2.0 if missing)
  - zone:str (optional)

Edges (blocks):
  - u:str, v:str
  - block_id:str (unique nationwide)
  - min_run_time:float (minutes)
  - headway:float (minutes)
  - capacity:int (>=1, default 1)

"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

import pandas as pd

__all__ = ["SectionGraph", "load_graph"]


@dataclass
class SectionGraph:
    nodes: pd.DataFrame
    edges: pd.DataFrame
    # Lookups
    block_attr: Dict[str, Tuple[float, float, int]]  # block_id -> (min_run, headway, capacity)
    pair_to_block: Dict[Tuple[str, str], str]  # (u,v) -> block_id
    station_attr: Dict[str, Tuple[int, float]]  # station_id -> (platforms, min_dwell)


def _ensure_nodes(nodes_df: pd.DataFrame) -> pd.DataFrame:
    df = nodes_df.copy()
    if "station_id" not in df.columns:
        raise KeyError("nodes_df must contain 'station_id'")
    if "platforms" not in df.columns:
        df["platforms"] = 1
    if "min_dwell_min" not in df.columns:
        df["min_dwell_min"] = 2.0
    df["platforms"] = pd.to_numeric(df["platforms"], errors="coerce").fillna(1).astype(int)
    df["min_dwell_min"] = pd.to_numeric(df["min_dwell_min"], errors="coerce").fillna(2.0).astype(float)
    return df


def _ensure_edges(edges_df: pd.DataFrame) -> pd.DataFrame:
    df = edges_df.copy()
    required = {"u", "v", "block_id"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        raise KeyError(f"edges_df missing columns: {missing}")
    if "min_run_time" not in df.columns:
        df["min_run_time"] = 0.0
    if "headway" not in df.columns:
        df["headway"] = 0.0
    if "capacity" not in df.columns:
        df["capacity"] = 1
    df["min_run_time"] = pd.to_numeric(df["min_run_time"], errors="coerce").fillna(0.0)
    df["headway"] = pd.to_numeric(df["headway"], errors="coerce").fillna(0.0)
    df["capacity"] = pd.to_numeric(df["capacity"], errors="coerce").fillna(1).astype(int)
    return df


def load_graph(nodes_df: pd.DataFrame, edges_df: pd.DataFrame) -> SectionGraph:
    nodes = _ensure_nodes(nodes_df)
    edges = _ensure_edges(edges_df)

    block_attr = {
        row["block_id"]: (float(row["min_run_time"]), float(row["headway"]), int(row["capacity"]))
        for _, row in edges.iterrows()
    }
    pair_to_block = {(row["u"], row["v"]): row["block_id"] for _, row in edges.iterrows()}
    station_attr = {
        row["station_id"]: (int(row["platforms"]), float(row["min_dwell_min"]))
        for _, row in nodes.iterrows()
    }

    return SectionGraph(nodes=nodes, edges=edges, block_attr=block_attr, pair_to_block=pair_to_block, station_attr=station_attr)

