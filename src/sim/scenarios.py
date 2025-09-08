"""Scenario application helpers.

Define and apply what-if scenarios on top of the baseline replay:
- Delay injection for a train at a station
- Block outage intervals
- Platform capacity outages
- Weather slowdowns by region/timeband (not applied here; hook provided)

These helpers prepare adjustment tables the simulator can consult.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Tuple

import pandas as pd

__all__ = ["Scenario", "Delays", "BlockOutages", "PlatformOutages"]


@dataclass
class Delays:
    # keys: (train_id, station_id) -> minutes to add
    by_stop: Dict[Tuple[str, str], float]


@dataclass
class BlockOutages:
    # per block_id list of (start, end) in UTC
    windows: Dict[str, List[Tuple[pd.Timestamp, pd.Timestamp]]]


@dataclass
class PlatformOutages:
    # station_id -> (start, end, reduced_capacity)
    windows: Dict[str, List[Tuple[pd.Timestamp, pd.Timestamp, int]]]


@dataclass
class Scenario:
    delays: Delays | None = None
    block_outages: BlockOutages | None = None
    platform_outages: PlatformOutages | None = None
    weather_factor: float = 0.0  # global slowdown factor placeholder

