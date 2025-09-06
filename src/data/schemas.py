"""Structured data contracts used across the project.

This module defines lightweight dataclasses that capture the minimal
fields required by downstream components. They intentionally avoid
pydantic or other heavy dependencies and perform only basic validation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

__all__ = ["TrainEvent", "Station", "Edge", "KPI"]


@dataclass(slots=True)
class TrainEvent:
    """Scheduled and actual timing for a train at a station.

    Attributes
    ----------
    train_id:
        Identifier of the train.
    station_id:
        Identifier of the station where the event occurs.
    sched_arr:
        Scheduled arrival timestamp.
    sched_dep:
        Scheduled departure timestamp.
    act_arr:
        Actual arrival timestamp, if known.
    act_dep:
        Actual departure timestamp, if known.
    day:
        Service day number (e.g., within dataset).
    priority:
        Operational priority; higher value means higher priority.
    """

    train_id: str
    station_id: str
    sched_arr: datetime
    sched_dep: datetime
    day: int
    priority: int
    act_arr: Optional[datetime] = None
    act_dep: Optional[datetime] = None

    def __post_init__(self) -> None:
        if self.sched_dep < self.sched_arr:
            raise ValueError("sched_dep must not precede sched_arr")
        if self.act_arr and self.act_dep and self.act_dep < self.act_arr:
            raise ValueError("act_dep must not precede act_arr")
        if self.day < 0:
            raise ValueError("day must be non-negative")
        if self.priority < 0:
            raise ValueError("priority must be non-negative")


@dataclass(slots=True)
class Station:
    """Railway station metadata."""

    station_id: str
    name: str

    def __post_init__(self) -> None:
        if not self.station_id:
            raise ValueError("station_id cannot be empty")
        if not self.name:
            raise ValueError("name cannot be empty")


@dataclass(slots=True)
class Edge:
    """Directed connection between two stations in the network graph."""

    u: str
    v: str
    min_run_time: float
    headway: float
    block_id: str
    platform_cap: int
    capacity: int = 1

    def __post_init__(self) -> None:
        if not self.u or not self.v:
            raise ValueError("edge endpoints u and v must be provided")
        if not self.block_id:
            raise ValueError("block_id cannot be empty")
        if self.min_run_time <= 0:
            raise ValueError("min_run_time must be positive")
        if self.headway < 0:
            raise ValueError("headway must be non-negative")
        if self.capacity <= 0:
            raise ValueError("capacity must be positive")
        if self.platform_cap < 0:
            raise ValueError("platform_cap must be non-negative")


@dataclass(slots=True)
class KPI:
    """Key performance indicators summarizing a simulation run."""

    throughput: int
    avg_delay: float
    p90_delay: float
    utilization: float
    total_holds: int
    overtakes: int

    def __post_init__(self) -> None:
        if self.throughput < 0:
            raise ValueError("throughput must be non-negative")
        if not 0 <= self.utilization <= 1:
            raise ValueError("utilization must be between 0 and 1")
        if self.total_holds < 0:
            raise ValueError("total_holds must be non-negative")
        if self.overtakes < 0:
            raise ValueError("overtakes must be non-negative")
