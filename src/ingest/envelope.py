from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass(frozen=True)
class EventEnvelope:
    """Normalized event envelope for ingestion adapters.

    Fields:
    - source: adapter/source id
    - event_key: stable key to dedupe (e.g., train_id+station_id+event_type)
    - ts: ISO-8601 UTC string or epoch seconds
    - train_id: str
    - station_id: Optional[str]
    - block_id: Optional[str]
    - event_type: str ('arr', 'dep', 'eta', 'hold', 'policy', ...)
    - fields: arbitrary map (arr/dep/eta timestamps, cause, quality_score)
    """

    source: str
    event_key: str
    ts: str
    train_id: str
    event_type: str
    station_id: Optional[str] = None
    block_id: Optional[str] = None
    fields: Dict[str, object] = field(default_factory=dict)

