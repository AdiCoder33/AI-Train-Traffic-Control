from __future__ import annotations

import json
import time
from dataclasses import asdict
from pathlib import Path
from typing import Callable, Deque, Dict, Iterable, Optional
from collections import deque

from .envelope import EventEnvelope


class CircuitBreaker:
    def __init__(self, max_fail: int = 3, reset_sec: int = 60) -> None:
        self.max_fail = max_fail
        self.reset_sec = reset_sec
        self.fail_count = 0
        self.open_until: float = 0.0

    def record_success(self) -> None:
        self.fail_count = 0
        self.open_until = 0.0

    def record_failure(self) -> None:
        self.fail_count += 1
        if self.fail_count >= self.max_fail:
            self.open_until = time.time() + self.reset_sec

    def allow(self) -> bool:
        return time.time() >= self.open_until


class Deduper:
    def __init__(self, maxlen: int = 10000) -> None:
        self.keys: Deque[str] = deque(maxlen=maxlen)
        self.set: Dict[str, None] = {}

    def seen(self, key: str) -> bool:
        return key in self.set

    def add(self, key: str) -> None:
        if key in self.set:
            return
        self.keys.append(key)
        self.set[key] = None
        if len(self.keys) == self.keys.maxlen:
            # prune oldest half
            for _ in range(len(self.keys) // 2):
                k = self.keys.popleft()
                self.set.pop(k, None)


class BaseAdapter:
    def __init__(self, source: str, emit: Callable[[EventEnvelope], None]) -> None:
        self.source = source
        self.emit = emit
        self.breaker = CircuitBreaker()
        self.deduper = Deduper()
        self.watermark_ts: Optional[float] = None

    def _should_process(self, env: EventEnvelope) -> bool:
        if self.deduper.seen(env.event_key):
            return False
        self.deduper.add(env.event_key)
        return True

    def tick(self) -> None:
        raise NotImplementedError


class FileDropAdapter(BaseAdapter):
    """Watches a folder for JSON lines of EventEnvelope and emits them.

    Format: one JSON per line matching EventEnvelope.asdict().
    """

    def __init__(self, path: str | Path, emit: Callable[[EventEnvelope], None]) -> None:
        super().__init__(source="file_drop", emit=emit)
        self.path = Path(path)
        self.offset = 0

    def tick(self) -> None:
        try:
            if not self.breaker.allow():
                return
            if not self.path.exists():
                return
            data = self.path.read_text(encoding="utf-8", errors="ignore")
            lines = data.splitlines()
            for i in range(self.offset, len(lines)):
                line = lines[i].strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    env = EventEnvelope(**obj)
                    if self._should_process(env):
                        self.emit(env)
                except Exception:
                    continue
            self.offset = len(lines)
            self.breaker.record_success()
        except Exception:
            self.breaker.record_failure()


class PollingRunningStatusAdapter(BaseAdapter):
    """Placeholder polling adapter: replace `fetch` with real API calls.
    Implements exponential backoff via sleep between ticks (runtime controls cadence).
    """

    def __init__(self, emit: Callable[[EventEnvelope], None]) -> None:
        super().__init__(source="polling_running_status", emit=emit)

    def tick(self) -> None:
        if not self.breaker.allow():
            return
        try:
            # Placeholder: no-op
            self.breaker.record_success()
        except Exception:
            self.breaker.record_failure()

