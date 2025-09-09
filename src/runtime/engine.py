from __future__ import annotations

import time
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Callable

import pandas as pd

from src.ingest.envelope import EventEnvelope
from src.ingest.adapters import FileDropAdapter, PollingRunningStatusAdapter
from src.sim.risk import analyze as risk_analyze
from src.opt.engine import propose as opt_propose


@dataclass
class EngineConfig:
    scope: str = "all_india"
    date: str = "2024-01-01"
    cadence_sec: int = 120  # 2 minutes
    sandbox: bool = True
    horizon_min: int = 60
    max_hold_min: int = 3
    max_holds_per_train: int = 2


@dataclass
class EngineState:
    plan_version: str = ""
    last_plan: List[dict] = field(default_factory=list)
    last_risks: List[dict] = field(default_factory=list)
    twin_snapshot: List[dict] = field(default_factory=list)
    last_runtime: Dict[str, float] = field(default_factory=dict)


class RuntimeEngine:
    def __init__(self, cfg: EngineConfig) -> None:
        self.cfg = cfg
        self.state = EngineState()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.adapters: List[Callable[[], None]] = []
        # Build adapters (file_drop under artifacts/<scope>/<date>/events_live.jsonl)
        from pathlib import Path

        live_path = Path("artifacts") / cfg.scope / cfg.date / "events_live.jsonl"
        self.adapters = [FileDropAdapter(live_path, self._on_event).tick, PollingRunningStatusAdapter(self._on_event).tick]

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="RuntimeEngine", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def _on_event(self, env: EventEnvelope) -> None:
        # Placeholder: In a real twin, incorporate env into state and recompute snapshot incrementally
        pass

    def snapshot(self) -> List[dict]:
        return list(self.state.twin_snapshot)

    def apply_action(self, action_id: str, modifiers: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        # Gate live emission
        if self.cfg.sandbox:
            return {"status": "sandbox", "applied": False}
        # Placeholder: In real system, push to instruction channel
        return {"status": "ok", "applied": True}

    def _run(self) -> None:
        while not self._stop.is_set():
            t0 = time.time()
            try:
                for tick in self.adapters:
                    tick()
                self._recompute()
            except Exception:
                pass
            dt = time.time() - t0
            sleep_for = max(1.0, float(self.cfg.cadence_sec) - dt)
            self._stop.wait(timeout=sleep_for)

    def _recompute(self) -> None:
        # Load latest occupancy artifacts as current twin snapshot
        from pathlib import Path
        base = Path("artifacts") / self.cfg.scope / self.cfg.date
        plat = base / "national_platform_occupancy.parquet"
        if not plat.exists():
            plat = base / "platform_occupancy.parquet"
        block = base / "national_block_occupancy.parquet"
        if not block.exists():
            block = base / "block_occupancy.parquet"
        edges_p = base / "section_edges.parquet"
        nodes_p = base / "section_nodes.parquet"
        if not (edges_p.exists() and nodes_p.exists() and block.exists()):
            return
        edges = pd.read_parquet(edges_p)
        nodes = pd.read_parquet(nodes_p)
        bo = pd.read_parquet(block)
        plat_df = pd.read_parquet(plat) if plat.exists() else None

        # Snapshot (compact): last known presence per train
        try:
            if not bo.empty:
                last = bo.sort_values("exit_time").groupby("train_id").tail(1)
                snap = [
                    {
                        "train_id": str(r.train_id),
                        "block_id": str(r.block_id),
                        "u": str(r.u),
                        "v": str(r.v),
                        "progress_pct": 100.0,
                    }
                    for _, r in last.iterrows()
                ]
                self.state.twin_snapshot = snap
        except Exception:
            self.state.twin_snapshot = []

        # Risks
        try:
            risks, _, _, _ = risk_analyze(edges, nodes, bo, platform_occ_df=plat_df, t0=None, horizon_min=self.cfg.horizon_min)
            self.state.last_risks = risks
        except Exception:
            self.state.last_risks = []

        # Optimization (heuristic) with basic hysteresis
        try:
            rec, alts, metrics, audit = opt_propose(
                edges, nodes, bo, self.state.last_risks, horizon_min=self.cfg.horizon_min, max_hold_min=self.cfg.max_hold_min, max_holds_per_train=self.cfg.max_holds_per_train
            )
            # Hysteresis: prefer keeping prior holds if still present
            if self.state.last_plan:
                prev_ids = {json.dumps(x, sort_keys=True) for x in self.state.last_plan}
                rec_sorted = sorted(rec, key=lambda x: (0 if json.dumps(x, sort_keys=True) in prev_ids else 1))
                rec = rec_sorted
            self.state.last_plan = rec
        except Exception:
            self.state.last_plan = []

