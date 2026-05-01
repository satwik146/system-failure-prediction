"""
simulator/digital_twin.py — Digital twin state engine

Maintains a mirror of the real system's last known state.
The agent can query it, compare predictions against it,
and the fault injector mutates it for what-if simulations.
"""
from __future__ import annotations
import time, copy, logging
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Optional

log = logging.getLogger(__name__)

@dataclass
class TwinState:
    """Snapshot of one monitored source at a point in time."""
    source_id  : str
    timestamp  : float
    metrics    : dict[str, float]   # tag → value
    severity   : str = "info"
    is_injected: bool = False       # True when fault-injected (not real data)

    def clone(self) -> "TwinState":
        return TwinState(
            source_id   = self.source_id,
            timestamp   = self.timestamp,
            metrics     = copy.deepcopy(self.metrics),
            severity    = self.severity,
            is_injected = self.is_injected,
        )


class DigitalTwin:
    """
    Keeps the latest TwinState per source and a short history.
    Updated by live ingestion; read by the simulator and agent.
    """

    def __init__(self, history_len: int = 200):
        self._states  : dict[str, TwinState]       = {}
        self._history : dict[str, deque[TwinState]] = {}
        self._hist_len = history_len

    # ── Write (from ingestion bus) ─────────────────────────────────────────────

    def update(self, source_id: str, tag: str, value: float,
               severity: str = "info", injected: bool = False) -> None:
        if source_id not in self._states:
            self._states[source_id]  = TwinState(source_id, time.time(), {})
            self._history[source_id] = deque(maxlen=self._hist_len)

        state = self._states[source_id]
        state.metrics[tag] = value
        state.timestamp    = time.time()
        state.severity     = severity
        state.is_injected  = injected

        # snapshot to history every 10 updates per source
        self._history[source_id].append(state.clone())

    def bulk_update(self, source_id: str, metrics: dict[str, float],
                    severity: str = "info", injected: bool = False) -> None:
        for tag, val in metrics.items():
            self.update(source_id, tag, val, severity, injected)

    # ── Read ──────────────────────────────────────────────────────────────────

    def get_state(self, source_id: str) -> Optional[TwinState]:
        return self._states.get(source_id)

    def get_history(self, source_id: str, n: int = 50) -> list[TwinState]:
        h = self._history.get(source_id)
        if not h:
            return []
        items = list(h)
        return items[-n:]

    def get_metric_series(self, source_id: str, tag: str, n: int = 50) -> list[float]:
        """Return last N values for a specific metric tag."""
        return [s.metrics.get(tag, 0.0) for s in self.get_history(source_id, n)
                if tag in s.metrics]

    def all_sources(self) -> list[str]:
        return list(self._states.keys())

    def snapshot(self) -> dict:
        return {sid: {
            "metrics"    : s.metrics,
            "severity"   : s.severity,
            "timestamp"  : s.timestamp,
            "is_injected": s.is_injected,
        } for sid, s in self._states.items()}
