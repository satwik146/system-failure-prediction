"""
simulator/__init__.py — Simulator facade

Single import point: from simulator import Simulator
"""
from __future__ import annotations
import asyncio, logging
from typing import Optional
from simulator.digital_twin   import DigitalTwin
from simulator.fault_injector import FaultInjector
from simulator.scenario_runner import ScenarioRunner

log = logging.getLogger(__name__)


class Simulator:
    """
    Convenience wrapper that owns twin + injector + runner.
    Pass prediction_queue from AgentCore to measure detection latency.
    """

    def __init__(self, prediction_queue: Optional[asyncio.Queue] = None):
        self.twin     = DigitalTwin()
        self.injector = FaultInjector(self.twin)
        self.runner   = ScenarioRunner(self.twin, self.injector)
        self._pq      = prediction_queue

    # ── Ingestion bridge ──────────────────────────────────────────────────────

    def feed(self, source_id: str, tag: str, value: float,
             severity: str = "info") -> None:
        """Call this from the ingestion consumer to keep twin current."""
        self.twin.update(source_id, tag, value, severity)

    # ── Quick inject helpers ──────────────────────────────────────────────────

    def spike(self, source_id: str, tag: str, magnitude: float, duration: int = 30):
        return self.injector.inject(source_id, tag, "spike",
                                    magnitude=magnitude, duration=duration)

    def drift(self, source_id: str, tag: str, target: float, duration: int = 60):
        return self.injector.inject(source_id, tag, "drift",
                                    target=target, duration=duration)

    def stuck(self, source_id: str, tag: str, duration: int = 60):
        return self.injector.inject(source_id, tag, "stuck", duration=duration)

    async def run_scenario(self, name: str):
        return await self.runner.run(name, self._pq)

    # ── Status ────────────────────────────────────────────────────────────────

    def status(self) -> dict:
        return {
            "twin_snapshot" : self.twin.snapshot(),
            "active_faults" : self.injector.active_faults(),
            "scenario_results": self.runner.results,
        }
