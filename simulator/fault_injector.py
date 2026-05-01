"""
simulator/fault_injector.py — Injects fault conditions into the digital twin

Supports gradual drift, instant spike, oscillation, and stuck-sensor faults.
All injections are timestamped and reversible (twin tracks is_injected flag).
"""
from __future__ import annotations
import asyncio, logging, time, math
from dataclasses import dataclass
from typing import Optional
from simulator.digital_twin import DigitalTwin

log = logging.getLogger(__name__)


@dataclass
class InjectionHandle:
    """Returned by inject(); cancel() stops the injection task."""
    fault_id  : str
    source_id : str
    fault_type: str
    started_at: float
    _task     : Optional[asyncio.Task] = None

    def cancel(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            log.info("Injection cancelled: %s", self.fault_id)


class FaultInjector:
    """
    Injects synthetic faults into a DigitalTwin for simulation/testing.

    Fault types
    -----------
    spike       — single instant value spike then return to normal
    drift       — gradual linear drift toward a target value
    oscillate   — sine-wave oscillation around current value
    stuck       — freeze a metric at its current value (sensor stuck)
    flood_errors— inject repeated ERROR log severities
    """

    def __init__(self, twin: DigitalTwin):
        self._twin    = twin
        self._active  : dict[str, InjectionHandle] = {}

    # ── Public API ────────────────────────────────────────────────────────────

    def inject(self, source_id: str, tag: str, fault_type: str,
               **kwargs) -> InjectionHandle:
        """
        Start a fault injection. Returns a handle to cancel it.

        Common kwargs:
          magnitude  — how much to deviate (spike/drift/oscillate)
          duration   — seconds the fault lasts (default: 60)
          target     — absolute target value (drift)
          frequency  — oscillation Hz (oscillate, default 0.1)
        """
        fault_id = f"{source_id}:{tag}:{fault_type}:{int(time.time())}"
        handle = InjectionHandle(fault_id, source_id, fault_type, time.time())

        coro = self._dispatch(source_id, tag, fault_type, kwargs)
        handle._task = asyncio.create_task(coro, name=f"fault-{fault_id}")
        self._active[fault_id] = handle

        log.warning("Fault injected: %s | %s | %s | %s", fault_type, source_id, tag, kwargs)
        return handle

    def active_faults(self) -> list[dict]:
        return [{
            "fault_id"  : h.fault_id,
            "source_id" : h.source_id,
            "fault_type": h.fault_type,
            "started_at": h.started_at,
            "running"   : h._task and not h._task.done(),
        } for h in self._active.values()]

    def cancel_all(self) -> None:
        for h in self._active.values():
            h.cancel()
        self._active.clear()

    # ── Fault coroutines ──────────────────────────────────────────────────────

    async def _dispatch(self, source_id: str, tag: str,
                        fault_type: str, kw: dict) -> None:
        state   = self._twin.get_state(source_id)
        current = state.metrics.get(tag, 0.0) if state else 0.0
        dur     = kw.get("duration", 60)

        try:
            if fault_type == "spike":
                await self._spike(source_id, tag, current, kw, dur)
            elif fault_type == "drift":
                await self._drift(source_id, tag, current, kw, dur)
            elif fault_type == "oscillate":
                await self._oscillate(source_id, tag, current, kw, dur)
            elif fault_type == "stuck":
                await self._stuck(source_id, tag, current, dur)
            elif fault_type == "flood_errors":
                await self._flood_errors(source_id, tag, dur)
            else:
                log.warning("Unknown fault type: %s", fault_type)
        except asyncio.CancelledError:
            pass
        finally:
            # Restore original value (mark as no longer injected)
            self._twin.update(source_id, tag, current, injected=False)

    async def _spike(self, sid, tag, base, kw, dur):
        mag = kw.get("magnitude", base * 2.0)
        self._twin.update(sid, tag, base + mag, severity="critical", injected=True)
        await asyncio.sleep(kw.get("spike_duration", 5))
        self._twin.update(sid, tag, base, injected=True)
        await asyncio.sleep(max(0, dur - 5))

    async def _drift(self, sid, tag, base, kw, dur):
        target  = kw.get("target", base * 1.5)
        steps   = max(1, int(dur / 2))
        delta   = (target - base) / steps
        current = base
        for _ in range(steps):
            current += delta
            sev = "warning" if abs(current - base) > abs(target - base) * 0.5 else "info"
            self._twin.update(sid, tag, current, severity=sev, injected=True)
            await asyncio.sleep(2)

    async def _oscillate(self, sid, tag, base, kw, dur):
        mag  = kw.get("magnitude", base * 0.3)
        freq = kw.get("frequency", 0.1)   # Hz
        end  = time.time() + dur
        while time.time() < end:
            val = base + mag * math.sin(2 * math.pi * freq * time.time())
            self._twin.update(sid, tag, val, severity="warning", injected=True)
            await asyncio.sleep(0.5)

    async def _stuck(self, sid, tag, val, dur):
        end = time.time() + dur
        while time.time() < end:
            self._twin.update(sid, tag, val, severity="warning", injected=True)
            await asyncio.sleep(1)

    async def _flood_errors(self, sid, tag, dur):
        end = time.time() + dur
        while time.time() < end:
            self._twin.update(sid, tag, -9999.0, severity="error", injected=True)
            await asyncio.sleep(0.5)
