"""
event_bus.py — Lightweight async event bus

Uses asyncio.Queue (bounded) so memory usage is capped.
No threads, no external broker needed for internal routing.
All producers await put(); consumers await get() — zero CPU when idle.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

log = logging.getLogger(__name__)


class SourceType(str, Enum):
    MQTT    = "mqtt"
    MODBUS  = "modbus"
    SERIAL  = "serial"
    LOG     = "log"
    SYSTEM  = "system"


@dataclass(slots=True)
class Event:
    """
    Minimal event envelope.
    slots=True cuts per-instance memory ~30% vs regular dataclass.
    """
    source_type : SourceType
    source_id   : str           # device/file identifier from config
    tag         : str           # logical label (e.g. "motor_rpm", "syslog")
    value       : Any           # raw payload — string, number, or dict
    timestamp   : float = field(default_factory=time.time)
    severity    : str   = "info"   # info | warning | error | critical

    def as_dict(self) -> dict:
        return {
            "source_type" : self.source_type.value,
            "source_id"   : self.source_id,
            "tag"         : self.tag,
            "value"       : self.value,
            "timestamp"   : self.timestamp,
            "severity"    : self.severity,
        }


class EventBus:
    """
    Single bounded asyncio queue shared across the whole app.
    Bounded size prevents unbounded RAM growth if consumers fall behind.
    When full, oldest event is dropped and a warning is logged.
    """

    def __init__(self, maxsize: int = 512):
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=maxsize)
        self._dropped = 0
        self._total   = 0

    async def publish(self, event: Event) -> None:
        """Non-blocking publish — drops oldest if queue is full."""
        if self._queue.full():
            try:
                self._queue.get_nowait()   # discard oldest
                self._dropped += 1
                if self._dropped % 100 == 1:
                    log.warning(
                        "Event queue full — dropped %d events total. "
                        "Consider increasing event_queue_maxsize.",
                        self._dropped,
                    )
            except asyncio.QueueEmpty:
                pass
        await self._queue.put(event)
        self._total += 1

    async def consume(self) -> Event:
        """Blocks (zero CPU) until an event is available."""
        return await self._queue.get()

    def task_done(self) -> None:
        self._queue.task_done()

    @property
    def qsize(self) -> int:
        return self._queue.qsize()

    @property
    def stats(self) -> dict:
        return {"queued": self.qsize, "total": self._total, "dropped": self._dropped}


# ── Module-level singleton ────────────────────────────────────────────────────
_bus: Optional[EventBus] = None


def init_bus(maxsize: int = 512) -> EventBus:
    global _bus
    _bus = EventBus(maxsize=maxsize)
    return _bus


def get_bus() -> EventBus:
    if _bus is None:
        raise RuntimeError("EventBus not initialised. Call init_bus() first.")
    return _bus
