"""
ingestion_router.py — Ingestion orchestrator

Reads config, starts enabled sources as asyncio Tasks.
All sources share one EventBus instance.
Tasks are supervised — if one crashes, it's restarted with backoff
without taking down the others.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Callable, Coroutine, Any

from ingestion.event_bus import EventBus, init_bus, get_bus
from ingestion.mqtt_listener import run_mqtt_listener
from ingestion.modbus_reader  import run_modbus_reader
from ingestion.serial_reader  import run_serial_reader
from ingestion.log_tailer     import run_log_tailer

log = logging.getLogger(__name__)

# Type alias for a source coroutine factory
SourceFn = Callable[..., Coroutine[Any, Any, None]]


async def start_ingestion(app_cfg: dict) -> EventBus:
    """
    Boot all enabled ingestion sources.
    Returns the shared EventBus so consumers can subscribe.
    """
    perf_cfg = app_cfg.get("performance", {})
    bus = init_bus(maxsize=perf_cfg.get("event_queue_maxsize", 512))

    sources: list[tuple[str, SourceFn, dict]] = [
        ("mqtt",    run_mqtt_listener, app_cfg.get("mqtt",        {})),
        ("modbus",  run_modbus_reader, app_cfg.get("modbus",      {})),
        ("serial",  run_serial_reader, app_cfg.get("serial",      {})),
        ("log",     run_log_tailer,    app_cfg.get("log_tailing", {})),
    ]

    enabled = []
    for name, fn, cfg in sources:
        if cfg.get("enabled", False):
            enabled.append((name, fn, cfg))
            log.info("Ingestion source enabled: %s", name)
        else:
            log.debug("Ingestion source disabled: %s", name)

    if not enabled:
        log.warning(
            "No ingestion sources are enabled. "
            "Set enabled: true under mqtt/modbus/serial/log_tailing in config.yaml"
        )

    # Wrap each source in a supervised task that restarts on crash
    for name, fn, cfg in enabled:
        asyncio.create_task(
            _supervised(name, fn, cfg, bus),
            name=f"ingest-{name}",
        )

    return bus


async def _supervised(
    name: str,
    fn: SourceFn,
    cfg: dict,
    bus: EventBus,
    max_backoff: float = 60.0,
) -> None:
    """
    Run `fn(cfg, bus)` and restart it if it raises an unhandled exception.
    Uses exponential backoff to avoid hammering a broken resource.
    """
    backoff = 1.0
    while True:
        try:
            await fn(cfg, bus)
            # If fn returns normally (shouldn't happen for long-running sources):
            log.warning("Source '%s' exited cleanly — restarting in %.1fs", name, backoff)
        except asyncio.CancelledError:
            log.info("Source '%s' cancelled.", name)
            return
        except Exception as exc:
            log.error("Source '%s' crashed: %s — restarting in %.1fs", name, exc, backoff)

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, max_backoff)
