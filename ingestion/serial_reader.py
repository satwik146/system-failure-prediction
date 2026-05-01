"""
serial_reader.py — Async serial port reader (RS-232 / RS-485 / UART)

Uses pyserial-asyncio: the OS delivers bytes via interrupt — zero CPU polling.
Lines are buffered and emitted one-at-a-time to the event bus.
Reconnects with backoff if device is unplugged.

Requires: pip install pyserial pyserial-asyncio
"""

from __future__ import annotations

import asyncio
import logging

from ingestion.event_bus import Event, EventBus, SourceType

log = logging.getLogger(__name__)


async def run_serial_reader(cfg: dict, bus: EventBus) -> None:
    """Spawns one reader task per configured serial device."""
    devices = cfg.get("devices", [])
    if not devices:
        log.info("No serial devices configured — skipping.")
        return

    tasks = [
        asyncio.create_task(
            _read_serial(dev, bus),
            name=f"serial-{dev['id']}",
        )
        for dev in devices
    ]
    await asyncio.gather(*tasks, return_exceptions=True)


async def _read_serial(dev_cfg: dict, bus: EventBus) -> None:
    """Read lines from one serial port; reconnect on error."""
    try:
        import serial_asyncio
    except ImportError:
        log.error("pyserial-asyncio not installed. Run: pip install pyserial-asyncio")
        return

    dev_id     = dev_cfg["id"]
    port       = dev_cfg["port"]
    baudrate   = dev_cfg.get("baudrate", 9600)
    bytesize   = dev_cfg.get("bytesize", 8)
    parity     = dev_cfg.get("parity", "N")
    stopbits   = dev_cfg.get("stopbits", 1)
    terminator = dev_cfg.get("line_terminator", "\n").encode()
    backoff    = 1.0

    while True:
        try:
            reader, _ = await serial_asyncio.open_serial_connection(
                url      = port,
                baudrate = baudrate,
                bytesize = bytesize,
                parity   = parity,
                stopbits = stopbits,
            )
            log.info("Serial connected: %s (%s @ %d baud)", dev_id, port, baudrate)
            backoff = 1.0

            while True:
                # readuntil blocks (no CPU) until terminator byte arrives
                line_bytes = await reader.readuntil(terminator)
                line = line_bytes.decode("utf-8", errors="replace").strip()
                if line:
                    await bus.publish(Event(
                        source_type = SourceType.SERIAL,
                        source_id   = dev_id,
                        tag         = "line",
                        value       = line,
                    ))

        except Exception as exc:
            log.warning("Serial %s error: %s — retrying in %.1fs", dev_id, exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
