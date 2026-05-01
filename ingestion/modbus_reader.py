"""
modbus_reader.py — Async Modbus TCP/RTU reader

Each device is polled in its own asyncio task at its configured interval.
Sleep between polls means near-zero CPU consumption at rest.
Uses pymodbus async client — no blocking calls on the event loop.

Requires: pip install pymodbus
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ingestion.event_bus import Event, EventBus, SourceType

log = logging.getLogger(__name__)

# Modbus register read functions by type name
_READ_FN = {
    "holding" : "read_holding_registers",
    "input"   : "read_input_registers",
    "coil"    : "read_coils",
    "discrete": "read_discrete_inputs",
}


async def run_modbus_reader(cfg: dict, bus: EventBus) -> None:
    """Spawns one polling task per configured Modbus device."""
    devices = cfg.get("devices", [])
    if not devices:
        log.info("No Modbus devices configured — skipping.")
        return

    tasks = [
        asyncio.create_task(
            _poll_device(dev, bus),
            name=f"modbus-{dev['id']}",
        )
        for dev in devices
    ]
    await asyncio.gather(*tasks, return_exceptions=True)


async def _poll_device(dev_cfg: dict, bus: EventBus) -> None:
    """Poll one Modbus device. Reconnects with backoff on error."""
    try:
        from pymodbus.client import AsyncModbusTcpClient
    except ImportError:
        log.error("pymodbus not installed. Run: pip install pymodbus")
        return

    dev_id   = dev_cfg["id"]
    host     = dev_cfg["host"]
    port     = dev_cfg.get("port", 502)
    unit_id  = dev_cfg.get("unit_id", 1)
    interval = dev_cfg.get("poll_interval", 2.0)
    registers= dev_cfg.get("registers", [])

    backoff  = 1.0

    while True:
        try:
            async with AsyncModbusTcpClient(host, port=port) as client:
                log.info("Modbus connected: %s (%s:%d)", dev_id, host, port)
                backoff = 1.0

                while True:
                    for reg in registers:
                        value = await _read_register(client, reg, unit_id)
                        if value is not None:
                            await bus.publish(Event(
                                source_type = SourceType.MODBUS,
                                source_id   = dev_id,
                                tag         = reg["name"],
                                value       = value,
                            ))
                    # Sleep between polls — yields control, near-zero CPU
                    await asyncio.sleep(interval)

        except Exception as exc:
            log.warning("Modbus %s error: %s — retrying in %.1fs", dev_id, exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)


async def _read_register(client: Any, reg: dict, unit_id: int) -> Any:
    """Read a single register and return its decoded value."""
    fn_name = _READ_FN.get(reg.get("type", "holding"))
    if not fn_name:
        log.warning("Unknown register type: %s", reg.get("type"))
        return None

    fn = getattr(client, fn_name, None)
    if fn is None:
        return None

    try:
        result = await fn(reg["address"], count=1, slave=unit_id)
        if result.isError():
            log.debug("Modbus read error for %s", reg["name"])
            return None
        # Coils/discretes return bits, registers return registers list
        if hasattr(result, "registers"):
            return result.registers[0]
        if hasattr(result, "bits"):
            return int(result.bits[0])
    except Exception as exc:
        log.debug("Register read exception %s: %s", reg["name"], exc)
    return None
