"""
mqtt_listener.py — Async MQTT ingestion

Uses asyncio-mqtt which wraps paho-mqtt in a coroutine-friendly API.
Event-driven: CPU usage is ~0% when no messages arrive.
No threads — runs entirely in the asyncio event loop.

Requires: pip install asyncio-mqtt paho-mqtt
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ingestion.event_bus import Event, EventBus, SourceType

log = logging.getLogger(__name__)


async def run_mqtt_listener(cfg: dict, bus: EventBus) -> None:
    """
    Connects to the MQTT broker and forwards messages onto the event bus.
    Reconnects automatically with exponential backoff on failure.
    """
    try:
        import asyncio_mqtt as aiomqtt
    except ImportError:
        log.error("asyncio-mqtt not installed. Run: pip install asyncio-mqtt")
        return

    host       = cfg.get("host", "localhost")
    port       = cfg.get("port", 1883)
    keepalive  = cfg.get("keepalive", 60)
    client_id  = cfg.get("client_id", "iot-predictor")
    username   = cfg.get("username")
    password   = cfg.get("password")
    topics     = cfg.get("topics", [])

    backoff = 1.0

    while True:
        try:
            log.info("MQTT connecting to %s:%d …", host, port)
            async with aiomqtt.Client(
                hostname  = host,
                port      = port,
                keepalive = keepalive,
                identifier= client_id,
                username  = username,
                password  = password,
            ) as client:
                backoff = 1.0   # reset on successful connect
                log.info("MQTT connected. Subscribing to %d topic(s).", len(topics))

                for t in topics:
                    await client.subscribe(t["topic"], qos=t.get("qos", 0))

                async for message in client.messages:
                    payload = _decode_payload(message.payload)
                    event = Event(
                        source_type = SourceType.MQTT,
                        source_id   = client_id,
                        tag         = str(message.topic),
                        value       = payload,
                    )
                    await bus.publish(event)

        except Exception as exc:
            log.warning("MQTT error: %s — retrying in %.1fs", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)   # cap at 60s


def _decode_payload(raw: bytes) -> Any:
    """Try JSON first, fallback to utf-8 string, then raw bytes repr."""
    try:
        import ujson as json
    except ImportError:
        import json  # type: ignore

    text = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except Exception:
        return text
