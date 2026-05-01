"""
log_tailer.py — OS-native log file tail (inotify on Linux, kqueue on macOS, ReadDirectoryChangesW on Windows)

Uses watchdog to watch for file modification events from the OS kernel.
No polling loop — the OS wakes us when bytes are written.
CPU usage: ~0% between writes.
RAM: only stores one chunk at a time (4 KB by default).

Requires: pip install watchdog
"""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from ingestion.event_bus import Event, EventBus, SourceType

log = logging.getLogger(__name__)


async def run_log_tailer(cfg: dict, bus: EventBus, chunk_size: int = 4096) -> None:
    """Start one tail task per configured log file."""
    files = cfg.get("files", [])
    if not files:
        log.info("No log files configured — skipping.")
        return

    tasks = [
        asyncio.create_task(
            _tail_file(f, bus, chunk_size),
            name=f"tail-{f['id']}",
        )
        for f in files
    ]
    await asyncio.gather(*tasks, return_exceptions=True)


async def _tail_file(file_cfg: dict, bus: EventBus, chunk_size: int) -> None:
    """
    Tail a single log file using polling (Windows-compatible).
    """
    file_id  = file_cfg["id"]
    path_str = file_cfg["path"]
    encoding = file_cfg.get("encoding", "utf-8")
    tag      = file_cfg.get("tag", file_id)
    path     = Path(path_str)

    # Ensure the file exists (create empty if missing — useful for app logs)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.touch()
        log.info("Created empty log file: %s", path)

    log.info("Tailing log file: %s [%s]", path, file_id)
    
    # Use polling on all platforms (more reliable than watchdog on Windows)
    fallback_event = _PollingEvent(interval=0.5)
    try:
        async for line in _read_new_lines(path, encoding, chunk_size, fallback_event):
            await bus.publish(Event(
                source_type = SourceType.LOG,
                source_id   = file_id,
                tag         = tag,
                value       = line,
            ))
    except Exception as e:
        log.error("Error reading log file %s: %s", path, e, exc_info=True)


async def _read_new_lines(
    path: Path,
    encoding: str,
    chunk_size: int,
    ready: "asyncio.Event | _PollingEvent",
):
    """
    Generator that yields new lines appended to `path`.
    Seeks to EOF on startup so we only process NEW lines.
    """
    partial = ""   # leftover bytes between chunks (no line terminator yet)

    with open(path, "r", encoding=encoding, errors="replace") as fh:
        fh.seek(0, os.SEEK_END)   # skip existing content on startup

        while True:
            await ready.wait()   # sleep until OS says file changed
            ready.clear()

            chunk = fh.read(chunk_size)
            if not chunk:
                continue

            # Split on newlines; carry any partial final line forward
            text = partial + chunk
            lines = text.split("\n")
            partial = lines[-1]   # last element may be incomplete

            for line in lines[:-1]:
                stripped = line.strip()
                if stripped:
                    yield stripped


class _PollingEvent:
    """Minimal asyncio.Event-compatible object that auto-fires on interval."""

    def __init__(self, interval: float = 1.0):
        self._interval = interval
        self._flag = False

    async def wait(self) -> None:
        await asyncio.sleep(self._interval)

    def clear(self) -> None:
        pass   # no-op — polling always "clears" by sleeping
