"""
storage/timeseries_db.py — Async SQLite time-series store

Key optimisations for low CPU/RAM:
- WAL mode: writes don't block reads; no full-file locks
- Batch inserts: events are buffered and flushed in one transaction
- PRAGMA synchronous=NORMAL: safe but faster than FULL
- No ORM overhead — raw SQL with aiosqlite
- Auto-pruning: old rows are deleted to cap disk + RAM usage

Requires: pip install aiosqlite
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT    NOT NULL,
    source_id   TEXT    NOT NULL,
    tag         TEXT    NOT NULL,
    value       TEXT,
    timestamp   REAL    NOT NULL,
    severity    TEXT    NOT NULL DEFAULT 'info'
);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
CREATE INDEX IF NOT EXISTS idx_events_source    ON events(source_id, tag);
"""

_INSERT = """
INSERT INTO events (source_type, source_id, tag, value, timestamp, severity)
VALUES (?, ?, ?, ?, ?, ?)
"""


class TimeSeriesDB:
    """
    Batched async SQLite writer.

    Events are buffered in-memory and flushed to disk every
    `flush_interval` seconds OR when `batch_max` events accumulate,
    whichever comes first. This dramatically reduces write syscalls.
    """

    def __init__(
        self,
        db_path: str = "./data/events.db",
        flush_interval: float = 5.0,
        batch_max: int = 64,
        retention_days: int = 30,
    ):
        self._path          = Path(db_path)
        self._flush_interval= flush_interval
        self._batch_max     = batch_max
        self._retention_days= retention_days
        self._buffer: list[tuple] = []
        self._db = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """Open DB, apply schema, start flush loop."""
        import aiosqlite

        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(str(self._path))

        # Performance pragmas
        await self._db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA cache_size=-2000;
            PRAGMA temp_store=MEMORY;
        """)
        await self._db.executescript(_CREATE_TABLE)
        await self._db.commit()
        log.info("TimeSeriesDB ready: %s", self._path)

        # Background flush task
        asyncio.create_task(self._flush_loop(), name="db-flush")

    async def write(self, event_dict: dict) -> None:
        """Buffer one event. Thread-safe via asyncio.Lock."""
        row = (
            event_dict.get("source_type"),
            event_dict.get("source_id"),
            event_dict.get("tag"),
            str(event_dict.get("value", "")),
            event_dict.get("timestamp", time.time()),
            event_dict.get("severity", "info"),
        )
        async with self._lock:
            self._buffer.append(row)
            should_flush = len(self._buffer) >= self._batch_max

        if should_flush:
            await self._flush()

    async def _flush(self) -> None:
        """Write buffered rows in one transaction."""
        async with self._lock:
            if not self._buffer:
                return
            batch, self._buffer = self._buffer, []

        try:
            await self._db.executemany(_INSERT, batch)
            await self._db.commit()
            log.debug("DB flushed %d events.", len(batch))
        except Exception as exc:
            log.error("DB flush failed: %s", exc)
            # Put rows back so they're not lost
            async with self._lock:
                self._buffer = batch + self._buffer

    async def _flush_loop(self) -> None:
        """Periodic flush — ensures events land on disk even if buffer stays small."""
        while True:
            await asyncio.sleep(self._flush_interval)
            await self._flush()
            await self._prune_old_rows()

    async def _prune_old_rows(self) -> None:
        """Delete rows older than retention_days to cap disk usage."""
        cutoff = time.time() - self._retention_days * 86400
        try:
            await self._db.execute("DELETE FROM events WHERE timestamp < ?", (cutoff,))
            await self._db.commit()
        except Exception as exc:
            log.debug("Prune error: %s", exc)

    async def query_recent(self, source_id: Optional[str] = None, limit: int = 100) -> list[dict]:
        """Fetch most recent events, optionally filtered by source."""
        if source_id:
            cur = await self._db.execute(
                "SELECT * FROM events WHERE source_id=? ORDER BY timestamp DESC LIMIT ?",
                (source_id, limit),
            )
        else:
            cur = await self._db.execute(
                "SELECT * FROM events ORDER BY timestamp DESC LIMIT ?", (limit,)
            )
        rows = await cur.fetchall()
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in rows]

    async def close(self) -> None:
        await self._flush()
        if self._db:
            await self._db.close()
