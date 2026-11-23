#!/usr/bin/env python3
"""Always-on AIS collector (spec Stage 1).

* One websocket to aisstream.io with permessage-deflate; the AOI subscription is
  sent immediately after connecting (aisstream drops sockets that have not
  subscribed within 3 seconds).
* Messages are buffered in RAM and flushed as durable micro-batches to a
  per-hour SQLite (WAL) staging file every MICRO_FLUSH_SEC (300 s) or
  MICRO_FLUSH_ROWS (10,000) rows, whichever comes first, and on every
  disconnect / hour change / shutdown:

      data/staging/YYYY-MM-DD/HH.sqlite   (tables: messages, collector_health)

* Watchdog: if recv() yields nothing for WS_RECV_TIMEOUT_SEC (60 s) the socket
  is dropped and re-opened with exponential backoff + jitter (2 s .. 120 s).
* Heartbeat: one collector_health row per minute (connection_state,
  pings_received, unique_mmsis, buffer_rows, disconnect_reason,
  reconnect_attempts, unsynced_files_48h), also logged to the journal.

The compactor turns finished staging hours into ZSTD Parquet.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone
from typing import List, Optional, Set, Tuple

import websockets

import config

log = logging.getLogger("ais.collector")

STAGING_SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    recv_ts  TEXT NOT NULL,
    msg_type TEXT,
    raw      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS collector_health (
    ts_utc             TEXT NOT NULL,
    connection_state   TEXT,
    pings_received     INTEGER,
    unique_mmsis       INTEGER,
    buffer_rows        INTEGER,
    disconnect_reason  TEXT,
    reconnect_attempts INTEGER,
    unsynced_files_48h INTEGER
);
"""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.isoformat(timespec="milliseconds")


def hour_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d/%H")


def open_staging(key: str) -> sqlite3.Connection:
    path = config.STAGING_DIR / f"{key}.sqlite"
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    con.executescript(STAGING_SCHEMA)
    return con


class Stager:
    """RAM buffer of messages, flushed as micro-batches to the hour's staging DB."""

    def __init__(self) -> None:
        self._buf: List[Tuple[str, Optional[str], str]] = []
        self._key: Optional[str] = None
        self._last_flush = time.monotonic()

    @property
    def buffer_rows(self) -> int:
        return len(self._buf)

    def add(self, now: datetime, msg_type: Optional[str], raw: str) -> None:
        key = hour_key(now)
        if self._buf and key != self._key:
            self.flush()  # never let a batch straddle two hour files
        self._key = key
        self._buf.append((iso(now), msg_type, raw))
        if len(self._buf) >= config.MICRO_FLUSH_ROWS:
            self.flush()

    def maybe_flush(self) -> None:
        if time.monotonic() - self._last_flush >= config.MICRO_FLUSH_SEC:
            self.flush()

    def flush(self) -> None:
        self._last_flush = time.monotonic()
        if not self._buf or self._key is None:
            return
        try:
            con = open_staging(self._key)
            try:
                with con:
                    con.executemany("INSERT INTO messages VALUES (?, ?, ?)", self._buf)
            finally:
                con.close()
        except Exception:  # noqa: BLE001 - keep the rows and retry next flush
            log.exception("micro-batch flush failed; keeping %d rows in RAM", len(self._buf))
            return
        log.debug("flushed %d rows to %s", len(self._buf), self._key)
        self._buf.clear()


def read_sync_alert() -> Optional[int]:
    try:
        return int(config.SYNC_ALERT_FILE.read_text().strip())
    except (OSError, ValueError):
        return None


class Health:
    """Counters for the 1-minute collector_health heartbeat."""

    def __init__(self) -> None:
        self.state = "starting"
        self.pings = 0
        self.mmsis: Set[int] = set()
        self.reason: Optional[str] = None
        self.attempts = 0

    def record(self, mmsi: Optional[int]) -> None:
        self.pings += 1
        if mmsi:
            self.mmsis.add(mmsi)

    def write(self, stager: Stager) -> None:
        now = utcnow()
        unsynced = read_sync_alert()
        row = (
            iso(now), self.state, self.pings, len(self.mmsis), stager.buffer_rows,
            self.reason, self.attempts, unsynced,
        )
        try:
            con = open_staging(hour_key(now))
            try:
                with con:
                    con.execute("INSERT INTO collector_health VALUES (?, ?, ?, ?, ?, ?, ?, ?)", row)
            finally:
                con.close()
        except Exception:  # noqa: BLE001
            log.exception("failed to write collector_health row")
        log.info(
            "heartbeat state=%s pings=%d unique_mmsis=%d buffer_rows=%d reconnect_attempts=%d "
            "disconnect_reason=%s unsynced_files_48h=%s",
            self.state, self.pings, len(self.mmsis), stager.buffer_rows, self.attempts,
            self.reason, unsynced,
        )
        if unsynced:
            log.warning("SYNC ALERT: %d Parquet file(s) older than 48h are not verified on the remote", unsynced)
        self.pings = 0
        self.mmsis.clear()
        if self.state == "connected":
            self.reason = None


def subscription_message() -> str:
    sub = {
        "APIKey": config.AISSTREAM_API_KEY,
        "BoundingBoxes": json.loads(config.AIS_BBOXES_JSON),
    }
    if config.AIS_MESSAGE_TYPES:
        sub["FilterMessageTypes"] = config.AIS_MESSAGE_TYPES
    return json.dumps(sub)


async def stream_once(stager: Stager, health: Health) -> int:
    """Run one websocket session. Returns number of messages received."""
    count = 0
    health.state = "connecting"
    async with websockets.connect(
        config.AISSTREAM_URL,
        compression="deflate",  # permessage-deflate
        open_timeout=20,
        ping_interval=20,
        ping_timeout=20,
        max_size=2**20,
    ) as ws:
        await ws.send(subscription_message())  # immediately: aisstream's 3 s limit
        health.state = "connected"
        log.info("connected and subscribed")
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=config.WS_RECV_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                health.reason = f"watchdog: no data for {config.WS_RECV_TIMEOUT_SEC:.0f}s"
                log.warning("%s, reconnecting", health.reason)
                return count
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                log.debug("skipping non-JSON frame")
                continue
            if "error" in msg:
                raise RuntimeError(f"aisstream error: {msg['error']}")
            mmsi = (msg.get("MetaData") or {}).get("MMSI")
            stager.add(utcnow(), msg.get("MessageType"), raw)
            health.record(mmsi if isinstance(mmsi, int) else None)
            count += 1
            if count == 1:
                health.attempts = 0
            if count % 10000 == 0:
                log.info("received %d messages this session", count)


async def run_forever(stager: Stager, health: Health) -> None:
    backoff = config.RECONNECT_MIN_SECONDS
    while True:
        try:
            n = await stream_once(stager, health)
            if n > 0:
                backoff = config.RECONNECT_MIN_SECONDS
        except Exception as exc:  # noqa: BLE001 - keep the collector alive
            health.reason = f"{type(exc).__name__}: {exc}"[:500]
            log.warning("stream error: %s", health.reason)
        stager.flush()
        health.state = "backoff"
        health.attempts += 1
        # Exponential backoff with full jitter, bounded to [min, max].
        delay = random.uniform(config.RECONNECT_MIN_SECONDS, backoff)
        log.info("reconnecting in %.1fs (attempt %d)", delay, health.attempts)
        await asyncio.sleep(delay)
        backoff = min(backoff * 2, config.RECONNECT_MAX_SECONDS)


async def ticker(stager: Stager, health: Health, stop: asyncio.Event) -> None:
    """Timed micro-batch flushes and the 1-minute heartbeat."""
    next_hb = time.monotonic() + config.HEARTBEAT_SEC
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass
        stager.maybe_flush()
        if time.monotonic() >= next_hb:
            health.write(stager)
            next_hb = time.monotonic() + config.HEARTBEAT_SEC


async def main() -> None:
    if not config.AISSTREAM_API_KEY:
        log.error("AISSTREAM_API_KEY is not set")
        sys.exit(2)
    config.ensure_dirs()
    stager = Stager()
    health = Health()
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    runner = asyncio.create_task(run_forever(stager, health))
    tick = asyncio.create_task(ticker(stager, health, stop))
    stop_wait = asyncio.create_task(stop.wait())
    await asyncio.wait({runner, stop_wait}, return_when=asyncio.FIRST_COMPLETED)
    stop.set()
    runner.cancel()
    await asyncio.gather(runner, tick, stop_wait, return_exceptions=True)
    stager.flush()
    health.state = "stopped"
    health.write(stager)
    log.info("collector stopped")


if __name__ == "__main__":
    logging.basicConfig(
        level=config.LOG_LEVEL,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(main())
