"""WS-0 resilience: one fault must not zombify the daemon."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from sqloutbox._schema import open_write_conn, thread_conn


def _busy_timeout(conn: sqlite3.Connection) -> int:
    return conn.execute("PRAGMA busy_timeout").fetchone()[0]


def test_open_write_conn_sets_busy_timeout(tmp_path: Path):
    conn = open_write_conn(tmp_path / "t.db")
    try:
        assert _busy_timeout(conn) == 30000
    finally:
        conn.close()


def test_thread_conn_sets_busy_timeout(tmp_path: Path):
    # thread_conn needs the file to exist first (open_write_conn creates it).
    open_write_conn(tmp_path / "t.db").close()
    conn = thread_conn(tmp_path / "t.db")
    try:
        assert _busy_timeout(conn) == 30000
    finally:
        conn.close()


import asyncio
import json
import pytest

from sqloutbox._outbox import Outbox
from sqloutbox.config import OutboxConfig, TargetConfig
from sqloutbox.sync import OutboxSyncService


class _CollectingWriter:
    """Test writer: records every statement it is asked to write, all ok."""
    def __init__(self) -> None:
        self.seen: list[tuple[str, list]] = []

    async def write_batch(self, stmts):
        self.seen.extend(stmts)
        return [{"ok": True} for _ in stmts]


def _make_service(tmp_path: Path, writer, *, table="evt"):
    # auto_schema=False + inject_outbox_seq=False so run() does NOT call
    # write_batch() during _ensure_schema()/_seed_from_remote() at startup —
    # those calls would otherwise pollute writer.seen and break the assertions.
    cfg = OutboxConfig(
        db_dir=tmp_path,
        targets=(TargetConfig(name="primary", tables=(table,),
                              inject_outbox_seq=False),),
        flush_interval=0.01,
        table_flush_threshold=1,
        table_max_wait=0.0,
        auto_schema=False,
    )
    return OutboxSyncService(config=cfg, writers={"primary": writer}), cfg


def _enqueue_raw(db_path: Path, namespace: str, tag: str, payload: bytes):
    """Enqueue a row, then overwrite its payload with raw bytes (bypass JSON)."""
    ob = Outbox(db_path=db_path, namespace=namespace)
    ob.enqueue(tag, b'{}')
    # Corrupt the stored payload directly to simulate a non-JSON row on disk.
    conn = ob._write_conn  # persistent connection
    conn.execute("UPDATE outbox_queue SET payload=?", (payload.decode("latin-1"),))
    conn.commit()


@pytest.mark.asyncio
async def test_undecodable_row_does_not_kill_loop(tmp_path: Path):
    writer = _CollectingWriter()
    svc, cfg = _make_service(tmp_path, writer)
    db_path = tmp_path / "evt.db"
    # One poison row: payload is not valid JSON.
    _enqueue_raw(db_path, "evt", "INSERT INTO evt (a) VALUES (?)", b"not json{{{")

    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.2)            # let several cycles run
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # The loop survived (task did not die on its own before cancel) and the
    # poison row was NOT delivered to the writer.
    assert writer.seen == []
