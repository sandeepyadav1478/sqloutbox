"""WS-0 resilience: one fault must not zombify the daemon."""
from __future__ import annotations

import sqlite3
import tempfile
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


class _BoomOutbox:
    """Stand-in outbox whose reads always raise — simulates a corrupt file."""
    def pending_count(self):
        raise sqlite3.DatabaseError("database disk image is malformed")
    def fetch_unsynced(self):
        raise sqlite3.DatabaseError("database disk image is malformed")


@pytest.mark.asyncio
async def test_corrupt_namespace_isolated_siblings_drain(tmp_path: Path):
    writer = _CollectingWriter()
    # Two tables on one target: 'bad' is corrupt, 'good' is healthy.
    # inject_outbox_seq=False + auto_schema=False keeps startup write_batch
    # calls out of writer.seen (see _make_service note).
    cfg = OutboxConfig(
        db_dir=tmp_path,
        targets=(TargetConfig(name="primary", tables=("bad", "good"),
                              inject_outbox_seq=False),),
        flush_interval=0.01,
        table_flush_threshold=1,
        table_max_wait=0.0,
        auto_schema=False,
    )
    svc = OutboxSyncService(config=cfg, writers={"primary": writer})

    # Seed a healthy row in 'good'.
    Outbox(db_path=tmp_path / "good.db", namespace="good").enqueue(
        "INSERT INTO good (a) VALUES (?)", json.dumps([1]).encode()
    )
    # Replace the 'bad' outbox with one that raises on every read.
    svc._target_outboxes["primary"]["bad"] = _BoomOutbox()

    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # 'good' was delivered despite 'bad' raising every cycle.
    assert any("good" in sql for sql, _ in writer.seen)


from sqloutbox import _runner


@pytest.mark.asyncio
async def test_runner_exits_nonzero_when_worker_dies(monkeypatch, tmp_path: Path):
    """If the drain task faults, the runner must raise SystemExit(1), not hang."""

    class _DyingService:
        async def run(self):
            raise RuntimeError("worker exploded")

    # Patch the pieces run_service_main needs so we exercise ONLY the task-watch logic.
    monkeypatch.setattr(
        _runner, "load_config_toml",
        lambda _p: (_FakeConfig(), {}),
    )
    monkeypatch.setattr(
        "sqloutbox.sync.OutboxSyncService",
        lambda **_kw: _DyingService(),
    )

    with pytest.raises(SystemExit) as ei:
        await asyncio.wait_for(_runner.run_service_main(tmp_path / "outbox.toml"), timeout=2.0)
    assert ei.value.code == 1


class _FakeConfig:
    """Minimal stand-in for the loaded config (only what run_service_main reads)."""
    flush_interval = 1.0
    table_flush_threshold = 15
    table_max_wait = 6.0
    db_dir = Path(tempfile.mkdtemp(prefix="sqloutbox-faketest-"))
    targets = ()


@pytest.mark.asyncio
async def test_runner_clean_stop_does_not_raise(monkeypatch, tmp_path: Path):
    """A normal stop signal shuts down cleanly (no SystemExit)."""
    import signal as _signal

    started = asyncio.Event()

    class _LiveService:
        def __init__(self):
            self._stop = False

        def request_stop(self):
            self._stop = True

        async def run(self):
            started.set()
            while not self._stop:
                await asyncio.sleep(0.01)

    monkeypatch.setattr(_runner, "load_config_toml", lambda _p: (_FakeConfig(), {}))
    monkeypatch.setattr("sqloutbox.sync.OutboxSyncService", lambda **_kw: _LiveService())

    # Capture the handlers the runner registers, without touching process-wide
    # signal state. Shadows the bound method on this loop instance only.
    loop = asyncio.get_running_loop()
    handlers: dict[int, object] = {}

    def _capture(sig, cb, *a):
        handlers[sig] = cb

    monkeypatch.setattr(loop, "add_signal_handler", _capture)

    runner_task = asyncio.create_task(_runner.run_service_main(tmp_path / "outbox.toml"))
    await started.wait()                 # service is running, handlers registered
    handlers[_signal.SIGINT]()           # fire the stop handler the runner set
    # Completes WITHOUT SystemExit (clean-stop path), within the timeout.
    await asyncio.wait_for(runner_task, timeout=2.0)


@pytest.mark.asyncio
async def test_poison_and_healthy_coexist(tmp_path: Path):
    writer = _CollectingWriter()
    cfg = OutboxConfig(
        db_dir=tmp_path,
        targets=(TargetConfig(name="primary", tables=("poison", "healthy"),
                              inject_outbox_seq=False),),
        flush_interval=0.01,
        table_flush_threshold=1,
        table_max_wait=0.0,
        auto_schema=False,
    )
    svc = OutboxSyncService(config=cfg, writers={"primary": writer})

    _enqueue_raw(tmp_path / "poison.db", "poison",
                 "INSERT INTO poison (a) VALUES (?)", b"}{ not json")
    Outbox(db_path=tmp_path / "healthy.db", namespace="healthy").enqueue(
        "INSERT INTO healthy (a) VALUES (?)", json.dumps([7]).encode()
    )

    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.25)
    assert not task.done()                       # daemon still alive
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    delivered = [sql for sql, _ in writer.seen]
    assert any("healthy" in s for s in delivered)
    assert not any("poison" in s for s in delivered)
