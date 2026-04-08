"""Tests for OutboxSyncService, inject_outbox_seq, and OutboxWriter protocol."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from sqloutbox import (
    OutboxConfig,
    OutboxSyncService,
    OutboxWriter,
    SQLMiddleware,
    TargetConfig,
    clear_registry,
    inject_outbox_seq,
)


# ── inject_outbox_seq ─────────────────────────────────────────────────────────


def test_inject_outbox_seq_basic():
    """Transforms INSERT INTO → INSERT OR IGNORE INTO with outbox_seq."""
    sql = "INSERT INTO orders (id, amount) VALUES (?, ?)"
    args = [42, 99.99]
    new_sql, new_args = inject_outbox_seq(sql, args, outbox_seq=100)

    assert "INSERT OR IGNORE INTO" in new_sql
    assert "outbox_seq" in new_sql
    assert new_args == [42, 99.99, 100]


def test_inject_outbox_seq_preserves_table():
    """Table name and columns are preserved."""
    sql = "INSERT INTO my_table (a, b, c) VALUES (?, ?, ?)"
    new_sql, _ = inject_outbox_seq(sql, [1, 2, 3], outbox_seq=5)

    assert "my_table" in new_sql
    assert "a, b, c, outbox_seq" in new_sql


def test_inject_outbox_seq_case_insensitive():
    """Works with mixed-case INSERT INTO."""
    sql = "insert into events (id) values (?)"
    new_sql, new_args = inject_outbox_seq(sql, [1], outbox_seq=7)

    assert "INSERT OR IGNORE INTO" in new_sql
    assert new_args == [1, 7]


def test_inject_outbox_seq_does_not_mutate_original():
    """Original args list is not modified."""
    original_args = [42, "hello"]
    _, new_args = inject_outbox_seq(
        "INSERT INTO t (a, b) VALUES (?, ?)", original_args, outbox_seq=1,
    )
    assert original_args == [42, "hello"]  # unchanged
    assert new_args == [42, "hello", 1]


# ── OutboxWriter protocol ───────────────────────────────────────────────────


class MockWriter:
    """Test writer that records calls and returns configurable results."""

    def __init__(self, results: list[dict[str, Any]] | None = None):
        self.calls: list[list[tuple[str, list]]] = []
        self._results = results

    async def write_batch(self, stmts: list[tuple[str, list]]) -> list[dict]:
        self.calls.append(stmts)
        if self._results is not None:
            return self._results
        return [{"ok": True, "rows_affected": 1} for _ in stmts]


def test_mock_writer_is_outbox_writer():
    """MockWriter satisfies the OutboxWriter protocol."""
    assert isinstance(MockWriter(), OutboxWriter)


# ── OutboxSyncService ─────────────────────────────────────────────────────────


class ProducerMiddleware(SQLMiddleware):
    """Simple middleware for producing test events."""

    def __init__(self, db_dir: Path):
        self._config = OutboxConfig(db_dir=db_dir)


@pytest.fixture(autouse=True)
def _clean_registry():
    yield
    clear_registry()


@pytest.mark.asyncio
async def test_sync_service_drains_outbox(tmp_path):
    """OutboxSyncService fetches, delivers, and deletes rows."""
    target = TargetConfig(name="primary", tables=("events",))
    config = OutboxConfig(
        db_dir=tmp_path,
        targets=(target,),
        flush_interval=0.1,  # fast for testing
    )

    # Produce events
    mw = ProducerMiddleware(db_dir=tmp_path)
    mw._push("events", "INSERT INTO events (id, val) VALUES (?, ?)", [1, "a"])
    mw._push("events", "INSERT INTO events (id, val) VALUES (?, ?)", [2, "b"])

    # Create sync service
    writer = MockWriter()
    svc = OutboxSyncService(config=config, writers={"primary": writer})

    # Verify pending before drain
    assert svc.total_pending() == 2

    # Run for a short time (enough for one cycle)
    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.5)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Writer should have been called
    assert len(writer.calls) >= 1

    # All stmts should have outbox_seq injected (default inject_outbox_seq=True)
    for call in writer.calls:
        for sql, args in call:
            assert "INSERT OR IGNORE INTO" in sql
            assert "outbox_seq" in sql

    # Outbox should be drained
    assert svc.total_pending() == 0


@pytest.mark.asyncio
async def test_sync_service_no_inject_when_disabled(tmp_path):
    """inject_outbox_seq=False leaves SQL unchanged."""
    target = TargetConfig(
        name="audit", tables=("audit_log",), inject_outbox_seq=False,
    )
    config = OutboxConfig(
        db_dir=tmp_path,
        targets=(target,),
        flush_interval=0.1,
    )

    mw = ProducerMiddleware(db_dir=tmp_path)
    mw._push("audit_log", "INSERT INTO audit_log (msg) VALUES (?)", ["test"])

    writer = MockWriter()
    svc = OutboxSyncService(config=config, writers={"audit": writer})

    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.5)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Writer should have been called with ORIGINAL sql (no outbox_seq)
    assert len(writer.calls) >= 1
    for call in writer.calls:
        for sql, args in call:
            assert "outbox_seq" not in sql
            assert "INSERT OR IGNORE" not in sql


@pytest.mark.asyncio
async def test_sync_service_multi_target(tmp_path):
    """Multiple targets each get their own writer calls."""
    t1 = TargetConfig(name="analytics", tables=("events",))
    t2 = TargetConfig(name="billing", tables=("invoices",), inject_outbox_seq=False)
    config = OutboxConfig(
        db_dir=tmp_path,
        targets=(t1, t2),
        flush_interval=0.1,
    )

    mw = ProducerMiddleware(db_dir=tmp_path)
    mw._push("events", "INSERT INTO events (id) VALUES (?)", [1])
    mw._push("invoices", "INSERT INTO invoices (id) VALUES (?)", [100])

    w1 = MockWriter()
    w2 = MockWriter()
    svc = OutboxSyncService(
        config=config,
        writers={"analytics": w1, "billing": w2},
    )

    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.5)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Both writers should have received calls
    assert len(w1.calls) >= 1, "analytics writer should have received calls"
    assert len(w2.calls) >= 1, "billing writer should have received calls"
    assert svc.total_pending() == 0


@pytest.mark.asyncio
async def test_sync_service_writer_failure_retries(tmp_path):
    """Failed writes are retried on the next cycle."""
    target = TargetConfig(name="primary", tables=("events",))
    config = OutboxConfig(
        db_dir=tmp_path,
        targets=(target,),
        flush_interval=0.1,
    )

    mw = ProducerMiddleware(db_dir=tmp_path)
    mw._push("events", "INSERT INTO events (id) VALUES (?)", [1])

    # Writer that fails first, succeeds second
    call_count = 0

    class FailOnceWriter:
        async def write_batch(self, stmts):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("simulated network failure")
            return [{"ok": True, "rows_affected": 1} for _ in stmts]

    svc = OutboxSyncService(
        config=config, writers={"primary": FailOnceWriter()},
    )

    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.8)  # enough for ~2 cycles
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Should have retried and succeeded
    assert call_count >= 2
    assert svc.total_pending() == 0


def test_sync_service_pending_count(tmp_path):
    """pending_count() and total_pending() track correctly."""
    t1 = TargetConfig(name="a", tables=("t1", "t2"))
    config = OutboxConfig(db_dir=tmp_path, targets=(t1,), flush_interval=1.0)

    mw = ProducerMiddleware(db_dir=tmp_path)
    mw._push("t1", "INSERT INTO t1 (x) VALUES (?)", [1])
    mw._push("t1", "INSERT INTO t1 (x) VALUES (?)", [2])
    mw._push("t2", "INSERT INTO t2 (x) VALUES (?)", [3])

    svc = OutboxSyncService(config=config, writers={"a": MockWriter()})

    counts = svc.pending_count()
    assert counts["t1"] == 2
    assert counts["t2"] == 1
    assert svc.total_pending() == 3
