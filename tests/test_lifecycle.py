"""WS-4 lifecycle: single-drain lock, cooperative shutdown, per-target isolation."""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from sqloutbox import _runner


def test_acquire_single_drain_lock_then_second_fails(tmp_path: Path):
    """First acquisition succeeds and holds; a second on the same db_dir fails."""
    handle1 = _runner.acquire_single_drain_lock(tmp_path)
    assert handle1 is not None
    # The lock file was created in the db_dir.
    assert (tmp_path / ".sqloutbox.lock").exists()

    # A second acquisition on the SAME dir must raise SystemExit(1) with a
    # clear message (the first handle is still open / lock still held).
    with pytest.raises(SystemExit) as ei:
        _runner.acquire_single_drain_lock(tmp_path)
    assert ei.value.code == 1

    # Releasing the first handle frees the lock so a later drain can re-acquire.
    _runner.release_single_drain_lock(handle1)
    handle2 = _runner.acquire_single_drain_lock(tmp_path)
    assert handle2 is not None
    _runner.release_single_drain_lock(handle2)


def test_distinct_db_dirs_do_not_contend(tmp_path: Path):
    """Two different db_dirs each get their own lock — no contention."""
    a = _runner.acquire_single_drain_lock(tmp_path / "a")
    b = _runner.acquire_single_drain_lock(tmp_path / "b")
    assert a is not None and b is not None
    _runner.release_single_drain_lock(a)
    _runner.release_single_drain_lock(b)


import json

from sqloutbox._outbox import Outbox
from sqloutbox.config import OutboxConfig, TargetConfig
from sqloutbox.sync import OutboxSyncService


class _SlowConfirmWriter:
    """Writer that delivers instantly but lets us assert the confirm completes.

    write_batch records what it delivered and returns ok for everything.
    """
    def __init__(self) -> None:
        self.delivered: list[tuple[str, list]] = []

    async def write_batch(self, stmts):
        self.delivered.extend(stmts)
        return [{"ok": True} for _ in stmts]


def _make_recording_service(tmp_path: Path, writer, *, table: str):
    # auto_schema=False + inject_outbox_seq=False so run() does NOT call
    # write_batch() during _ensure_schema()/_seed_from_remote() at startup —
    # those startup calls would otherwise pollute writer.delivered.
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


@pytest.mark.asyncio
async def test_request_stop_ends_loop_at_top_of_cycle(tmp_path: Path):
    """request_stop() makes the worker return cleanly (no new cycle, no error)."""
    writer = _SlowConfirmWriter()
    svc, _cfg = _make_recording_service(tmp_path, writer, table="evt")

    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.1)        # let it run a few cycles
    svc.request_stop()
    # The worker returns on its own (top-of-cycle check) — NOT cancelled.
    await asyncio.wait_for(task, timeout=2.0)
    assert task.done() and task.exception() is None


@pytest.mark.asyncio
async def test_confirm_completes_then_no_redelivery_after_restart(tmp_path: Path):
    """Stop set after write returns ok → confirm (mark+delete) still runs →
    the row is gone from the local queue → a restarted drain re-delivers nothing."""
    writer = _SlowConfirmWriter()
    svc, cfg = _make_recording_service(tmp_path, writer, table="evt")

    # One healthy row to deliver.
    Outbox(db_path=tmp_path / "evt.db", namespace="evt").enqueue(
        "INSERT INTO evt (a) VALUES (?)", json.dumps([1]).encode()
    )

    task = asyncio.create_task(svc.run())
    # Wait until the row was delivered (write_batch ran) AND confirmed (row
    # deleted from the local queue), then stop.
    for _ in range(200):
        await asyncio.sleep(0.01)
        if writer.delivered and Outbox(
            db_path=tmp_path / "evt.db", namespace="evt"
        ).pending_count() == 0:
            break
    svc.request_stop()
    await asyncio.wait_for(task, timeout=2.0)

    # Delivered exactly once and the local queue is empty (confirm completed).
    assert len([s for s, _ in writer.delivered if "evt" in s]) == 1
    assert Outbox(db_path=tmp_path / "evt.db", namespace="evt").pending_count() == 0

    # Simulate a restart: a fresh drain over the same dir delivers NOTHING new
    # (the row was deleted by the confirm before shutdown — no redelivery).
    writer2 = _SlowConfirmWriter()
    svc2, _ = _make_recording_service(tmp_path, writer2, table="evt")
    task2 = asyncio.create_task(svc2.run())
    await asyncio.sleep(0.1)
    svc2.request_stop()
    await asyncio.wait_for(task2, timeout=2.0)
    assert writer2.delivered == []


import sqlite3


def test_writerless_target_fails_fast_at_init(tmp_path: Path):
    """A target with no matching writer raises ValueError at construction —
    it never silently black-holes rows."""
    cfg = OutboxConfig(
        db_dir=tmp_path,
        targets=(
            TargetConfig(name="primary", tables=("evt",), inject_outbox_seq=False),
            TargetConfig(name="missing", tables=("other",), inject_outbox_seq=False),
        ),
        auto_schema=False,
    )
    with pytest.raises(ValueError) as ei:
        OutboxSyncService(config=cfg, writers={"primary": _SlowConfirmWriter()})
    # The error names the offending target so the misconfiguration is obvious.
    assert "missing" in str(ei.value)


def test_all_targets_have_writers_constructs_ok(tmp_path: Path):
    """When every target has a writer, construction succeeds (no false positive)."""
    cfg = OutboxConfig(
        db_dir=tmp_path,
        targets=(TargetConfig(name="primary", tables=("evt",),
                              inject_outbox_seq=False),),
        auto_schema=False,
    )
    svc = OutboxSyncService(
        config=cfg, writers={"primary": _SlowConfirmWriter()},
    )
    assert svc is not None


class _ExplodingWriter:
    """Writer whose write_batch always raises — simulates a broken target."""
    async def write_batch(self, stmts):
        raise RuntimeError("writer exploded")


@pytest.mark.asyncio
async def test_broken_writer_isolated_siblings_drain(tmp_path: Path):
    """One target whose writer always raises must not halt a sibling target.

    (Proves the WS-0 Layer-2 isolation property holds through the per-target
    flush boundary — the broken target's write failure is caught and the
    healthy sibling still delivers.)"""
    good = _SlowConfirmWriter()
    bad = _ExplodingWriter()
    cfg = OutboxConfig(
        db_dir=tmp_path,
        targets=(
            TargetConfig(name="broken", tables=("bad",), inject_outbox_seq=False),
            TargetConfig(name="healthy", tables=("good",), inject_outbox_seq=False),
        ),
        flush_interval=0.01,
        table_flush_threshold=1,
        table_max_wait=0.0,
        auto_schema=False,
    )
    svc = OutboxSyncService(
        config=cfg, writers={"broken": bad, "healthy": good},
    )

    Outbox(db_path=tmp_path / "bad.db", namespace="bad").enqueue(
        "INSERT INTO bad (a) VALUES (?)", json.dumps([1]).encode()
    )
    Outbox(db_path=tmp_path / "good.db", namespace="good").enqueue(
        "INSERT INTO good (a) VALUES (?)", json.dumps([2]).encode()
    )

    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.2)
    svc.request_stop()
    await asyncio.wait_for(task, timeout=2.0)

    # Healthy sibling delivered despite the broken target raising every cycle.
    assert any("good" in s for s, _ in good.delivered)
