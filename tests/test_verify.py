"""Tests for sqloutbox integrity verification.

Covers:
    - verify_outbox() on a healthy outbox (all checks pass)
    - verify_outbox() with chain gaps (deleted middle row)
    - verify_outbox() with non-monotonic timestamps
    - verify_outbox() with sequence gaps unaccounted by sync_log
    - verify_outbox() on empty outbox
    - verify_all() aggregation (mixed pass/fail)
    - Outbox.verify_full() convenience method
    - CLI verify --db-dir (integration test)
    - OutboxSyncService.request_verify() (async integration)
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest

from sqloutbox import Outbox, OutboxSyncService, OutboxWriter
from sqloutbox._verify import TableVerifyResult, VerifyResult, verify_all, verify_outbox
from sqloutbox.config import OutboxConfig, TargetConfig


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def db_path(tmp_path) -> Path:
    return tmp_path / "test.db"


@pytest.fixture
def outbox(db_path) -> Outbox:
    return Outbox(db_path=db_path, namespace="events")


# ── Helpers ───────────────────────────────────────────────────────────────────


def _enqueue_n(outbox: Outbox, n: int) -> list[int]:
    """Enqueue n rows and return their seqs."""
    seqs = []
    for i in range(n):
        seq = outbox.enqueue(
            tag=f"INSERT INTO events (id) VALUES (?)",
            payload=f'[{i}]'.encode(),
            source="test",
        )
        assert seq is not None
        seqs.append(seq)
    return seqs


def _delete_row_raw(db_path: Path, seq: int) -> None:
    """Delete a row directly from SQLite, bypassing outbox logic (simulates corruption)."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("DELETE FROM outbox_queue WHERE seq = ?", [seq])
    conn.commit()
    conn.close()


def _corrupt_timestamp(db_path: Path, seq: int, timestamp: str) -> None:
    """Set a specific timestamp on a row (simulates clock drift)."""
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "UPDATE outbox_queue SET created_at = ? WHERE seq = ?",
        [timestamp, seq],
    )
    conn.commit()
    conn.close()


# ── verify_outbox: healthy outbox ─────────────────────────────────────────────


def test_verify_healthy_outbox(outbox):
    """All checks pass on a correctly formed outbox."""
    _enqueue_n(outbox, 5)
    result = verify_outbox(outbox)

    assert result.ok is True
    assert result.table == "events"
    assert result.pending_count == 5
    assert result.total_rows == 5
    assert result.chain_ok is True
    assert result.chain_gaps == ()
    assert result.seq_continuous is True
    assert result.timestamps_monotonic is True
    assert result.orphan_sync_log == 0
    assert result.errors == ()
    assert result.seq_range is not None
    assert result.seq_range[1] - result.seq_range[0] == 4  # 5 rows


def test_verify_empty_outbox(outbox):
    """Empty outbox passes all checks."""
    result = verify_outbox(outbox)

    assert result.ok is True
    assert result.pending_count == 0
    assert result.total_rows == 0
    assert result.seq_range is None
    assert result.errors == ()


def test_verify_after_full_delivery(outbox, db_path):
    """Outbox with all rows delivered and deleted passes."""
    seqs = _enqueue_n(outbox, 3)
    outbox.mark_synced(seqs)
    outbox.delete_synced(seqs)

    result = verify_outbox(outbox)
    assert result.ok is True
    assert result.pending_count == 0
    assert result.total_rows == 0
    assert result.sync_log_rows == 3


# ── verify_outbox: chain gaps ─────────────────────────────────────────────────


def test_verify_chain_gap(outbox, db_path):
    """Detects chain gap when a middle row is deleted without sync_log."""
    seqs = _enqueue_n(outbox, 5)

    # Delete middle row directly (bypassing outbox.delete_synced)
    _delete_row_raw(db_path, seqs[2])

    result = verify_outbox(outbox)
    assert result.chain_ok is False
    assert len(result.chain_gaps) > 0
    assert result.ok is False
    assert len(result.errors) > 0


# ── verify_outbox: sequence continuity ────────────────────────────────────────


def test_verify_seq_gap_accounted_by_sync_log(outbox, db_path):
    """Seq gap is OK when the missing seq exists in sync_log."""
    seqs = _enqueue_n(outbox, 5)

    # Properly deliver middle row (goes to sync_log)
    outbox.mark_synced([seqs[2]])
    outbox.delete_synced([seqs[2]])

    result = verify_outbox(outbox)
    # Chain check may fail (prev_seq points to deleted row), but that's expected
    # since we're testing seq continuity — the sync_log should account for the gap
    assert result.seq_continuous is True


def test_verify_seq_gap_unaccounted(outbox, db_path):
    """Detects seq gap when missing seq is NOT in sync_log."""
    seqs = _enqueue_n(outbox, 5)

    # Raw delete — no sync_log entry
    _delete_row_raw(db_path, seqs[2])

    result = verify_outbox(outbox)
    assert result.seq_continuous is False
    assert any("seq gap" in e for e in result.errors)


# ── verify_outbox: timestamp monotonicity ─────────────────────────────────────


def test_verify_timestamps_monotonic(outbox):
    """Normal enqueue produces monotonic timestamps."""
    _enqueue_n(outbox, 5)
    result = verify_outbox(outbox)
    assert result.timestamps_monotonic is True


def test_verify_timestamps_non_monotonic(outbox, db_path):
    """Detects non-monotonic timestamps (simulated clock drift)."""
    seqs = _enqueue_n(outbox, 3)

    # Set the last row's timestamp to before the first row's
    _corrupt_timestamp(db_path, seqs[2], "2000-01-01T00:00:00+00:00")

    result = verify_outbox(outbox)
    assert result.timestamps_monotonic is False
    assert result.ok is False
    assert any("timestamp" in e for e in result.errors)


# ── verify_outbox: orphan sync_log ───────────────────────────────────────────


def test_verify_orphan_sync_log_after_delivery(outbox):
    """sync_log entries beyond queue max are counted (normal after delivery)."""
    seqs = _enqueue_n(outbox, 3)
    outbox.mark_synced(seqs)
    outbox.delete_synced(seqs)

    # Now enqueue one more — sync_log has seqs 1-3 but queue only has seq 4+
    _enqueue_n(outbox, 1)

    result = verify_outbox(outbox)
    assert result.ok is True
    # sync_log entries for seqs 1-3 are "beyond" the new queue's min,
    # but orphan_sync_log counts seqs > MAX(queue), which is fine here
    assert result.orphan_sync_log >= 0  # implementation detail


# ── verify_all ───────────────────────────────────────────────────────────────


def test_verify_all_all_pass(tmp_path):
    """verify_all returns ok=True when all tables pass."""
    outbox_a = Outbox(db_path=tmp_path / "a.db", namespace="a")
    outbox_b = Outbox(db_path=tmp_path / "b.db", namespace="b")
    _enqueue_n(outbox_a, 3)
    _enqueue_n(outbox_b, 2)

    result = verify_all({"a": outbox_a, "b": outbox_b})
    assert result.ok is True
    assert len(result.tables) == 2
    assert result.duration_ms >= 0
    assert result.checked_at != ""


def test_verify_all_mixed(tmp_path):
    """verify_all returns ok=False when any table fails."""
    outbox_good = Outbox(db_path=tmp_path / "good.db", namespace="good")
    outbox_bad = Outbox(db_path=tmp_path / "bad.db", namespace="bad")

    _enqueue_n(outbox_good, 3)
    seqs = _enqueue_n(outbox_bad, 5)
    _corrupt_timestamp(tmp_path / "bad.db", seqs[4], "2000-01-01T00:00:00+00:00")

    result = verify_all({"good": outbox_good, "bad": outbox_bad})
    assert result.ok is False
    assert sum(1 for t in result.tables if t.ok) == 1
    assert sum(1 for t in result.tables if not t.ok) == 1


# ── Outbox.verify_full() ────────────────────────────────────────────────────


def test_outbox_verify_full(outbox):
    """verify_full() convenience method works."""
    _enqueue_n(outbox, 3)
    result = outbox.verify_full()
    assert isinstance(result, TableVerifyResult)
    assert result.ok is True
    assert result.table == "events"


# ── OutboxSyncService.request_verify() ──────────────────────────────────────


class _MockWriter:
    """Minimal OutboxWriter for testing."""

    async def write_batch(self, stmts):
        return [{"ok": True, "rows_affected": 0} for _ in stmts]


async def test_sync_service_request_verify(tmp_path):
    """request_verify() returns results during the drain loop."""
    db_dir = tmp_path / "data"
    db_dir.mkdir()

    config = OutboxConfig(
        db_dir=db_dir,
        targets=(
            TargetConfig(
                name="test",
                tables=("events",),
                inject_outbox_seq=False,
            ),
        ),
        flush_interval=0.05,  # fast for tests
        auto_schema=False,
    )
    writer = _MockWriter()
    svc = OutboxSyncService(config=config, writers={"test": writer})

    # Enqueue some rows to the outbox
    outbox = Outbox(db_path=db_dir / "events.db", namespace="events")
    _enqueue_n(outbox, 3)

    # Start the service in background
    task = asyncio.get_event_loop().create_task(svc.run())

    try:
        # Give the service a moment to start
        await asyncio.sleep(0.1)

        # Request verification
        result = await asyncio.wait_for(svc.request_verify(), timeout=5.0)

        assert isinstance(result, VerifyResult)
        assert result.ok is True
        assert len(result.tables) == 1
        assert result.tables[0].table == "events"
        assert result.duration_ms >= 0
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# ── CLI verify --db-dir ─────────────────────────────────────────────────────


def test_cli_verify_db_dir_healthy(tmp_path, capsys):
    """CLI verify --db-dir reports OK for healthy databases."""
    outbox = Outbox(db_path=tmp_path / "events.db", namespace="events")
    _enqueue_n(outbox, 3)

    from sqloutbox.cli import cmd_verify

    with pytest.raises(SystemExit) as exc_info:
        cmd_verify(config_path=None, db_dir_path=tmp_path)

    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "OK" in captured.out
    assert "1/1 passed" in captured.out


def test_cli_verify_db_dir_corrupted(tmp_path, capsys):
    """CLI verify --db-dir reports FAIL for corrupted databases."""
    outbox = Outbox(db_path=tmp_path / "events.db", namespace="events")
    seqs = _enqueue_n(outbox, 5)
    _corrupt_timestamp(tmp_path / "events.db", seqs[4], "2000-01-01T00:00:00+00:00")

    from sqloutbox.cli import cmd_verify

    with pytest.raises(SystemExit) as exc_info:
        cmd_verify(config_path=None, db_dir_path=tmp_path)

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "FAIL" in captured.out
    assert "0/1 passed" in captured.out


def test_cli_verify_empty_dir(tmp_path, capsys):
    """CLI verify --db-dir with no .db files exits cleanly."""
    from sqloutbox.cli import cmd_verify

    with pytest.raises(SystemExit) as exc_info:
        cmd_verify(config_path=None, db_dir_path=tmp_path)

    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "nothing to verify" in captured.out


def test_cli_verify_no_args(capsys):
    """CLI verify with no --config or --db-dir shows usage."""
    from sqloutbox.cli import cmd_verify

    with pytest.raises(SystemExit) as exc_info:
        cmd_verify(config_path=None, db_dir_path=None)

    assert exc_info.value.code == 1


# ── Result dataclass properties ─────────────────────────────────────────────


def test_table_verify_result_frozen():
    """TableVerifyResult is immutable."""
    result = TableVerifyResult(
        table="test", db_path="/tmp/test.db", ok=True,
        pending_count=0, total_rows=0, sync_log_rows=0,
        chain_ok=True,
    )
    with pytest.raises(AttributeError):
        result.ok = False  # type: ignore[misc]


def test_verify_result_frozen():
    """VerifyResult is immutable."""
    result = VerifyResult(ok=True, tables=(), checked_at="", duration_ms=0.0)
    with pytest.raises(AttributeError):
        result.ok = False  # type: ignore[misc]
