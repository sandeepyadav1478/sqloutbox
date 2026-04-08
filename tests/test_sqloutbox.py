"""Unit tests for the sqloutbox package — full coverage of the outbox mechanics.

Tests the core sqloutbox guarantees:
    - enqueue()           single-event insertion, prev_seq chain linking
    - enqueue_batch()     atomic multi-event insertion in one SQLite commit
    - fetch_unsynced()    strict insertion-order retrieval, namespace isolation
    - verify_chain()      correct chain passes, gap detection, sync_log bridging
    - mark_synced()       marks rows as synced in batch
    - delete_synced()     refuses to delete unsynced rows, records in sync_log
    - prune_sync_log()    deletes old delivered entries
    - pending_count()     accurate count of unsynced rows
    - namespace isolation two namespaces in the same DB file never interfere
    - two-outbox sharing  two Outbox instances on the same file+namespace

No external dependencies — all SQLite, no network.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from sqloutbox import Outbox, QueueRow


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def db_path(tmp_path) -> Path:
    return tmp_path / "test_outbox.db"


@pytest.fixture
def outbox(db_path) -> Outbox:
    return Outbox(db_path=db_path, namespace="test-ns")


# ── Schema initialisation ─────────────────────────────────────────────────────

def test_schema_created_on_init(db_path):
    """Outbox creates all required tables and indexes on first use."""
    Outbox(db_path=db_path, namespace="ns")
    conn = sqlite3.connect(db_path)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "outbox_queue"    in tables
    assert "outbox_sync_log" in tables
    conn.close()


# ── enqueue() ─────────────────────────────────────────────────────────────────

def test_enqueue_returns_seq(outbox):
    """enqueue() returns the assigned integer seq."""
    seq = outbox.enqueue("tag", b"payload")
    assert isinstance(seq, int)
    assert seq >= 1


def test_enqueue_first_row_has_no_prev(outbox):
    """First row in a namespace has prev_seq=None (chain head)."""
    outbox.enqueue("t", b"a")
    rows = outbox.fetch_unsynced()
    assert len(rows) == 1
    assert rows[0].prev_seq is None


def test_enqueue_chain_links(outbox):
    """Each row's prev_seq points to the previous row's seq."""
    seqs = [outbox.enqueue("t", f"payload-{i}".encode()) for i in range(5)]
    rows = outbox.fetch_unsynced()
    assert len(rows) == 5
    # First row: no predecessor
    assert rows[0].prev_seq is None
    # Each subsequent row links to the one before it
    for i in range(1, 5):
        assert rows[i].prev_seq == seqs[i - 1], (
            f"row[{i}].prev_seq={rows[i].prev_seq} expected={seqs[i-1]}"
        )


def test_enqueue_payload_roundtrip(outbox):
    """Payload bytes are stored and retrieved exactly (UTF-8 text, matches middleware usage)."""
    # The outbox stores payload as TEXT decoded from UTF-8 — matching how the
    # middleware always sends JSON-encoded events (valid UTF-8 by definition).
    payload = b'{"loan_id": "LOA-123", "stage": "criteria", "env": "test"}'
    outbox.enqueue("RejectionEvent", payload)
    rows = outbox.fetch_unsynced()
    assert len(rows) == 1
    assert rows[0].payload == payload
    assert rows[0].tag == "RejectionEvent"


# ── enqueue_batch() ───────────────────────────────────────────────────────────

def test_enqueue_batch_atomicity(db_path):
    """enqueue_batch() inserts N rows in a single SQLite transaction."""
    ob = Outbox(db_path=db_path, namespace="batch-ns")
    items = [("t", f"msg-{i}".encode()) for i in range(10)]
    seqs = ob.enqueue_batch(items)
    assert len(seqs) == 10
    rows = ob.fetch_unsynced()
    assert len(rows) == 10


def test_enqueue_batch_chain_correct(outbox):
    """enqueue_batch builds a valid chain across all batch items."""
    items = [(f"tag-{i}", f"p-{i}".encode()) for i in range(4)]
    seqs = outbox.enqueue_batch(items)
    rows = outbox.fetch_unsynced()
    # First: no predecessor
    assert rows[0].prev_seq is None
    # Each links to previous
    for i in range(1, 4):
        assert rows[i].prev_seq == seqs[i - 1]


def test_enqueue_batch_extends_existing_chain(outbox):
    """Batch appended after single enqueue correctly extends the chain."""
    single_seq = outbox.enqueue("first", b"a")
    batch_seqs = outbox.enqueue_batch([("b", b"x"), ("c", b"y")])

    rows = outbox.fetch_unsynced()
    assert len(rows) == 3
    assert rows[0].seq == single_seq
    assert rows[1].prev_seq == single_seq      # batch item 0 → single
    assert rows[2].prev_seq == batch_seqs[0]   # batch item 1 → batch item 0


def test_enqueue_batch_empty_is_noop(outbox):
    """Empty batch returns empty list without touching the DB."""
    result = outbox.enqueue_batch([])
    assert result == []
    assert outbox.pending_count() == 0


# ── fetch_unsynced() ──────────────────────────────────────────────────────────

def test_fetch_unsynced_strict_order(outbox):
    """fetch_unsynced returns rows in ascending seq (insertion) order."""
    for i in range(5):
        outbox.enqueue("t", f"{i}".encode())
    rows = outbox.fetch_unsynced()
    seqs = [r.seq for r in rows]
    assert seqs == sorted(seqs)


def test_fetch_unsynced_limit(outbox):
    """fetch_unsynced respects the limit parameter."""
    for _ in range(10):
        outbox.enqueue("t", b"x")
    rows = outbox.fetch_unsynced(limit=3)
    assert len(rows) == 3


def test_fetch_unsynced_excludes_synced(outbox):
    """Rows marked synced do not appear in fetch_unsynced."""
    seq = outbox.enqueue("t", b"payload")
    outbox.mark_synced([seq])
    rows = outbox.fetch_unsynced()
    assert len(rows) == 0


# ── namespace isolation ───────────────────────────────────────────────────────

def test_namespace_isolation(db_path):
    """Two namespaces in the same file never see each other's rows."""
    ob_a = Outbox(db_path=db_path, namespace="ns-A")
    ob_b = Outbox(db_path=db_path, namespace="ns-B")

    ob_a.enqueue("t", b"for-A")
    ob_a.enqueue("t", b"also-A")
    ob_b.enqueue("t", b"for-B")

    rows_a = ob_a.fetch_unsynced()
    rows_b = ob_b.fetch_unsynced()

    assert len(rows_a) == 2
    assert len(rows_b) == 1
    assert all(r.payload in (b"for-A", b"also-A") for r in rows_a)
    assert rows_b[0].payload == b"for-B"


def test_namespace_chain_independent(db_path):
    """Each namespace has its own independent chain (prev_seq tracking)."""
    ob_a = Outbox(db_path=db_path, namespace="ns-A")
    ob_b = Outbox(db_path=db_path, namespace="ns-B")

    # Interleave inserts
    s_a1 = ob_a.enqueue("t", b"a1")
    s_b1 = ob_b.enqueue("t", b"b1")
    s_a2 = ob_a.enqueue("t", b"a2")

    rows_a = ob_a.fetch_unsynced()
    assert rows_a[0].prev_seq is None         # a1 has no predecessor in ns-A
    assert rows_a[1].prev_seq == s_a1         # a2 links to a1, NOT b1

    rows_b = ob_b.fetch_unsynced()
    assert rows_b[0].prev_seq is None         # b1 has no predecessor in ns-B


# ── verify_chain() ────────────────────────────────────────────────────────────

def test_verify_chain_intact(outbox):
    """A freshly inserted chain verifies as intact."""
    for _ in range(5):
        outbox.enqueue("t", b"x")
    rows = outbox.fetch_unsynced()
    ok, gaps = outbox.verify_chain(rows)
    assert ok is True
    assert gaps == []


def test_verify_chain_empty_rows(outbox):
    """Empty row list is a valid (vacuously true) chain."""
    ok, gaps = outbox.verify_chain([])
    assert ok is True
    assert gaps == []


def test_verify_chain_detects_gap(db_path):
    """verify_chain returns False + gap seq when a row is missing from the chain."""
    ob = Outbox(db_path=db_path, namespace="gap-ns")
    s1 = ob.enqueue("t", b"a")
    s2 = ob.enqueue("t", b"b")
    s3 = ob.enqueue("t", b"c")

    # Surgically delete row s2 from the DB (simulates data loss)
    conn = sqlite3.connect(db_path)
    conn.execute("DELETE FROM outbox_queue WHERE seq = ?", (s2,))
    conn.commit()
    conn.close()

    rows = ob.fetch_unsynced()           # returns [s1, s3] (s2 missing)
    ok, gaps = ob.verify_chain(rows)
    assert ok is False

    # verify_chain checks batch-consecutive linkage: rows[i].prev_seq == rows[i-1].seq
    # s3 (seq=3) follows s1 (seq=1) in the batch, but s3.prev_seq=2 ≠ s1.seq=1
    # → gap reported at rows[i-1].seq = s1 (the expected predecessor of s3)
    assert s1 in gaps, (
        f"Expected gap at s1={s1} (s3's expected batch predecessor); got gaps={gaps}"
    )


def test_verify_chain_sync_log_bridges_gap(db_path):
    """Rows delivered in a previous cycle bridge the gap in verify_chain."""
    ob = Outbox(db_path=db_path, namespace="bridge-ns")
    s1 = ob.enqueue("t", b"a")
    s2 = ob.enqueue("t", b"b")
    s3 = ob.enqueue("t", b"c")

    # Simulate: s1 and s2 were delivered and deleted, sync_log records them
    ob.mark_synced([s1, s2])
    ob.delete_synced([s1, s2])

    rows = ob.fetch_unsynced()           # only s3 remains
    ok, gaps = ob.verify_chain(rows)
    # s3.prev_seq = s2, which is in sync_log — should be bridged
    assert ok is True, f"Chain should be bridged by sync_log; gaps={gaps}"


# ── mark_synced() + delete_synced() ──────────────────────────────────────────

def test_mark_synced(outbox):
    """mark_synced sets synced=1 on the target rows."""
    seqs = [outbox.enqueue("t", b"x") for _ in range(3)]
    outbox.mark_synced([seqs[0], seqs[2]])

    rows = outbox.fetch_unsynced()
    remaining_seqs = {r.seq for r in rows}
    assert seqs[0] not in remaining_seqs   # marked synced → hidden from fetch
    assert seqs[1] in remaining_seqs       # not marked → still pending
    assert seqs[2] not in remaining_seqs


def test_delete_synced_records_to_sync_log(db_path):
    """delete_synced records deleted seqs in outbox_sync_log."""
    ob = Outbox(db_path=db_path, namespace="del-ns")
    seq = ob.enqueue("t", b"x")
    ob.mark_synced([seq])
    ob.delete_synced([seq])

    conn = sqlite3.connect(db_path)
    logged = {r[0] for r in conn.execute(
        "SELECT seq FROM outbox_sync_log WHERE namespace = 'del-ns'"
    ).fetchall()}
    conn.close()

    assert seq in logged


def test_delete_synced_removes_from_queue(outbox):
    """delete_synced removes the row from outbox_queue."""
    seq = outbox.enqueue("t", b"x")
    outbox.mark_synced([seq])
    outbox.delete_synced([seq])

    rows = outbox.fetch_unsynced()
    assert not any(r.seq == seq for r in rows)
    assert outbox.pending_count() == 0


def test_delete_synced_refuses_unsynced(outbox, capsys):
    """delete_synced refuses to delete rows that haven't been synced."""
    seq = outbox.enqueue("t", b"x")
    # Do NOT call mark_synced — try to delete directly
    outbox.delete_synced([seq])

    # Row must still be in the queue (refused, not deleted)
    rows = outbox.fetch_unsynced()
    assert any(r.seq == seq for r in rows), (
        "Unsynced row was deleted — safety guard failed!"
    )


def test_delete_synced_idempotent(outbox):
    """Calling delete_synced twice on the same seq does not crash."""
    seq = outbox.enqueue("t", b"x")
    outbox.mark_synced([seq])
    outbox.delete_synced([seq])
    outbox.delete_synced([seq])   # second call — row already gone, must not raise


# ── pending_count() ───────────────────────────────────────────────────────────

def test_pending_count_tracks_correctly(outbox):
    """pending_count reflects exactly the number of unsynced rows."""
    assert outbox.pending_count() == 0

    outbox.enqueue("t", b"a")
    outbox.enqueue("t", b"b")
    assert outbox.pending_count() == 2

    outbox.enqueue("t", b"c")
    assert outbox.pending_count() == 3

    rows = outbox.fetch_unsynced()
    outbox.mark_synced([rows[0].seq])
    assert outbox.pending_count() == 2   # one synced, but not yet deleted

    outbox.delete_synced([rows[0].seq])
    assert outbox.pending_count() == 2   # delete doesn't change pending count


def test_pending_count_namespace_isolated(db_path):
    """pending_count only counts rows for the instance's own namespace."""
    ob_a = Outbox(db_path=db_path, namespace="cnt-A")
    ob_b = Outbox(db_path=db_path, namespace="cnt-B")

    ob_a.enqueue("t", b"x")
    ob_a.enqueue("t", b"y")
    ob_b.enqueue("t", b"z")

    assert ob_a.pending_count() == 2
    assert ob_b.pending_count() == 1


# ── prune_sync_log() ─────────────────────────────────────────────────────────

def test_prune_sync_log_removes_old_entries(db_path):
    """prune_sync_log deletes entries older than retain_log_days."""
    from datetime import datetime, timedelta, timezone
    ob = Outbox(db_path=db_path, namespace="prune-ns", retain_log_days=7)

    seq = ob.enqueue("t", b"x")
    ob.mark_synced([seq])
    ob.delete_synced([seq])

    # Back-date the sync_log entry to simulate it being old
    old_ts = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE outbox_sync_log SET synced_at = ? WHERE seq = ?",
        (old_ts, seq),
    )
    conn.commit()
    conn.close()

    ob.prune_sync_log()

    conn = sqlite3.connect(db_path)
    remaining = conn.execute(
        "SELECT COUNT(*) FROM outbox_sync_log WHERE namespace = 'prune-ns'"
    ).fetchone()[0]
    conn.close()

    assert remaining == 0, f"Expected 0 after prune, got {remaining}"


def test_prune_sync_log_keeps_recent(db_path):
    """prune_sync_log keeps entries newer than retain_log_days."""
    ob = Outbox(db_path=db_path, namespace="keep-ns", retain_log_days=7)

    seq = ob.enqueue("t", b"x")
    ob.mark_synced([seq])
    ob.delete_synced([seq])   # synced_at = now

    ob.prune_sync_log()       # nothing should be pruned

    conn = sqlite3.connect(db_path)
    remaining = conn.execute(
        "SELECT COUNT(*) FROM outbox_sync_log WHERE namespace = 'keep-ns'"
    ).fetchone()[0]
    conn.close()

    assert remaining == 1, "Recent sync_log entry was pruned prematurely"


# ── QueueRow dataclass ────────────────────────────────────────────────────────

def test_queue_row_immutable():
    """QueueRow is frozen (immutable after construction)."""
    row = QueueRow(seq=1, tag="t", payload=b"x", prev_seq=None)
    with pytest.raises((AttributeError, TypeError)):
        row.seq = 2  # type: ignore[misc]


def test_queue_row_fields():
    """QueueRow carries all expected fields."""
    row = QueueRow(seq=42, tag="event.created", payload=b"data", prev_seq=10)
    assert row.seq == 42
    assert row.tag == "event.created"
    assert row.payload == b"data"
    assert row.prev_seq == 10


# ── Full cycle simulation ─────────────────────────────────────────────────────

def test_full_delivery_cycle(db_path):
    """Simulate a complete produce → fetch → verify → mark → delete cycle."""
    ob = Outbox(db_path=db_path, namespace="cycle-ns")

    # Producer inserts 4 events
    ob.enqueue_batch([("e", f"msg-{i}".encode()) for i in range(4)])
    assert ob.pending_count() == 4

    # Consumer fetches
    rows = ob.fetch_unsynced()
    assert len(rows) == 4

    # Consumer verifies chain integrity
    ok, gaps = ob.verify_chain(rows)
    assert ok is True

    # Consumer delivers (simulate network call) then marks synced
    seqs = [r.seq for r in rows]
    ob.mark_synced(seqs)

    # Consumer deletes confirmed rows
    ob.delete_synced(seqs)

    # Queue is empty; sync_log has all 4 entries
    assert ob.pending_count() == 0
    rows_after = ob.fetch_unsynced()
    assert len(rows_after) == 0

    conn = sqlite3.connect(db_path)
    n_log = conn.execute(
        "SELECT COUNT(*) FROM outbox_sync_log WHERE namespace = 'cycle-ns'"
    ).fetchone()[0]
    conn.close()
    assert n_log == 4


def test_full_cycle_next_batch_fresh_chain(db_path):
    """After all rows are delivered+deleted, the next batch starts a fresh chain.

    enqueue() uses MAX(seq) FROM outbox_queue for prev_seq. When the queue is
    fully drained, MAX returns NULL so the new batch starts with prev_seq=None
    (new chain head). This is by design — a fresh chain after a clean delivery
    needs no sync_log bridging and verify_chain passes immediately.
    """
    ob = Outbox(db_path=db_path, namespace="next-ns")

    # First batch: 3 events, fully deliver
    first_seqs = ob.enqueue_batch([("t", b"a"), ("t", b"b"), ("t", b"c")])
    ob.mark_synced(first_seqs)
    ob.delete_synced(first_seqs)

    # Queue is now empty — next batch starts a fresh chain
    second_seqs = ob.enqueue_batch([("t", b"d"), ("t", b"e")])
    rows = ob.fetch_unsynced()
    assert len(rows) == 2

    # First row of new batch: prev_seq=None (fresh head, queue was empty)
    assert rows[0].prev_seq is None, (
        f"After full queue drain, new chain should start with prev_seq=None; "
        f"got {rows[0].prev_seq}"
    )
    # Internal link within the new batch is correct
    assert rows[1].prev_seq == second_seqs[0]

    # Chain verifies cleanly (no sync_log bridging needed — fresh head)
    ok, gaps = ob.verify_chain(rows)
    assert ok is True, f"Fresh chain should verify cleanly; gaps={gaps}"
