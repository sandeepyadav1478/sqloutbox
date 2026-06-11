"""WS-1 + WS-2: honest ordered delivery, backoff, classification, dead-letter."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from sqloutbox._schema import open_write_conn


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def test_outbox_queue_has_retry_columns(tmp_path: Path):
    conn = open_write_conn(tmp_path / "t.db")
    try:
        cols = _columns(conn, "outbox_queue")
        assert {"attempts", "last_attempt_at", "last_error", "last_error_class"} <= cols
        # attempts defaults to 0 NOT NULL.
        conn.execute(
            "INSERT INTO outbox_queue (created_at, namespace, source, tag, payload, prev_seq) "
            "VALUES ('2026-01-01T00:00:00+00:00', 'ns', '', 'SQL', '[]', NULL)"
        )
        row = conn.execute(
            "SELECT attempts, last_attempt_at, last_error, last_error_class "
            "FROM outbox_queue"
        ).fetchone()
        assert row == (0, None, None, None)
    finally:
        conn.close()


def test_outbox_dead_log_table_created(tmp_path: Path):
    conn = open_write_conn(tmp_path / "t.db")
    try:
        assert _table_exists(conn, "outbox_dead_log")
        cols = _columns(conn, "outbox_dead_log")
        assert cols == {
            "seq", "namespace", "tag", "payload", "prev_seq", "source",
            "attempts", "last_error", "last_error_class",
            "dead_lettered_at", "reason",
        }
    finally:
        conn.close()


def test_schema_migration_idempotent(tmp_path: Path):
    # Re-opening the same DB must not raise (ALTERs are wrapped; CREATE IF NOT EXISTS).
    p = tmp_path / "t.db"
    open_write_conn(p).close()
    conn = open_write_conn(p)
    try:
        assert {"attempts", "last_attempt_at", "last_error", "last_error_class"} <= _columns(
            conn, "outbox_queue"
        )
        assert _table_exists(conn, "outbox_dead_log")
    finally:
        conn.close()


from sqloutbox._models import DeadRow, QueueRow
from sqloutbox._outbox import Outbox


def test_queue_row_has_retry_fields_defaulted():
    # Old call sites (5 positional args) must still construct.
    r = QueueRow(seq=1, tag="SQL", payload=b"[]", prev_seq=None, source="src")
    assert r.attempts == 0
    assert r.last_attempt_at is None
    assert r.last_error is None
    assert r.last_error_class is None


def test_fetch_unsynced_populates_retry_fields(tmp_path: Path):
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    seq = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")
    # Simulate two prior failed attempts persisted on the row.
    conn = ob._write_conn
    conn.execute(
        "UPDATE outbox_queue SET attempts=2, last_attempt_at='2026-06-11T00:00:00+00:00', "
        "last_error='boom', last_error_class='TRANSIENT' WHERE seq=?",
        (seq,),
    )
    conn.commit()
    rows = ob.fetch_unsynced()
    assert len(rows) == 1
    r = rows[0]
    assert r.attempts == 2
    assert r.last_attempt_at == "2026-06-11T00:00:00+00:00"
    assert r.last_error == "boom"
    assert r.last_error_class == "TRANSIENT"


def test_dead_row_is_frozen():
    d = DeadRow(
        seq=5, namespace="evt", tag="SQL", payload=b"[]", prev_seq=4, source="s",
        attempts=10, last_error="boom", last_error_class="DETERMINISTIC",
        dead_lettered_at="2026-06-11T00:00:00+00:00", reason="max_attempts",
    )
    assert d.reason == "max_attempts"
    import dataclasses
    try:
        d.seq = 6  # type: ignore[misc]
        raise AssertionError("DeadRow should be frozen")
    except dataclasses.FrozenInstanceError:
        pass


def test_peek_head_returns_lowest_seq_unsynced(tmp_path: Path):
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    s1 = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")
    ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[2]")
    head = ob.peek_head()
    assert head is not None
    assert head.seq == s1
    assert head.attempts == 0


def test_peek_head_none_when_empty(tmp_path: Path):
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    assert ob.peek_head() is None


def test_record_attempt_increments_and_persists(tmp_path: Path):
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    seq = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")
    ob.record_attempt(seq, error="connection refused", error_class="TRANSIENT")
    head = ob.peek_head()
    assert head is not None
    assert head.attempts == 1
    assert head.last_error == "connection refused"
    assert head.last_error_class == "TRANSIENT"
    assert head.last_attempt_at is not None
    # A second failure increments again.
    ob.record_attempt(seq, error="still down", error_class="TRANSIENT")
    assert ob.peek_head().attempts == 2


def test_seq_accounted_consults_dead_log(tmp_path: Path):
    import sqlite3
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    # Manually insert a dead-lettered row at seq=7 (no queue/sync_log row exists).
    conn = ob._write_conn
    conn.execute(
        "INSERT INTO outbox_dead_log "
        "(seq, namespace, tag, payload, prev_seq, source, attempts, "
        " last_error, last_error_class, dead_lettered_at, reason) "
        "VALUES (7, 'evt', 'SQL', '[]', NULL, 's', 10, 'boom', 'UNKNOWN', "
        "        '2026-06-11T00:00:00+00:00', 'max_attempts')"
    )
    conn.commit()
    with sqlite3.connect(str(ob.db_path)) as c:
        assert ob._seq_accounted(c, 7) is True
        assert ob._seq_accounted(c, 999) is False
