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


def test_dead_letter_moves_row_atomically(tmp_path: Path):
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    seq = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]", source="prod")
    ob.record_attempt(seq, error="boom", error_class="DETERMINISTIC")
    ob.dead_letter(seq, reason="max_attempts")

    # Row gone from the queue, present in dead_log with full metadata.
    assert ob.pending_count() == 0
    dead = ob.list_dead()
    assert len(dead) == 1
    d = dead[0]
    assert d.seq == seq
    assert d.namespace == "evt"
    assert d.tag == "INSERT INTO evt (a) VALUES (?)"
    assert d.payload == b"[1]"
    assert d.source == "prod"
    assert d.attempts == 1
    assert d.last_error == "boom"
    assert d.last_error_class == "DETERMINISTIC"
    assert d.reason == "max_attempts"
    assert d.dead_lettered_at is not None


def test_dead_letter_unknown_seq_is_noop(tmp_path: Path):
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    # No such row — must not raise, must not create a dead_log entry.
    ob.dead_letter(999, reason="manual_skip")
    assert ob.list_dead() == []


def test_replay_reenqueues_at_tail_with_new_seq(tmp_path: Path):
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    s1 = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")
    s2 = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[2]")
    # Dead-letter the head (s1), then advance the chain past it by leaving s2.
    ob.dead_letter(s1, reason="manual_skip")

    new_seq = ob.replay(s1)
    assert new_seq is not None
    assert new_seq > s2  # at the tail, a brand-new seq (old seq never reused)

    # Removed from dead_log, present again in the queue with the original payload.
    assert ob.list_dead() == []
    rows = ob.fetch_unsynced()
    payloads = {r.payload for r in rows}
    assert b"[1]" in payloads
    # The replayed row links to the previous tail (s2), a valid new chain link.
    replayed = next(r for r in rows if r.seq == new_seq)
    assert replayed.prev_seq == s2


def test_get_dead_returns_one_row(tmp_path: Path):
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    seq = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[9]")
    ob.dead_letter(seq, reason="undecodable")
    d = ob.get_dead(seq)
    assert d is not None and d.reason == "undecodable"
    assert ob.get_dead(12345) is None


from sqloutbox.sync import classify_write_error


def test_classify_transient():
    for msg in ["connection reset", "HTTP 503 Service Unavailable",
                "request timed out", "database is locked"]:
        assert classify_write_error(msg) == "TRANSIENT"


def test_classify_deterministic():
    for msg in ["FOREIGN KEY constraint failed", "NOT NULL constraint failed",
                "no such column: foo", "no such table: bar",
                "syntax error near \"VALUE\""]:
        assert classify_write_error(msg) == "DETERMINISTIC"


def test_classify_already_applied():
    for msg in ["UNIQUE constraint failed: events.outbox_seq",
                "duplicate key value violates unique constraint"]:
        assert classify_write_error(msg) == "ALREADY_APPLIED"


def test_classify_unknown():
    assert classify_write_error("") == "UNKNOWN"
    assert classify_write_error("some wholly unrecognised message") == "UNKNOWN"
    assert classify_write_error(None) == "UNKNOWN"


import asyncio

import pytest

from sqloutbox.config import OutboxConfig, TargetConfig
from sqloutbox.sync import OutboxSyncService


def _service(tmp_path, writer, *, tables=("evt",), max_attempts=10):
    # auto_schema=False + inject_outbox_seq=False so startup _ensure_schema()/
    # _seed_from_remote() do NOT call write_batch() and pollute writer.seen.
    cfg = OutboxConfig(
        db_dir=tmp_path,
        targets=(TargetConfig(name="primary", tables=tables,
                              inject_outbox_seq=False),),
        flush_interval=0.01,
        table_flush_threshold=1,
        table_max_wait=0.0,
        auto_schema=False,
        max_attempts=max_attempts,
    )
    return OutboxSyncService(config=cfg, writers={"primary": writer}), cfg


class _SeqWriter:
    """Writer whose ok/err verdict per stmt is driven by an index→verdict map.

    Verdict is matched on the position of the stmt within the batch it receives.
    """
    def __init__(self, verdicts):
        # verdicts: list of dicts, applied in order to each call's stmts.
        self.verdicts = verdicts
        self.seen = []

    async def write_batch(self, stmts):
        self.seen.extend(stmts)
        return [self.verdicts[i] for i in range(len(stmts))]


@pytest.mark.asyncio
async def test_head_hold_no_leapfrog(tmp_path: Path):
    """Head fails, a later row in the same namespace must NOT be confirmed."""
    # Writer: first stmt fails (TRANSIENT), second would succeed.
    writer = _SeqWriter([
        {"ok": False, "error": "connection reset"},
        {"ok": True, "rows_affected": 1},
    ])
    svc, cfg = _service(tmp_path, writer)
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    s1 = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")
    s2 = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[2]")

    # Drive ONE flush directly (deterministic — no loop timing).
    stmts = [("INSERT INTO evt (a) VALUES (?)", [1]),
             ("INSERT INTO evt (a) VALUES (?)", [2])]
    stmt_info = [("evt", s1), ("evt", s2)]
    await svc._flush_to_target(
        writer, stmts, stmt_info,
        svc._target_outboxes["primary"], "primary", 0.0,
    )

    # NEITHER row confirmed: s1 failed (held), s2 must not leapfrog it.
    assert ob.pending_count() == 2
    # The head recorded one failed attempt; the successor recorded none.
    head = ob.peek_head()
    assert head.seq == s1 and head.attempts == 1
    assert head.last_error_class == "TRANSIENT"


@pytest.mark.asyncio
async def test_head_success_advances(tmp_path: Path):
    """Head succeeds → confirmed + deleted; successor becomes the new head."""
    writer = _SeqWriter([
        {"ok": True, "rows_affected": 1},
        {"ok": True, "rows_affected": 1},
    ])
    svc, cfg = _service(tmp_path, writer)
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    s1 = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")
    s2 = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[2]")

    await svc._flush_to_target(
        writer,
        [("INSERT INTO evt (a) VALUES (?)", [1]),
         ("INSERT INTO evt (a) VALUES (?)", [2])],
        [("evt", s1), ("evt", s2)],
        svc._target_outboxes["primary"], "primary", 0.0,
    )
    assert ob.pending_count() == 0  # both delivered in order


@pytest.mark.asyncio
async def test_auto_dead_letter_at_max_attempts(tmp_path: Path):
    """Head at attempts==max_attempts-1 fails once more → dead-lettered; ns unblocks."""
    writer = _SeqWriter([{"ok": False, "error": "no such column: x"}])
    svc, cfg = _service(tmp_path, writer, max_attempts=3)
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    s1 = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")
    # Pre-seed two prior failures (attempts=2). This flush is the 3rd → hits cap.
    ob.record_attempt(s1, error="x", error_class="DETERMINISTIC")
    ob.record_attempt(s1, error="x", error_class="DETERMINISTIC")

    await svc._flush_to_target(
        writer, [("INSERT INTO evt (a) VALUES (?)", [1])],
        [("evt", s1)],
        svc._target_outboxes["primary"], "primary", 0.0,
    )

    # Moved to dead_log, queue empty, namespace unblocked.
    assert ob.pending_count() == 0
    dead = ob.list_dead()
    assert len(dead) == 1 and dead[0].seq == s1
    assert dead[0].reason == "max_attempts"
    assert dead[0].attempts == 3


@pytest.mark.asyncio
async def test_max_attempts_none_plateaus_forever(tmp_path: Path):
    """max_attempts=None never auto-dead-letters; the head just keeps holding."""
    writer = _SeqWriter([{"ok": False, "error": "no such column: x"}])
    svc, cfg = _service(tmp_path, writer, max_attempts=None)
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    s1 = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")
    for _ in range(20):
        await svc._flush_to_target(
            writer, [("INSERT INTO evt (a) VALUES (?)", [1])],
            [("evt", s1)],
            svc._target_outboxes["primary"], "primary", 0.0,
        )
    # Still in the queue, never dead-lettered.
    assert ob.pending_count() == 1
    assert ob.list_dead() == []
    assert ob.peek_head().attempts == 20


@pytest.mark.asyncio
async def test_already_applied_advances_head(tmp_path: Path):
    """A UNIQUE-collision result advances the head (data present = success)."""
    writer = _SeqWriter([{"ok": False, "error": "UNIQUE constraint failed: evt.outbox_seq"}])
    svc, cfg = _service(tmp_path, writer)
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    s1 = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")

    await svc._flush_to_target(
        writer, [("INSERT INTO evt (a) VALUES (?)", [1])],
        [("evt", s1)],
        svc._target_outboxes["primary"], "primary", 0.0,
    )
    # Head advanced (treated as delivered); nothing dead-lettered.
    assert ob.pending_count() == 0
    assert ob.list_dead() == []


from datetime import datetime, timedelta, timezone


def _set_head_backoff(ob: Outbox, seq: int, attempts: int, last_attempt_at: str):
    conn = ob._write_conn
    conn.execute(
        "UPDATE outbox_queue SET attempts=?, last_attempt_at=?, "
        "last_error='boom', last_error_class='TRANSIENT' WHERE seq=?",
        (attempts, last_attempt_at, seq),
    )
    conn.commit()


@pytest.mark.asyncio
async def test_backoff_gate_suppresses_table_before_eligible(tmp_path: Path):
    """A head in backoff is NOT re-sent before next_eligible, even with max_wait=0."""
    writer = _SeqWriter([])  # would record a stmt if the gate let the table through
    svc, cfg = _service(tmp_path, writer)
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    s1 = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")
    # attempts=3 → delay = 2^3 = 8 min; last attempt was 1 minute ago → NOT eligible.
    recent = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    _set_head_backoff(ob, s1, attempts=3, last_attempt_at=recent)

    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.15)   # several cycles
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Suppressed: writer never saw the data row.
    assert writer.seen == []
    assert ob.pending_count() == 1


@pytest.mark.asyncio
async def test_backoff_gate_allows_after_elapsed(tmp_path: Path):
    """Once next_eligible has passed, the head is retried."""
    writer = _SeqWriter([{"ok": True, "rows_affected": 1}])
    svc, cfg = _service(tmp_path, writer)
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    s1 = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")
    # attempts=1 → delay = 2 min; last attempt 10 min ago → ELIGIBLE.
    old = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    _set_head_backoff(ob, s1, attempts=1, last_attempt_at=old)

    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert ob.pending_count() == 0  # retried and delivered


@pytest.mark.asyncio
async def test_stuck_head_fetches_only_head(tmp_path: Path):
    """While a head is stuck (attempts>0) and eligible, only ONE row is fetched."""
    sent_batches = []

    class _Capture:
        async def write_batch(self, stmts):
            sent_batches.append(list(stmts))
            return [{"ok": False, "error": "connection reset"} for _ in stmts]

    writer = _Capture()
    svc, cfg = _service(tmp_path, writer)
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    s1 = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")
    s2 = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[2]")
    s3 = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[3]")
    # Head stuck but eligible (attempts=1, last attempt long ago).
    old = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    _set_head_backoff(ob, s1, attempts=1, last_attempt_at=old)

    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.15)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Every batch the writer saw contained exactly ONE row (head-only fetch).
    assert sent_batches, "writer should have been called at least once"
    assert all(len(b) == 1 for b in sent_batches)


def _enqueue_then_corrupt(db_path: Path, namespace: str, tag: str, raw: bytes) -> int:
    """Enqueue a valid row, then overwrite its payload with non-JSON bytes."""
    ob = Outbox(db_path=db_path, namespace=namespace)
    seq = ob.enqueue(tag, b"{}")
    conn = ob._write_conn
    conn.execute("UPDATE outbox_queue SET payload=? WHERE seq=?",
                 (raw.decode("latin-1"), seq))
    conn.commit()
    return seq


@pytest.mark.asyncio
async def test_undecodable_row_is_dead_lettered(tmp_path: Path):
    """A non-JSON payload is dead-lettered (reason='undecodable'), not retried forever."""
    writer = _SeqWriter([{"ok": True, "rows_affected": 1}])
    svc, cfg = _service(tmp_path, writer)
    db_path = tmp_path / "evt.db"
    seq = _enqueue_then_corrupt(db_path, "evt",
                                "INSERT INTO evt (a) VALUES (?)", b"not json{{{")

    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    ob = Outbox(db_path=db_path, namespace="evt")
    assert ob.pending_count() == 0          # no longer stuck in the queue
    dead = ob.list_dead()
    assert len(dead) == 1
    assert dead[0].seq == seq
    assert dead[0].reason == "undecodable"
    assert writer.seen == []                # the bad row was never sent


from sqloutbox.cli import cmd_dead_letter, cmd_skip


def _write_toml(tmp_path: Path, db_dir: Path) -> Path:
    toml = tmp_path / "outbox.toml"
    toml.write_text(
        f'[app.t]\n'
        f'db_dir = "{db_dir.as_posix()}"\n\n'
        f'[app.t.db.primary]\n'
        f'writer_class = "sqloutbox.cli:TursoWriter"\n'
        f'tables = ["evt"]\n\n'
        f'[app.t.db.primary.connection]\n'
        f'db_url = "http://x"\n'
        f'db_token = "x"\n'
    )
    return toml


def test_cli_dead_letter_list_show_replay(tmp_path: Path, capsys):
    data = tmp_path / "data"
    data.mkdir()
    ob = Outbox(db_path=data / "evt.db", namespace="evt")
    seq = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]", source="prod")
    ob.dead_letter(seq, reason="max_attempts")
    toml = _write_toml(tmp_path, data)

    # list
    cmd_dead_letter(toml, action="list", namespace=None, seq=None)
    out = capsys.readouterr().out
    assert "evt" in out and str(seq) in out and "max_attempts" in out

    # show
    cmd_dead_letter(toml, action="show", namespace="evt", seq=seq)
    out = capsys.readouterr().out
    assert "INSERT INTO evt" in out and "[1]" in out

    # replay — row leaves dead_log and re-enters the queue
    cmd_dead_letter(toml, action="replay", namespace="evt", seq=seq)
    capsys.readouterr()
    assert Outbox(db_path=data / "evt.db", namespace="evt").list_dead() == []
    assert Outbox(db_path=data / "evt.db", namespace="evt").pending_count() == 1


def test_cli_skip_moves_head(tmp_path: Path, capsys):
    data = tmp_path / "data"
    data.mkdir()
    ob = Outbox(db_path=data / "evt.db", namespace="evt")
    seq = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")
    toml = _write_toml(tmp_path, data)

    cmd_skip(toml, namespace="evt", seq=seq)
    capsys.readouterr()
    ob2 = Outbox(db_path=data / "evt.db", namespace="evt")
    assert ob2.pending_count() == 0
    dead = ob2.list_dead()
    assert len(dead) == 1 and dead[0].reason == "manual_skip"
