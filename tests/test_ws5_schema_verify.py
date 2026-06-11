"""WS-5: read-only verify, crash-safe forked-chain migration, producer seed."""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from sqloutbox._outbox import Outbox


def _enqueue_n(outbox: Outbox, n: int) -> list[int]:
    """Enqueue n rows and return their seqs (mirrors tests/test_verify.py)."""
    seqs = []
    for i in range(n):
        seq = outbox.enqueue(
            tag="INSERT INTO events (id) VALUES (?)",
            payload=f"[{i}]".encode(),
            source="test",
        )
        assert seq is not None
        seqs.append(seq)
    return seqs


def _journal_mode(db_path: Path) -> str:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        return conn.execute("PRAGMA journal_mode").fetchone()[0]
    finally:
        conn.close()


# ── open_read_conn: truly read-only ────────────────────────────────────────


def test_open_read_conn_cannot_write(tmp_path: Path):
    """A read-only connection refuses writes (proves mode=ro)."""
    from sqloutbox._schema import open_read_conn, open_write_conn

    db = tmp_path / "events.db"
    open_write_conn(db).close()  # create a real outbox DB first

    conn = open_read_conn(db)
    try:
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("INSERT INTO outbox_queue (created_at, namespace, tag, payload) "
                         "VALUES ('x','n','t','p')")
    finally:
        conn.close()


def test_open_read_conn_missing_file_not_created(tmp_path: Path):
    """Opening a missing path read-only must NOT create the file."""
    from sqloutbox._schema import open_read_conn

    missing = tmp_path / "nope.db"
    with pytest.raises(sqlite3.OperationalError):
        conn = open_read_conn(missing)
        # The error fires on first access for a missing file.
        conn.execute("SELECT 1 FROM outbox_queue").fetchone()
    assert not missing.exists()


# ── verify_db_path: inspection-by-path ──────────────────────────────────────


def test_verify_db_path_missing_reports_not_an_outbox(tmp_path: Path):
    """verify on a missing path reports 'not an outbox DB', does NOT create it."""
    from sqloutbox._verify import verify_db_path

    missing = tmp_path / "ghost.db"
    result = verify_db_path(missing)
    assert result.ok is False
    assert any("not an outbox" in e.lower() for e in result.errors)
    assert not missing.exists()


def test_verify_db_path_foreign_file_reports_not_an_outbox(tmp_path: Path):
    """A real SQLite file with no outbox_queue table is reported, not migrated."""
    from sqloutbox._verify import verify_db_path

    foreign = tmp_path / "foreign.db"
    c = sqlite3.connect(str(foreign))
    c.execute("CREATE TABLE unrelated (x)")
    c.commit()
    c.close()
    mtime_before = os.path.getmtime(foreign)

    result = verify_db_path(foreign)
    assert result.ok is False
    assert any("not an outbox" in e.lower() for e in result.errors)
    # File was NOT migrated (no outbox_queue table added, mtime unchanged).
    c2 = sqlite3.connect(f"file:{foreign}?mode=ro", uri=True)
    tables = {r[0] for r in c2.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    c2.close()
    assert "outbox_queue" not in tables
    assert os.path.getmtime(foreign) == mtime_before


def test_verify_db_path_existing_db_unchanged(tmp_path: Path):
    """verify on a healthy DB does not switch journal_mode or change the file."""
    from sqloutbox._verify import verify_db_path

    db = tmp_path / "events.db"
    ob = Outbox(db_path=db, namespace="events")
    _enqueue_n(ob, 4)
    ob._write_conn.close()  # release the writer so we can snapshot cleanly

    # Snapshot: journal mode + set of sidecar files + mtime.
    jm_before = _journal_mode(db)
    files_before = sorted(p.name for p in tmp_path.iterdir())
    mtime_before = os.path.getmtime(db)

    result = verify_db_path(db)
    assert result.ok is True
    assert result.table == "events"
    assert result.total_rows == 4

    # Nothing changed: same journal mode, same files, same mtime.
    assert _journal_mode(db) == jm_before
    assert sorted(p.name for p in tmp_path.iterdir()) == files_before
    assert os.path.getmtime(db) == mtime_before


# ── Forked-chain migration ──────────────────────────────────────────────────


def _build_forked_db(db_path: Path) -> None:
    """Hand-build an outbox DB with two rows sharing the same prev_seq (a fork).

    We create the queue table WITHOUT the UNIQUE constraint / index so the fork
    can be inserted, then leave the file for open_write_conn to migrate — that
    is exactly the upgrade path that must fail safely.

    We ALSO create an (empty) ``outbox_sync_log`` table. A real forked
    production DB always has it — ``open_write_conn`` creates ``outbox_sync_log``
    (line ~90) BEFORE the UNIQUE-index creation that crashes (line ~92), so any
    DB old enough to fork already carries the sync_log table. Without it, the
    read-only ``verify_db_path`` (which SELECTs from ``outbox_sync_log``) would
    raise ``OperationalError: no such table`` instead of REPORTING the fork —
    breaking ``test_forked_db_read_only_verify_reports_without_crashing``.
    """
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE outbox_queue ("
        "  seq        INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  created_at TEXT    NOT NULL,"
        "  namespace  TEXT    NOT NULL,"
        "  source     TEXT    NOT NULL DEFAULT '',"
        "  tag        TEXT    NOT NULL,"
        "  payload    TEXT    NOT NULL,"
        "  prev_seq   INTEGER,"
        "  synced     INTEGER NOT NULL DEFAULT 0"
        ")"
    )
    # Mirror the real schema: open_write_conn creates outbox_sync_log before the
    # UNIQUE-index migration, so a real forked DB always has this table.
    conn.execute(
        "CREATE TABLE outbox_sync_log ("
        "  seq       INTEGER PRIMARY KEY,"
        "  namespace TEXT    NOT NULL,"
        "  synced_at TEXT    NOT NULL"
        ")"
    )
    # seq=1 head (prev_seq NULL), then TWO rows both pointing at seq=1 → fork.
    conn.execute("INSERT INTO outbox_queue (seq, created_at, namespace, tag, payload, prev_seq) "
                 "VALUES (1, '2026-01-01T00:00:00+00:00', 'evt', 't', 'a', NULL)")
    conn.execute("INSERT INTO outbox_queue (seq, created_at, namespace, tag, payload, prev_seq) "
                 "VALUES (2, '2026-01-01T00:00:01+00:00', 'evt', 't', 'b', 1)")
    conn.execute("INSERT INTO outbox_queue (seq, created_at, namespace, tag, payload, prev_seq) "
                 "VALUES (3, '2026-01-01T00:00:02+00:00', 'evt', 't', 'c', 1)")
    conn.commit()
    conn.close()


def test_forked_db_open_write_conn_raises_chain_integrity_error(tmp_path: Path):
    """open_write_conn on a forked DB raises typed ChainIntegrityError, not IntegrityError."""
    from sqloutbox._schema import open_write_conn
    from sqloutbox.exceptions import ChainIntegrityError

    db = tmp_path / "forked.db"
    _build_forked_db(db)

    with pytest.raises(ChainIntegrityError) as ei:
        open_write_conn(db)
    msg = str(ei.value)
    assert "prev_seq" in msg
    assert "1" in msg            # names the duplicated prev_seq value
    assert "recover" in msg.lower() or "skip" in msg.lower()  # recovery pointer


def test_forked_db_outbox_init_raises_chain_integrity_error(tmp_path: Path):
    """Outbox.__init__ (producer hot path) raises ChainIntegrityError, not a bare crash."""
    from sqloutbox.exceptions import ChainIntegrityError

    db = tmp_path / "forked.db"
    _build_forked_db(db)

    with pytest.raises(ChainIntegrityError):
        Outbox(db_path=db, namespace="evt")


def test_forked_db_chain_integrity_error_is_sqloutbox_error(tmp_path: Path):
    """ChainIntegrityError is part of the SqloutboxError hierarchy (catchable broadly)."""
    from sqloutbox._schema import open_write_conn
    from sqloutbox.exceptions import ChainIntegrityError, SqloutboxError

    db = tmp_path / "forked.db"
    _build_forked_db(db)
    with pytest.raises(SqloutboxError):
        open_write_conn(db)
    assert issubclass(ChainIntegrityError, SqloutboxError)


def test_forked_db_read_only_verify_reports_without_crashing(tmp_path: Path):
    """Read-only verify OPENS and REPORTS the fork (does not raise) — diagnostic stays usable."""
    from sqloutbox._verify import verify_db_path

    db = tmp_path / "forked.db"
    _build_forked_db(db)

    # No exception — verify must remain usable on a DB that crashes the writer.
    result = verify_db_path(db, namespace="evt")
    assert result.ok is False
    assert result.chain_ok is False
    assert any("fork" in e.lower() for e in result.errors)


# ── Producer-side seed (persisted high-water mark, mechanism (a)) ────────────


def test_hwm_recorded_on_enqueue(tmp_path: Path):
    """Each enqueue persists a per-namespace high-water mark in outbox_hwm."""
    db = tmp_path / "events.db"
    ob = Outbox(db_path=db, namespace="evt")
    seqs = _enqueue_n(ob, 3)
    assert ob._persisted_hwm() == max(seqs)


def test_seed_sequence_persists_hwm(tmp_path: Path):
    """The drain's seed_sequence (remote max) is persisted as a floor."""
    db = tmp_path / "events.db"
    ob = Outbox(db_path=db, namespace="evt")
    ob.seed_sequence(5000)
    assert ob._persisted_hwm() == 5000


def test_fresh_host_lazy_seed_from_persisted_hwm(tmp_path: Path):
    """A NEW Outbox on the same file lazily seeds its counter from the persisted hwm.

    Simulates: drain ran seed_sequence(remote_max) once; later the producer
    process restarts and constructs a fresh Outbox — it must pick up the floor
    and NOT restart numbering low.
    """
    db = tmp_path / "events.db"
    # First instance learns the remote max (e.g. via the drain's _seed_from_remote).
    first = Outbox(db_path=db, namespace="evt")
    first.seed_sequence(10_000)
    first._write_conn.close()

    # Producer restarts: a brand-new Outbox on the same file.
    producer = Outbox(db_path=db, namespace="evt")
    seq = producer.enqueue("INSERT INTO evt (id) VALUES (?)", b"[1]")
    assert seq is not None
    assert seq > 10_000, f"producer must start above persisted hwm, got {seq}"


def test_fresh_host_populated_remote_no_collision(tmp_path: Path):
    """End-to-end mechanism (a): fresh local file + known remote max → no colliding seqs.

    Reproduces the F004 collision scenario: the local file is fresh (counter
    would start at 1), but we know the remote already holds outbox_seq 1..200.
    After recording that high-water mark, the producer's enqueues all land
    ABOVE 200, so INSERT OR IGNORE on the remote can never silently drop them.
    """
    db = tmp_path / "events.db"
    remote_max = 200

    # Producer-side seed from the persisted high-water mark (mechanism (a)):
    # in production this floor is established by the drain's seed_sequence(remote_max)
    # OR by a prior run; here we set it explicitly to model "populated remote".
    ob = Outbox(db_path=db, namespace="evt")
    ob.record_hwm(remote_max)
    # A fresh Outbox constructed afterwards lazily seeds from the persisted hwm.
    ob2 = Outbox(db_path=db, namespace="evt")

    seqs = [ob2.enqueue("INSERT INTO evt (id) VALUES (?)", f"[{i}]".encode())
            for i in range(5)]
    assert all(s is not None for s in seqs)
    assert all(s > remote_max for s in seqs), \
        f"all producer seqs must exceed remote max {remote_max}; got {seqs}"
    # No value in 1..remote_max is reused → INSERT OR IGNORE cannot drop them.
    assert not set(seqs) & set(range(1, remote_max + 1))


def test_hwm_does_not_break_chain_integrity(tmp_path: Path):
    """Persisted-hwm seeding leaves the singly-linked chain verifiable."""
    db = tmp_path / "events.db"
    ob = Outbox(db_path=db, namespace="evt")
    ob.record_hwm(9000)
    ob2 = Outbox(db_path=db, namespace="evt")
    ob2.enqueue_batch([("t", b"a"), ("t", b"b"), ("t", b"c")])
    rows = ob2.fetch_unsynced()
    ok, gaps = ob2.verify_chain(rows)
    assert ok is True, f"chain must stay intact after hwm seed; gaps={gaps}"
