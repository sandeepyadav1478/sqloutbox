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
