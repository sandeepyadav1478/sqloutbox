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
