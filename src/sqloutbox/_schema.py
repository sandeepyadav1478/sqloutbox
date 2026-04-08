"""SQLite schema, connection setup, and SQL helpers for sqloutbox."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path


# ── SQL statements ────────────────────────────────────────────────────────────

_CREATE_QUEUE = """
CREATE TABLE IF NOT EXISTS outbox_queue (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT    NOT NULL,
    namespace  TEXT    NOT NULL,
    source     TEXT    NOT NULL DEFAULT '',
    tag        TEXT    NOT NULL,
    payload    TEXT    NOT NULL,
    prev_seq   INTEGER UNIQUE,
    synced     INTEGER NOT NULL DEFAULT 0
)
"""
# prev_seq is UNIQUE — each row in the singly-linked chain points to exactly
# one predecessor. Two rows with the same prev_seq would mean a fork, which
# violates chain integrity.  SQLite allows multiple NULLs in a UNIQUE column,
# so the first row of each namespace (prev_seq = NULL) is always valid.
#
# next_seq was intentionally omitted:
# - Successor can always be found with WHERE prev_seq = this_seq (uses idx_outbox_prev)
# - Storing it requires an UPDATE on every INSERT, adding overhead without benefit
# - prev_seq alone is sufficient for full chain integrity verification

# Applied once after CREATE TABLE to add `source` to existing DBs that were
# created before this column existed.
_MIGRATE_ADD_SOURCE = (
    "ALTER TABLE outbox_queue ADD COLUMN source TEXT NOT NULL DEFAULT ''"
)

# Idempotent: adds UNIQUE enforcement to prev_seq on DBs created before the
# UNIQUE keyword was added to the column definition. CREATE UNIQUE INDEX IF
# NOT EXISTS is a no-op when the index already exists (fresh DBs).
_MIGRATE_PREV_SEQ_UNIQUE = (
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_prev_unique "
    "ON outbox_queue (prev_seq)"
)

_CREATE_SYNC_LOG = """
CREATE TABLE IF NOT EXISTS outbox_sync_log (
    seq        INTEGER PRIMARY KEY,
    namespace  TEXT    NOT NULL,
    synced_at  TEXT    NOT NULL
)
"""

# Covers the primary worker query: unsynced rows per namespace in order
_IDX_WORKER = (
    "CREATE INDEX IF NOT EXISTS idx_outbox_worker "
    "ON outbox_queue (namespace, synced, seq)"
)

# Covers gap detection: look up rows by prev_seq (UNIQUE enforces chain integrity)
_IDX_PREV = (
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_prev "
    "ON outbox_queue (prev_seq)"
)

# Covers chain verification: look up sync_log by namespace + seq
_IDX_SYNC_LOG = (
    "CREATE INDEX IF NOT EXISTS idx_sync_log "
    "ON outbox_sync_log (namespace, seq)"
)


# ── Connection helpers ────────────────────────────────────────────────────────

def open_write_conn(db_path: Path) -> sqlite3.Connection:
    """Open (or create) the DB and apply schema. Returns a persistent connection.

    WAL mode: concurrent reads never block writes.
    synchronous=NORMAL: safe with WAL; ~3× faster than FULL; survives OS crashes.
    check_same_thread=False: the connection is used only from the thread that
    calls enqueue() — safe because enqueue() is never called concurrently.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(_CREATE_QUEUE)
    conn.execute(_CREATE_SYNC_LOG)
    conn.execute(_IDX_WORKER)
    conn.execute(_IDX_PREV)
    conn.execute(_IDX_SYNC_LOG)
    # Idempotent migration: add `source` column to DBs created before it existed.
    try:
        conn.execute(_MIGRATE_ADD_SOURCE)
    except Exception:
        pass  # column already exists — OperationalError, safe to ignore
    # Idempotent migration: enforce UNIQUE on prev_seq for existing DBs.
    # CREATE UNIQUE INDEX IF NOT EXISTS is a no-op when the index already exists.
    conn.execute(_MIGRATE_PREV_SEQ_UNIQUE)
    conn.commit()
    return conn


def thread_conn(db_path: Path) -> sqlite3.Connection:
    """Open a short-lived connection for use inside asyncio.to_thread() calls.

    Each thread-pool task opens and closes its own connection.
    No state is shared — sqlite3 WAL handles concurrent access safely.
    """
    return sqlite3.connect(str(db_path))


# ── Utilities ─────────────────────────────────────────────────────────────────

def now_iso() -> str:
    """Current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def placeholders(n: int) -> str:
    """Return n comma-separated '?' placeholders for use in SQL IN clauses."""
    return ",".join("?" * n)
