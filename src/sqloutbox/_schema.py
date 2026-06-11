"""SQLite schema, connection setup, and SQL helpers for sqloutbox."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# Wait up to 30s for a contended write lock before raising "database is locked".
# Two processes (producer + drain) on one WAL file routinely contend briefly;
# without this they raise after SQLite's 5s default and (pre-L2) crash the loop.
_BUSY_TIMEOUT_MS = 30_000


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

# Idempotent migrations: retry/backoff tracking columns on outbox_queue.
# Each is wrapped in try/except in open_write_conn() (same pattern as
# _MIGRATE_ADD_SOURCE) — a second open is a no-op (column already exists).
_MIGRATE_ADD_ATTEMPTS = (
    "ALTER TABLE outbox_queue ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0"
)
_MIGRATE_ADD_LAST_ATTEMPT_AT = (
    "ALTER TABLE outbox_queue ADD COLUMN last_attempt_at TEXT"
)
_MIGRATE_ADD_LAST_ERROR = (
    "ALTER TABLE outbox_queue ADD COLUMN last_error TEXT"
)
_MIGRATE_ADD_LAST_ERROR_CLASS = (
    "ALTER TABLE outbox_queue ADD COLUMN last_error_class TEXT"
)

# Audited dead-letter store. A row that cannot be delivered (max_attempts hit,
# undecodable payload, operator skip, or an unsupported inject_outbox_seq shape)
# is MOVED here atomically — never lost. Replayable via Outbox.replay().
# Same idempotent CREATE-IF-NOT-EXISTS pattern as outbox_sync_log.
_CREATE_DEAD_LOG = """
CREATE TABLE IF NOT EXISTS outbox_dead_log (
    seq              INTEGER NOT NULL,
    namespace        TEXT    NOT NULL,
    tag              TEXT    NOT NULL,
    payload          TEXT    NOT NULL,
    prev_seq         INTEGER,
    source           TEXT,
    attempts         INTEGER NOT NULL,
    last_error       TEXT,
    last_error_class TEXT,
    dead_lettered_at TEXT    NOT NULL,
    reason           TEXT    NOT NULL,
    PRIMARY KEY (namespace, seq)
)
"""

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

def _find_forked_prev_seqs(conn: sqlite3.Connection) -> list[tuple[int, int]]:
    """Return [(prev_seq, count), …] for every prev_seq shared by >1 row.

    A non-NULL prev_seq pointed at by two or more rows is a forked chain —
    it violates the singly-linked invariant and is why CREATE UNIQUE INDEX
    fails. Used to build a precise, actionable ChainIntegrityError message.
    """
    return conn.execute(
        "SELECT prev_seq, COUNT(*) c FROM outbox_queue "
        "WHERE prev_seq IS NOT NULL "
        "GROUP BY prev_seq HAVING c > 1 "
        "ORDER BY prev_seq"
    ).fetchall()


def _create_prev_seq_index_guarded(
    conn: sqlite3.Connection, index_sql: str, db_path: Path
) -> None:
    """Run a ``CREATE UNIQUE INDEX … ON outbox_queue (prev_seq)`` statement.

    On a forked-chain DB (two rows sharing a non-NULL prev_seq) the UNIQUE
    index creation raises ``sqlite3.IntegrityError``. Unguarded, that bare
    error crashes ``Outbox.__init__`` — the producer HOT PATH. Convert it to a
    typed ``ChainIntegrityError`` (spec §6.2, F006/F029) that names the
    duplicate rows and points at the recovery tool, so the failure is
    diagnosable. ``ChainIntegrityError`` is owned by Plan 3 (lazy import inside
    the except to keep ``_schema.py`` import-order-independent — same pattern as
    ``_outbox.verify_full``).
    """
    try:
        conn.execute(index_sql)
    except sqlite3.IntegrityError as exc:
        from sqloutbox.exceptions import ChainIntegrityError

        forks = _find_forked_prev_seqs(conn)
        try:
            conn.close()  # do not leak the half-migrated write connection
        except Exception:
            pass
        fork_desc = ", ".join(f"prev_seq={p} (×{c})" for p, c in forks) or "unknown"
        raise ChainIntegrityError(
            f"forked chain in {db_path}: {fork_desc} — two or more rows share a "
            f"prev_seq, violating the singly-linked invariant. The UNIQUE index "
            f"on prev_seq cannot be created. Recover with: inspect read-only via "
            f"`sqloutbox verify --db-dir <dir>`, then skip/replay the offending "
            f"row(s) with the dead-letter CLI (`sqloutbox skip --namespace <ns> "
            f"--seq <S>`) so each prev_seq is pointed at by exactly one row."
        ) from exc


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
    conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(_CREATE_QUEUE)
    conn.execute(_CREATE_SYNC_LOG)
    conn.execute(_IDX_WORKER)
    # GUARDED (spec §6.2): _IDX_PREV is a UNIQUE index on prev_seq — on a forked
    # chain it raises IntegrityError FIRST, before the migration below. Convert
    # to ChainIntegrityError so Outbox.__init__ (the producer hot path) never
    # dies with a bare IntegrityError.
    _create_prev_seq_index_guarded(conn, _IDX_PREV, db_path)
    conn.execute(_IDX_SYNC_LOG)
    # Idempotent migration: add `source` column to DBs created before it existed.
    try:
        conn.execute(_MIGRATE_ADD_SOURCE)
    except Exception:
        pass  # column already exists — OperationalError, safe to ignore
    # Idempotent migration: enforce UNIQUE on prev_seq for existing DBs.
    # CREATE UNIQUE INDEX IF NOT EXISTS is a no-op when the index already exists.
    # GUARDED (spec §6.2, F006/F029): same forked-chain conversion as _IDX_PREV
    # above — kept guarded for defense in depth (e.g. if _IDX_PREV were ever
    # made non-UNIQUE, this migration would then be the one that raises).
    _create_prev_seq_index_guarded(conn, _MIGRATE_PREV_SEQ_UNIQUE, db_path)
    # Idempotent migrations: retry/backoff tracking columns (wrapped — a second
    # open raises "duplicate column" which is safe to ignore, exactly like source).
    for _stmt in (
        _MIGRATE_ADD_ATTEMPTS,
        _MIGRATE_ADD_LAST_ATTEMPT_AT,
        _MIGRATE_ADD_LAST_ERROR,
        _MIGRATE_ADD_LAST_ERROR_CLASS,
    ):
        try:
            conn.execute(_stmt)
        except Exception:
            pass  # column already exists — OperationalError, safe to ignore
    # Audited dead-letter store (idempotent create).
    conn.execute(_CREATE_DEAD_LOG)
    conn.commit()
    return conn


def thread_conn(db_path: Path) -> sqlite3.Connection:
    """Open a short-lived connection for use inside asyncio.to_thread() calls.

    Each thread-pool task opens and closes its own connection.
    No state is shared — sqlite3 WAL handles concurrent access safely.
    busy_timeout makes a contended write wait rather than raise immediately.
    """
    conn = sqlite3.connect(str(db_path))
    conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
    return conn


def open_read_conn(db_path: Path) -> sqlite3.Connection:
    """Open a SQLite connection in TRUE read-only mode for inspection.

    Unlike ``open_write_conn``/``thread_conn`` this NEVER:
        - creates the parent directory (no ``mkdir``),
        - creates the file (``mode=ro`` fails on a missing path),
        - runs DDL (``CREATE TABLE`` / ``ALTER`` / ``CREATE INDEX``),
        - switches journal mode (no ``PRAGMA journal_mode=WAL`` write).

    Used by the verify/diagnostic path so inspecting a ``.db`` file — or a
    stray non-outbox file — can never mutate it (spec §6.1, F005/F050).

    A missing file raises ``sqlite3.OperationalError`` on first statement
    execution (SQLite opens lazily). Callers that want a soft "not found"
    must catch it — see ``_verify.verify_db_path``.
    """
    # uri=True enables the file: URI form; mode=ro forbids any write and
    # forbids creating the file if it does not exist.
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    # busy_timeout is harmless on a read-only handle (readers don't take the
    # write lock) but keeps behaviour uniform with the write path.
    conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
    return conn


# ── Utilities ─────────────────────────────────────────────────────────────────

def now_iso() -> str:
    """Current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def placeholders(n: int) -> str:
    """Return n comma-separated '?' placeholders for use in SQL IN clauses."""
    return ",".join("?" * n)
