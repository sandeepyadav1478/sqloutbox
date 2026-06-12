# Honest Ordered Delivery, Backoff & Dead-Letter (WS-1 + WS-2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the drain deliver each namespace strictly head-first (a failed head holds the rows behind it instead of letting them leapfrog), apply per-namespace exponential backoff with error classification, and auto-quarantine a row to an audited, replayable `outbox_dead_log` after `max_attempts` — never *losing* a row, only relocating it.

**Architecture:** Three coupled pieces. (1) New idempotent schema: four retry/backoff columns on `outbox_queue` and a new `outbox_dead_log` table. (2) `Outbox` gains `dead_letter(seq, reason)` / `replay(seq)`, attempt accounting, `peek_head()`, and `_seq_accounted` now also consults `outbox_dead_log` so a dead-lettered head keeps its successor's chain check passing. (3) `OutboxSyncService._flush_to_target` / `_worker_loop` are reworked: head-of-line hold (stop confirming after the first failed row in chain order), per-namespace exponential backoff gated into the scheduler, write-error classification, and auto-dead-letter at `max_attempts`. An operator CLI (`dead-letter {list,show,replay}`, `skip`) is added.

**Tech Stack:** Python 3.10+ stdlib only (sqlite3, asyncio, json, logging); pytest + pytest-asyncio.

**Spec:** Implements `docs/specs/2026-06-11-durable-ordered-retry-and-health-signal.md` §3.1 (retry/error columns), §3.2 (head-of-line hold + 2→4→8→16→32→64 min per-namespace backoff), §3.3 (TRANSIENT/DETERMINISTIC/ALREADY_APPLIED/UNKNOWN classification); and `docs/specs/2026-06-11-standalone-hardening-design.md` §3.1 (`outbox_dead_log` DDL), §3.2 (D1 auto-dead-letter), §3.3 (chain re-stitch invariant + operator CLI), §7 (the "never lose / may quarantine" reconciliation), §9 testing matrix.

**Recommended execution order:** This is **Plan 2** in the locked cross-plan order: `Plan 1 WS-0 (done) → Plan 3 WS-3 → Plan 2 WS-1+2 → Plan 4 WS-4 → Plan 5 WS-5 → Plan 6 WS-6+7`. By the time this plan runs:
- **Plan 1 (WS-0)** has already: added `busy_timeout=30000` to `open_write_conn` + `thread_conn` (`_schema.py`); wrapped the per-row decode/transform in `_worker_loop` (L1 — for an undecodable row it currently *logs-and-skips-this-cycle*); wrapped each per-table drain unit in try/except (L2); and made `_runner.run_service_main` observe the drain task and `raise SystemExit(1)` on worker death (L3). **Build on those guards — do not duplicate them.** This plan *upgrades* L1's "skip this cycle" for an undecodable payload into `dead_letter(reason='undecodable')`.
- **Plan 3 (WS-3)** has already created `src/sqloutbox/exceptions.py` with `SqloutboxError`, `ConfigError`, `QueueFullError`, `UnsupportedStatementError`, `ChainIntegrityError`; added `max_attempts: int | None = 10`, `max_pending: int | None = None`, `max_batch_bytes: int | None = None` to `OutboxConfig` (and where sensible `TargetConfig`) with `__post_init__` validation; and added the `inject_outbox_seq` grammar guard. **Reference `config.max_attempts` and the exceptions by their exact contract names — do not redefine them.**

This plan therefore assumes `OutboxConfig(max_attempts=...)` exists and validates. If you are running this plan in isolation (Plan 3 not yet merged), the *tests* that pass `max_attempts=` to `OutboxConfig` will fail at construction — that is expected only in isolation; in the locked order Plan 3 is already merged.

---

## File Structure

| File | Responsibility | Create/Modify |
|------|----------------|---------------|
| `src/sqloutbox/_schema.py` | Add `outbox_dead_log` CREATE (idempotent) + four `outbox_queue` retry/backoff columns via wrapped-`try/except` ALTERs (same pattern as `_MIGRATE_ADD_SOURCE`). | Modify |
| `src/sqloutbox/_outbox.py` | `dead_letter(seq, reason)`, `replay(seq)`, `peek_head()`, `record_attempt(seq, error, error_class)`, `list_dead()`, `get_dead(seq)`; `_seq_accounted` also consults `outbox_dead_log`; `fetch_unsynced` selects new columns into `QueueRow`. | Modify |
| `src/sqloutbox/_models.py` | `QueueRow` gains `attempts`, `last_attempt_at`, `last_error`, `last_error_class`; new frozen `DeadRow`. | Modify |
| `src/sqloutbox/sync.py` | Rework `_flush_to_target` (head-of-line hold + classification + attempt/dead-letter); add backoff gate + head-only fetch in `_worker_loop`; classifier helper `classify_write_error`. | Modify |
| `src/sqloutbox/cli.py` | New subcommands `dead-letter {list,show,replay}` and `skip`; handlers `cmd_dead_letter` / `cmd_skip`. | Modify |
| `tests/test_dead_letter.py` | All WS-1/WS-2 tests (schema, head-hold, backoff, classification, auto-dead-letter, dead_letter/replay, chain re-stitch, CLI). | Create |
| `docs/specs/2026-06-11-durable-ordered-retry-and-health-signal.md` | §9 reconciliation note (the "never lose, may quarantine" softening). **Maintainer go-ahead required before editing — see Task 9.** | Modify (gated) |

---

## Task 1: Schema — retry/backoff columns on `outbox_queue` + `outbox_dead_log` table

Add the four per-row retry columns (idempotent ALTERs wrapped like `_MIGRATE_ADD_SOURCE`) and the `outbox_dead_log` table (idempotent CREATE, same pattern as `outbox_sync_log`). Both ship in `open_write_conn()` so every DB gets them on open.

**Files:**
- Modify: `src/sqloutbox/_schema.py`
- Test: `tests/test_dead_letter.py` (Create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_dead_letter.py` with:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dead_letter.py -v -k "retry_columns or dead_log_table or migration_idempotent"`
Expected: FAIL — `outbox_queue` lacks the four columns and `outbox_dead_log` does not exist; the assertions on column sets / table existence fail.

- [ ] **Step 3: Add the DDL and apply it in `open_write_conn`**

In `src/sqloutbox/_schema.py`, locate the existing `_MIGRATE_ADD_SOURCE` block (the `ALTER TABLE outbox_queue ADD COLUMN source ...` string). Immediately AFTER `_MIGRATE_PREV_SEQ_UNIQUE` (the `CREATE UNIQUE INDEX ... idx_outbox_prev_unique` string), add:

```python
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
```

Then in `open_write_conn`, locate the block that ends with `conn.execute(_MIGRATE_PREV_SEQ_UNIQUE)` followed by `conn.commit()`. Insert the new migrations and the dead-log CREATE **before** `conn.commit()`:

```python
    # Idempotent migration: enforce UNIQUE on prev_seq for existing DBs.
    # CREATE UNIQUE INDEX IF NOT EXISTS is a no-op when the index already exists.
    conn.execute(_MIGRATE_PREV_SEQ_UNIQUE)
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
```

> Note: do NOT touch the `busy_timeout` PRAGMA lines — Plan 1 (WS-0) already added them. If you do not see a `PRAGMA busy_timeout=30000` line near the `journal_mode=WAL` line, Plan 1 has not been merged; stop and confirm execution order before proceeding.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_dead_letter.py -v -k "retry_columns or dead_log_table or migration_idempotent"`
Expected: PASS (3 passed).

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest -q`
Expected: all green (no regression — the changes are additive schema).

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/_schema.py tests/test_dead_letter.py
git commit -m "feat(dead-letter): add outbox_queue retry columns + outbox_dead_log table (WS-2)"
```

---

## Task 2: Models — `QueueRow` retry fields + `DeadRow`; `fetch_unsynced` selects them

`QueueRow` currently carries `seq, tag, payload, prev_seq, source`. The backoff gate needs `attempts` and `last_attempt_at` on the head row. Add the four columns to `QueueRow` (defaulted, so existing constructors keep working) and a `DeadRow` model for the CLI/list path. Update `fetch_unsynced` to populate them.

**Files:**
- Modify: `src/sqloutbox/_models.py`, `src/sqloutbox/_outbox.py`
- Test: `tests/test_dead_letter.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_dead_letter.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dead_letter.py -v -k "retry_fields or dead_row"`
Expected: FAIL — `QueueRow` has no `attempts`/`last_attempt_at`/... attributes (AttributeError) and `DeadRow` does not exist (ImportError at collection of this file's new symbols).

- [ ] **Step 3a: Add fields to `QueueRow` and the `DeadRow` model**

In `src/sqloutbox/_models.py`, replace the `QueueRow` field block (currently ending at the `source: str = ""` line) so the four new fields follow `source` (all defaulted, so positional callers are unaffected):

```python
    seq:      int
    tag:      str
    payload:  bytes
    prev_seq: int | None
    source:   str = ""   # middleware that produced this row (e.g. "SchedulerMiddleware")
    # WS-1/WS-2 retry tracking (persisted on outbox_queue). Defaulted so existing
    # callers that build QueueRow with 5 args keep working.
    attempts:         int = 0
    last_attempt_at:  str | None = None   # ISO-8601 UTC, NULL until first attempt
    last_error:       str | None = None   # destination error of the last failed attempt
    last_error_class: str | None = None   # TRANSIENT|DETERMINISTIC|ALREADY_APPLIED|UNKNOWN
```

At the end of `src/sqloutbox/_models.py`, add the `DeadRow` dataclass:

```python
@dataclass(frozen=True)
class DeadRow:
    """One row read from outbox_dead_log — a quarantined, replayable event.

    Mirrors the outbox_dead_log table columns (see _schema.py::_CREATE_DEAD_LOG).
    A row lands here only via Outbox.dead_letter(); it is never lost, only moved.
    """
    seq:              int
    namespace:        str
    tag:              str
    payload:          bytes
    prev_seq:         int | None
    source:           str | None
    attempts:         int
    last_error:       str | None
    last_error_class: str | None
    dead_lettered_at: str
    reason:           str
```

- [ ] **Step 3b: Update `fetch_unsynced` to select the new columns**

In `src/sqloutbox/_outbox.py`, replace the `fetch_unsynced` body's SELECT + row construction (currently selecting `seq, tag, payload, prev_seq, source`) with:

```python
    def fetch_unsynced(self, limit: int | None = None) -> list[QueueRow]:
        """Return up to `limit` undelivered rows in strict insertion order.

        Opens its own connection — safe to call from any thread.
        """
        n = limit or self.batch_size
        with thread_conn(self.db_path) as conn:
            rows = conn.execute(
                "SELECT seq, tag, payload, prev_seq, source, "
                "attempts, last_attempt_at, last_error, last_error_class "
                "FROM outbox_queue "
                "WHERE namespace = ? AND synced = 0 "
                "ORDER BY seq LIMIT ?",
                [self.namespace, n],
            ).fetchall()
        return [
            QueueRow(
                seq=r[0], tag=r[1], payload=r[2].encode(), prev_seq=r[3],
                source=r[4] or "",
                attempts=r[5], last_attempt_at=r[6],
                last_error=r[7], last_error_class=r[8],
            )
            for r in rows
        ]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_dead_letter.py -v -k "retry_fields or dead_row"`
Expected: PASS (3 passed).

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest -q`
Expected: all green (defaulted fields keep all existing `QueueRow(...)` constructions valid).

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/_models.py src/sqloutbox/_outbox.py tests/test_dead_letter.py
git commit -m "feat(dead-letter): QueueRow retry fields + DeadRow model; fetch_unsynced selects them (WS-1/WS-2)"
```

---

## Task 3: `Outbox.peek_head`, `record_attempt`, and `_seq_accounted` consults dead-log

`peek_head()` returns the lowest-seq unsynced row (the backoff gate reads `attempts`/`last_attempt_at` from it). `record_attempt()` increments `attempts` and persists the error fields on a single row (used by the drain when a head fails but is not yet at `max_attempts`). `_seq_accounted` must also consult `outbox_dead_log` so a dead-lettered head keeps its successor's `verify_chain` passing.

**Files:**
- Modify: `src/sqloutbox/_outbox.py`
- Test: `tests/test_dead_letter.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_dead_letter.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dead_letter.py -v -k "peek_head or record_attempt or seq_accounted"`
Expected: FAIL — `peek_head`/`record_attempt` do not exist (AttributeError), and `_seq_accounted(7)` returns False because it only queries `outbox_queue`/`outbox_sync_log`.

- [ ] **Step 3a: Add `peek_head` and `record_attempt`**

In `src/sqloutbox/_outbox.py`, add these two methods inside the `Outbox` class. Place them right after `pending_count` (which ends with `return row[0] if row else 0`):

```python
    def peek_head(self) -> QueueRow | None:
        """Return the lowest-seq undelivered row in this namespace, or None.

        This is the row whose backoff clock the drain reads (attempts /
        last_attempt_at) to decide whether the namespace is eligible to retry.
        Read-only; opens its own connection (safe from any thread).
        """
        with thread_conn(self.db_path) as conn:
            r = conn.execute(
                "SELECT seq, tag, payload, prev_seq, source, "
                "attempts, last_attempt_at, last_error, last_error_class "
                "FROM outbox_queue "
                "WHERE namespace = ? AND synced = 0 "
                "ORDER BY seq LIMIT 1",
                [self.namespace],
            ).fetchone()
        if r is None:
            return None
        return QueueRow(
            seq=r[0], tag=r[1], payload=r[2].encode(), prev_seq=r[3],
            source=r[4] or "",
            attempts=r[5], last_attempt_at=r[6],
            last_error=r[7], last_error_class=r[8],
        )

    def record_attempt(self, seq: int, error: str, error_class: str) -> None:
        """Record a failed delivery attempt on one row.

        Increments ``attempts`` and stores the destination error + its class
        and the attempt timestamp (ISO-8601 UTC). Persisted so the §3.2 backoff
        gate and the §3.4 health signal can read it back, possibly cross-process.
        Opens its own connection — safe to call from any thread.
        """
        with thread_conn(self.db_path) as conn:
            conn.execute(
                "UPDATE outbox_queue "
                "SET attempts = attempts + 1, last_attempt_at = ?, "
                "    last_error = ?, last_error_class = ? "
                "WHERE namespace = ? AND seq = ?",
                [now_iso(), error, error_class, self.namespace, seq],
            )
            conn.commit()
```

- [ ] **Step 3b: Make `_seq_accounted` consult `outbox_dead_log`**

In `src/sqloutbox/_outbox.py`, replace `_seq_accounted` (the method whose body is the single `SELECT 1 FROM outbox_queue ... UNION SELECT 1 FROM outbox_sync_log ... LIMIT 1`) with:

```python
    def _seq_accounted(self, conn: sqlite3.Connection, seq: int) -> bool:
        """Return True if seq exists in queue OR sync_log OR dead_log.

        A row moved to outbox_dead_log is "accounted": it has been durably
        relocated (not lost), so its successor's prev_seq chain check must still
        pass. One UNION query across the three stores.
        """
        return bool(conn.execute(
            "SELECT 1 FROM outbox_queue WHERE seq = ? "
            "UNION SELECT 1 FROM outbox_sync_log WHERE seq = ? "
            "UNION SELECT 1 FROM outbox_dead_log WHERE seq = ? "
            "LIMIT 1",
            [seq, seq, seq],
        ).fetchone())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_dead_letter.py -v -k "peek_head or record_attempt or seq_accounted"`
Expected: PASS (4 passed).

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest -q`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/_outbox.py tests/test_dead_letter.py
git commit -m "feat(dead-letter): Outbox.peek_head/record_attempt; _seq_accounted consults dead_log (WS-1/WS-2)"
```

---

## Task 4: `Outbox.dead_letter(seq, reason)` and `Outbox.replay(seq)`

`dead_letter` atomically MOVES the head row from `outbox_queue` to `outbox_dead_log` (INSERT then DELETE in one transaction) — never lost, only relocated. `replay` re-enqueues a dead-lettered row at the TAIL with a NEW seq (a fresh chain link) and removes it from `outbox_dead_log`. `list_dead` / `get_dead` back the CLI.

**Files:**
- Modify: `src/sqloutbox/_outbox.py`
- Test: `tests/test_dead_letter.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_dead_letter.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dead_letter.py -v -k "dead_letter_moves or dead_letter_unknown or replay or get_dead"`
Expected: FAIL — `dead_letter`, `replay`, `list_dead`, `get_dead` do not exist (AttributeError).

- [ ] **Step 3: Implement the four methods**

In `src/sqloutbox/_outbox.py`, add these methods inside the `Outbox` class, right after `record_attempt` (added in Task 3). They use the persistent write connection so the move is one transaction on the producer's own connection:

```python
    def dead_letter(self, seq: int, reason: str) -> bool:
        """Atomically MOVE one queue row to outbox_dead_log (never lose it).

        INSERT INTO outbox_dead_log (...) SELECT (...) FROM outbox_queue WHERE seq=?
        then DELETE FROM outbox_queue WHERE seq=? — in one transaction. The row is
        relocated to an audited, replayable store, not destroyed. After the move,
        _seq_accounted() still returns True for ``seq`` (it consults dead_log), so
        the successor's chain check passes and the namespace head advances.

        ``reason`` is one of: 'max_attempts' | 'manual_skip' | 'undecodable' |
        'unsupported_stmt'. Returns True if a row was moved, False if ``seq`` was
        absent (no-op). Never raises on a missing row.
        """
        try:
            self._write_conn.execute("BEGIN IMMEDIATE")
            cur = self._write_conn.execute(
                "INSERT OR IGNORE INTO outbox_dead_log "
                "(seq, namespace, tag, payload, prev_seq, source, attempts, "
                " last_error, last_error_class, dead_lettered_at, reason) "
                "SELECT seq, namespace, tag, payload, prev_seq, source, attempts, "
                "       last_error, last_error_class, ?, ? "
                "FROM outbox_queue WHERE namespace = ? AND seq = ?",
                [now_iso(), reason, self.namespace, seq],
            )
            moved = cur.rowcount > 0
            if moved:
                self._write_conn.execute(
                    "DELETE FROM outbox_queue WHERE namespace = ? AND seq = ?",
                    [self.namespace, seq],
                )
            self._write_conn.commit()
            if moved:
                logger.warning(
                    "sqloutbox[%s]: dead-lettered seq=%d reason=%s",
                    self.namespace, seq, reason,
                )
            return moved
        except Exception as exc:
            try:
                self._write_conn.rollback()
            except Exception:
                pass
            logger.error(
                "sqloutbox[%s]: dead_letter failed seq=%d: %s",
                self.namespace, seq, exc,
            )
            return False

    def replay(self, seq: int) -> int | None:
        """Re-enqueue a dead-lettered row at the TAIL with a NEW seq.

        Reads the dead_log row, enqueues a fresh copy (preserving tag/payload/
        source) — which assigns a brand-new AUTOINCREMENT seq and links it to the
        current tail — then deletes the dead_log entry. The OLD seq is never
        reused (AUTOINCREMENT guarantees this), so this is a new chain link, not a
        gap-fill. Returns the new seq, or None if ``seq`` was not in the dead_log.
        """
        d = self.get_dead(seq)
        if d is None:
            return None
        new_seq = self.enqueue(d.tag, d.payload, source=d.source or "")
        if new_seq is None:
            return None
        with thread_conn(self.db_path) as conn:
            conn.execute(
                "DELETE FROM outbox_dead_log WHERE namespace = ? AND seq = ?",
                [self.namespace, seq],
            )
            conn.commit()
        logger.info(
            "sqloutbox[%s]: replayed dead seq=%d → new seq=%d",
            self.namespace, seq, new_seq,
        )
        return new_seq

    def list_dead(self) -> list[DeadRow]:
        """Return all dead-lettered rows for this namespace, oldest seq first."""
        with thread_conn(self.db_path) as conn:
            rows = conn.execute(
                "SELECT seq, namespace, tag, payload, prev_seq, source, attempts, "
                "last_error, last_error_class, dead_lettered_at, reason "
                "FROM outbox_dead_log WHERE namespace = ? ORDER BY seq",
                [self.namespace],
            ).fetchall()
        return [
            DeadRow(
                seq=r[0], namespace=r[1], tag=r[2], payload=r[3].encode(),
                prev_seq=r[4], source=r[5], attempts=r[6],
                last_error=r[7], last_error_class=r[8],
                dead_lettered_at=r[9], reason=r[10],
            )
            for r in rows
        ]

    def get_dead(self, seq: int) -> DeadRow | None:
        """Return one dead-lettered row by seq, or None if absent."""
        with thread_conn(self.db_path) as conn:
            r = conn.execute(
                "SELECT seq, namespace, tag, payload, prev_seq, source, attempts, "
                "last_error, last_error_class, dead_lettered_at, reason "
                "FROM outbox_dead_log WHERE namespace = ? AND seq = ?",
                [self.namespace, seq],
            ).fetchone()
        if r is None:
            return None
        return DeadRow(
            seq=r[0], namespace=r[1], tag=r[2], payload=r[3].encode(),
            prev_seq=r[4], source=r[5], attempts=r[6],
            last_error=r[7], last_error_class=r[8],
            dead_lettered_at=r[9], reason=r[10],
        )
```

At the top of `src/sqloutbox/_outbox.py`, update the model import so `DeadRow` is available. Replace `from sqloutbox._models import QueueRow` with:

```python
from sqloutbox._models import DeadRow, QueueRow
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_dead_letter.py -v -k "dead_letter_moves or dead_letter_unknown or replay or get_dead"`
Expected: PASS (4 passed).

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest -q`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/_outbox.py tests/test_dead_letter.py
git commit -m "feat(dead-letter): Outbox.dead_letter/replay/list_dead/get_dead (WS-2)"
```

---

## Task 5: Error classification helper (`classify_write_error`)

Per FIRST spec §3.3: map a destination error message to one of `TRANSIENT | DETERMINISTIC | ALREADY_APPLIED | UNKNOWN`. Pure function, substring-based (the spec leaves exact substrings open at implementation — these are the spec's own examples). Classification changes *reporting only* — `ALREADY_APPLIED` is the only class that advances a still-undelivered row (a UNIQUE collision proves the row's key already exists at the destination).

**Files:**
- Modify: `src/sqloutbox/sync.py`
- Test: `tests/test_dead_letter.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_dead_letter.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dead_letter.py -v -k classify`
Expected: FAIL — `classify_write_error` does not exist (ImportError on the new import line fails the whole file at collection).

- [ ] **Step 3: Implement the classifier**

In `src/sqloutbox/sync.py`, add this function in the `# ── SQL helpers ──` section, immediately AFTER the `inject_outbox_seq` function (after its `return s, list(args) + [outbox_seq]` final line):

```python
def classify_write_error(error: str | None) -> str:
    """Classify a destination write error per FIRST spec §3.3.

    Returns one of: TRANSIENT | DETERMINISTIC | ALREADY_APPLIED | UNKNOWN.
    Substring-based and conservative — UNKNOWN is the safe default (retry).
    Classification changes REPORTING only: no class drops data. ALREADY_APPLIED
    is the single class the drain treats as success (a UNIQUE collision on an
    idempotent INSERT proves the row's key already exists at the destination).
    """
    if not error:
        return "UNKNOWN"
    e = error.lower()

    # ALREADY_APPLIED first: a UNIQUE/duplicate-key collision means the row is
    # provably present at the destination (idempotent INSERT OR IGNORE). Checked
    # before DETERMINISTIC because "constraint" also appears in FK/NOT NULL text.
    if "unique constraint" in e or "duplicate key" in e or "already exists" in e:
        return "ALREADY_APPLIED"

    # TRANSIENT: network / 5xx / timeout / contended lock — retry with backoff.
    if (
        "timeout" in e or "timed out" in e
        or "connection" in e or "reset" in e
        or "temporarily unavailable" in e or "503" in e or "502" in e
        or "504" in e or "database is locked" in e or "busy" in e
    ):
        return "TRANSIENT"

    # DETERMINISTIC: schema / SQL faults — retry w/ backoff (may clear after a
    # destination migration or once a prior row lands), but never dropped.
    if (
        "foreign key" in e or "not null" in e
        or "no such column" in e or "no such table" in e
        or "syntax error" in e or "constraint failed" in e
    ):
        return "DETERMINISTIC"

    return "UNKNOWN"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_dead_letter.py -v -k classify`
Expected: PASS (4 passed).

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest -q`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/sync.py tests/test_dead_letter.py
git commit -m "feat(retry): classify_write_error TRANSIENT/DETERMINISTIC/ALREADY_APPLIED/UNKNOWN (WS-1)"
```

---

## Task 6: Head-of-line hold + per-row confirm rework + auto-dead-letter in `_flush_to_target`

**This is the core behavior change (WS-1) + auto-dead-letter (WS-2/D1).** Today `_flush_to_target` confirms each row INDEPENDENTLY by `result["ok"]` — a later row leapfrogs a failed earlier row in the same namespace. Rework it so that, **per namespace, in chain (seq) order, confirmation STOPS at the first non-ok row.** On that failing head: classify the error, `record_attempt`, and — if `config.max_attempts` is not None and `attempts >= max_attempts` — `dead_letter(reason='max_attempts')` and advance (the namespace unblocks); otherwise hold it (backoff in Task 7 gates the retry). `ALREADY_APPLIED` is treated as a confirmed delivery (advance the head). After the move, the next head's `attempts` is naturally 0 (different row, no explicit reset).

> Build on Plan 1's L1/L2 guards. The per-row decode/transform `try/except` (L1) and the per-table `try/except` wrap (L2) in `_worker_loop` are already present — do not remove or duplicate them. This task touches ONLY `_flush_to_target`. (Task 8 upgrades L1's "skip this cycle" for an undecodable row into a dead-letter.)

**Files:**
- Modify: `src/sqloutbox/sync.py` — `_flush_to_target` (the method beginning `async def _flush_to_target(`).
- Test: `tests/test_dead_letter.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_dead_letter.py`:

```python
import asyncio

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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_dead_letter.py -v -k "head_hold or head_success or auto_dead_letter or plateaus or already_applied"`
Expected: FAIL — today's `_flush_to_target` confirms each row independently (s2 leapfrogs the failed s1 → `pending_count()==1` not 2 in the head-hold test), never records attempts, never dead-letters, and treats `ALREADY_APPLIED` as a plain failure (does not advance).

- [ ] **Step 3: Rewrite `_flush_to_target`**

In `src/sqloutbox/sync.py`, replace the ENTIRE `_flush_to_target` method (from `async def _flush_to_target(` through its final `write=%.0fms  cycle=%.0fms` logger call and the closing of the method, i.e. everything before the `# ── Maintenance ──` comment) with:

```python
    async def _flush_to_target(
        self,
        writer: OutboxWriter,
        stmts: list[tuple[str, list[Any]]],
        stmt_info: list[tuple[str, int]],
        outboxes: dict[str, Outbox],
        target_name: str,
        cycle_start: float,
    ) -> None:
        """Send a batch and confirm delivery HEAD-FIRST per namespace.

        WS-1 head-of-line hold: rows are grouped by namespace and confirmed in
        seq order. Confirmation STOPS at the first non-ok row in a namespace —
        later rows do NOT leapfrog a failed predecessor. The failed head is
        classified (§3.3), its attempt recorded (§3.1), and:
          * ALREADY_APPLIED → treated as delivered (advance the head).
          * else, max_attempts hit → dead-lettered reason='max_attempts' (D1),
            namespace unblocks.
          * else → held; the §3.2 backoff gate (in _worker_loop) defers its retry.
        """
        logger.debug(
            "[outbox_sync] cycle #%d  sending %d rows across %d tables to %s",
            self._cycle_count, len(stmts),
            len({t for t, _ in stmt_info}), target_name,
        )

        t_write = time.monotonic()
        try:
            results = await writer.write_batch(stmts)
        except Exception as exc:
            logger.warning(
                "[outbox_sync] cycle #%d  %s write failed (%d rows, %.0fms) "
                "— will retry: %s",
                self._cycle_count, target_name, len(stmts),
                (time.monotonic() - t_write) * 1000, exc,
            )
            return
        write_ms = (time.monotonic() - t_write) * 1000

        # Group (result, seq) by namespace, preserving the batch order (which is
        # seq order — _worker_loop fetched ORDER BY seq).
        by_table: dict[str, list[tuple[int, dict[str, Any]]]] = defaultdict(list)
        for i, result in enumerate(results):
            table, outbox_seq = stmt_info[i]
            by_table[table].append((outbox_seq, result))

        max_attempts = self._config.max_attempts
        total_confirmed = 0
        total_failed = 0
        total_dead = 0

        for table, items in by_table.items():
            outbox = outboxes[table]
            confirmed: list[int] = []
            # Walk this namespace's rows in seq order; stop at the first hold.
            for seq, result in items:
                if result.get("ok"):
                    confirmed.append(seq)
                    continue

                # First non-ok row → classify and decide head's fate.
                err = result.get("error", "") or ""
                err_class = classify_write_error(err)

                if err_class == "ALREADY_APPLIED":
                    # The row's key already exists at the destination — success.
                    # Confirm it and keep walking (it does NOT block successors).
                    confirmed.append(seq)
                    logger.info(
                        "[outbox_sync] %s '%s' seq=%d already applied — advancing",
                        target_name, table, seq,
                    )
                    continue

                # A genuine failure: HOLD. Record the attempt, then either
                # dead-letter (cap hit) or stop confirming this namespace.
                await asyncio.to_thread(
                    outbox.record_attempt, seq, err, err_class,
                )
                attempts_now = (
                    outbox.peek_head().attempts
                    if outbox.peek_head() is not None else 0
                )
                if max_attempts is not None and attempts_now >= max_attempts:
                    moved = await asyncio.to_thread(
                        outbox.dead_letter, seq, "max_attempts",
                    )
                    if moved:
                        total_dead += 1
                        logger.warning(
                            "[outbox_sync] dead-lettered ns=%s seq=%s after %d "
                            "attempts: %s",
                            table, seq, attempts_now, err,
                        )
                    # Namespace unblocks — but we deliberately do NOT continue
                    # confirming rows that were sent AFTER this one in the same
                    # batch: they were fetched assuming this head; let the next
                    # cycle re-fetch from the (now advanced) head in clean order.
                    total_failed += 1
                    break
                else:
                    logger.warning(
                        "[outbox_sync] %s '%s' seq=%d held (attempt %d, class=%s): %s",
                        target_name, table, seq, attempts_now, err_class, err,
                    )
                    total_failed += 1
                    break

            if confirmed:
                await asyncio.to_thread(outbox.mark_synced, confirmed)
                await asyncio.to_thread(outbox.delete_synced, confirmed)
                total_confirmed += len(confirmed)
                if logger.isEnabledFor(_VERBOSE):
                    logger.log(
                        _VERBOSE,
                        "[outbox_sync]   confirmed %s table='%s'  %d rows  seqs=%s",
                        target_name, table, len(confirmed), confirmed[:10],
                    )

        cycle_ms = (time.monotonic() - cycle_start) * 1000
        if total_confirmed or total_failed or total_dead:
            level = (
                logging.INFO
                if (total_confirmed >= 10 or total_failed or total_dead)
                else logging.DEBUG
            )
            logger.log(
                level,
                "[outbox_sync] cycle #%d  %s delivered=%d  held=%d  dead=%d  "
                "tables=%s  write=%.0fms  cycle=%.0fms",
                self._cycle_count, target_name, total_confirmed, total_failed,
                total_dead, list(by_table.keys()), write_ms, cycle_ms,
            )
```

> Why `break` after the first hold within a namespace: the rows behind the held head must NOT be confirmed (no leapfrog). Walking the rest of the batch and confirming the successes would be exactly the leapfrog bug. We stop this namespace's confirmation at the hold; the next cycle re-fetches cleanly from the new head.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_dead_letter.py -v -k "head_hold or head_success or auto_dead_letter or plateaus or already_applied"`
Expected: PASS (5 passed).

- [ ] **Step 5: Update any existing test that asserted the OLD leapfrog semantics, then run the full suite**

Run: `python -m pytest -q`
Expected: mostly green. If a test in `tests/test_sync.py` (or elsewhere) FAILS because it asserted that a later row was confirmed while an earlier row in the SAME namespace failed (the old independent-per-row-confirm), that is the deliberate WS-1 semantics change (FIRST spec §3.2, §7). Update that test to the no-leapfrog contract: a failed head holds the rows behind it. Do NOT weaken or skip a test without confirming it is asserting the old leapfrog behavior — if a failure is anything else, investigate it as a regression. Re-run `python -m pytest -q` until all green.

> Note: tests where all rows of a namespace succeed (the common case in `test_sync.py`) are unaffected — head-first confirmation of all-ok batches is identical to the old behavior.

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/sync.py tests/test_dead_letter.py tests/test_sync.py
git commit -m "feat(retry): head-of-line hold + auto-dead-letter in _flush_to_target (WS-1/WS-2/D1)"
```

---

## Task 7: Per-namespace exponential backoff gate + head-only fetch in `_worker_loop`

Per FIRST spec §3.2: a stuck head (attempts > 0) is not retried until `last_attempt_at + min(2^attempts, cap_minutes)` has elapsed, and while stuck the namespace fetches ONLY the head (limit=1) so there is nothing behind it to leapfrog. The cap is the FIRST spec's `backoff_cap_minutes` (default 64); per the CONTRACT, config fields beyond `max_attempts`/`max_pending`/`max_batch_bytes` are not owned by this plan — so this plan reads the cap defensively via `getattr(self._config, "backoff_cap_minutes", 64)` (works whether or not Plan 6/health adds it). Use `time.monotonic` is NOT possible across the persisted `last_attempt_at` (wall-clock ISO) — so the gate parses `last_attempt_at` (UTC ISO) and compares to `datetime.now(timezone.utc)`; we still avoid wall-clock step-back risk by treating a parse failure or a `last_attempt_at` in the future as "eligible now" (never stall forever).

**Files:**
- Modify: `src/sqloutbox/sync.py` — `_worker_loop` (the inner `for table, outbox in outboxes.items():` body) + imports.
- Test: `tests/test_dead_letter.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_dead_letter.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_dead_letter.py -v -k "backoff_gate or stuck_head_fetches"`
Expected: FAIL — there is no backoff gate (a stuck head is re-sent every cycle → `writer.seen != []` / `pending_count()==0` in the suppress test) and no head-only fetch (the stuck-head test sees a 3-row batch, not 1).

- [ ] **Step 3a: Add imports and a backoff-gate helper**

In `src/sqloutbox/sync.py`, the import block currently has `import time`. Add `datetime` imports right after the existing `import time` line:

```python
import time
from datetime import datetime, timedelta, timezone
```

Then add this module-level helper in the `# ── SQL helpers ──` section, immediately after `classify_write_error` (added in Task 5):

```python
def _backoff_eligible(attempts: int, last_attempt_at: str | None, cap_minutes: int) -> bool:
    """Return True if a stuck head is eligible to retry now (§3.2 backoff gate).

    delay = min(2^attempts, cap) minutes after last_attempt_at (UTC ISO).
    A missing/unparseable/future last_attempt_at → eligible now (never stall
    forever — fail open, the row is retried rather than stranded).
    """
    if attempts <= 0 or not last_attempt_at:
        return True
    try:
        last = datetime.fromisoformat(last_attempt_at)
    except ValueError:
        return True
    delay = timedelta(minutes=min(2 ** attempts, cap_minutes))
    next_eligible = last + delay
    now = datetime.now(timezone.utc)
    # last_attempt_at in the future (clock skew) → treat as eligible.
    if next_eligible <= now or last > now:
        return True
    return False
```

- [ ] **Step 3b: Gate the table and fetch head-only when stuck**

> **Precondition (Plan 1 / WS-0 must be merged):** this step edits inside Plan 1's L2 `try:` wrap in `_worker_loop`. On the pre-Plan-1 source the inner loop body is NOT indented under a `try:` — it reads `for table, outbox in outboxes.items():` then directly `pending = outbox.pending_count()` with no `try:`. If you do NOT see the L2 `try:`/`except (sqlite3.DatabaseError, ...)` wrap around this loop body, Plan 1 has not been merged — STOP and confirm execution order before editing (locate by the quoted code, not the line number).

In `_worker_loop`, the inner loop currently starts (after Plan 1's L2 `try:` wrap) with:

```python
                for table, outbox in outboxes.items():
                  try:
                    pending = outbox.pending_count()
                    if pending == 0:
                        continue

                    elapsed = now - last_flush.get(table, 0.0)
```

Insert the backoff gate immediately after `if pending == 0: continue` and before the `elapsed = ...` line, and capture whether the head is stuck so the fetch can be head-only. Replace the block above with:

```python
                for table, outbox in outboxes.items():
                  try:
                    pending = outbox.pending_count()
                    if pending == 0:
                        continue

                    # ── WS-1 backoff gate ────────────────────────────────
                    # A stuck head (attempts>0) is suppressed until its
                    # min(2^attempts, cap) backoff has elapsed — regardless of
                    # the threshold / max_wait triggers below. While stuck we
                    # fetch ONLY the head (limit=1) so nothing behind it can
                    # leapfrog (the no-skip / head-of-line invariant, §3.2).
                    cap_minutes = getattr(
                        self._config, "backoff_cap_minutes", 64,
                    )
                    head = outbox.peek_head()
                    head_stuck = bool(head and head.attempts > 0)
                    if head_stuck:
                        if not _backoff_eligible(
                            head.attempts, head.last_attempt_at, cap_minutes,
                        ):
                            if logger.isEnabledFor(_VERBOSE):
                                logger.log(
                                    _VERBOSE,
                                    "[outbox_sync] backoff: table='%s' head seq=%d "
                                    "attempts=%d not yet eligible — skipping",
                                    table, head.seq, head.attempts,
                                )
                            continue

                    elapsed = now - last_flush.get(table, 0.0)
```

Then update the fetch line. The current line is:

```python
                    # ── Table is ready — fetch rows ──────────────────────
                    rows = await asyncio.to_thread(outbox.fetch_unsynced)
```

Replace it with a head-only fetch when stuck:

```python
                    # ── Table is ready — fetch rows ──────────────────────
                    # Head-of-line hold: while the head is stuck, fetch ONLY it
                    # (limit=1). A healthy namespace fetches a normal batch.
                    if head_stuck:
                        rows = await asyncio.to_thread(outbox.fetch_unsynced, 1)
                    else:
                        rows = await asyncio.to_thread(outbox.fetch_unsynced)
```

> Do not change the `verify_chain` / per-row decode / `flushed_tables.append(table)` lines, nor Plan 1's L1 per-row `try/except` and L2 `except (sqlite3.DatabaseError, ...)` wrap — they stay as-is.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_dead_letter.py -v -k "backoff_gate or stuck_head_fetches"`
Expected: PASS (3 passed).

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest -q`
Expected: all green. (Existing `test_sync.py` cases enqueue fresh rows with `attempts=0`, so the gate never triggers for them — `head_stuck` is False and behavior is unchanged.)

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/sync.py tests/test_dead_letter.py
git commit -m "feat(retry): per-namespace exponential backoff gate + head-only fetch (WS-1)"
```

---

## Task 8: Upgrade L1 undecodable-row handling into `dead_letter(reason='undecodable')`

Plan 1 (WS-0) made an undecodable payload *log-and-skip-this-cycle* (the row stays pending and is retried forever). Per FIRST spec note + standalone §3.2, an undecodable payload can NEVER succeed — so it must be dead-lettered immediately with `reason='undecodable'` (the attempts gate is moot). This upgrades L1 in `_worker_loop`.

**Files:**
- Modify: `src/sqloutbox/sync.py` — the L1 per-row `try/except` in `_worker_loop` (added by Plan 1).
- Test: `tests/test_dead_letter.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_dead_letter.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dead_letter.py -v -k undecodable_row_is_dead_lettered`
Expected: FAIL — Plan 1's L1 only logs + `continue`s, leaving the row pending forever; `pending_count()` is 1 and `list_dead()` is empty.

- [ ] **Step 3: Upgrade the L1 except branch to dead-letter**

> **Precondition (Plan 1 / WS-0 must be merged):** this step rewrites the `except` body of Plan 1's L1 per-row `try`/`except`. On the pre-Plan-1 source the per-row body is NOT wrapped — `_worker_loop` does `for row in rows:` then directly `sql = row.tag` / `args = json.loads(row.payload.decode())` with no surrounding `try:`/`except`, so the `"skipping bad row ..."` log line does NOT exist. If you cannot find that L1 `except Exception as exc:` block, Plan 1 has not been merged — STOP and confirm execution order (locate by the quoted code, not the line number).

In `src/sqloutbox/sync.py`, locate Plan 1's L1 guard inside `_worker_loop` — the per-row `try`/`except` block that wraps the decode/transform. Its `except` body currently logs `"skipping bad row table='%s' seq=%d: %s"` and `continue`s. Replace that `except` block with a dead-letter:

```python
                        except Exception as exc:
                            # L1 (WS-2 upgrade): an undecodable / untransformable
                            # payload can never succeed — dead-letter it now
                            # (reason='undecodable') instead of retrying forever.
                            # The move is atomic (Outbox.dead_letter); the row is
                            # quarantined + replayable, never lost. The namespace
                            # then advances cleanly on the next fetch.
                            logger.error(
                                "[outbox_sync] dead-lettering undecodable row "
                                "table='%s' seq=%d: %s",
                                table, row.seq, exc,
                            )
                            await asyncio.to_thread(
                                outbox.dead_letter, row.seq, "undecodable",
                            )
                            continue
```

> The surrounding `for row in rows:` loop and the `try:` line above it are Plan 1's — leave them untouched. Only the `except Exception as exc:` body changes.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_dead_letter.py -v -k undecodable_row_is_dead_lettered`
Expected: PASS.

- [ ] **Step 5: Reconcile the WS-0 resilience tests, then run the full suite**

Run: `python -m pytest -q`
Expected: mostly green. `tests/test_resilience.py` (from Plan 1) has `test_undecodable_row_does_not_kill_loop` and `test_poison_and_healthy_coexist`, which assert the bad row is NOT delivered (`writer.seen == []` / poison not in delivered). Those assertions STILL HOLD after this change (a dead-lettered row is never delivered). The change is additive to that contract — the row now also leaves the queue. If either test additionally asserted the row *stays pending*, update it to assert it is dead-lettered (it does not — both only check non-delivery — so they should remain green). If anything fails, investigate before editing.

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/sync.py tests/test_dead_letter.py
git commit -m "feat(dead-letter): upgrade L1 undecodable-row skip into dead_letter (WS-2, F002)"
```

---

## Task 9: Operator CLI — `dead-letter {list,show,replay}` and `skip`

Per standalone §3.3: operator escape hatch. `dead-letter list` shows what is quarantined and why; `dead-letter show` prints one row's full SQL/args/error; `dead-letter replay` re-enqueues at the tail; `skip` moves a stuck head → dead_log with `reason='manual_skip'`. Mirror the existing `cmd_verify` config-discovery pattern (resolve `.db` files from a TOML config's targets) and the argparse subparser block.

**Files:**
- Modify: `src/sqloutbox/cli.py`
- Test: `tests/test_dead_letter.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_dead_letter.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dead_letter.py -v -k "cli_dead_letter or cli_skip"`
Expected: FAIL — `cmd_dead_letter` and `cmd_skip` do not exist (ImportError at the new import line fails the file at collection).

- [ ] **Step 3a: Add the CLI handlers**

In `src/sqloutbox/cli.py`, add these handlers AFTER `cmd_verify` (after its final `sys.exit(0 if result.ok else 1)` line) and BEFORE the `# ── Entry point ──` comment. They reuse the `cmd_verify` config-discovery pattern (resolve `.db` files from the TOML config's targets):

```python
# ── dead-letter / skip commands ───────────────────────────────────────────────


def _outboxes_from_config(config_path: Path) -> dict:
    """Map namespace → Outbox for every existing .db file in the TOML config.

    Mirrors cmd_verify's discovery. Returns {namespace: Outbox}.
    """
    from sqloutbox._outbox import Outbox
    from sqloutbox._runner import load_config_toml

    config, _writers = load_config_toml(config_path)
    outboxes: dict = {}
    for target in config.targets:
        db_dir = target.db_dir or config.db_dir
        for table in target.tables:
            db_path = db_dir / f"{table}.db"
            if db_path.exists():
                outboxes[table] = Outbox(db_path=db_path, namespace=table)
    return outboxes


def cmd_dead_letter(
    config_path: Path | None,
    action: str,
    namespace: str | None,
    seq: int | None,
) -> None:
    """Inspect / replay the dead-letter store.

    actions: list | show | replay
    """
    if config_path is None:
        print("error: --config <file.toml> is required", file=sys.stderr)
        sys.exit(1)

    outboxes = _outboxes_from_config(config_path)
    if not outboxes:
        print("no .db files found — nothing to inspect")
        sys.exit(0)

    if action == "list":
        any_dead = False
        print()
        print("sqloutbox dead-letter — quarantined rows")
        print("-" * 70)
        for ns, ob in sorted(outboxes.items()):
            if namespace is not None and ns != namespace:
                continue
            for d in ob.list_dead():
                any_dead = True
                print(
                    f"  {ns:<20s}  seq={d.seq:<8d}  reason={d.reason:<14s}  "
                    f"attempts={d.attempts}  class={d.last_error_class or '-'}"
                )
        if not any_dead:
            print("  (none)")
        print()
        sys.exit(0)

    # show / replay need a specific namespace + seq.
    if namespace is None or seq is None:
        print("error: --namespace and --seq are required for show/replay",
              file=sys.stderr)
        sys.exit(1)
    ob = outboxes.get(namespace)
    if ob is None:
        print(f"error: namespace '{namespace}' not found", file=sys.stderr)
        sys.exit(1)

    if action == "show":
        d = ob.get_dead(seq)
        if d is None:
            print(f"error: no dead row seq={seq} in namespace '{namespace}'",
                  file=sys.stderr)
            sys.exit(1)
        print()
        print(f"  namespace        {d.namespace}")
        print(f"  seq              {d.seq}")
        print(f"  reason           {d.reason}")
        print(f"  attempts         {d.attempts}")
        print(f"  last_error_class {d.last_error_class}")
        print(f"  last_error       {d.last_error}")
        print(f"  dead_lettered_at {d.dead_lettered_at}")
        print(f"  source           {d.source}")
        print(f"  prev_seq         {d.prev_seq}")
        print(f"  sql              {d.tag}")
        print(f"  args             {d.payload.decode('utf-8', 'replace')}")
        print()
        sys.exit(0)

    if action == "replay":
        new_seq = ob.replay(seq)
        if new_seq is None:
            print(f"error: no dead row seq={seq} in namespace '{namespace}'",
                  file=sys.stderr)
            sys.exit(1)
        print(f"replayed namespace='{namespace}' seq={seq} → new seq={new_seq}")
        sys.exit(0)

    print(f"error: unknown dead-letter action '{action}'", file=sys.stderr)
    sys.exit(1)


def cmd_skip(config_path: Path | None, namespace: str | None, seq: int | None) -> None:
    """Manually move a stuck head row → dead_log (reason='manual_skip')."""
    if config_path is None or namespace is None or seq is None:
        print("error: --config, --namespace, and --seq are all required",
              file=sys.stderr)
        sys.exit(1)
    outboxes = _outboxes_from_config(config_path)
    ob = outboxes.get(namespace)
    if ob is None:
        print(f"error: namespace '{namespace}' not found", file=sys.stderr)
        sys.exit(1)
    if ob.dead_letter(seq, reason="manual_skip"):
        print(f"skipped namespace='{namespace}' seq={seq} → dead_log")
        sys.exit(0)
    print(f"error: no queue row seq={seq} in namespace '{namespace}'",
          file=sys.stderr)
    sys.exit(1)
```

- [ ] **Step 3b: Register the subparsers and dispatch**

In `src/sqloutbox/cli.py`'s `main(argv)`, after the `p_verify` block (the `p_verify.add_argument("--db-dir", ...)` call) and before `args = parser.parse_args(argv)`, add:

```python
    p_dl = sub.add_parser(
        "dead-letter",
        help="inspect / replay quarantined (dead-lettered) rows",
    )
    p_dl.add_argument(
        "dl_action", choices=("list", "show", "replay"),
        help="list all, show one, or replay one dead-lettered row",
    )
    p_dl.add_argument(
        "--config", "-c", type=Path, default=None, dest="dl_config",
        help="TOML config file (discover .db files from targets)",
    )
    p_dl.add_argument("--namespace", "-n", default=None,
                      help="namespace (table) to filter / target")
    p_dl.add_argument("--seq", "-s", type=int, default=None,
                      help="dead-letter row seq (required for show/replay)")

    p_skip = sub.add_parser(
        "skip",
        help="manually move a stuck head row → dead_log (reason=manual_skip)",
    )
    p_skip.add_argument(
        "--config", "-c", type=Path, default=None, dest="skip_config",
        help="TOML config file (discover .db files from targets)",
    )
    p_skip.add_argument("--namespace", "-n", default=None, required=False,
                        help="namespace (table) whose head to skip")
    p_skip.add_argument("--seq", "-s", type=int, default=None, required=False,
                        help="seq of the stuck head row to skip")
```

Then in the dispatch chain at the bottom of `main` (the `if args.command == "init": ... elif args.command == "verify": ...` ladder), add two more branches after the `verify` branch:

```python
    elif args.command == "dead-letter":
        cmd_dead_letter(args.dl_config, args.dl_action, args.namespace, args.seq)
    elif args.command == "skip":
        cmd_skip(args.skip_config, args.namespace, args.seq)
```

> The `argparse` namespace exposes `--namespace`/`--seq` as `args.namespace`/`args.seq` for BOTH `dead-letter` and `skip` (same dest names), and the per-command config flags are `args.dl_config` / `args.skip_config` to avoid clashing with `verify`'s `args.verify_config`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_dead_letter.py -v -k "cli_dead_letter or cli_skip"`
Expected: PASS (2 passed).

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest -q`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/cli.py tests/test_dead_letter.py
git commit -m "feat(cli): dead-letter list/show/replay + skip subcommands (WS-2)"
```

---

## Task 10: Doc reconciliation — soften the FIRST spec §9 "never abandons a row" (GATED on maintainer go-ahead)

Per standalone §7, the FIRST spec's §9 locked bullet *"library never abandons a row"* contradicts D1 (auto-dead-letter). The standalone spec requires a **separate, explicit edit** to the FIRST spec so the two are consistent: soften "never *abandons*" to "never *loses* a row; may *quarantine* to an audited dead-letter after `max_attempts`."

> **DO NOT assume this edit is already done, and DO NOT silently fold it in.** The pushed FIRST spec is a shared design doc. Editing it needs the maintainer's go-ahead. This task is to (a) surface the exact diff to the maintainer, and (b) apply it ONLY after explicit approval.

**Files:**
- Modify (gated): `docs/specs/2026-06-11-durable-ordered-retry-and-health-signal.md` §9.

- [ ] **Step 1: Surface the proposed diff to the maintainer**

Present this exact change for approval (do not apply yet). In `docs/specs/2026-06-11-durable-ordered-retry-and-health-signal.md` §9 "Decisions locked", the bullet:

```
- ✅ **Never-skip head + 2ⁿ backoff (cap configurable, default 64 min)**; plateau-retry forever (library never abandons a row).
```

becomes:

```
- ✅ **Never-skip head + 2ⁿ backoff (cap configurable, default 64 min)**; plateau-retry forever **when `max_attempts=None`**. The library never *loses* a row; in standalone mode (finite `max_attempts`, default 10) it *quarantines* a persistently failing head to an audited, replayable `outbox_dead_log` after `max_attempts` and advances so the namespace unblocks (see the standalone-hardening spec §3.2/§7). Set `max_attempts=None` to restore plateau-forever (embedded-consumer mode).
```

And add, near the §9 cross-references:

```
- ✅ Reconciled with `docs/specs/2026-06-11-standalone-hardening-design.md` §7: "never DROP / never LOSE data" is the deep invariant (preserved — dead-letter is an atomic move, not a delete); "never abandon a row in the delivery queue without a human" is the shallow rule, revised for standalone daemons by the opt-out `max_attempts` safety valve.
```

- [ ] **Step 2: Apply ONLY after explicit maintainer approval**

If — and only if — the maintainer approves, apply the two edits above with the Edit tool against `docs/specs/2026-06-11-durable-ordered-retry-and-health-signal.md`. If not approved, leave the FIRST spec untouched and note in the PR description that the §9 amendment is pending maintainer sign-off (the code is already consistent with the standalone spec's reconciliation regardless).

- [ ] **Step 3: Commit (only if the edit was applied)**

```bash
git add docs/specs/2026-06-11-durable-ordered-retry-and-health-signal.md
git commit -m "docs(spec): reconcile §9 'never abandons' → 'never loses; may quarantine' (WS-2 §7)"
```

---

## Self-Review notes (for the executor)

- **Spec coverage:** Task 1 = FIRST §3.1 columns + standalone §3.1 dead-log DDL. Tasks 2–4 = `Outbox` dead-letter/replay/peek/record + `_seq_accounted` chain re-stitch (standalone §3.3). Task 5 = FIRST §3.3 classification. Task 6 = FIRST §3.2 head-of-line hold + standalone §3.2 D1 auto-dead-letter + ALREADY_APPLIED advance. Task 7 = FIRST §3.2 backoff gate + head-only fetch. Task 8 = standalone §3.2 undecodable-immediate-dead-letter (upgrading Plan 1's L1). Task 9 = standalone §3.3 operator CLI. Task 10 = standalone §7 doc reconciliation (gated).
- **Cross-plan names (do NOT redefine):** `config.max_attempts` and `exceptions.py`/`ConfigError`/`QueueFullError`/`UnsupportedStatementError`/`ChainIntegrityError` are owned by Plan 3 (WS-3) — this plan only *reads* `max_attempts`. The grammar guard (`inject_outbox_seq` → `UnsupportedStatementError` + `reason='unsupported_stmt'`) is Plan 3's; this plan's `reason` literals are `'max_attempts' | 'manual_skip' | 'undecodable'` (Plan 3 adds `'unsupported_stmt'`). `busy_timeout` on connections is Plan 1's (do not re-add). `health()` / `peek_head` usage by the signal is Plan 6 — this plan adds `peek_head` (FIRST §5 lists it as additive) and Plan 6 builds on it.
- **WRITER POLLUTION guard:** every test that builds a service and asserts on `writer.seen`/delivered statements sets `auto_schema=False` AND `inject_outbox_seq=False` (see `_service` helper) so startup `_ensure_schema()`/`_seed_from_remote()` never call `write_batch`. This is the exact Plan 1 lesson #1.
- **Backoff timing note:** the brief says "use `time.monotonic` for backoff timing." That is impossible for a value persisted as wall-clock ISO across processes — `last_attempt_at` is UTC ISO (`now_iso()`), so the gate parses it and compares to `datetime.now(timezone.utc)`. The monotonic-spirit (avoid wall-clock step-back) is honored by failing OPEN: a future/unparseable timestamp → eligible now, never a permanent stall. Verifier: confirm this is acceptable vs. a literal monotonic interpretation.
- **`backoff_cap_minutes`:** read via `getattr(self._config, "backoff_cap_minutes", 64)` because, per the CONTRACT, the only config fields this plan can assume are `max_attempts`/`max_pending`/`max_batch_bytes`. The FIRST spec §5 adds `backoff_cap_minutes=64`; if Plan 6 (or a config plan) adds it, the `getattr` picks it up with no change here. Verifier: confirm no plan in the locked order is meant to own `backoff_cap_minutes` *before* Plan 2 — if one is, replace the `getattr` with `self._config.backoff_cap_minutes`.
- **Line numbers:** all symbols were verified against the real source on `main`/the worktree at plan-writing time: `_flush_to_target` (`sync.py:632`), per-row L1 site (`sync.py:607` pre-Plan-1, wrapped by Plan 1), `_seq_accounted` (`_outbox.py:414`), `fetch_unsynced` (`_outbox.py:202`), `_MIGRATE_ADD_SOURCE`/`_MIGRATE_PREV_SEQ_UNIQUE` (`_schema.py:36/43`), CLI subparser block (`cli.py:579-606`). If they have drifted, locate by the quoted code, not the number.
- **Test gate:** baseline is 180 (`python -m pytest --collect-only -q`); this plan adds ~25 tests across `tests/test_dead_letter.py` plus possible 1–2 updates to `tests/test_sync.py`/`tests/test_resilience.py` for the WS-1 semantics change. Gate on "all green," not a hard number.
- **Behavior-change tests (Task 6 Step 5, Task 8 Step 5):** updating an existing test that asserted the OLD leapfrog/skip behavior is EXPECTED, not a regression — but verify each failure is exactly that before editing. Never silently skip a finance-adjacent durability test.
- **Atomicity of dead_letter:** `dead_letter` uses the persistent `_write_conn` under `BEGIN IMMEDIATE` so INSERT-into-dead-log + DELETE-from-queue commit together (move-not-delete). The drain calls it via `asyncio.to_thread`; since `_write_conn` is owned by the producer thread, in the drain we accept that the move runs on the to_thread worker — confirm in review that no concurrent `enqueue()` races the same connection in tests (tests construct a fresh `Outbox` per assertion, so there is no contention). If a real producer shares the file, `busy_timeout` (Plan 1) absorbs the brief lock contention.
