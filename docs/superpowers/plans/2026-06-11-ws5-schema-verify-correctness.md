# WS-5: Read-Only Verify, Crash-Safe Forked-Chain Migration & Producer Seed Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `sqloutbox verify` a *truly* read-only diagnostic that never creates or migrates a SQLite file, make the `_MIGRATE_PREV_SEQ_UNIQUE` migration crash-safe (a forked chain raises a typed `ChainIntegrityError` instead of a bare `IntegrityError` out of the producer hot path), and add a producer-side persisted high-water-mark seed so a fresh host with a populated remote never assigns colliding `seq` values that `INSERT OR IGNORE` would silently drop.

**Architecture:** Three independent correctness fixes in `_schema.py`, `_verify.py`, `cli.py`, and `_outbox.py`. (A) A dedicated read-only open path `open_read_conn()` uses `sqlite3.connect("file:{path}?mode=ro", uri=True)` — no `mkdir`, no `CREATE TABLE`, no `PRAGMA journal_mode=WAL`, no `ALTER`; verify re-routes through it and reports a missing file instead of creating it. (B) The unguarded `CREATE UNIQUE INDEX` migration is wrapped in `try/except` like its sibling `_MIGRATE_ADD_SOURCE`; on a duplicate-`prev_seq` fork it raises `ChainIntegrityError` (owned by Plan 3) naming the duplicate rows + a recovery pointer, so `Outbox.__init__` no longer dies with a bare `IntegrityError` — while read-only verify still *opens and reports* the fork. (C) A new `outbox_hwm` table persists a per-namespace high-water mark; the producer lazily seeds its local `AUTOINCREMENT` from it in `Outbox.__init__` so fresh-host seqs start above the remote max.

**Tech Stack:** Python 3.10+ stdlib only (sqlite3, asyncio, json, logging); pytest + pytest-asyncio.

**Spec:** `docs/specs/2026-06-11-standalone-hardening-design.md` §6 — §6.1 (truly read-only verify, F005/F050), §6.2 (crash-safe migration / forked chain, F006/F029), §6.3 (producer-side seed, F004 — **recommended mechanism (a)**: producer lazily seeds local AUTOINCREMENT from a persisted high-water mark). Companion: `docs/specs/2026-06-11-durable-ordered-retry-and-health-signal.md`.

**Recommended execution order:** This is **Plan 5 (WS-5)** in the locked cross-plan order: Plan 1 WS-0 (done) → Plan 3 WS-3 → Plan 2 WS-1+2 → Plan 4 WS-4 → **Plan 5 WS-5** → Plan 6 WS-6+7. By the time this plan runs, the following shared-file changes are already present and **must not be re-added**:
- **From Plan 1 (WS-0, done):** `_schema.py` already sets `PRAGMA busy_timeout=30000` on both `open_write_conn()` and `thread_conn()` (constant `_BUSY_TIMEOUT_MS`). `sync.py` `_worker_loop` already has the L1 per-row and L2 per-unit guards; `_runner.py` already observes the drain task (L3). Do not duplicate any of these.
- **From Plan 3 (WS-3):** `src/sqloutbox/exceptions.py` exists and exports `SqloutboxError`, `ConfigError`, `QueueFullError`, `UnsupportedStatementError`, **`ChainIntegrityError`**. This plan **imports `ChainIntegrityError` from `sqloutbox.exceptions`** — do NOT define it here. `config.py` already has `max_attempts` / `max_pending` / `max_batch_bytes` with `__post_init__` validation (not touched by this plan).
- **From Plan 2 (WS-2):** `_schema.py` has an `outbox_dead_log` table and `outbox_queue` retry columns; `_outbox.py` has `dead_letter`/`replay` and `_seq_accounted` consults `outbox_dead_log`. This plan does not touch those, but the read-only verify path in Task 1 must tolerate their presence (it only SELECTs from `outbox_queue` / `outbox_sync_log`, so it is unaffected).

> **Line numbers in this plan were accurate against the working tree at plan-writing time. If they have drifted, locate the code by the quoted snippet, not the number.** The full suite baseline was ~180 tests (`python -m pytest --collect-only -q`) before Plans 1-4 added more; gate every step on **"all green"**, never on an exact count.

---

## File Structure

| File | Responsibility | Create/Modify |
|------|----------------|---------------|
| `src/sqloutbox/_schema.py` | Add `open_read_conn()` (true read-only path); add `_CREATE_HWM` table to `open_write_conn`; wrap `_MIGRATE_PREV_SEQ_UNIQUE` in try/except raising `ChainIntegrityError`. | Modify |
| `src/sqloutbox/_verify.py` | Re-route all read connections through a read-only opener; tolerate a forked DB (report, never crash); add `verify_db_path()` free function that opens read-only by path and reports "not an outbox DB" for a missing/foreign file. | Modify |
| `src/sqloutbox/cli.py` | `cmd_verify`: skip files that are missing / not an outbox DB (report, never create); use `verify_db_path()` so globbing a stray file no longer migrates it. | Modify |
| `src/sqloutbox/_outbox.py` | Producer-side seed: persist a high-water mark in `outbox_hwm` on every enqueue; lazily seed local AUTOINCREMENT from it in `__init__`; expose `_persisted_hwm()` / `record_hwm()`. | Modify |
| `tests/test_ws5_schema_verify.py` | All WS-5 tests (read-only verify, forked-chain migration, producer seed). | Create |

**Why one new test file:** these tests share helpers (a hand-built forked DB, a populated-remote stand-in, a file-mtime/journal-mode snapshot) and all assert the same family of properties — "inspection never mutates, a fork never crashes the hot path, a fresh host never collides." Keeping them in one file makes those invariants auditable in one place. Match the `tests/test_sync.py` / `tests/test_verify.py` conventions; `asyncio_mode = "auto"` (in `pyproject.toml`) makes `@pytest.mark.asyncio` optional but harmless — none of WS-5's tests need async.

---

## Task 1: Read-only verify path — inspection must never create or migrate a file (§6.1, F005/F050)

**Problem (verified against source):** `cmd_verify` (`cli.py:459`) globs `*.db` (`cli.py:492`) and constructs `Outbox(db_path=db_file, namespace=name)` (`cli.py:494`). `Outbox.__init__` (`_outbox.py:50-64`) calls `open_write_conn(db_path)` (`_schema.py:77`), which does `db_path.parent.mkdir(parents=True, exist_ok=True)` (`_schema.py:85`), `PRAGMA journal_mode=WAL` (`:87`), `CREATE TABLE …` (`:89-90`), `ALTER TABLE …` (`:96`), and `CREATE UNIQUE INDEX …` (`:101`) — i.e. a supposedly read-only `verify` **creates and migrates files**. `verify_outbox` (`_verify.py:65`) and `verify_full` (`_outbox.py:401`) also open via `thread_conn` (`_schema.py:106`), which is writable. This task adds a dedicated read-only opener and routes verification through it.

**Files:**
- Modify: `src/sqloutbox/_schema.py` (add `open_read_conn`)
- Modify: `src/sqloutbox/_verify.py` (route reads through it; add `verify_db_path`)
- Modify: `src/sqloutbox/cli.py` (`cmd_verify` uses `verify_db_path`, skips missing/foreign files)
- Test: `tests/test_ws5_schema_verify.py` (Create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_ws5_schema_verify.py` with:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_ws5_schema_verify.py -v -k "open_read_conn or verify_db_path"`
Expected: FAIL at collection / import — `open_read_conn` and `verify_db_path` do not exist yet (`ImportError` / `AttributeError`), so every test in the selection errors.

- [ ] **Step 3: Add `open_read_conn()` to `_schema.py`**

In `src/sqloutbox/_schema.py`, add this function immediately after `thread_conn` (currently ending at `:112`, before the `# ── Utilities ──` divider at `:115`):

```python
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
```

> `_BUSY_TIMEOUT_MS` was added by Plan 1 (WS-0). If for any reason it is absent, locate the `busy_timeout` PRAGMA in `open_write_conn` and reuse the same constant; do not invent a new one.

- [ ] **Step 4: Add `verify_db_path()` and route reads through the read-only opener in `_verify.py`**

In `src/sqloutbox/_verify.py`, update the import line (currently `:28`):

```python
from sqloutbox._schema import now_iso, open_read_conn, thread_conn
```

Then replace the body of `verify_outbox` (currently `:65-215`) so every connection is read-only. Replace the whole function with:

```python
def verify_outbox(outbox: Outbox) -> TableVerifyResult:
    """Run a comprehensive integrity check on a single outbox.

    All checks are READ-ONLY — opened via ``open_read_conn`` so inspecting a
    file can never create, migrate, or WAL-switch it (spec §6.1). Delegates
    the actual checks to ``verify_db_path`` against the outbox's file/namespace.
    """
    return verify_db_path(outbox.db_path, namespace=outbox.namespace)


def verify_db_path(db_path: Path, namespace: str | None = None) -> TableVerifyResult:
    """Inspect a ``.db`` file READ-ONLY and report its integrity.

    Opens with ``open_read_conn`` (``mode=ro``): never creates the file, never
    migrates it, never switches journal mode. A missing file or a file without
    an ``outbox_queue`` table is REPORTED as "not an outbox DB" (``ok=False``)
    rather than crashing or being created (spec §6.1, F005/F050).

    A forked-chain DB (two rows sharing a ``prev_seq``) is OPENED and the fork
    is reported — read-only verify must remain usable as a diagnostic even on a
    DB that would crash the writable migration path (spec §6.2).

    Parameters
    ----------
    db_path:
        Path to the SQLite file to inspect.
    namespace:
        Namespace to scope the checks to. When ``None`` (e.g. CLI scanning a
        stray file), the file's single namespace is auto-detected; if the file
        holds multiple namespaces the first (by name) is used and the rest are
        ignored — the CLI constructs one Outbox per file/namespace anyway.
    """
    import sqlite3

    db_path = Path(db_path)
    errors: list[str] = []
    label = namespace if namespace is not None else db_path.stem

    # ── Open read-only; a missing file or a foreign file is reported, never
    #    created/migrated. ────────────────────────────────────────────────
    try:
        probe = open_read_conn(db_path)
    except sqlite3.OperationalError as exc:
        return TableVerifyResult(
            table=label, db_path=str(db_path), ok=False,
            pending_count=0, total_rows=0, sync_log_rows=0, chain_ok=False,
            errors=(f"not an outbox DB: cannot open {db_path} read-only ({exc})",),
        )

    try:
        # Is this actually an outbox DB? (foreign/empty file → report, skip.)
        has_queue = probe.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='outbox_queue'"
        ).fetchone()
        if not has_queue:
            return TableVerifyResult(
                table=label, db_path=str(db_path), ok=False,
                pending_count=0, total_rows=0, sync_log_rows=0, chain_ok=False,
                errors=(f"not an outbox DB: no outbox_queue table in {db_path}",),
            )

        # Auto-detect namespace when not supplied (CLI stray-file scan).
        ns = namespace
        if ns is None:
            ns_row = probe.execute(
                "SELECT namespace FROM outbox_queue ORDER BY namespace LIMIT 1"
            ).fetchone()
            ns = ns_row[0] if ns_row else label
        label = ns

        # ── Row counts ──────────────────────────────────────────────
        total_rows = probe.execute(
            "SELECT COUNT(*) FROM outbox_queue WHERE namespace = ?", [ns],
        ).fetchone()[0]
        pending_count = probe.execute(
            "SELECT COUNT(*) FROM outbox_queue WHERE namespace = ? AND synced = 0",
            [ns],
        ).fetchone()[0]
        sync_log_rows = probe.execute(
            "SELECT COUNT(*) FROM outbox_sync_log WHERE namespace = ?", [ns],
        ).fetchone()[0]

        # ── Seq range ───────────────────────────────────────────────
        range_row = probe.execute(
            "SELECT MIN(seq), MAX(seq) FROM outbox_queue WHERE namespace = ?",
            [ns],
        ).fetchone()
        seq_range: tuple[int, int] | None = (
            (range_row[0], range_row[1]) if range_row[0] is not None else None
        )

        # ── Forked-chain detection (read-only; never crashes) ───────
        # Two rows sharing a non-NULL prev_seq = a fork. The writable
        # migration would raise IntegrityError here (Task 2 turns that into
        # ChainIntegrityError) — but the diagnostic must still REPORT it.
        forked = probe.execute(
            "SELECT prev_seq, COUNT(*) c FROM outbox_queue "
            "WHERE namespace = ? AND prev_seq IS NOT NULL "
            "GROUP BY prev_seq HAVING c > 1",
            [ns],
        ).fetchall()
        chain_forked = bool(forked)
        if chain_forked:
            for prev_seq, c in forked:
                errors.append(f"forked chain: {c} rows share prev_seq={prev_seq}")

        # ── 1. Chain integrity (unsynced rows) ──────────────────────
        unsynced = probe.execute(
            "SELECT seq, tag, payload, prev_seq, source "
            "FROM outbox_queue "
            "WHERE namespace = ? AND synced = 0 "
            "ORDER BY seq",
            [ns],
        ).fetchall()

        from sqloutbox._models import QueueRow

        rows = [
            QueueRow(seq=r[0], tag=r[1], payload=r[2].encode(),
                     prev_seq=r[3], source=r[4] or "")
            for r in unsynced
        ]
        chain_ok, chain_gaps_list = _verify_chain_rows(probe, ns, rows)
        if not chain_ok:
            errors.append(f"chain gap: missing seq(s) {chain_gaps_list}")

        # ── 2. Sequence continuity ──────────────────────────────────
        all_seqs = [
            r[0] for r in probe.execute(
                "SELECT seq FROM outbox_queue WHERE namespace = ? ORDER BY seq",
                [ns],
            ).fetchall()
        ]
        sync_log_seqs = {
            r[0] for r in probe.execute(
                "SELECT seq FROM outbox_sync_log WHERE namespace = ?", [ns],
            ).fetchall()
        }
        seq_continuous = True
        if len(all_seqs) >= 2:
            for i in range(1, len(all_seqs)):
                prev_s, curr_s = all_seqs[i - 1], all_seqs[i]
                if curr_s != prev_s + 1:
                    for gap_seq in range(prev_s + 1, curr_s):
                        if gap_seq not in sync_log_seqs:
                            seq_continuous = False
                            errors.append(
                                f"seq gap: {prev_s} -> {curr_s}, "
                                f"seq {gap_seq} not in queue or sync_log"
                            )
                            break
                    if not seq_continuous:
                        break

        # ── 3. Timestamp monotonicity ───────────────────────────────
        ts_rows = probe.execute(
            "SELECT seq, created_at FROM outbox_queue "
            "WHERE namespace = ? ORDER BY seq",
            [ns],
        ).fetchall()
        timestamps_monotonic = True
        prev_ts = ""
        for seq, created_at in ts_rows:
            if created_at < prev_ts:
                timestamps_monotonic = False
                errors.append(
                    f"timestamp not monotonic: seq {seq} has {created_at} "
                    f"< previous {prev_ts}"
                )
                break
            prev_ts = created_at

        # ── 4. Orphan sync_log detection ────────────────────────────
        max_queue_seq = probe.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM outbox_queue WHERE namespace = ?",
            [ns],
        ).fetchone()[0]
        orphan_sync_log = probe.execute(
            "SELECT COUNT(*) FROM outbox_sync_log "
            "WHERE namespace = ? AND seq > ?",
            [ns, max_queue_seq],
        ).fetchone()[0]
    finally:
        probe.close()

    ok = chain_ok and seq_continuous and timestamps_monotonic and not chain_forked
    return TableVerifyResult(
        table=label,
        db_path=str(db_path),
        ok=ok,
        pending_count=pending_count,
        total_rows=total_rows,
        sync_log_rows=sync_log_rows,
        chain_ok=chain_ok and not chain_forked,
        chain_gaps=tuple(chain_gaps_list),
        seq_continuous=seq_continuous,
        seq_range=seq_range,
        timestamps_monotonic=timestamps_monotonic,
        orphan_sync_log=orphan_sync_log,
        errors=tuple(errors),
    )


def _verify_chain_rows(
    conn: "sqlite3.Connection", namespace: str, rows: list,
) -> tuple[bool, list[int]]:
    """Read-only re-implementation of Outbox.verify_chain over an open conn.

    We cannot call ``Outbox.verify_chain`` here: that method opens its own
    WRITABLE ``thread_conn`` (and would construct an Outbox via open_write_conn,
    migrating the file). The chain rule is identical to ``_outbox.verify_chain``:
        - consecutive rows must satisfy rows[i].prev_seq == rows[i-1].seq
        - the head's predecessor (if any) must be ACCOUNTED — present in
          outbox_queue OR outbox_sync_log (and outbox_dead_log when present).
    """
    if not rows:
        return True, []
    missing: list[int] = []
    for i, row in enumerate(rows):
        if i == 0:
            if row.prev_seq is not None and not _seq_accounted_ro(conn, row.prev_seq):
                missing.append(row.prev_seq)
        else:
            expected_prev = rows[i - 1].seq
            if row.prev_seq != expected_prev:
                missing.append(expected_prev)
    return len(missing) == 0, missing


def _seq_accounted_ro(conn: "sqlite3.Connection", seq: int) -> bool:
    """True if seq exists in outbox_queue OR outbox_sync_log (read-only).

    Also consults outbox_dead_log when that table exists (Plan 2 / WS-2),
    matching Outbox._seq_accounted's broadened rule. Detect the table via
    sqlite_master so this works on DBs created before WS-2.
    """
    has_dead = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='outbox_dead_log'"
    ).fetchone()
    if has_dead:
        return bool(conn.execute(
            "SELECT 1 FROM outbox_queue WHERE seq = ? "
            "UNION SELECT 1 FROM outbox_sync_log WHERE seq = ? "
            "UNION SELECT 1 FROM outbox_dead_log WHERE seq = ? "
            "LIMIT 1",
            [seq, seq, seq],
        ).fetchone())
    return bool(conn.execute(
        "SELECT 1 FROM outbox_queue WHERE seq = ? "
        "UNION SELECT 1 FROM outbox_sync_log WHERE seq = ? "
        "LIMIT 1",
        [seq, seq],
    ).fetchone())
```

> **Why a local `_verify_chain_rows` instead of `outbox.verify_chain`:** the original `verify_outbox` (`_verify.py:122-129`) called `outbox.verify_chain(rows)`, which opens a *writable* `thread_conn` internally (`_outbox.py:242`). Routing through that re-introduces the writable-open we are eliminating. We re-implement the identical rule read-only. The signature/return of `TableVerifyResult` is unchanged, so the CLI report and all existing assertions keep working.

Also add `from pathlib import Path` to the imports at the top of `_verify.py` if it is not already imported (check the current import block near `:21-28`; add it under the other stdlib imports if missing).

- [ ] **Step 5: Route the CLI through `verify_db_path` so a stray file is reported, not migrated**

In `src/sqloutbox/cli.py`, update the `cmd_verify` import (currently `:469-470`):

```python
    from sqloutbox._outbox import Outbox
    from sqloutbox._verify import VerifyResult, verify_all, verify_db_path
```

Add `now_iso` for the aggregate result and replace the `--db-dir` scan branch (currently `:488-494`) so it inspects each file by path read-only and never constructs an `Outbox` (which would migrate). Replace:

```python
    elif db_dir_path is not None:
        if not db_dir_path.is_dir():
            print(f"error: not a directory: {db_dir_path}", file=sys.stderr)
            sys.exit(1)
        for db_file in sorted(db_dir_path.glob("*.db")):
            name = db_file.stem
            outboxes[name] = Outbox(db_path=db_file, namespace=name)
```

with:

```python
    elif db_dir_path is not None:
        if not db_dir_path.is_dir():
            print(f"error: not a directory: {db_dir_path}", file=sys.stderr)
            sys.exit(1)
        # Read-only scan: inspect each *.db by path. Never construct an Outbox
        # here — that calls open_write_conn() and would CREATE/MIGRATE the file
        # (spec §6.1). Files that are missing/foreign are reported, not created.
        db_paths = sorted(db_dir_path.glob("*.db"))
```

And in the `--config` branch (currently `:474-486`), the existing code constructs `Outbox(...)` per discovered file (`cli.py:482-483`), which migrates. Replace that branch:

```python
    if config_path is not None:
        from sqloutbox._runner import load_config_toml
        config, _writers = load_config_toml(config_path)
        for target in config.targets:
            db_dir = target.db_dir or config.db_dir
            for table in target.tables:
                db_path = db_dir / f"{table}.db"
                if db_path.exists():
                    outboxes[f"{target.name}.{table}"] = Outbox(
                        db_path=db_path, namespace=table,
                    )
                else:
                    print(f"  skip {table}.db — file not found at {db_path}")
```

with:

```python
    if config_path is not None:
        from sqloutbox._runner import load_config_toml
        config, _writers = load_config_toml(config_path)
        for target in config.targets:
            db_dir = target.db_dir or config.db_dir
            for table in target.tables:
                db_path = db_dir / f"{table}.db"
                if db_path.exists():
                    # Read-only inspect by path + explicit namespace — no Outbox
                    # construction, so the file is never created/migrated.
                    config_paths_seen.append(
                        (f"{target.name}.{table}", db_path, table)
                    )
                else:
                    print(f"  skip {table}.db — file not found at {db_path}")
```

Now restructure the body of `cmd_verify` so both branches accumulate `(label, db_path, namespace)` tuples and run `verify_db_path` over them. Replace the section from the `outboxes: dict[str, Outbox] = {}` declaration (currently `:472`) down to the `result = verify_all(outboxes)` call (currently `:511`). Replace:

```python
    outboxes: dict[str, Outbox] = {}

    if config_path is not None:
```

with:

```python
    from sqloutbox._schema import now_iso
    import time

    # Each entry: (display_label, db_path, namespace).
    config_paths_seen: list[tuple[str, Path, str]] = []
    db_paths: list[Path] = []

    if config_path is not None:
```

Then replace the empty-check + run section (currently `:506-511`):

```python
    if not outboxes:
        print("no .db files found — nothing to verify")
        sys.exit(0)

    # Run verification
    result = verify_all(outboxes)
```

with:

```python
    # Build the work list. --config gives explicit (label, path, ns); --db-dir
    # gives a list of paths whose namespace is auto-detected by verify_db_path.
    work: list[tuple[str, Path, str | None]] = []
    if config_path is not None:
        for label, path, ns in config_paths_seen:
            work.append((label, path, ns))
    else:
        for path in db_paths:
            work.append((path.stem, path, None))

    if not work:
        print("no .db files found — nothing to verify")
        sys.exit(0)

    # Run verification — every open is read-only (verify_db_path).
    t0 = time.monotonic()
    tables = tuple(verify_db_path(path, namespace=ns) for _label, path, ns in work)
    result = VerifyResult(
        ok=all(t.ok for t in tables),
        tables=tables,
        checked_at=now_iso(),
        duration_ms=round((time.monotonic() - t0) * 1000, 1),
    )
```

> The `Outbox` import at `cli.py:469` is now unused in `cmd_verify`. Leave it imported only if other code in the function uses it; otherwise remove `from sqloutbox._outbox import Outbox` from the function to keep `ruff` happy. After editing, run `python -m py_compile src/sqloutbox/cli.py` before testing.

- [ ] **Step 6: Run the WS-5 read-only tests to verify they pass**

Run: `python -m py_compile src/sqloutbox/_schema.py src/sqloutbox/_verify.py src/sqloutbox/cli.py && python -m pytest tests/test_ws5_schema_verify.py -v -k "open_read_conn or verify_db_path"`
Expected: compile OK; PASS (5 passed: `test_open_read_conn_cannot_write`, `test_open_read_conn_missing_file_not_created`, `test_verify_db_path_missing_reports_not_an_outbox`, `test_verify_db_path_foreign_file_reports_not_an_outbox`, `test_verify_db_path_existing_db_unchanged`).

- [ ] **Step 7: Run the existing verify + CLI suites to confirm no regression**

Run: `python -m pytest tests/test_verify.py tests/test_cli.py -v`
Expected: all green. In particular `test_cli_verify_db_dir_healthy`, `test_cli_verify_db_dir_corrupted`, `test_cli_verify_empty_dir`, and `test_cli_verify_no_args` still pass (the report format and exit codes are unchanged; `verify_db_path` returns the same `TableVerifyResult` shape that `verify_outbox` did).

- [ ] **Step 8: Run the full suite**

Run: `python -m pytest -q`
Expected: all green.

- [ ] **Step 9: Commit**

```bash
git add src/sqloutbox/_schema.py src/sqloutbox/_verify.py src/sqloutbox/cli.py tests/test_ws5_schema_verify.py
git commit -m "feat(verify): truly read-only verify path — never create or migrate a .db (WS-5, F005/F050)"
```

---

## Task 2: Crash-safe forked-chain migration — wrap `_MIGRATE_PREV_SEQ_UNIQUE`, raise `ChainIntegrityError` (§6.2, F006/F029)

**Problem (verified against source):** There are **TWO** `CREATE UNIQUE INDEX … ON outbox_queue (prev_seq)` statements in `open_write_conn`, and on a forked-chain DB **the FIRST one is what actually raises** — the plan must guard both:
1. `_IDX_PREV` (`_schema.py:63-66`) — `CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_prev ON outbox_queue (prev_seq)` — runs at `_schema.py:92` (right after `_IDX_WORKER`). **This is the statement that raises first on a fork.**
2. `_MIGRATE_PREV_SEQ_UNIQUE` (`_schema.py:43-46`) — `CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_prev_unique ON outbox_queue (prev_seq)` — runs **unguarded** at `_schema.py:101`, unlike its sibling `_MIGRATE_ADD_SOURCE` (wrapped at `:95-98`). It never gets reached on a fork because `_IDX_PREV` blows up first.

On a forked-chain DB (two rows sharing a non-NULL `prev_seq`), SQLite raises `sqlite3.IntegrityError: UNIQUE constraint failed: outbox_queue.prev_seq` from `_IDX_PREV` at `:92` (empirically confirmed: with rows 2 and 3 both pointing at prev_seq=1, the `idx_outbox_prev` creation raises before line 101 is ever reached) — and because `Outbox.__init__` (`_outbox.py:64`) calls `open_write_conn`, **the producer hot path crashes with a bare `IntegrityError`** (and so does any writable verify path). This task converts the fork into a typed `ChainIntegrityError` (owned by Plan 3, `sqloutbox.exceptions`) that names the duplicate rows + a recovery pointer. **The wrap must cover BOTH index creations** (it is not enough to wrap only `_MIGRATE_PREV_SEQ_UNIQUE`, since execution never reaches it on a fork). Read-only verify (Task 1) already opens and reports the fork without crashing, so this task does not change that.

**Files:**
- Modify: `src/sqloutbox/_schema.py` (wrap BOTH prev_seq unique-index creations; add `_find_forked_prev_seqs` helper)
- Test: `tests/test_ws5_schema_verify.py` (Append)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_ws5_schema_verify.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_ws5_schema_verify.py -v -k forked`
Expected: FAIL — `test_forked_db_open_write_conn_raises_chain_integrity_error` and `test_forked_db_outbox_init_raises_chain_integrity_error` currently raise a bare `sqlite3.IntegrityError` (not `ChainIntegrityError`), so `pytest.raises(ChainIntegrityError)` fails. (`test_forked_db_read_only_verify_reports_without_crashing` should already PASS from Task 1 — that is fine; the gate for this step is the two migration tests failing.)

- [ ] **Step 3: Guard BOTH prev_seq unique-index creations in `open_write_conn` and add a fork-naming helper**

In `src/sqloutbox/_schema.py`, add this helper just below the `# ── Connection helpers ──` divider (currently `:75`), before `open_write_conn`:

```python
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
```

There are TWO `CREATE UNIQUE INDEX … (prev_seq)` executions in `open_write_conn`; **both must be routed through the guard** because the first (`_IDX_PREV`) is what actually raises on a fork. (a) Replace the FIRST one — the `conn.execute(_IDX_PREV)` line (currently `:92`, between `conn.execute(_IDX_WORKER)` and `conn.execute(_IDX_SYNC_LOG)`):

```python
    conn.execute(_IDX_WORKER)
    conn.execute(_IDX_PREV)
    conn.execute(_IDX_SYNC_LOG)
```

with:

```python
    conn.execute(_IDX_WORKER)
    # GUARDED (spec §6.2): _IDX_PREV is a UNIQUE index on prev_seq — on a forked
    # chain it raises IntegrityError FIRST, before the migration below. Convert
    # to ChainIntegrityError so Outbox.__init__ (the producer hot path) never
    # dies with a bare IntegrityError.
    _create_prev_seq_index_guarded(conn, _IDX_PREV, db_path)
    conn.execute(_IDX_SYNC_LOG)
```

(b) Replace the SECOND one — the unguarded migration block (currently `:99-101`):

```python
    # Idempotent migration: enforce UNIQUE on prev_seq for existing DBs.
    # CREATE UNIQUE INDEX IF NOT EXISTS is a no-op when the index already exists.
    conn.execute(_MIGRATE_PREV_SEQ_UNIQUE)
```

with:

```python
    # Idempotent migration: enforce UNIQUE on prev_seq for existing DBs.
    # CREATE UNIQUE INDEX IF NOT EXISTS is a no-op when the index already exists.
    # GUARDED (spec §6.2, F006/F029): same forked-chain conversion as _IDX_PREV
    # above — kept guarded for defense in depth (e.g. if _IDX_PREV were ever
    # made non-UNIQUE, this migration would then be the one that raises).
    _create_prev_seq_index_guarded(conn, _MIGRATE_PREV_SEQ_UNIQUE, db_path)
```

> **Why guard both:** empirically, on a forked DB the FIRST `CREATE UNIQUE INDEX … (prev_seq)` to execute is `_IDX_PREV` at `:92` — it raises `sqlite3.IntegrityError: UNIQUE constraint failed: outbox_queue.prev_seq` and execution never reaches `_MIGRATE_PREV_SEQ_UNIQUE` at `:101`. Wrapping only the migration (line 101) leaves the bare `IntegrityError` escaping from line 92, so `Outbox.__init__` still crashes and Task 2's `pytest.raises(ChainIntegrityError)` assertions fail. Routing both through `_create_prev_seq_index_guarded` makes whichever fires first raise the typed error. The `from sqloutbox.exceptions import ChainIntegrityError` is a **lazy import inside the helper's except** — `ChainIntegrityError` is owned by Plan 3, do NOT define it here.

- [ ] **Step 4: Run the forked-chain tests to verify they pass**

Run: `python -m py_compile src/sqloutbox/_schema.py && python -m pytest tests/test_ws5_schema_verify.py -v -k forked`
Expected: compile OK; PASS (4 passed: the two migration tests now get `ChainIntegrityError`, the hierarchy test passes, and the read-only-report test still passes).

- [ ] **Step 5: Run the full suite — confirm normal (non-forked) DBs still construct**

Run: `python -m pytest -q`
Expected: all green. The wrap only triggers on `IntegrityError`; healthy DBs (the overwhelming majority, including every existing test fixture) run the `CREATE UNIQUE INDEX IF NOT EXISTS` exactly as before. `tests/test_sqloutbox.py`, `tests/test_verify.py`, and `tests/test_sync.py` must remain green.

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/_schema.py tests/test_ws5_schema_verify.py
git commit -m "feat(schema): crash-safe forked-chain migration — raise ChainIntegrityError, not bare IntegrityError (WS-5, F006/F029)"
```

---

## Task 3: Producer-side seed — persisted high-water mark prevents fresh-host seq collision (§6.3, mechanism (a), F004)

**Problem (verified against source & spec §6.3):** On a fresh host with a *populated remote*, the producer (`Outbox.enqueue`, `_outbox.py:68`) assigns `seq=1,2,3…` from a brand-new local `sqlite_sequence` **before** the drain's one-time `_seed_from_remote` (`sync.py:435`) runs. Those seqs collide with the remote's existing `outbox_seq` values, and `inject_outbox_seq`'s `INSERT OR IGNORE` (`sync.py:148`) silently drops them → silent data loss. The drain's existing `seed_sequence` (`_outbox.py:360`) fixes this only *after* the drain starts — too late if the producer writes first. **Mechanism (a)** (recommended in spec §6.3): the producer persists a per-namespace high-water mark to a local table (`outbox_hwm`) on every enqueue, and lazily seeds its local `AUTOINCREMENT` from that mark in `__init__`. Combined with the drain's `seed_sequence` (which advances the same persisted mark when it learns the remote max), a restart on the same host reads the persisted floor and never re-collides — no startup-ordering constraint on the producer.

**Design (mechanism (a), concrete):**
- New idempotent table `outbox_hwm(namespace TEXT PRIMARY KEY, hwm INTEGER NOT NULL)` created in `open_write_conn`.
- `Outbox.record_hwm(seq)` upserts `MAX(existing, seq)` for the namespace — called from `enqueue`/`enqueue_batch` after a successful insert (cheap; same write txn cadence).
- `seed_sequence(min_seq)` (drain path, `_outbox.py:360`) also calls `record_hwm(min_seq)` so the remote max is persisted as a floor that survives a producer restart.
- `Outbox.__init__` calls `_seed_from_hwm()`: read the persisted `hwm`; if it exceeds the current `sqlite_sequence` counter, advance the counter (reuse `seed_sequence`). This is the **lazy producer-side seed** — it runs on the producer at construction, before its first `enqueue`, with no dependency on the drain having started.

**Files:**
- Modify: `src/sqloutbox/_schema.py` (add `_CREATE_HWM` table + index; create it in `open_write_conn`)
- Modify: `src/sqloutbox/_outbox.py` (add `record_hwm`, `_persisted_hwm`, `_seed_from_hwm`; call `record_hwm` from enqueue paths and `seed_sequence`; call `_seed_from_hwm` in `__init__`)
- Test: `tests/test_ws5_schema_verify.py` (Append)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_ws5_schema_verify.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_ws5_schema_verify.py -v -k "hwm or seed_from_persisted or no_collision"`
Expected: FAIL — `Outbox` has no `record_hwm` / `_persisted_hwm` methods (`AttributeError`), and `outbox_hwm` table does not exist, so `test_fresh_host_lazy_seed_from_persisted_hwm` and `test_fresh_host_populated_remote_no_collision` produce colliding low seqs.

- [ ] **Step 3: Add the `outbox_hwm` table to `_schema.py`**

In `src/sqloutbox/_schema.py`, add the table SQL after `_CREATE_SYNC_LOG` (currently ending at `:54`):

```python
# Per-namespace producer-side high-water mark (spec §6.3, mechanism (a)).
# Persists the highest seq ever ASSIGNED for a namespace (or the remote MAX
# learned by the drain). On a fresh host with a populated remote, the producer
# reads this floor at construction and seeds its local AUTOINCREMENT above it,
# so INSERT OR IGNORE on the remote can never silently drop a colliding seq.
_CREATE_HWM = """
CREATE TABLE IF NOT EXISTS outbox_hwm (
    namespace TEXT    NOT NULL PRIMARY KEY,
    hwm       INTEGER NOT NULL
)
"""
```

Then create it inside `open_write_conn`, right after the `_CREATE_SYNC_LOG` execute (currently `:90`):

```python
    conn.execute(_CREATE_QUEUE)
    conn.execute(_CREATE_SYNC_LOG)
    conn.execute(_CREATE_HWM)
```

> `outbox_hwm` is **only** created on the write path (`open_write_conn`); the read-only verify path (Task 1) never needs it and never creates it. Its absence in an older DB is fine — `_persisted_hwm` (Step 4) tolerates a missing table.

- [ ] **Step 4: Add hwm methods to `Outbox` and wire them into enqueue / seed / `__init__`**

In `src/sqloutbox/_outbox.py`, add the lazy seed call at the end of `__init__` (currently the body ends at `:64` with `self._write_conn = open_write_conn(db_path)`):

```python
        self.db_path         = db_path
        self.namespace       = namespace
        self.retain_log_days = retain_log_days
        self.batch_size      = batch_size
        self.cleanup_every   = cleanup_every
        # Persistent write connection — used exclusively by enqueue() from one thread
        self._write_conn = open_write_conn(db_path)
        # Producer-side seed (spec §6.3, mechanism (a)): on a fresh host with a
        # populated remote, lazily advance the local AUTOINCREMENT above the
        # persisted high-water mark BEFORE the first enqueue, so new seqs never
        # collide with remote outbox_seq values (which INSERT OR IGNORE drops).
        self._seed_from_hwm()
```

Add the three new methods in the `# ── Seeding ──` section, right after `seed_sequence` (currently ending at `:397`). Also update `seed_sequence` to persist the floor. First, append to the end of `seed_sequence` — insert a `self.record_hwm(min_seq)` call just before its `return True` (currently `:397`). Replace the tail of `seed_sequence`:

```python
        self._write_conn.commit()
        logger.info(
            "sqloutbox[%s]: seeded sequence from %d → %d (remote max)",
            self.namespace, current, min_seq,
        )
        return True
```

with:

```python
        self._write_conn.commit()
        # Persist the remote max as the durable floor so a producer restart on
        # this host re-seeds from it (mechanism (a)) even before the drain runs.
        self.record_hwm(min_seq)
        logger.info(
            "sqloutbox[%s]: seeded sequence from %d → %d (remote max)",
            self.namespace, current, min_seq,
        )
        return True
```

Then add these methods immediately after `seed_sequence`:

```python
    def record_hwm(self, seq: int) -> None:
        """Persist ``seq`` as this namespace's high-water mark (idempotent MAX).

        Stores ``MAX(existing_hwm, seq)`` in ``outbox_hwm``. Called after a
        successful enqueue (the assigned seq) and by ``seed_sequence`` (the
        learned remote max). Cheap upsert; never raises — a failure here must
        not break the hot-path enqueue (the seq is already committed).
        """
        try:
            self._write_conn.execute(
                "INSERT INTO outbox_hwm (namespace, hwm) VALUES (?, ?) "
                "ON CONFLICT(namespace) DO UPDATE SET hwm = MAX(hwm, excluded.hwm)",
                [self.namespace, seq],
            )
            self._write_conn.commit()
        except Exception as exc:
            logger.warning(
                "sqloutbox[%s]: record_hwm(%d) failed (non-fatal): %s",
                self.namespace, seq, exc,
            )

    def _persisted_hwm(self) -> int:
        """Return the persisted high-water mark for this namespace, or 0.

        Tolerates a DB created before the outbox_hwm table existed (returns 0).
        """
        try:
            row = self._write_conn.execute(
                "SELECT hwm FROM outbox_hwm WHERE namespace = ?",
                [self.namespace],
            ).fetchone()
        except Exception:
            return 0  # table absent (pre-WS-5 DB) — no floor recorded
        return int(row[0]) if row and row[0] is not None else 0

    def _seed_from_hwm(self) -> None:
        """Lazily advance the local AUTOINCREMENT above the persisted hwm.

        Runs once in __init__ (producer-side, mechanism (a)). If the persisted
        high-water mark exceeds the current sqlite_sequence counter, seed the
        counter up to it so the first enqueue lands above any remote value.
        No-op when there is no recorded floor (fresh DB with empty remote).
        """
        hwm = self._persisted_hwm()
        if hwm > 0:
            self.seed_sequence(hwm)
```

> **Recursion guard:** `_seed_from_hwm` → `seed_sequence` → `record_hwm`. `record_hwm` does NOT call `seed_sequence`, and `seed_sequence` only calls `record_hwm`, so there is no cycle. Note the new `record_hwm(min_seq)` call lives at the END of `seed_sequence` (just before `return True`), so the early `if current >= min_seq: return False` exit does NOT re-record — that is fine, the hwm is already persisted at that point (this is why `test_fresh_host_lazy_seed_from_persisted_hwm` still works: the floor was written by the earlier `seed_sequence(10_000)` call's success path). Either way there is no recursion.

Now persist the assigned seq from the hot path. In `enqueue` (currently `:99-108`), after `self._write_conn.commit()` and before `return new_seq` (currently `:107-108`), add the hwm record. Replace:

```python
            assert cur.lastrowid is not None
            new_seq: int = cur.lastrowid
            self._write_conn.commit()
            return new_seq
```

with:

```python
            assert cur.lastrowid is not None
            new_seq: int = cur.lastrowid
            self._write_conn.commit()
            # Persist the high-water mark so a fresh producer on this host
            # (mechanism (a)) re-seeds above it after a restart.
            self.record_hwm(new_seq)
            return new_seq
```

And in `enqueue_batch` (currently `:181-188`), after the batch commit and before the `return`, record the highest seq. Replace:

```python
            self._write_conn.executemany(
                "INSERT INTO outbox_queue "
                "(created_at, namespace, source, tag, payload, prev_seq) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                rows_data,
            )
            self._write_conn.commit()   # ONE commit for the entire batch
            return list(range(start_seq, start_seq + len(items)))
```

with:

```python
            self._write_conn.executemany(
                "INSERT INTO outbox_queue "
                "(created_at, namespace, source, tag, payload, prev_seq) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                rows_data,
            )
            self._write_conn.commit()   # ONE commit for the entire batch
            last_seq = start_seq + len(items) - 1
            # Persist the high-water mark (highest seq in this batch) so a fresh
            # producer on this host re-seeds above it after a restart.
            self.record_hwm(last_seq)
            return list(range(start_seq, start_seq + len(items)))
```

> **Why `record_hwm` is a separate commit, not folded into the enqueue txn:** the enqueue is already committed (the seq is durable) by the time we record the hwm; a failure recording the hwm must NOT roll back or fail the enqueue. The hwm is a best-effort *acceleration* of correctness — even if it is never written, the drain's `_seed_from_remote` + `seed_sequence` still establishes the floor on the next drain start. The hot-path cost is one extra tiny upsert+commit on a 1-row PK table; acceptable for the durable-correctness guarantee.

- [ ] **Step 5: Run the producer-seed tests to verify they pass**

Run: `python -m py_compile src/sqloutbox/_schema.py src/sqloutbox/_outbox.py && python -m pytest tests/test_ws5_schema_verify.py -v -k "hwm or seed_from_persisted or no_collision"`
Expected: compile OK; PASS (5 passed: `test_hwm_recorded_on_enqueue`, `test_seed_sequence_persists_hwm`, `test_fresh_host_lazy_seed_from_persisted_hwm`, `test_fresh_host_populated_remote_no_collision`, `test_hwm_does_not_break_chain_integrity`).

- [ ] **Step 6: Run the existing seed_sequence + outbox suites to confirm no regression**

Run: `python -m pytest tests/test_sqloutbox.py -v -k "seed or enqueue or chain"`
Expected: all green. The four existing `seed_sequence` tests (`test_seed_sequence_advances_counter`, `test_seed_sequence_noop_when_already_higher`, `test_seed_sequence_on_fresh_db`, `test_seed_sequence_chain_integrity_after_seed`) still pass — `seed_sequence` keeps the same return contract and only adds an idempotent `record_hwm` call.

- [ ] **Step 7: Run the full suite**

Run: `python -m pytest -q`
Expected: all green. Note: `enqueue`/`enqueue_batch` now add one tiny upsert+commit; tests that count rows in `outbox_queue` are unaffected (the hwm lives in a separate table), and tests asserting on `sqlite_sequence` / chain integrity still hold (the seed only ever advances the counter, never lowers it).

- [ ] **Step 8: Commit**

```bash
git add src/sqloutbox/_schema.py src/sqloutbox/_outbox.py tests/test_ws5_schema_verify.py
git commit -m "feat(outbox): producer-side persisted high-water-mark seed — no fresh-host seq collision (WS-5, F004)"
```

---

## Task 4: Integration — verify a forked + seeded DB end-to-end, and confirm `__init__.py` exports

A single integration test proves the three fixes compose, plus a guard that the public surface still imports cleanly (Plan 3's `exceptions` module is a hard dependency of Task 2).

**Files:**
- Test: `tests/test_ws5_schema_verify.py` (Append)

- [ ] **Step 1: Write the test**

Append to `tests/test_ws5_schema_verify.py`:

```python
# ── Integration ──────────────────────────────────────────────────────────────


def test_chain_integrity_error_importable_from_package():
    """ChainIntegrityError (Plan 3) is importable — Task 2 depends on it."""
    from sqloutbox.exceptions import (
        ChainIntegrityError,
        SqloutboxError,
    )
    assert issubclass(ChainIntegrityError, SqloutboxError)


def test_seeded_producer_then_read_only_verify_clean(tmp_path: Path):
    """A producer seeded above a remote floor verifies clean, read-only."""
    from sqloutbox._verify import verify_db_path

    db = tmp_path / "events.db"
    ob = Outbox(db_path=db, namespace="evt")
    ob.record_hwm(500)
    producer = Outbox(db_path=db, namespace="evt")
    producer.enqueue_batch([("INSERT INTO evt (id) VALUES (?)", f"[{i}]".encode())
                            for i in range(4)])
    producer._write_conn.close()

    result = verify_db_path(db, namespace="evt")
    assert result.ok is True
    assert result.chain_ok is True
    assert result.total_rows == 4
    assert result.seq_range is not None
    assert result.seq_range[0] > 500  # all seqs above the seeded floor


def test_db_dir_verify_skips_foreign_and_missing(tmp_path: Path, capsys):
    """CLI --db-dir scan: a foreign file is reported FAIL, a healthy outbox OK, neither mutated."""
    from sqloutbox.cli import cmd_verify

    # One healthy outbox file + one foreign sqlite file in the same dir.
    Outbox(db_path=tmp_path / "good.db", namespace="good").enqueue(
        "INSERT INTO good (id) VALUES (?)", b"[1]"
    )
    foreign = tmp_path / "foreign.db"
    c = sqlite3.connect(str(foreign))
    c.execute("CREATE TABLE x (a)")
    c.commit()
    c.close()
    foreign_mtime = os.path.getmtime(foreign)

    with pytest.raises(SystemExit) as ei:
        cmd_verify(config_path=None, db_dir_path=tmp_path)
    # Exit 1 because the foreign file fails ("not an outbox DB").
    assert ei.value.code == 1
    out = capsys.readouterr().out
    assert "good" in out
    assert "FAIL" in out  # foreign.db reported, not migrated
    # Foreign file untouched.
    assert os.path.getmtime(foreign) == foreign_mtime
    c2 = sqlite3.connect(f"file:{foreign}?mode=ro", uri=True)
    tables = {r[0] for r in c2.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    c2.close()
    assert "outbox_queue" not in tables
```

- [ ] **Step 2: Run the integration tests**

Run: `python -m pytest tests/test_ws5_schema_verify.py -v -k "integration or importable or seeded_producer or skips_foreign"`
Expected: PASS (3 passed). If `test_db_dir_verify_skips_foreign_and_missing` shows the foreign file as `OK` instead of `FAIL`, re-check Task 1 Step 4/5 — `verify_db_path` must return `ok=False` with a "not an outbox" error for a file lacking `outbox_queue`, and `cmd_verify` must run it via `verify_db_path` (not via an `Outbox`).

- [ ] **Step 3: Run the entire WS-5 test file**

Run: `python -m pytest tests/test_ws5_schema_verify.py -v`
Expected: all green (Task 1: 5, Task 2: 4, Task 3: 5, Task 4: 3 = 17 tests).

- [ ] **Step 4: Run the full suite one final time**

Run: `python -m pytest -q`
Expected: all green (existing suite + 17 WS-5 tests). Gate on "all green", not an exact count.

- [ ] **Step 5: Commit**

```bash
git add tests/test_ws5_schema_verify.py
git commit -m "test(ws5): integration — seeded producer + read-only verify + foreign-file skip (WS-5)"
```

---

## Self-Review notes (for the executor)

- **Spec coverage:** Task 1 = §6.1 (truly read-only verify, F005/F050); Task 2 = §6.2 (crash-safe forked-chain migration, F006/F029); Task 3 = §6.3 mechanism **(a)** (producer-side persisted high-water-mark seed, F004). Task 4 is the integration gate. This plan does NOT touch dead-letter (Plan 2), backoff/health (Plan 6), config validation (Plan 3), or the flock/shutdown (Plan 4).
- **Hard cross-plan dependency:** Task 2 imports `ChainIntegrityError` (and Task 4 imports `SqloutboxError`) from `sqloutbox.exceptions`, **owned by Plan 3 (WS-3)**. Per the locked execution order, Plan 3 runs before this plan, so the module already exists. If `sqloutbox/exceptions.py` is somehow missing, STOP and run Plan 3 first — do not stub `ChainIntegrityError` locally (that would diverge from the contract and a later import would conflict).
- **Do NOT re-add Plan 1 work:** `_BUSY_TIMEOUT_MS` and `busy_timeout` PRAGMAs already exist on `open_write_conn`/`thread_conn`; `open_read_conn` reuses the constant. The L1/L2 sync guards and L3 runner observation are already present — this plan does not touch `sync.py` or `_runner.py`.
- **Read-only chain check duplication is deliberate:** `_verify._verify_chain_rows` re-implements `Outbox.verify_chain`'s rule read-only because the original opens a *writable* `thread_conn` (`_outbox.py:242`). Routing through `Outbox.verify_chain` would re-introduce the writable open Task 1 eliminates. The rule (consecutive-link + head-accounted, with `outbox_dead_log` consulted when present) is intentionally kept identical.
- **`verify_db_path` return shape:** it returns the SAME `TableVerifyResult` that `verify_outbox` returned, so the CLI report (`cli.py:519-548`) and every existing `tests/test_verify.py` assertion keep working. `verify_outbox(outbox)` is preserved as a thin wrapper delegating to `verify_db_path` — `Outbox.verify_full` (`_outbox.py:401`) and `OutboxSyncService.request_verify` paths are unaffected. NOTE: `verify_full`/`verify_all` callers already hold a live `Outbox` (writable conn open); `verify_db_path` opens its OWN read-only handle to the same file, which is safe alongside the writer (WAL + busy_timeout). The healthy-DB test in Task 1 closes the writer first only to snapshot mtime/journal-mode deterministically.
- **hwm is best-effort, not transactional with enqueue:** `record_hwm` commits separately after the enqueue is already durable, and swallows its own errors — a hwm write failure must never fail or roll back the committed enqueue. Correctness still holds via the drain's `seed_sequence` floor on the next drain start; the persisted hwm only *accelerates* it so the producer doesn't need to wait for the drain.
- **Line-number drift:** every `path:line` cited (e.g. `_schema.py:99-101`, `_outbox.py:64/360/397`, `cli.py:459/492/494`, `sync.py:148/435`) was accurate against the working tree at plan-writing time. If they have drifted (Plans 2/3/4 modify `_schema.py`, `_outbox.py`, and `cli.py`), locate the code by the quoted snippet, not the number — in particular Plan 2 adds an `outbox_dead_log` create in `open_write_conn` and Plan 2/3 may shift the `_MIGRATE_PREV_SEQ_UNIQUE` block; wrap whichever line currently runs `_MIGRATE_PREV_SEQ_UNIQUE`.
- **Compile gates:** every task that edits multiple modules runs `python -m py_compile …` before pytest, because the CLI/verify edits change control flow and indentation (the `cmd_verify` restructure especially) — catch syntax errors before the slower test run.
