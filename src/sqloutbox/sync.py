"""OutboxSyncService — multi-target outbox drain daemon.

Reads local SQLite outbox files and delivers rows to N remote database targets.
Transport is injected via the OutboxWriter protocol — sqloutbox stays
zero-dependency (stdlib only).

The service runs as a continuous round-robin loop:
    1. Sleep ``flush_interval`` seconds (default 1.0s — the scan interval)
    2. For each target, iterate over its tables:
       a. Count pending rows for this table
       b. If pending >= ``table_flush_threshold`` (default 15) → include
       c. Elif pending > 0 AND last flush >= ``table_max_wait`` (default 6.0s) → include
       d. Else → skip (round-robin to next table)
    3. For included tables:
       a. fetch_unsynced()  — read pending rows
       b. verify_chain()    — check singly-linked chain integrity
       c. Decode payload    — tag = SQL, payload = JSON args
       d. inject_outbox_seq — append outbox_seq to INSERTs (per target config)
    4. Send ONE writer.write_batch() call per target (minimise round-trips)
    5. For confirmed rows: mark_synced() + delete_synced()
    6. Every ``cleanup_every`` cycles: prune_sync_log()

Example
-------
    from sqloutbox import OutboxSyncService, OutboxConfig, TargetConfig

    config = OutboxConfig(
        db_dir=Path("/var/data/outbox"),
        targets=(
            TargetConfig(name="primary", tables=("events", "metrics")),
            TargetConfig(name="audit", tables=("audit_log",),
                         inject_outbox_seq=False),
        ),
    )

    svc = OutboxSyncService(
        config=config,
        writers={"primary": my_http_writer, "audit": my_audit_writer},
    )
    await svc.run()
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Any, Protocol, runtime_checkable

from sqloutbox._outbox import Outbox
from sqloutbox._verify import VerifyResult, verify_all
from sqloutbox.config import OutboxConfig, TargetConfig
from sqloutbox.exceptions import UnsupportedStatementError

logger = logging.getLogger(__name__)

# Custom VERBOSE level (5) — registered once, shared across all processes.
_VERBOSE: int = 5
if logging.getLevelName(_VERBOSE).startswith("Level"):
    logging.addLevelName(_VERBOSE, "VERBOSE")


# ── Writer protocol ─────────────────────────────────────────────────────────


@runtime_checkable
class OutboxWriter(Protocol):
    """Protocol for async batch writers.

    Apps implement this to deliver SQL statements to their remote database.
    The write_batch() method receives a list of (sql, args) tuples and returns
    a list of per-statement result dicts.

    Example implementation (Turso HTTP)::

        class TursoWriter:
            async def write_batch(self, stmts):
                resp = await self._http.post(url, json=build_pipeline(stmts))
                return [{"ok": True, "rows_affected": 1} for _ in stmts]
    """

    async def write_batch(
        self, stmts: list[tuple[str, list[Any]]]
    ) -> list[dict[str, Any]]:
        """Send SQL statements to the remote database.

        Parameters
        ----------
        stmts:
            List of (sql_string, bind_args) tuples.

        Returns
        -------
        list[dict]
            One result dict per statement (in order):
                {"ok": True,  "rows_affected": N}           — write confirmed
                {"ok": True,  "rows": [[col, ...], ...]}    — SELECT result
                {"ok": False, "error": "..."}                — failed
        """
        ...


# ── SQL helpers ──────────────────────────────────────────────────────────────


def _mask_string_literals(sql: str) -> str:
    """Return ``sql`` with the *contents* of quoted string literals blanked.

    Replaces every character inside a single- or double-quoted literal with a
    space, leaving the quote characters and all structural SQL outside the
    literals intact. SQL's doubled-quote escape ('' inside a '…' literal, and
    "" inside a "…") is handled — a doubled quote does NOT close the literal.

    The result (the "skeleton") has the SAME LENGTH and SAME structural-char
    positions as the input, so index/count scans (find, rfind, count('?'))
    on the skeleton map 1:1 back onto the original string — but a '?' / ')' /
    'WHERE' that lived inside a literal is now a space and cannot mislead them.
    """
    out: list[str] = []
    i = 0
    n = len(sql)
    quote = ""  # "" when outside a literal; "'" or '"' when inside one
    while i < n:
        ch = sql[i]
        if quote:
            if ch == quote:
                # A doubled quote ('' or "") is an escape — stays inside the literal.
                if i + 1 < n and sql[i + 1] == quote:
                    out.append(quote)
                    out.append(quote)
                    i += 2
                    continue
                out.append(quote)   # closing quote
                quote = ""
                i += 1
                continue
            out.append(" ")          # blank the literal content
            i += 1
            continue
        if ch == "'" or ch == '"':
            quote = ch
            out.append(ch)           # opening quote
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _assert_supported(sql: str, masked: str) -> None:
    """Raise UnsupportedStatementError unless ``sql`` is a safe INSERT/UPDATE.

    ``masked`` is ``_mask_string_literals(sql)`` — structural scanning runs on
    it so a ')' / '?' / WHERE inside a string literal cannot fool the checks.

    Supported (and ONLY these):
        INSERT INTO t (cols) VALUES (?, …)     -- single-row, explicit columns
        UPDATE t SET c=? [, …] WHERE …          -- at least one real SET arg
    """
    upper = masked.upper().strip()

    if upper.startswith("INSERT"):
        vi = upper.find(") VALUES")
        if vi == -1:
            raise UnsupportedStatementError(
                f"INSERT must be single-row 'INSERT INTO t (cols) VALUES (...)'; "
                f"got: {sql!r}"
            )
        after = upper[vi + len(") VALUES"):]
        # Multi-row VALUES: a second '(' opens after the first VALUES group closes.
        first_close = after.find(")")
        if first_close != -1 and "(" in after[first_close + 1:]:
            raise UnsupportedStatementError(
                f"multi-row VALUES is not supported (one row per statement); "
                f"got: {sql!r}"
            )
        return

    if upper.startswith("UPDATE"):
        where_idx = upper.find(" WHERE ")
        set_part = masked[:where_idx] if where_idx != -1 else masked
        # The transform inserts ', outbox_seq = ?' after the real SET args. If
        # the skeleton has no real '?' in SET, the original only had a literal
        # '?' — ambiguous; reject rather than emit wrong SQL.
        if set_part.count("?") < 1:
            raise UnsupportedStatementError(
                f"UPDATE SET clause has no bind placeholder (a '?' inside a "
                f"string literal does not count); got: {sql!r}"
            )
        return

    raise UnsupportedStatementError(
        f"only single-row INSERT … VALUES (…) and UPDATE … SET …=? are "
        f"supported by inject_outbox_seq; got: {sql!r}"
    )


def inject_outbox_seq(
    sql: str, args: list[Any], outbox_seq: int,
) -> tuple[str, list[Any]]:
    """Append ``outbox_seq`` to an INSERT or UPDATE statement.

    INSERT transform::

        INSERT INTO table (a, b) VALUES (?, ?)
        → INSERT OR IGNORE INTO table (a, b, outbox_seq) VALUES (?, ?, ?)

    The ``INSERT OR IGNORE`` prefix provides idempotent delivery — if the
    row was already written to the remote DB (e.g. after a crash between
    write and local delete), the re-attempt silently succeeds.

    UPDATE transform::

        UPDATE table SET a=?, b=? WHERE id=?
        → UPDATE table SET a=?, b=?, outbox_seq=? WHERE id=?

    The ``outbox_seq`` value is inserted into the args list at the correct
    position (after SET args, before WHERE args).

    Parameters
    ----------
    sql:
        The original INSERT or UPDATE statement.
    args:
        The original bind values.
    outbox_seq:
        The outbox sequence number to append.

    Returns
    -------
    (modified_sql, modified_args)
    """
    s = sql.strip()
    # Mask string-literal contents so a ')' / '?' / WHERE inside a literal cannot
    # mislead the structural scans below, then reject any unsupported shape.
    masked = _mask_string_literals(s)
    _assert_supported(s, masked)

    upper = masked.upper()

    if upper.startswith("INSERT"):
        # Convert INSERT INTO → INSERT OR IGNORE INTO (operate on the original s;
        # the prefix length is identical in s and the skeleton).
        if upper.startswith("INSERT INTO"):
            s = "INSERT OR IGNORE INTO" + s[len("INSERT INTO"):]
            masked = "INSERT OR IGNORE INTO" + masked[len("INSERT INTO"):]
        upper = masked.upper()
        # Insert outbox_seq column before ') VALUES' (index from the skeleton).
        vi = upper.find(") VALUES")
        s = s[:vi] + ", outbox_seq" + s[vi:]
        masked = masked[:vi] + ", outbox_seq" + masked[vi:]
        # Insert '?' placeholder before the LAST ')' of the VALUES group. On the
        # skeleton the only ')' chars are structural, so rfind is now safe.
        lp = masked.rfind(")")
        s = s[:lp] + ", ?" + s[lp:]
        return s, list(args) + [outbox_seq]

    # UPDATE (the only other shape _assert_supported lets through).
    where_idx = upper.find(" WHERE ")
    if where_idx != -1:
        # Count real '?' placeholders in the SET clause via the skeleton.
        n_set_args = masked[:where_idx].count("?")
        s = s[:where_idx] + ", outbox_seq = ?" + s[where_idx:]
        new_args = list(args)
        new_args.insert(n_set_args, outbox_seq)
        return s, new_args
    # No WHERE — append outbox_seq to the SET clause.
    s = s + ", outbox_seq = ?"
    return s, list(args) + [outbox_seq]


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


# ── Sync service ─────────────────────────────────────────────────────────────


class OutboxSyncService:
    """Multi-target outbox drain service.

    Reads local SQLite outbox files and delivers rows to N remote databases.
    Each target has its own set of tables and its own writer. The service
    collects ALL pending stmts for a target into ONE writer.write_batch()
    call per cycle (minimises HTTP round-trips).

    Transport is injected via ``writers`` — sqloutbox never imports httpx,
    requests, or any external HTTP library.

    Parameters
    ----------
    config:
        OutboxConfig with db_dir, targets, batch_size, flush_interval, etc.

    writers:
        Dict mapping target name → OutboxWriter implementation.
        Keys must match ``config.targets[].name``.
    """

    def __init__(
        self,
        config: OutboxConfig,
        writers: dict[str, OutboxWriter],
    ) -> None:
        self._config = config
        self._writers = writers
        self._flush_interval = config.flush_interval
        self._cycle_count = 0

        # Verification support — request_verify() sets the event,
        # worker loop checks it between drain cycles.
        self._verify_requested = asyncio.Event()
        self._verify_result: VerifyResult | None = None
        self._verify_done = asyncio.Event()

        # Create per-target per-table Outbox instances from config.
        # Each target may have its own db_dir (set by TOML loader for
        # per-app isolation) or fall back to config.db_dir.
        self._target_outboxes: dict[str, dict[str, Outbox]] = {}
        _seen_dirs: set[str] = set()
        for target in config.targets:
            db_dir = target.db_dir or config.db_dir
            batch_size = target.batch_size or config.batch_size
            dir_key = str(db_dir)
            if dir_key not in _seen_dirs:
                db_dir.mkdir(parents=True, exist_ok=True)
                _seen_dirs.add(dir_key)
            self._target_outboxes[target.name] = {
                table: Outbox(
                    db_path=db_dir / f"{table}.db",
                    namespace=table,
                    batch_size=batch_size,
                    retain_log_days=target.get_retain_days(table),
                )
                for table in target.tables
            }

    # ── Verification ────────────────────────────────────────────────────────

    async def request_verify(self) -> VerifyResult:
        """Request a full integrity scan of all outbox databases.

        The scan runs between drain cycles — the current cycle finishes
        first, then verification runs, then normal drain resumes.

        This method blocks until the scan is complete and returns the
        result.  Safe to call from any coroutine in the same event loop.
        """
        self._verify_result = None
        self._verify_done.clear()
        self._verify_requested.set()
        await self._verify_done.wait()
        assert self._verify_result is not None
        return self._verify_result

    def _run_verify(self) -> VerifyResult:
        """Collect all outboxes and run verify_all(). Called in a thread."""
        all_outboxes: dict[str, Outbox] = {}
        for target_name, outboxes in self._target_outboxes.items():
            for table, outbox in outboxes.items():
                all_outboxes[f"{target_name}.{table}"] = outbox
        return verify_all(all_outboxes)

    # ── Entry point ──────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Run the drain worker forever. Call from an asyncio event loop."""
        logger.info(
            "sync worker started (poll=%.1fs, threshold=%d rows, "
            "max_wait=%.1fs, targets=%s)",
            self._flush_interval,
            self._config.table_flush_threshold,
            self._config.table_max_wait,
            [t.name for t in self._config.targets],
        )
        await self._ensure_schema()
        await self._seed_from_remote()
        await self._worker_loop()

    # ── Schema setup ────────────────────────────────────────────────────────

    async def _ensure_schema(self) -> None:
        """Manage ``outbox_seq`` column on remote tables at startup.

        When ``config.auto_schema=True`` (default):

        * Targets with ``inject_outbox_seq=True`` → ADD COLUMN outbox_seq
        * Targets with ``inject_outbox_seq=False`` → DROP COLUMN outbox_seq

        Both operations are idempotent — ADD silently skips if column exists,
        DROP silently skips if column doesn't exist.

        When ``config.auto_schema=False``: does nothing. Users manage schema
        themselves via ``config.schema_sql()`` / ``config.drop_schema_sql()``.
        """
        if not self._config.auto_schema:
            logger.info(
                "[outbox_sync] auto_schema=False — skipping schema management. "
                "Use config.schema_sql() / config.drop_schema_sql() for manual DDL.",
            )
            return

        for target in self._config.targets:
            writer = self._writers.get(target.name)
            if not writer:
                continue

            add_tables = [t for t in target.tables
                          if target.should_inject_seq(t)]
            drop_tables = [t for t in target.tables
                           if not target.should_inject_seq(t)]
            if add_tables:
                await self._add_outbox_seq(target, writer, add_tables)
            if drop_tables:
                await self._drop_outbox_seq(target, writer, drop_tables)

    async def _add_outbox_seq(
        self, target: TargetConfig, writer: OutboxWriter,
        tables: list[str],
    ) -> None:
        """ADD outbox_seq column + partial unique index to specified tables.

        Two statements per table:
            1. ALTER TABLE … ADD COLUMN outbox_seq INTEGER NOT NULL DEFAULT 0
            2. CREATE UNIQUE INDEX … WHERE outbox_seq != 0
        """
        stmts: list[tuple[str, list[Any]]] = []
        for table in tables:
            stmts.append((
                f"ALTER TABLE {table} ADD COLUMN "
                f"outbox_seq INTEGER NOT NULL DEFAULT 0",
                [],
            ))
            stmts.append((
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table}_outbox_seq "
                f"ON {table} (outbox_seq) WHERE outbox_seq != 0",
                [],
            ))
        if not stmts:
            return

        logger.info(
            "[outbox_sync] auto-schema: ADD outbox_seq to %s tables: %s",
            target.name, tables,
        )
        try:
            results = await writer.write_batch(stmts)
            for i, result in enumerate(results):
                # Two stmts per table: ALTER (even index) + CREATE INDEX (odd)
                table = tables[i // 2]
                if result.get("ok"):
                    if i % 2 == 0:
                        logger.info(
                            "[outbox_sync] added outbox_seq column to %s.%s",
                            target.name, table,
                        )
                else:
                    err = result.get("error", "")
                    if "duplic" in err.lower() or "already" in err.lower():
                        logger.debug(
                            "[outbox_sync] outbox_seq already exists on %s.%s",
                            target.name, table,
                        )
                    else:
                        logger.warning(
                            "[outbox_sync] could not add outbox_seq to %s.%s: %s",
                            target.name, table, err,
                        )
        except Exception as exc:
            logger.warning(
                "[outbox_sync] schema ADD failed for target '%s': %s",
                target.name, exc,
            )

    async def _drop_outbox_seq(
        self, target: TargetConfig, writer: OutboxWriter,
        tables: list[str],
    ) -> None:
        """DROP partial unique index + outbox_seq column from specified tables.

        Two statements per table:
            1. DROP INDEX IF EXISTS idx_{table}_outbox_seq
            2. ALTER TABLE … DROP COLUMN outbox_seq
        """
        stmts: list[tuple[str, list[Any]]] = []
        for table in tables:
            stmts.append((
                f"DROP INDEX IF EXISTS idx_{table}_outbox_seq",
                [],
            ))
            stmts.append((
                f"ALTER TABLE {table} DROP COLUMN outbox_seq",
                [],
            ))
        if not stmts:
            return

        logger.info(
            "[outbox_sync] auto-schema: DROP outbox_seq from %s tables: %s",
            target.name, tables,
        )
        try:
            results = await writer.write_batch(stmts)
            for i, result in enumerate(results):
                # Two stmts per table: DROP INDEX (even) + ALTER (odd)
                table = tables[i // 2]
                if result.get("ok"):
                    if i % 2 == 1:
                        logger.info(
                            "[outbox_sync] dropped outbox_seq column from %s.%s",
                            target.name, table,
                        )
                else:
                    err = result.get("error", "")
                    if "no such" in err.lower() or "does not exist" in err.lower():
                        logger.debug(
                            "[outbox_sync] outbox_seq not present on %s.%s — nothing to drop",
                            target.name, table,
                        )
                    else:
                        logger.warning(
                            "[outbox_sync] could not drop outbox_seq from %s.%s: %s",
                            target.name, table, err,
                        )
        except Exception as exc:
            logger.warning(
                "[outbox_sync] schema DROP failed for target '%s': %s",
                target.name, exc,
            )

    # ── Sequence seeding ─────────────────────────────────────────────────────

    async def _seed_from_remote(self) -> None:
        """Seed local AUTOINCREMENT counters from remote ``MAX(outbox_seq)``.

        On a fresh machine the local SQLite starts sequences from 1, which
        would collide with ``outbox_seq`` values already in the remote DB.
        ``INSERT OR IGNORE`` would then silently drop new events.

        For each target+table with ``inject_outbox_seq=True``, this method
        queries the remote DB for ``MAX(outbox_seq)`` and advances the
        local ``sqlite_sequence`` counter above that value.

        Runs once at startup, after ``_ensure_schema()`` (so the column
        exists) and before ``_worker_loop()`` (so new enqueues get safe
        sequences).

        Errors are logged but not fatal — the service continues with
        whatever local sequence exists. This is safe because:
        - If the remote DB is empty, the local counter is fine at 1.
        - If the remote DB has data but the query fails, the next
          successful seed (on restart) will catch up.
        """
        for target in self._config.targets:
            writer = self._writers.get(target.name)
            if not writer:
                continue

            inject_tables = [t for t in target.tables
                             if target.should_inject_seq(t)]
            if not inject_tables:
                continue

            # One SELECT per table, batched into a single write_batch call
            stmts: list[tuple[str, list[Any]]] = [
                (f"SELECT MAX(outbox_seq) FROM {table} WHERE outbox_seq != 0", [])
                for table in inject_tables
            ]

            try:
                results = await writer.write_batch(stmts)
            except Exception as exc:
                logger.warning(
                    "[outbox_sync] seed query failed for target '%s': %s "
                    "— continuing with local sequence",
                    target.name, exc,
                )
                continue

            outboxes = self._target_outboxes.get(target.name, {})
            for i, table in enumerate(inject_tables):
                result = results[i] if i < len(results) else {}
                if not result.get("ok"):
                    logger.debug(
                        "[outbox_sync] seed query failed for %s.%s: %s",
                        target.name, table, result.get("error", "unknown"),
                    )
                    continue

                rows = result.get("rows", [])
                if not rows or rows[0][0] is None:
                    continue  # no outbox_seq in remote — nothing to seed

                max_remote_seq = int(rows[0][0])
                outbox = outboxes.get(table)
                if outbox:
                    outbox.seed_sequence(max_remote_seq)

    # ── Background worker ────────────────────────────────────────────────────

    async def _worker_loop(self) -> None:
        """Round-robin drain: per-table flush based on row count or time.

        Each scan pass iterates over all targets and their tables.  A table
        is included in the flush batch when *either* trigger fires:

        * **Row threshold** — ``pending >= table_flush_threshold`` (default 15)
        * **Time threshold** — any pending rows AND last flush was
          ``>= table_max_wait`` seconds ago (default 6.0s)

        Tables that don't meet either trigger are skipped (round-robin to
        the next table).  All ready tables for one target are still batched
        into a single ``write_batch()`` call to minimise HTTP round-trips.
        """
        # Per-table last-flush timestamp.  Initialised to 0.0 so every table
        # with pending rows flushes on the very first scan.
        last_flush: dict[str, float] = {
            table: 0.0
            for outboxes in self._target_outboxes.values()
            for table in outboxes
        }

        threshold = self._config.table_flush_threshold
        max_wait = self._config.table_max_wait

        while True:
            await asyncio.sleep(self._flush_interval)

            # ── Verification hook ───────────────────────────────────
            # Runs between drain cycles — current cycle already finished,
            # next cycle starts after verification completes.
            if self._verify_requested.is_set():
                self._verify_requested.clear()
                logger.info("[outbox_sync] integrity verification requested")
                result = await asyncio.to_thread(self._run_verify)
                self._verify_result = result
                self._verify_done.set()
                status = "OK" if result.ok else "FAILED"
                logger.info(
                    "[outbox_sync] verification complete: %s  "
                    "tables=%d  duration=%.0fms",
                    status, len(result.tables), result.duration_ms,
                )
                continue  # skip this drain cycle, resume next iteration

            self._cycle_count += 1
            cycle_start = time.monotonic()
            now = cycle_start

            any_flushed = False

            for target in self._config.targets:
                target_name = target.name
                outboxes = self._target_outboxes.get(target_name, {})
                writer = self._writers.get(target_name)
                if not writer:
                    continue

                stmt_info: list[tuple[str, int]] = []   # (table, outbox_seq)
                all_stmts: list[tuple[str, list[Any]]] = []
                flushed_tables: list[str] = []

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

                    # ── Round-robin decision ─────────────────────────────
                    if pending < threshold and elapsed < max_wait:
                        if logger.isEnabledFor(_VERBOSE):
                            logger.log(
                                _VERBOSE,
                                "[outbox_sync] skip table='%s'  pending=%d  "
                                "elapsed=%.1fs  (need %d rows or %.1fs)",
                                table, pending, elapsed, threshold, max_wait,
                            )
                        continue

                    # ── Table is ready — fetch rows ──────────────────────
                    # Head-of-line hold: while the head is stuck, fetch ONLY it
                    # (limit=1). A healthy namespace fetches a normal batch.
                    if head_stuck:
                        rows = await asyncio.to_thread(outbox.fetch_unsynced, 1)
                    else:
                        rows = await asyncio.to_thread(outbox.fetch_unsynced)
                    if not rows:
                        continue

                    trigger = "threshold" if pending >= threshold else "max_wait"
                    logger.debug(
                        "[outbox_sync] table='%s'  fetched=%d rows  "
                        "trigger=%s  seqs=%s",
                        table, len(rows), trigger,
                        [r.seq for r in rows[:10]],
                    )

                    chain_ok, gap_seqs = await asyncio.to_thread(
                        outbox.verify_chain, rows,
                    )
                    if not chain_ok:
                        logger.error(
                            "[outbox_sync] chain gap in '%s' at seq(s) %s "
                            "— blocked until gap resolved",
                            table, gap_seqs,
                        )
                        continue

                    for row in rows:
                        try:
                            sql = row.tag
                            args = json.loads(row.payload.decode())
                            if target.should_inject_seq(table):
                                sql, args = inject_outbox_seq(sql, args, row.seq)
                        except Exception as exc:
                            # L1: a single undecodable / untransformable row must
                            # not escape the cycle and zombify the daemon. Log once
                            # and skip it THIS cycle (it stays pending; permanent
                            # dead-letter routing arrives in the WS-2 plan).
                            logger.error(
                                "[outbox_sync] skipping bad row table='%s' seq=%d: %s",
                                table, row.seq, exc,
                            )
                            continue
                        all_stmts.append((sql, args))
                        stmt_info.append((table, row.seq))

                    flushed_tables.append(table)
                  except (sqlite3.DatabaseError, sqlite3.OperationalError) as exc:
                    # L2: isolate a corrupt/locked namespace. Skip it THIS cycle;
                    # sibling tables keep draining. (Transient lock → retries next
                    # cycle; structural corruption → keeps logging until repaired.)
                    logger.error(
                        "[outbox_sync] table='%s' skipped this cycle (db error): %s",
                        table, exc,
                    )
                    continue
                  except Exception as exc:
                    # Any other per-table fault must not escape the loop either.
                    logger.exception(
                        "[outbox_sync] table='%s' skipped this cycle (unexpected): %s",
                        table, exc,
                    )
                    continue

                if all_stmts:
                    any_flushed = True
                    await self._flush_to_target(
                        writer, all_stmts, stmt_info,
                        outboxes, target_name, cycle_start,
                    )
                    # Update last-flush timestamp for included tables
                    for table in flushed_tables:
                        last_flush[table] = now

            if self._cycle_count % self._config.cleanup_every == 0:
                await self._prune_all()

    # ── Flush to one target ──────────────────────────────────────────────────

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

    # ── Maintenance ──────────────────────────────────────────────────────────

    async def _prune_all(self) -> None:
        """Prune sync_log on all outboxes across all targets."""
        for outboxes in self._target_outboxes.values():
            for outbox in outboxes.values():
                await asyncio.to_thread(outbox.prune_sync_log)

    # ── Monitoring ───────────────────────────────────────────────────────────

    def pending_count(self) -> dict[str, int]:
        """Pending row count per table across all targets."""
        counts: dict[str, int] = {}
        for outboxes in self._target_outboxes.values():
            for table, outbox in outboxes.items():
                counts[table] = outbox.pending_count()
        return counts

    def total_pending(self) -> int:
        """Total pending rows across all tables and targets."""
        return sum(
            outbox.pending_count()
            for outboxes in self._target_outboxes.values()
            for outbox in outboxes.values()
        )
