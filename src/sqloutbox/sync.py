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
import time
from collections import defaultdict
from typing import Any, Protocol, runtime_checkable

from sqloutbox._outbox import Outbox
from sqloutbox.config import OutboxConfig, TargetConfig

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
                {"ok": True,  "rows_affected": N}  — confirmed
                {"ok": False, "error": "..."}       — failed (retried next cycle)
        """
        ...


# ── SQL helpers ──────────────────────────────────────────────────────────────


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
    upper = s.upper()

    if upper.startswith("INSERT"):
        # Convert INSERT INTO → INSERT OR IGNORE INTO
        if upper.startswith("INSERT INTO"):
            s = "INSERT OR IGNORE INTO" + s[len("INSERT INTO"):]
        # Insert outbox_seq column before ) VALUES
        vi = s.upper().find(") VALUES")
        if vi != -1:
            s = s[:vi] + ", outbox_seq" + s[vi:]
        # Insert ? placeholder before last )
        lp = s.rfind(")")
        if lp != -1:
            s = s[:lp] + ", ?" + s[lp:]
        return s, list(args) + [outbox_seq]

    if upper.startswith("UPDATE"):
        where_idx = upper.find(" WHERE ")
        if where_idx != -1:
            # Count ? placeholders in SET clause (before WHERE)
            set_part = s[:where_idx]
            n_set_args = set_part.count("?")
            # Inject outbox_seq=? before WHERE
            s = s[:where_idx] + ", outbox_seq = ?" + s[where_idx:]
            new_args = list(args)
            new_args.insert(n_set_args, outbox_seq)
            return s, new_args
        # No WHERE clause — append to SET
        s = s + ", outbox_seq = ?"
        return s, list(args) + [outbox_seq]

    # Unknown statement type — return unchanged
    return s, list(args) + [outbox_seq]


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

        config.db_dir.mkdir(parents=True, exist_ok=True)

        # Create per-target per-table Outbox instances from config
        self._target_outboxes: dict[str, dict[str, Outbox]] = {}
        for target in config.targets:
            self._target_outboxes[target.name] = {
                table: Outbox(
                    db_path=config.db_dir / f"{table}.db",
                    namespace=table,
                    batch_size=config.batch_size,
                )
                for table in target.tables
            }

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

            if target.inject_outbox_seq:
                await self._add_outbox_seq(target, writer)
            else:
                await self._drop_outbox_seq(target, writer)

    async def _add_outbox_seq(
        self, target: TargetConfig, writer: OutboxWriter,
    ) -> None:
        """ADD outbox_seq column to tables for this target."""
        stmts: list[tuple[str, list[Any]]] = [
            (f"ALTER TABLE {table} ADD COLUMN outbox_seq INTEGER UNIQUE", [])
            for table in target.tables
        ]
        if not stmts:
            return

        logger.info(
            "[outbox_sync] auto-schema: ADD outbox_seq to %s tables: %s",
            target.name, list(target.tables),
        )
        try:
            results = await writer.write_batch(stmts)
            for i, result in enumerate(results):
                table = target.tables[i]
                if result.get("ok"):
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
    ) -> None:
        """DROP outbox_seq column from tables for this target (cleanup)."""
        stmts: list[tuple[str, list[Any]]] = [
            (f"ALTER TABLE {table} DROP COLUMN outbox_seq", [])
            for table in target.tables
        ]
        if not stmts:
            return

        logger.info(
            "[outbox_sync] auto-schema: DROP outbox_seq from %s tables: %s",
            target.name, list(target.tables),
        )
        try:
            results = await writer.write_batch(stmts)
            for i, result in enumerate(results):
                table = target.tables[i]
                if result.get("ok"):
                    logger.info(
                        "[outbox_sync] dropped outbox_seq column from %s.%s",
                        target.name, table,
                    )
                else:
                    err = result.get("error", "")
                    # Column doesn't exist — expected if never added
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
                    pending = outbox.pending_count()
                    if pending == 0:
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
                        sql = row.tag
                        args = json.loads(row.payload.decode())
                        if target.inject_outbox_seq:
                            sql, args = inject_outbox_seq(sql, args, row.seq)
                        all_stmts.append((sql, args))
                        stmt_info.append((table, row.seq))

                    flushed_tables.append(table)

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
        """Send a batch of statements to a target and confirm delivery."""
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

        confirmed_by_table: dict[str, list[int]] = defaultdict(list)
        failed_count = 0
        for i, result in enumerate(results):
            table, outbox_seq = stmt_info[i]
            if result["ok"]:
                confirmed_by_table[table].append(outbox_seq)
            else:
                failed_count += 1
                logger.warning(
                    "[outbox_sync] %s write failed for '%s' seq=%d: %s",
                    target_name, table, outbox_seq, result.get("error", ""),
                )

        total_confirmed = 0
        for table, seqs in confirmed_by_table.items():
            outbox = outboxes[table]
            await asyncio.to_thread(outbox.mark_synced, seqs)
            await asyncio.to_thread(outbox.delete_synced, seqs)
            total_confirmed += len(seqs)
            if logger.isEnabledFor(_VERBOSE):
                logger.log(
                    _VERBOSE,
                    "[outbox_sync]   confirmed %s table='%s'  %d rows  "
                    "seqs=%s",
                    target_name, table, len(seqs), seqs[:10],
                )

        cycle_ms = (time.monotonic() - cycle_start) * 1000
        if total_confirmed:
            level = (
                logging.INFO
                if (total_confirmed >= 10 or failed_count)
                else logging.DEBUG
            )
            logger.log(
                level,
                "[outbox_sync] cycle #%d  %s delivered=%d  failed=%d  "
                "tables=%s  write=%.0fms  cycle=%.0fms",
                self._cycle_count, target_name, total_confirmed, failed_count,
                list(confirmed_by_table.keys()),
                write_ms, cycle_ms,
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
