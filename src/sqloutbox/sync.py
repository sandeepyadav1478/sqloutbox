"""OutboxSyncService — multi-target outbox drain daemon.

Reads local SQLite outbox files and delivers rows to N remote database targets.
Transport is injected via the OutboxWriter protocol — sqloutbox stays
zero-dependency (stdlib only).

The service runs as a continuous loop:
    1. Sleep ``flush_interval`` seconds
    2. Count pending rows per table across all targets
    3. For each target with pending rows:
       a. fetch_unsynced()  — read pending rows per table
       b. verify_chain()    — check singly-linked chain integrity
       c. Decode payload    — tag = SQL, payload = JSON args
       d. inject_outbox_seq — append outbox_seq to INSERTs (per target config)
       e. Collect all stmts for this target into one batch
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
from sqloutbox.config import OutboxConfig

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
    """Append ``outbox_seq`` column to an INSERT statement.

    Transforms:
        INSERT INTO table (a, b) VALUES (?, ?)
    Into:
        INSERT OR IGNORE INTO table (a, b, outbox_seq) VALUES (?, ?, ?)

    The ``INSERT OR IGNORE`` prefix provides idempotent delivery — if the
    row was already written to the remote DB (e.g. after a crash between
    write and local delete), the re-attempt silently succeeds.

    Parameters
    ----------
    sql:
        The original INSERT statement.
    args:
        The original bind values.
    outbox_seq:
        The outbox sequence number to append.

    Returns
    -------
    (modified_sql, modified_args)
    """
    s = sql.strip()
    # Convert INSERT INTO → INSERT OR IGNORE INTO
    if s.upper().startswith("INSERT INTO"):
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
            "sync worker started (flush_interval=%.0fs, targets=%s)",
            self._flush_interval,
            [t.name for t in self._config.targets],
        )
        await self._worker_loop()

    # ── Background worker ────────────────────────────────────────────────────

    async def _worker_loop(self) -> None:
        """Drain all SQLite outbox files → ONE writer call per target per cycle."""
        while True:
            await asyncio.sleep(self._flush_interval)
            self._cycle_count += 1
            cycle_start = time.monotonic()

            # ── Early exit: nothing pending ───────────────────────────────
            total = 0
            for outboxes in self._target_outboxes.values():
                total += sum(o.pending_count() for o in outboxes.values())

            if logger.isEnabledFor(_VERBOSE):
                all_pending = {}
                for outboxes in self._target_outboxes.values():
                    for t, o in outboxes.items():
                        n = o.pending_count()
                        if n > 0:
                            all_pending[t] = n
                logger.log(
                    _VERBOSE,
                    "[outbox_sync] cycle #%d  total_pending=%d  per_table=%s",
                    self._cycle_count, total, all_pending or "{}",
                )

            if total == 0:
                if self._cycle_count % self._config.cleanup_every == 0:
                    await self._prune_all()
                continue

            # ── Collect and flush per target ──────────────────────────────
            any_flushed = False
            for target in self._config.targets:
                target_name = target.name
                outboxes = self._target_outboxes.get(target_name, {})
                writer = self._writers.get(target_name)
                if not writer:
                    continue

                stmt_info: list[tuple[str, int]] = []   # (table, outbox_seq)
                all_stmts: list[tuple[str, list[Any]]] = []

                for table, outbox in outboxes.items():
                    rows = await asyncio.to_thread(outbox.fetch_unsynced)
                    if not rows:
                        continue

                    logger.debug(
                        "[outbox_sync] table='%s'  fetched=%d rows  seqs=%s",
                        table, len(rows), [r.seq for r in rows[:10]],
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

                    if logger.isEnabledFor(_VERBOSE):
                        logger.log(
                            _VERBOSE,
                            "[outbox_sync] table='%s'  chain OK  "
                            "%d rows ready for %s",
                            table, len(rows), target_name,
                        )

                    for row in rows:
                        sql = row.tag
                        args = json.loads(row.payload.decode())
                        if target.inject_outbox_seq:
                            sql, args = inject_outbox_seq(sql, args, row.seq)
                        all_stmts.append((sql, args))
                        stmt_info.append((table, row.seq))

                        if logger.isEnabledFor(_VERBOSE):
                            logger.log(
                                _VERBOSE,
                                "[outbox_sync]   seq=%d  sql='%s'  args=%s",
                                row.seq, row.tag[:80], args,
                            )

                if all_stmts:
                    any_flushed = True
                    await self._flush_to_target(
                        writer, all_stmts, stmt_info,
                        outboxes, target_name, cycle_start,
                    )

            if not any_flushed:
                if self._cycle_count % self._config.cleanup_every == 0:
                    await self._prune_all()
                continue

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
