"""Integrity verification for sqloutbox databases.

Provides comprehensive legitimacy checks on outbox ``.db`` files:

- **Chain integrity** — singly-linked ``prev_seq`` chain is intact
- **Sequence continuity** — no unexpected gaps in ``seq`` values
- **Timestamp monotonicity** — ``created_at`` is non-decreasing
- **Orphan detection** — ``sync_log`` entries beyond queue range
- **Row counts** — pending, total queue, total sync_log

Access methods:

1. **CLI** ``sqloutbox verify`` — offline scan of ``.db`` files
2. **Signal** ``kill -USR1 <pid>`` — trigger scan on running daemon
3. **Python API** ``await svc.request_verify()`` — programmatic access
4. **Per-outbox** ``outbox.verify_full()`` — single-table check

All checks are read-only — they never modify the database.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from sqloutbox._schema import now_iso, open_read_conn, thread_conn

if TYPE_CHECKING:
    from sqloutbox._outbox import Outbox

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TableVerifyResult:
    """Verification result for a single outbox table/namespace."""

    table: str
    db_path: str
    ok: bool
    pending_count: int
    total_rows: int
    sync_log_rows: int
    chain_ok: bool
    chain_gaps: tuple[int, ...] = ()
    seq_continuous: bool = True
    seq_range: tuple[int, int] | None = None
    timestamps_monotonic: bool = True
    orphan_sync_log: int = 0
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class VerifyResult:
    """Aggregated verification result across all tables."""

    ok: bool
    tables: tuple[TableVerifyResult, ...] = ()
    checked_at: str = ""
    duration_ms: float = 0.0


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


def verify_all(outboxes: dict[str, Outbox]) -> VerifyResult:
    """Run integrity checks on multiple outboxes.

    Parameters
    ----------
    outboxes:
        Mapping of table/namespace name to Outbox instance.

    Returns
    -------
    VerifyResult
        Aggregated result. ``ok`` is True only if ALL tables pass.
    """
    t0 = time.monotonic()
    results: list[TableVerifyResult] = []

    for name, outbox in outboxes.items():
        result = verify_outbox(outbox)
        results.append(result)
        if result.ok:
            logger.info(
                "[verify] %s  OK  pending=%d  rows=%d  sync_log=%d",
                name, result.pending_count, result.total_rows,
                result.sync_log_rows,
            )
        else:
            logger.warning(
                "[verify] %s  FAIL  errors=%s",
                name, result.errors,
            )

    duration_ms = (time.monotonic() - t0) * 1000
    all_ok = all(r.ok for r in results)

    return VerifyResult(
        ok=all_ok,
        tables=tuple(results),
        checked_at=now_iso(),
        duration_ms=round(duration_ms, 1),
    )
