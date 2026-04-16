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
from typing import TYPE_CHECKING

from sqloutbox._schema import now_iso, thread_conn

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

    All checks are read-only — no writes to the database.

    Checks performed:

    1. **Chain integrity** — fetch all unsynced rows and verify the
       ``prev_seq`` singly-linked chain using ``Outbox.verify_chain()``.
    2. **Sequence continuity** — walk all ``seq`` values in
       ``outbox_queue`` and check that every gap is accounted for by
       a corresponding entry in ``outbox_sync_log``.
    3. **Timestamp monotonicity** — ``created_at`` must be
       non-decreasing when ordered by ``seq``.
    4. **Orphan sync_log** — count ``outbox_sync_log`` entries whose
       ``seq`` exceeds the current ``MAX(seq)`` in ``outbox_queue``
       (normal after full delivery, but flags truncation).
    5. **Row counts** — pending, total queue rows, total sync_log rows.
    """
    errors: list[str] = []
    ns = outbox.namespace
    db_path = str(outbox.db_path)

    with thread_conn(outbox.db_path) as conn:
        # ── Row counts ──────────────────────────────────────────────
        total_rows = conn.execute(
            "SELECT COUNT(*) FROM outbox_queue WHERE namespace = ?", [ns],
        ).fetchone()[0]

        pending_count = conn.execute(
            "SELECT COUNT(*) FROM outbox_queue WHERE namespace = ? AND synced = 0",
            [ns],
        ).fetchone()[0]

        sync_log_rows = conn.execute(
            "SELECT COUNT(*) FROM outbox_sync_log WHERE namespace = ?", [ns],
        ).fetchone()[0]

        # ── Seq range ───────────────────────────────────────────────
        range_row = conn.execute(
            "SELECT MIN(seq), MAX(seq) FROM outbox_queue WHERE namespace = ?",
            [ns],
        ).fetchone()
        if range_row[0] is not None:
            seq_range: tuple[int, int] | None = (range_row[0], range_row[1])
        else:
            seq_range = None

        # ── 1. Chain integrity (unsynced rows) ──────────────────────
        unsynced = conn.execute(
            "SELECT seq, tag, payload, prev_seq, source "
            "FROM outbox_queue "
            "WHERE namespace = ? AND synced = 0 "
            "ORDER BY seq",
            [ns],
        ).fetchall()

    # Use Outbox.verify_chain() for the chain check
    from sqloutbox._models import QueueRow

    rows = [
        QueueRow(seq=r[0], tag=r[1], payload=r[2].encode(), prev_seq=r[3], source=r[4] or "")
        for r in unsynced
    ]
    chain_ok, chain_gaps_list = outbox.verify_chain(rows)
    if not chain_ok:
        errors.append(f"chain gap: missing seq(s) {chain_gaps_list}")

    # ── 2. Sequence continuity ──────────────────────────────────
    seq_continuous = True
    with thread_conn(outbox.db_path) as conn:
        all_seqs = [
            r[0] for r in conn.execute(
                "SELECT seq FROM outbox_queue WHERE namespace = ? ORDER BY seq",
                [ns],
            ).fetchall()
        ]
        sync_log_seqs = {
            r[0] for r in conn.execute(
                "SELECT seq FROM outbox_sync_log WHERE namespace = ?", [ns],
            ).fetchall()
        }

    if len(all_seqs) >= 2:
        for i in range(1, len(all_seqs)):
            prev_s = all_seqs[i - 1]
            curr_s = all_seqs[i]
            if curr_s != prev_s + 1:
                # Check if every missing seq is in sync_log
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
    timestamps_monotonic = True
    with thread_conn(outbox.db_path) as conn:
        ts_rows = conn.execute(
            "SELECT seq, created_at FROM outbox_queue "
            "WHERE namespace = ? ORDER BY seq",
            [ns],
        ).fetchall()

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
    with thread_conn(outbox.db_path) as conn:
        max_queue_seq = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM outbox_queue WHERE namespace = ?",
            [ns],
        ).fetchone()[0]

        # sync_log entries beyond current queue max — normal after delivery,
        # but a very high count could indicate queue truncation
        orphan_sync_log = conn.execute(
            "SELECT COUNT(*) FROM outbox_sync_log "
            "WHERE namespace = ? AND seq > ?",
            [ns, max_queue_seq],
        ).fetchone()[0]

    ok = chain_ok and seq_continuous and timestamps_monotonic
    return TableVerifyResult(
        table=ns,
        db_path=db_path,
        ok=ok,
        pending_count=pending_count,
        total_rows=total_rows,
        sync_log_rows=sync_log_rows,
        chain_ok=chain_ok,
        chain_gaps=tuple(chain_gaps_list),
        seq_continuous=seq_continuous,
        seq_range=seq_range,
        timestamps_monotonic=timestamps_monotonic,
        orphan_sync_log=orphan_sync_log,
        errors=tuple(errors),
    )


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
