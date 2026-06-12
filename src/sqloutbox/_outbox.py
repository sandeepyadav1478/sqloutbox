"""Outbox class — core implementation of the durable singly-linked event queue."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqloutbox._verify import TableVerifyResult

from sqloutbox._models import DeadRow, NamespaceHealth, QueueRow
from sqloutbox._schema import (
    now_iso,
    open_write_conn,
    placeholders,
    thread_conn,
)
from sqloutbox.exceptions import QueueFullError

logger = logging.getLogger(__name__)

DEFAULT_RETAIN_LOG_DAYS = 30
DEFAULT_BATCH_SIZE      = 50
DEFAULT_CLEANUP_EVERY   = 500

# SQLite's SQLITE_MAX_VARIABLE_NUMBER is 999 on many builds. Chunk seq lists
# bound into IN (?,?,…) statements to stay safely below it. (WS-3, F025.)
_VAR_CHUNK = 900


def _chunked(seqs: list[int], size: int = _VAR_CHUNK) -> list[list[int]]:
    """Split a seq list into consecutive chunks of at most ``size`` items."""
    return [seqs[i:i + size] for i in range(0, len(seqs), size)]


class Outbox:
    """Durable, singly-linked SQLite event outbox.

    Parameters
    ----------
    db_path:
        Path to the SQLite file. Created (with all tables and indexes) if it
        does not exist. Multiple Outbox instances MAY share the same file —
        they are partitioned by namespace.

    namespace:
        String key that partitions rows within the shared DB file. Use one
        namespace per logical producer (e.g. class name, service name).

    retain_log_days:
        How long to keep sync_log entries. Older entries are pruned by
        prune_sync_log(). Default: 7 days.

    batch_size:
        Default maximum rows returned by fetch_unsynced(). Default: 50.

    cleanup_every:
        Hint to the caller: call prune_sync_log() every N consumer cycles.
        Not enforced internally. Default: 500.
    """

    def __init__(
        self,
        db_path: Path,
        namespace: str,
        retain_log_days: int = DEFAULT_RETAIN_LOG_DAYS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        cleanup_every: int = DEFAULT_CLEANUP_EVERY,
        max_pending: int | None = None,
    ) -> None:
        self.db_path         = db_path
        self.namespace       = namespace
        self.retain_log_days = retain_log_days
        self.batch_size      = batch_size
        self.cleanup_every   = cleanup_every
        # WS-3 D2: opt-in backpressure cap. None = unbounded (default, fast path
        # unchanged). When set, enqueue() raises QueueFullError at the cap.
        self.max_pending     = max_pending
        # Persistent write connection — used exclusively by enqueue() from one thread
        self._write_conn = open_write_conn(db_path)
        # Producer-side seed (spec §6.3, mechanism (a)): on a fresh host with a
        # populated remote, lazily advance the local AUTOINCREMENT above the
        # persisted high-water mark BEFORE the first enqueue, so new seqs never
        # collide with remote outbox_seq values (which INSERT OR IGNORE drops).
        self._seed_from_hwm()

    # ── Hot path ────────────────────────────────────────────────────────────

    def enqueue(self, tag: str, payload: bytes, source: str = "") -> int | None:
        """Insert one event and stitch it into the singly-linked chain.

        Two SQL statements in one atomic transaction:
            1. SELECT MAX(seq) — find current tail seq for this namespace.
            2. INSERT new row with prev_seq = tail seq.

        Returns the assigned seq (AUTOINCREMENT row ID), or None on failure.
        Cost: ~150µs for local SQLite. Never raises — drops with WARNING on error.

        Parameters
        ----------
        tag:
            Caller-defined event type label (e.g. SQL INSERT string).
        payload:
            Raw bytes — format chosen by the caller. Stored as text (decoded
            from UTF-8). Non-UTF-8 bytes are not supported.
        source:
            Identity of the middleware that produced this row
            (e.g. "SchedulerMiddleware"). Used for debugging and analytics.

        Raises
        ------
        QueueFullError
            Only when ``max_pending`` is set and the namespace is already at
            the cap. With the default (``max_pending=None``) this never raises;
            the fast INSERT path below is unchanged.
        """
        # WS-3 D2: opt-in hard backstop. Checked BEFORE the try/except (which
        # swallows-and-returns-None) so QueueFullError propagates to the caller.
        if self.max_pending is not None and self.pending_count() >= self.max_pending:
            raise QueueFullError(self.namespace, self.max_pending)
        try:
            # BEGIN IMMEDIATE acquires the write lock before the SELECT so no
            # other writer can insert between the chain-tail read and the INSERT.
            self._write_conn.execute("BEGIN IMMEDIATE")
            row = self._write_conn.execute(
                "SELECT MAX(seq) FROM outbox_queue WHERE namespace = ?",
                [self.namespace],
            ).fetchone()
            prev_seq = row[0] if row else None  # None if namespace is empty

            cur = self._write_conn.execute(
                "INSERT INTO outbox_queue "
                "(created_at, namespace, source, tag, payload, prev_seq) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [now_iso(), self.namespace, source, tag, payload.decode(), prev_seq],
            )
            assert cur.lastrowid is not None
            new_seq: int = cur.lastrowid
            self._write_conn.commit()
            # Persist the high-water mark so a fresh producer on this host
            # (mechanism (a)) re-seeds above it after a restart.
            self.record_hwm(new_seq)
            return new_seq
        except Exception as exc:
            try:
                self._write_conn.rollback()
            except Exception:
                pass
            logger.warning(
                "sqloutbox[%s]: enqueue failed — event dropped: %s",
                self.namespace, exc,
            )
            return None

    def enqueue_batch(self, items: list[tuple[str, bytes]], source: str = "") -> list[int]:
        """Insert multiple events in one atomic transaction.

        All rows are linked into the singly-linked chain and committed together
        — one SQLite transaction regardless of batch size. This is ~N× faster
        than calling enqueue() N times (each call commits its own transaction).

        Returns the list of assigned seqs in insertion order.
        Returns an empty list on failure (all items dropped with WARNING).

        The chain is stitched in insertion order:
            prev_tail → item[0] → item[1] → ... → item[N-1]

        Parameters
        ----------
        items:
            List of (tag, payload) pairs in delivery order.
        source:
            Identity of the middleware that produced all rows in this batch
            (e.g. "InvestorMiddleware"). Applied uniformly to every row.
        """
        if not items:
            return []
        ts = now_iso()
        try:
            # BEGIN IMMEDIATE acquires the write lock before any SELECT so no
            # other writer can insert between the two reads and the executemany.
            # Without this, two concurrent writers could both read the same
            # sqlite_sequence value, compute the same start_seq, then insert
            # rows with wrong prev_seq offsets within the batch.
            self._write_conn.execute("BEGIN IMMEDIATE")

            # Find the current tail (highest seq for this namespace)
            row = self._write_conn.execute(
                "SELECT MAX(seq) FROM outbox_queue WHERE namespace = ?",
                [self.namespace],
            ).fetchone()
            # chain_tail: current namespace tail (for prev_seq chain linking).
            # None when the namespace queue is empty (after full delivery+delete).
            chain_tail: int | None = row[0] if row and row[0] is not None else None

            # start_seq: actual next ROWID assigned by AUTOINCREMENT.
            # AUTOINCREMENT never re-uses IDs even after row deletion, so we
            # must read sqlite_sequence — not MAX(seq) from the (empty) queue.
            seq_row = self._write_conn.execute(
                "SELECT seq FROM sqlite_sequence WHERE name = 'outbox_queue'"
            ).fetchone()
            start_seq: int = (seq_row[0] if seq_row and seq_row[0] is not None else 0) + 1

            # Pre-compute all prev_seqs: one SELECT replaces N lastrowid lookups.
            # This reduces Python↔SQLite round-trips from N to 1 executemany call.
            rows_data: list[tuple] = []
            for i, (tag, payload) in enumerate(items):
                if chain_tail is None and i == 0:
                    prev = None                      # head of empty namespace
                elif chain_tail is None:
                    prev = start_seq + i - 1         # links to previous row in this batch
                else:
                    prev = chain_tail + i            # row[0] links to existing chain tail
                rows_data.append((ts, self.namespace, source, tag, payload.decode(), prev))

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
        except Exception as exc:
            try:
                self._write_conn.rollback()
            except Exception:
                pass
            logger.warning(
                "sqloutbox[%s]: enqueue_batch failed (%d items) — dropping: %s",
                self.namespace, len(items), exc,
            )
            return []

    # ── Consumer API (safe to call from any thread via asyncio.to_thread) ───

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

    def verify_chain(self, rows: list[QueueRow]) -> tuple[bool, list[int]]:
        """Verify the singly-linked chain is intact for this batch.

        Checks:
            1. Consecutive rows are correctly linked
               (rows[i].prev_seq == rows[i-1].seq).
            2. The first row's predecessor (if any) exists in outbox_queue
               OR in outbox_sync_log — meaning it was not lost.

        Returns
        -------
        (chain_ok, missing_seqs)
            chain_ok is False if any gap is found.
            missing_seqs lists the seq(s) that are absent.

        If chain_ok is False the caller MUST NOT deliver or delete this batch.
        """
        if not rows:
            return True, []

        missing: list[int] = []
        with thread_conn(self.db_path) as conn:
            for i, row in enumerate(rows):
                if i == 0:
                    if row.prev_seq is not None and not self._seq_accounted(conn, row.prev_seq):
                        missing.append(row.prev_seq)
                else:
                    expected_prev = rows[i - 1].seq
                    if row.prev_seq != expected_prev:
                        missing.append(expected_prev)

        if missing:
            logger.error(
                "sqloutbox[%s]: chain gap — missing seq(s): %s. "
                "Delivery blocked. See recovery SQL in sqloutbox docs.",
                self.namespace, missing,
            )
        return len(missing) == 0, missing

    def mark_synced(self, seqs: list[int]) -> None:
        """Mark rows as confirmed delivered. Does NOT delete them yet.

        Chunks the seq list to <= _VAR_CHUNK per statement so a large
        batch_size cannot trip SQLITE_MAX_VARIABLE_NUMBER.

        Opens its own connection — safe to call from any thread.
        """
        if not seqs:
            return
        with thread_conn(self.db_path) as conn:
            for chunk in _chunked(seqs):
                conn.execute(
                    f"UPDATE outbox_queue SET synced = 1 "
                    f"WHERE seq IN ({placeholders(len(chunk))})",
                    chunk,
                )

    def delete_synced(self, seqs: list[int]) -> None:
        """Delete delivered rows and record them in outbox_sync_log.

        SQL calls: O(3) regardless of batch size.
            1. SELECT seq, synced WHERE seq IN (...)   — verify synced flag
            2. INSERT OR IGNORE INTO outbox_sync_log (executemany)
            3. DELETE FROM outbox_queue WHERE seq IN (...)

        Chain integrity was already verified by verify_chain() earlier in the
        worker cycle. Successors are found via WHERE prev_seq = X when needed
        (using idx_outbox_prev) — no next_seq column required.

        Opens its own connection — safe to call from any thread.
        """
        if not seqs:
            return
        with thread_conn(self.db_path) as conn:
            # Step 1: Batch-fetch all candidate rows — verify each is synced.
            # Chunked to stay under SQLITE_MAX_VARIABLE_NUMBER.
            by_seq: dict[int, bool] = {}
            for chunk in _chunked(seqs):
                rows_data = conn.execute(
                    f"SELECT seq, synced FROM outbox_queue "
                    f"WHERE seq IN ({placeholders(len(chunk))})",
                    chunk,
                ).fetchall()
                for r in rows_data:
                    by_seq[r[0]] = bool(r[1])

            safe: list[int] = []
            for seq in seqs:
                if seq not in by_seq:
                    continue  # already deleted
                if not by_seq[seq]:
                    logger.error(
                        "sqloutbox[%s]: refusing to delete unsynced seq=%d",
                        self.namespace, seq,
                    )
                    continue
                safe.append(seq)

            if not safe:
                return

            # Step 2: Batch record in sync_log + batch delete
            now = now_iso()
            conn.executemany(
                "INSERT OR IGNORE INTO outbox_sync_log (seq, namespace, synced_at) "
                "VALUES (?, ?, ?)",
                [(s, self.namespace, now) for s in safe],
            )
            for chunk in _chunked(safe):
                conn.execute(
                    f"DELETE FROM outbox_queue "
                    f"WHERE seq IN ({placeholders(len(chunk))})",
                    chunk,
                )
            logger.debug(
                "sqloutbox[%s]: deleted %d delivered rows", self.namespace, len(safe)
            )

    def prune_sync_log(self) -> None:
        """Remove outbox_sync_log entries older than retain_log_days.

        Call periodically (e.g. every N consumer cycles) to keep the DB small.
        Opens its own connection — safe to call from any thread.
        """
        with thread_conn(self.db_path) as conn:
            cur = conn.execute(
                "DELETE FROM outbox_sync_log "
                "WHERE namespace = ? AND synced_at < datetime('now', ?)",
                [self.namespace, f"-{self.retain_log_days} days"],
            )
            if cur.rowcount:
                logger.debug(
                    "sqloutbox[%s]: pruned %d sync_log entries",
                    self.namespace, cur.rowcount,
                )

    def pending_count(self) -> int:
        """Count undelivered rows in this namespace. Useful for monitoring."""
        with thread_conn(self.db_path) as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM outbox_queue "
                "WHERE namespace = ? AND synced = 0",
                [self.namespace],
            ).fetchone()
        return row[0] if row else 0

    def health(self, max_pending: int | None = None) -> NamespaceHealth:
        """Return a read-only health snapshot for this namespace.

        PURE READ — a single SELECT against the WAL SQLite file. Never mutates
        a row, never calls back into the consuming app, never imports an app
        module. Safe to call from a different process than the drain (WAL: a
        read never blocks the writer). This is the consumer's *eyes*; the
        consumer pulls it on its own schedule and owns every threshold
        (control-direction invariant — durable-retry spec §3.4 / §4).

        Parameters
        ----------
        max_pending:
            Optional cap used only to derive ``capacity_pct = depth /
            max_pending``. None (default) → ``capacity_pct`` is None. The
            library does NOT own a default; callers that configured
            ``OutboxConfig.max_pending`` pass it in. The 80% stop watermark is
            a PRODUCING-APP threshold, not library config (hardening §4.2).

        Returns
        -------
        NamespaceHealth
        """
        with thread_conn(self.db_path) as conn:
            depth_row = conn.execute(
                "SELECT COUNT(*) FROM outbox_queue "
                "WHERE namespace = ? AND synced = 0",
                [self.namespace],
            ).fetchone()
            depth = depth_row[0] if depth_row else 0

            # The retry columns (attempts/last_attempt_at/last_error/
            # last_error_class) are added by Plan 2 (WS-2). If they are not yet
            # present (e.g. running on a pre-WS-2 file), degrade gracefully to a
            # not-stuck reading instead of raising — health() must never crash.
            cols = {r[1] for r in conn.execute("PRAGMA table_info(outbox_queue)")}
            head_attempts = 0
            last_error: str | None = None
            last_error_class: str | None = None
            last_attempt_at: str | None = None
            if {"attempts", "last_attempt_at", "last_error",
                "last_error_class"} <= cols:
                head = conn.execute(
                    "SELECT attempts, last_attempt_at, last_error, "
                    "last_error_class FROM outbox_queue "
                    "WHERE namespace = ? AND synced = 0 "
                    "ORDER BY seq LIMIT 1",
                    [self.namespace],
                ).fetchone()
                if head is not None:
                    head_attempts    = head[0] or 0
                    last_attempt_at  = head[1]
                    last_error       = head[2]
                    last_error_class = head[3]

        capacity_pct: float | None = None
        if max_pending is not None and max_pending > 0:
            capacity_pct = depth / max_pending

        return NamespaceHealth(
            namespace=self.namespace,
            depth=depth,
            head_attempts=head_attempts,
            is_stuck=head_attempts > 0,
            last_error=last_error,
            last_error_class=last_error_class,
            last_attempt_at=last_attempt_at,
            capacity_pct=capacity_pct,
        )

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

    def record_attempt(self, seq: int, error: str, error_class: str) -> int:
        """Record a failed delivery attempt on one row; return its new count.

        Increments ``attempts`` and stores the destination error + its class
        and the attempt timestamp (ISO-8601 UTC). Persisted so the §3.2 backoff
        gate and the §3.4 health signal can read it back, possibly cross-process.

        Returns the new ``attempts`` value for THIS ``seq`` (read back in the same
        transaction), or 0 if ``seq`` was absent (no row updated). The drain must
        use this return value — not ``peek_head()`` — for the failing row's count,
        because in a mixed batch ``peek_head()`` may point at a different,
        still-unsynced earlier row. Opens its own connection — safe from any thread.
        """
        with thread_conn(self.db_path) as conn:
            conn.execute(
                "UPDATE outbox_queue "
                "SET attempts = attempts + 1, last_attempt_at = ?, "
                "    last_error = ?, last_error_class = ? "
                "WHERE namespace = ? AND seq = ?",
                [now_iso(), error, error_class, self.namespace, seq],
            )
            row = conn.execute(
                "SELECT attempts FROM outbox_queue "
                "WHERE namespace = ? AND seq = ?",
                [self.namespace, seq],
            ).fetchone()
            conn.commit()
        return row[0] if row else 0

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

    # ── Seeding ──────────────────────────────────────────────────────────────

    def seed_sequence(self, min_seq: int) -> bool:
        """Ensure the AUTOINCREMENT counter is at least ``min_seq``.

        On a fresh machine the local SQLite file starts sequences from 1,
        which collides with ``outbox_seq`` values already delivered to the
        remote DB.  ``INSERT OR IGNORE`` would silently drop new events.

        Call this at startup with ``MAX(outbox_seq)`` from the remote DB
        so the local counter begins above the highest value already delivered.

        Returns True if the counter was advanced, False if it was already
        high enough (no-op).
        """
        row = self._write_conn.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = 'outbox_queue'"
        ).fetchone()
        current = row[0] if row and row[0] is not None else 0

        if current >= min_seq:
            return False

        if row is None:
            # Fresh DB — no rows ever inserted, sqlite_sequence has no entry.
            self._write_conn.execute(
                "INSERT INTO sqlite_sequence (name, seq) VALUES ('outbox_queue', ?)",
                [min_seq],
            )
        else:
            self._write_conn.execute(
                "UPDATE sqlite_sequence SET seq = ? WHERE name = 'outbox_queue'",
                [min_seq],
            )
        self._write_conn.commit()
        # Persist the remote max as the durable floor so a producer restart on
        # this host re-seeds from it (mechanism (a)) even before the drain runs.
        self.record_hwm(min_seq)
        logger.info(
            "sqloutbox[%s]: seeded sequence from %d → %d (remote max)",
            self.namespace, current, min_seq,
        )
        return True

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

    # ── Verification ───────────────────────────────────────────────────────

    def verify_full(self) -> TableVerifyResult:
        """Run a comprehensive integrity check on this outbox.

        Checks chain integrity, sequence continuity, timestamp monotonicity,
        orphan sync_log entries, and row counts. All read-only — no writes.

        Returns a :class:`TableVerifyResult` with ``ok=True`` if all checks pass.
        """
        from sqloutbox._verify import verify_outbox
        return verify_outbox(self)

    # ── Internal ─────────────────────────────────────────────────────────────

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


def health_all(
    db_dir: Path, max_pending: int | None = None
) -> list[NamespaceHealth]:
    """Enumerate health for every namespace under ``db_dir`` (free function).

    NOT a method — it takes a directory, not ``self``. Globs ``{db_dir}/*.db``
    and, for each file, emits one :class:`NamespaceHealth` per distinct
    namespace it contains (``SELECT DISTINCT namespace``). The common layout is
    one file per table (namespace == table), giving one snapshot per file; the
    multiple-namespaces-per-file case (allowed by ``shared_outbox``) is handled.

    PURE READ — same control-direction invariant as :meth:`Outbox.health`.
    Results are sorted by namespace for stable output.

    Parameters
    ----------
    db_dir:
        Directory containing the per-table ``*.db`` outbox files.
    max_pending:
        Forwarded to each ``health()`` for ``capacity_pct`` (None → None).

    Returns
    -------
    list[NamespaceHealth]
        Empty list if ``db_dir`` does not exist or contains no ``*.db`` files.
    """
    if not db_dir.is_dir():
        return []
    out: list[NamespaceHealth] = []
    for db_file in sorted(db_dir.glob("*.db")):
        # Read-only enumerate of namespaces in this file.
        with thread_conn(db_file) as conn:
            names = [
                r[0] for r in conn.execute(
                    "SELECT DISTINCT namespace FROM outbox_queue "
                    "ORDER BY namespace"
                ).fetchall()
            ]
        if not names:
            # File exists but has no rows yet — report the file's stem as an
            # empty namespace so depth=0 is still observable.
            names = [db_file.stem]
        for namespace in names:
            ob = Outbox(db_path=db_file, namespace=namespace)
            out.append(ob.health(max_pending=max_pending))
    out.sort(key=lambda h: h.namespace)
    return out
