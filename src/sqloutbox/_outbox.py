"""Outbox class — core implementation of the durable singly-linked event queue."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from sqloutbox._models import QueueRow
from sqloutbox._schema import (
    now_iso,
    open_write_conn,
    placeholders,
    thread_conn,
)

logger = logging.getLogger(__name__)

DEFAULT_RETAIN_LOG_DAYS = 30
DEFAULT_BATCH_SIZE      = 50
DEFAULT_CLEANUP_EVERY   = 500


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
    ) -> None:
        self.db_path         = db_path
        self.namespace       = namespace
        self.retain_log_days = retain_log_days
        self.batch_size      = batch_size
        self.cleanup_every   = cleanup_every
        # Persistent write connection — used exclusively by enqueue() from one thread
        self._write_conn = open_write_conn(db_path)

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
        """
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
                "SELECT seq, tag, payload, prev_seq, source "
                "FROM outbox_queue "
                "WHERE namespace = ? AND synced = 0 "
                "ORDER BY seq LIMIT ?",
                [self.namespace, n],
            ).fetchall()
        return [
            QueueRow(seq=r[0], tag=r[1], payload=r[2].encode(), prev_seq=r[3], source=r[4] or "")
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

        Opens its own connection — safe to call from any thread.
        """
        if not seqs:
            return
        with thread_conn(self.db_path) as conn:
            conn.execute(
                f"UPDATE outbox_queue SET synced = 1 "
                f"WHERE seq IN ({placeholders(len(seqs))})",
                seqs,
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
            # Step 1: Batch-fetch all candidate rows — verify each is synced
            rows_data = conn.execute(
                f"SELECT seq, synced FROM outbox_queue "
                f"WHERE seq IN ({placeholders(len(seqs))})",
                seqs,
            ).fetchall()
            by_seq = {r[0]: bool(r[1]) for r in rows_data}

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
            conn.execute(
                f"DELETE FROM outbox_queue "
                f"WHERE seq IN ({placeholders(len(safe))})",
                safe,
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
        logger.info(
            "sqloutbox[%s]: seeded sequence from %d → %d (remote max)",
            self.namespace, current, min_seq,
        )
        return True

    # ── Internal ─────────────────────────────────────────────────────────────

    def _seq_accounted(self, conn: sqlite3.Connection, seq: int) -> bool:
        """Return True if seq exists in queue OR sync_log. One UNION query."""
        return bool(conn.execute(
            "SELECT 1 FROM outbox_queue WHERE seq = ? "
            "UNION SELECT 1 FROM outbox_sync_log WHERE seq = ? "
            "LIMIT 1",
            [seq, seq],
        ).fetchone())
