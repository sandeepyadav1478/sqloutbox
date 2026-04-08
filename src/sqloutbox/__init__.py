"""sqloutbox — durable, singly-linked SQLite event outbox.

A local SQLite-backed transactional outbox queue for Python applications.
Zero external dependencies (stdlib only). Designed for single-process,
single-machine deployments with asyncio.

Pattern
-------
The producer writes events synchronously to a local SQLite file (~150µs,
no network). A background consumer reads events in strict insertion order,
delivers them to a destination, marks them as delivered, and deletes them.
Local storage stays minimal — only un-delivered events are kept.

    from pathlib import Path
    from sqloutbox import Outbox

    outbox = Outbox(db_path=Path("/var/data/events.db"), namespace="my-service")

    # Producer (synchronous, hot path):
    outbox.enqueue("order.paid", b'{"order_id": 42}')

    # Consumer (background, via asyncio.to_thread):
    rows = await asyncio.to_thread(outbox.fetch_unsynced)
    ok, gaps = await asyncio.to_thread(outbox.verify_chain, rows)
    if ok:
        for row in rows:
            await deliver(row.tag, row.payload)
        await asyncio.to_thread(outbox.mark_synced, [r.seq for r in rows])
        await asyncio.to_thread(outbox.delete_synced, [r.seq for r in rows])

Singly-linked chain (prev_seq only)
------------------------------------
Each row stores only its predecessor (prev_seq). Successors are found on
demand via WHERE prev_seq = X (covered by idx_outbox_prev index). Storing
next_seq was considered and rejected — it would require an UPDATE on every
INSERT, doubling write operations without adding correctness.

The consumer verifies the prev_seq chain before every delivery — a gap
blocks delivery and logs an error. After delivery and deletion, rows are
recorded in outbox_sync_log so future gap checks can confirm the row was
delivered (not lost).

Recovery SQL
------------
-- View pending rows:
SELECT seq, namespace, tag, created_at, prev_seq
FROM outbox_queue WHERE synced = 0 ORDER BY namespace, seq;

-- Find chain gaps:
SELECT q.seq, q.namespace, q.prev_seq FROM outbox_queue q
WHERE q.prev_seq IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM outbox_queue    WHERE seq = q.prev_seq)
  AND NOT EXISTS (SELECT 1 FROM outbox_sync_log WHERE seq = q.prev_seq);

-- Force-skip a lost row to unblock the chain (accept the data loss):
INSERT OR IGNORE INTO outbox_sync_log (seq, namespace, synced_at)
VALUES (<lost_seq>, '<namespace>', datetime('now'));

CLI
---
    sqloutbox init [DIR]      Scaffold a config directory (default: ./outbox)

    The scaffolded directory contains outbox_config.py (config + writer factory),
    run_service.py (entry point with signal handling), and logging.json.
    Start the service with: python outbox/run_service.py

Modules
-------
    sqloutbox.config      OutboxConfig, TargetConfig
    sqloutbox.middleware   SQLMiddleware base class
    sqloutbox.sync        OutboxSyncService, OutboxWriter, inject_outbox_seq
    sqloutbox.cli         CLI entry point (init, start, status)
    sqloutbox._outbox     Outbox class (low-level queue)
    sqloutbox._worker     OutboxWorker abstract base
    sqloutbox._models     QueueRow dataclass
    sqloutbox._schema     SQL definitions, connection helpers, WAL setup
    sqloutbox._registry   shared_outbox() singleton registry
"""

from sqloutbox._models import QueueRow
from sqloutbox._outbox import Outbox
from sqloutbox._registry import shared_outbox, clear_registry
from sqloutbox._worker import OutboxWorker
from sqloutbox.config import OutboxConfig, TargetConfig
from sqloutbox.middleware import SQLMiddleware
from sqloutbox.sync import OutboxSyncService, OutboxWriter, inject_outbox_seq

__all__ = [
    # Core queue
    "Outbox",
    "OutboxWorker",
    "QueueRow",
    "shared_outbox",
    "clear_registry",
    # Configuration
    "OutboxConfig",
    "TargetConfig",
    # SQL middleware
    "SQLMiddleware",
    # Sync service
    "OutboxSyncService",
    "OutboxWriter",
    "inject_outbox_seq",
]
