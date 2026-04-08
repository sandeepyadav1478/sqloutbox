"""SQLMiddleware — base class for writing SQL statements to the outbox.

Provides the shared hot-path pattern used by all SQL-based outbox producers:
    tag     = SQL string  (stored once in outbox_queue.tag)
    payload = JSON args   (bind values as a plain JSON array)

Works as both a standalone class and a cooperative mixin. Subclass must set
``self._config`` (an OutboxConfig) before calling ``_push()`` or ``_push_many()``.

Example
-------
    from sqloutbox import SQLMiddleware, OutboxConfig

    class MyMiddleware(SQLMiddleware):
        def __init__(self, db_dir: Path, env: str = "prd"):
            self._config = OutboxConfig(db_dir=db_dir)
            self._env = env

        def push_event(self, event):
            self._push("my_table", "INSERT INTO my_table (col) VALUES (?)", [event.value])
"""

from __future__ import annotations

import json
import logging
from typing import Any

from sqloutbox._outbox import Outbox
from sqloutbox._registry import shared_outbox
from sqloutbox.config import OutboxConfig

logger = logging.getLogger(__name__)


class SQLMiddleware:
    """Base for components that write SQL statements to the outbox (hot path).

    Works as both a standalone class and a mixin for multiple inheritance.
    Subclass must ensure ``self._config`` is set (an OutboxConfig) before
    calling ``_push()`` or ``_push_many()``.

    Hot path: ~150us per event (local SQLite INSERT, no network, no await).
    """

    _config: OutboxConfig

    @property
    def _source(self) -> str:
        """Identity label stamped on every outbox row.

        Default: the concrete class name (e.g. "SchedulerMiddleware").
        Override to customise (e.g. return a fixed string for all instances).
        """
        return type(self).__name__

    def _outbox(self, table: str) -> Outbox:
        """Return the shared Outbox for this table.

        shared_outbox() guarantees ONE write connection per (file, namespace)
        pair — no concurrent-writer contention when multiple components write
        to the same table.
        """
        self._config.db_dir.mkdir(parents=True, exist_ok=True)
        return shared_outbox(
            db_path=self._config.db_dir / f"{table}.db",
            namespace=table,
            batch_size=self._config.batch_size,
        )

    def _push(self, table: str, sql: str, args: list[Any]) -> None:
        """Enqueue one SQL statement to local SQLite. Non-blocking, ~150us.

        tag     = sql               (the INSERT/UPDATE statement)
        payload = json.dumps(args)  (just the bind values — plain JSON array)
        """
        payload = json.dumps(args).encode()
        outbox = self._outbox(table)
        outbox.enqueue(sql, payload, source=self._source)

    def _push_many(self, table: str, stmts: list[tuple[str, list[Any]]]) -> None:
        """Enqueue multiple SQL statements in one SQLite transaction."""
        items = [
            (sql, json.dumps(args).encode())
            for sql, args in stmts
        ]
        if not items:
            return
        outbox = self._outbox(table)
        outbox.enqueue_batch(items, source=self._source)
