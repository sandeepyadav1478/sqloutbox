"""Outbox deployment configuration — centralized settings for any project.

Apps create OutboxConfig with their specific tables, targets, and tuning params,
then pass it to OutboxSyncService (drain daemon) or SQLMiddleware (hot path).

Example
-------
    from sqloutbox import OutboxConfig, TargetConfig

    config = OutboxConfig(
        db_dir=Path("/var/data/outbox"),
        targets=(
            TargetConfig(name="analytics", tables=("events", "metrics")),
            TargetConfig(name="audit", tables=("audit_log",), inject_outbox_seq=False),
        ),
    )

    # Generate ALTER TABLE DDL for remote tables that need outbox_seq:
    for stmt in config.schema_sql():
        print(stmt)
    # → ALTER TABLE events ADD COLUMN outbox_seq INTEGER UNIQUE
    # → ALTER TABLE metrics ADD COLUMN outbox_seq INTEGER UNIQUE
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TargetConfig:
    """One remote database target for outbox delivery.

    Parameters
    ----------
    name:
        Human-readable label for the target (e.g. "pulseview", "autopulse").
        Must match the key in the ``writers`` dict passed to OutboxSyncService.

    tables:
        Tuple of outbox namespace strings routed to this target.
        Each namespace corresponds to one SQLite file: ``{db_dir}/{table}.db``.

    inject_outbox_seq:
        When True (default), the sync service appends an ``outbox_seq`` column
        to every INSERT and UPDATE statement before sending it to the remote DB.

        For INSERTs: rewrites to ``INSERT OR IGNORE INTO ... (cols, outbox_seq)``
        providing idempotent delivery — duplicate re-attempts silently succeed.

        For UPDATEs: appends ``outbox_seq = ?`` to the SET clause so the remote
        row records which outbox sequence wrote it.

        The remote table must have ``outbox_seq INTEGER UNIQUE`` (NULLs allowed
        so direct queries that bypass the outbox still work). Use
        ``OutboxConfig.schema_sql()`` to generate the ALTER TABLE DDL.

        Set to False when the remote schema has no ``outbox_seq`` column.
    """
    name: str
    tables: tuple[str, ...]
    inject_outbox_seq: bool = True


@dataclass(frozen=True)
class OutboxConfig:
    """Complete outbox deployment configuration.

    Immutable (frozen) so it can be shared safely across threads and coroutines.

    Parameters
    ----------
    db_dir:
        Directory where per-table SQLite outbox files live.
        Created automatically if it does not exist.

    targets:
        Tuple of TargetConfig entries. Each defines a remote DB and the tables
        routed to it. An empty tuple is valid for middleware-only use (no sync).

    batch_size:
        Maximum rows fetched per table per sync cycle. Default: 500.

    flush_interval:
        Seconds between round-robin scan passes. Default: 1.0.

    table_flush_threshold:
        Minimum pending rows to trigger an immediate flush for a table,
        regardless of how recently it was last flushed. Default: 15.

    table_max_wait:
        Maximum seconds a table can have *any* pending rows before it is
        included in the next flush — even if the row count is below
        ``table_flush_threshold``. Default: 6.0.

    auto_schema:
        When True (default), the sync service automatically manages the
        ``outbox_seq`` column on remote tables at startup:

        * ``inject_outbox_seq=True``  → ADD COLUMN (if not exists)
        * ``inject_outbox_seq=False`` → DROP COLUMN (cleanup, if exists)

        Set to False if you manage schema via migration tools (Alembic,
        Django migrations, etc.) and want full control. Use ``schema_sql()``
        and ``drop_schema_sql()`` to generate the DDL yourself.

    cleanup_every:
        Run ``prune_sync_log()`` every N sync cycles. Default: 500.

    retain_log_days:
        Days to keep entries in ``outbox_sync_log``. Default: 7.
    """
    db_dir: Path
    targets: tuple[TargetConfig, ...] = ()
    batch_size: int = 500
    flush_interval: float = 1.0
    table_flush_threshold: int = 15
    table_max_wait: float = 6.0
    auto_schema: bool = True
    cleanup_every: int = 500
    retain_log_days: int = 7

    def tables_for_target(self, name: str) -> tuple[str, ...]:
        """Return table names routed to the named target."""
        for t in self.targets:
            if t.name == name:
                return t.tables
        return ()

    def target_for_table(self, table: str) -> TargetConfig | None:
        """Return the TargetConfig that owns this table, or None."""
        for t in self.targets:
            if table in t.tables:
                return t
        return None

    def all_tables(self) -> tuple[str, ...]:
        """All tables across all targets, in target order."""
        result: list[str] = []
        for t in self.targets:
            result.extend(t.tables)
        return tuple(result)

    def schema_sql(self) -> list[str]:
        """Return ALTER TABLE DDL to add ``outbox_seq`` to remote tables.

        Only includes tables where ``inject_outbox_seq=True``.  Run these
        statements on your remote database before starting the sync service.

        The column is ``INTEGER UNIQUE`` — unique for dedup / gap detection,
        but NULLable so direct queries that bypass the outbox still work.

        Example::

            for stmt in config.schema_sql():
                await db.execute(stmt)
        """
        stmts: list[str] = []
        for target in self.targets:
            if target.inject_outbox_seq:
                for table in target.tables:
                    stmts.append(
                        f"ALTER TABLE {table} "
                        f"ADD COLUMN outbox_seq INTEGER UNIQUE"
                    )
        return stmts

    def drop_schema_sql(self) -> list[str]:
        """Return ALTER TABLE DDL to remove ``outbox_seq`` from remote tables.

        Only includes tables where ``inject_outbox_seq=False``.  Useful for
        cleanup when disabling the outbox_seq verification feature.

        Example::

            for stmt in config.drop_schema_sql():
                await db.execute(stmt)
        """
        stmts: list[str] = []
        for target in self.targets:
            if not target.inject_outbox_seq:
                for table in target.tables:
                    stmts.append(
                        f"ALTER TABLE {table} "
                        f"DROP COLUMN outbox_seq"
                    )
        return stmts
