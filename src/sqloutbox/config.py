"""Outbox deployment configuration — centralized settings for any project.

Apps create OutboxConfig with their specific tables, targets, and tuning params,
then pass it to OutboxSyncService (drain daemon) or SQLMiddleware (hot path).

Example
-------
    from sqloutbox import OutboxConfig, TargetConfig

    config = OutboxConfig(
        db_dir=Path("/var/data/outbox"),
        targets=(
            TargetConfig(name="analytics", tables=("events", "metrics"),
                         inject_outbox_seq=True),
            TargetConfig(name="audit", tables=("audit_log",),
                         inject_outbox_seq=False),
        ),
    )

    # Per-table control (frozenset of table names that get outbox_seq):
    config2 = OutboxConfig(
        db_dir=Path("/var/data/outbox"),
        targets=(
            TargetConfig(
                name="mixed",
                tables=("events", "metrics", "raw_log"),
                inject_outbox_seq=frozenset({"events", "metrics"}),
            ),
        ),
    )

    # Generate DDL for remote tables that need outbox_seq:
    for stmt in config.schema_sql():
        print(stmt)
    # → ALTER TABLE events ADD COLUMN outbox_seq INTEGER NOT NULL DEFAULT 0
    # → CREATE UNIQUE INDEX IF NOT EXISTS idx_events_outbox_seq ON events (outbox_seq) WHERE outbox_seq != 0
    # → ALTER TABLE metrics ADD COLUMN outbox_seq INTEGER NOT NULL DEFAULT 0
    # → CREATE UNIQUE INDEX IF NOT EXISTS idx_metrics_outbox_seq ON metrics (outbox_seq) WHERE outbox_seq != 0
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
        Controls which tables get ``outbox_seq`` column injection.

        * ``True`` (default) — all tables in this target get injection
        * ``False`` — no tables get injection
        * ``frozenset({"table1", "table2"})`` — only named tables get injection

        When injected, INSERTs become ``INSERT OR IGNORE INTO ... (cols, outbox_seq)``
        providing idempotent delivery. UPDATEs get ``outbox_seq = ?`` appended to SET.

        The remote table must have ``outbox_seq INTEGER NOT NULL DEFAULT 0``
        with a partial unique index ``WHERE outbox_seq != 0``. Use
        ``OutboxConfig.schema_sql()`` to generate the DDL.

    db_dir:
        Per-target override for the outbox file directory. When set (by the
        TOML config loader), the sync service uses this instead of
        ``OutboxConfig.db_dir``. When None (default), falls back to the
        global ``OutboxConfig.db_dir``.

    batch_size:
        Per-target override for max rows fetched per table per sync cycle.
        Default: 500.

    retain_log_days:
        Days to keep ``outbox_sync_log`` entries for this target's tables.
        The sync_log is the local audit trail proving a row was delivered.
        After this period, entries are pruned to keep the SQLite files small.

        Configurable per-target in TOML::

            [app.myapp.db.primary]
            retain_log_days = 14

        Or per-table via inline tables::

            tables = [
                "events",
                { name = "raw_log", retain_log_days = 7 },
            ]

        Default: 30 days.
    """
    name: str
    tables: tuple[str, ...]
    inject_outbox_seq: bool | frozenset[str] = True
    db_dir: Path | None = None
    batch_size: int = 500
    retain_log_days: int = 30
    # Per-table retention overrides — tuple of (table_name, days) pairs.
    # Tables not listed here use retain_log_days as the default.
    # Built by the TOML parser from inline table entries:
    #   tables = [{ name = "raw_log", retain_log_days = 7 }]
    table_retain_overrides: tuple[tuple[str, int], ...] = ()

    def should_inject_seq(self, table: str) -> bool:
        """Check whether a specific table should get outbox_seq injection."""
        if isinstance(self.inject_outbox_seq, bool):
            return self.inject_outbox_seq
        return table in self.inject_outbox_seq

    def get_retain_days(self, table: str) -> int:
        """Get retention days for a specific table's sync_log.

        Returns the per-table override if configured, else the target default.
        """
        for name, days in self.table_retain_overrides:
            if name == table:
                return days
        return self.retain_log_days


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
        Default max rows fetched per table per sync cycle. Targets can
        override this via ``TargetConfig.batch_size``. Default: 500.

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
        Global default for days to keep ``outbox_sync_log`` entries.
        Targets can override via ``TargetConfig.retain_log_days``.
        Default: 30 days.
    """
    db_dir: Path
    targets: tuple[TargetConfig, ...] = ()
    batch_size: int = 500
    flush_interval: float = 1.0
    table_flush_threshold: int = 15
    table_max_wait: float = 6.0
    auto_schema: bool = True
    cleanup_every: int = 500
    retain_log_days: int = 30

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
        """Return DDL to add ``outbox_seq`` to remote tables.

        Only includes tables where ``should_inject_seq()`` is True.

        Two statements per table:
            1. ``ALTER TABLE … ADD COLUMN outbox_seq INTEGER NOT NULL DEFAULT 0``
            2. ``CREATE UNIQUE INDEX … WHERE outbox_seq != 0`` (partial)

        The partial unique index allows unlimited rows with ``outbox_seq=0``
        (direct inserts that bypass the outbox) while enforcing uniqueness on
        delivered rows.  ``DEFAULT 0`` matches Turso/libsql convention.

        Example::

            for stmt in config.schema_sql():
                await db.execute(stmt)
        """
        stmts: list[str] = []
        for target in self.targets:
            for table in target.tables:
                if target.should_inject_seq(table):
                    stmts.append(
                        f"ALTER TABLE {table} "
                        f"ADD COLUMN outbox_seq INTEGER NOT NULL DEFAULT 0"
                    )
                    stmts.append(
                        f"CREATE UNIQUE INDEX IF NOT EXISTS "
                        f"idx_{table}_outbox_seq "
                        f"ON {table} (outbox_seq) WHERE outbox_seq != 0"
                    )
        return stmts

    def drop_schema_sql(self) -> list[str]:
        """Return DDL to remove ``outbox_seq`` from remote tables.

        Only includes tables where ``should_inject_seq()`` is False.

        Two statements per table:
            1. ``DROP INDEX IF EXISTS idx_{table}_outbox_seq``
            2. ``ALTER TABLE … DROP COLUMN outbox_seq``

        Example::

            for stmt in config.drop_schema_sql():
                await db.execute(stmt)
        """
        stmts: list[str] = []
        for target in self.targets:
            for table in target.tables:
                if not target.should_inject_seq(table):
                    stmts.append(
                        f"DROP INDEX IF EXISTS idx_{table}_outbox_seq"
                    )
                    stmts.append(
                        f"ALTER TABLE {table} "
                        f"DROP COLUMN outbox_seq"
                    )
        return stmts
