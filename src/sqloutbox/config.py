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
        batch_size=500,
        flush_interval=15.0,
    )
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
        to every INSERT statement before sending it to the remote DB. This
        provides deduplication on the remote side (INSERT OR IGNORE).
        Set to False for UPDATE statements or when the remote schema has no
        ``outbox_seq`` column.
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
        Seconds between sync cycles. Default: 15.0.

    cleanup_every:
        Run ``prune_sync_log()`` every N sync cycles. Default: 500.

    retain_log_days:
        Days to keep entries in ``outbox_sync_log``. Default: 7.
    """
    db_dir: Path
    targets: tuple[TargetConfig, ...] = ()
    batch_size: int = 500
    flush_interval: float = 15.0
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
