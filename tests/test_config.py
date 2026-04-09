"""Tests for OutboxConfig and TargetConfig."""

from __future__ import annotations

from pathlib import Path

import pytest

from sqloutbox import OutboxConfig, TargetConfig


def test_target_config_frozen():
    """TargetConfig is immutable."""
    t = TargetConfig(name="db1", tables=("a", "b"))
    with pytest.raises(AttributeError):
        t.name = "changed"  # type: ignore[misc]


def test_target_config_defaults():
    """inject_outbox_seq defaults to True."""
    t = TargetConfig(name="db1", tables=("a",))
    assert t.inject_outbox_seq is True


def test_outbox_config_frozen():
    """OutboxConfig is immutable."""
    cfg = OutboxConfig(db_dir=Path("/tmp"))
    with pytest.raises(AttributeError):
        cfg.batch_size = 999  # type: ignore[misc]


def test_outbox_config_defaults():
    """OutboxConfig has sensible defaults."""
    cfg = OutboxConfig(db_dir=Path("/tmp"))
    assert cfg.targets == ()
    assert cfg.batch_size == 500
    assert cfg.flush_interval == 1.0
    assert cfg.table_flush_threshold == 15
    assert cfg.table_max_wait == 6.0
    assert cfg.cleanup_every == 500
    assert cfg.retain_log_days == 7


def test_tables_for_target():
    """tables_for_target returns the correct tables."""
    cfg = OutboxConfig(
        db_dir=Path("/tmp"),
        targets=(
            TargetConfig(name="analytics", tables=("events", "metrics")),
            TargetConfig(name="billing", tables=("invoices",)),
        ),
    )
    assert cfg.tables_for_target("analytics") == ("events", "metrics")
    assert cfg.tables_for_target("billing") == ("invoices",)
    assert cfg.tables_for_target("nonexistent") == ()


def test_target_for_table():
    """target_for_table finds the owning target."""
    t1 = TargetConfig(name="analytics", tables=("events", "metrics"))
    t2 = TargetConfig(name="billing", tables=("invoices",))
    cfg = OutboxConfig(db_dir=Path("/tmp"), targets=(t1, t2))

    assert cfg.target_for_table("events") is t1
    assert cfg.target_for_table("invoices") is t2
    assert cfg.target_for_table("nonexistent") is None


def test_all_tables():
    """all_tables returns tables across all targets in order."""
    cfg = OutboxConfig(
        db_dir=Path("/tmp"),
        targets=(
            TargetConfig(name="a", tables=("t1", "t2")),
            TargetConfig(name="b", tables=("t3",)),
        ),
    )
    assert cfg.all_tables() == ("t1", "t2", "t3")


def test_all_tables_empty_targets():
    """all_tables with no targets returns empty tuple."""
    cfg = OutboxConfig(db_dir=Path("/tmp"))
    assert cfg.all_tables() == ()


# ── schema_sql ───────────────────────────────────────────────────────────────


def test_schema_sql_generates_alter_table():
    """schema_sql returns ALTER TABLE DDL for inject_outbox_seq=True targets."""
    cfg = OutboxConfig(
        db_dir=Path("/tmp"),
        targets=(
            TargetConfig(name="a", tables=("events", "metrics")),
            TargetConfig(name="b", tables=("audit_log",), inject_outbox_seq=False),
        ),
    )
    stmts = cfg.schema_sql()
    assert len(stmts) == 2
    assert stmts[0] == "ALTER TABLE events ADD COLUMN outbox_seq INTEGER UNIQUE"
    assert stmts[1] == "ALTER TABLE metrics ADD COLUMN outbox_seq INTEGER UNIQUE"


def test_schema_sql_excludes_disabled_targets():
    """schema_sql skips targets with inject_outbox_seq=False."""
    cfg = OutboxConfig(
        db_dir=Path("/tmp"),
        targets=(
            TargetConfig(name="a", tables=("t1",), inject_outbox_seq=False),
        ),
    )
    assert cfg.schema_sql() == []


def test_schema_sql_empty_targets():
    """schema_sql with no targets returns empty list."""
    cfg = OutboxConfig(db_dir=Path("/tmp"))
    assert cfg.schema_sql() == []


# ── auto_schema flag ────────────────────────────────────────────────────────


def test_auto_schema_defaults_true():
    """auto_schema defaults to True."""
    cfg = OutboxConfig(db_dir=Path("/tmp"))
    assert cfg.auto_schema is True


def test_auto_schema_can_be_disabled():
    """auto_schema=False is accepted."""
    cfg = OutboxConfig(db_dir=Path("/tmp"), auto_schema=False)
    assert cfg.auto_schema is False


# ── drop_schema_sql ─────────────────────────────────────────────────────────


def test_drop_schema_sql_generates_alter_table():
    """drop_schema_sql returns DDL for inject_outbox_seq=False targets."""
    cfg = OutboxConfig(
        db_dir=Path("/tmp"),
        targets=(
            TargetConfig(name="a", tables=("events", "metrics")),
            TargetConfig(name="b", tables=("audit_log",), inject_outbox_seq=False),
        ),
    )
    stmts = cfg.drop_schema_sql()
    assert len(stmts) == 1
    assert stmts[0] == "ALTER TABLE audit_log DROP COLUMN outbox_seq"


def test_drop_schema_sql_excludes_enabled_targets():
    """drop_schema_sql skips targets with inject_outbox_seq=True."""
    cfg = OutboxConfig(
        db_dir=Path("/tmp"),
        targets=(TargetConfig(name="a", tables=("t1",)),),
    )
    assert cfg.drop_schema_sql() == []


def test_drop_schema_sql_empty_targets():
    """drop_schema_sql with no targets returns empty list."""
    cfg = OutboxConfig(db_dir=Path("/tmp"))
    assert cfg.drop_schema_sql() == []
