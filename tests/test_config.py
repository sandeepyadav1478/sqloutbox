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
    assert cfg.retain_log_days == 30


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


def test_schema_sql_generates_alter_and_index():
    """schema_sql returns ALTER + partial unique index per inject_outbox_seq=True table."""
    cfg = OutboxConfig(
        db_dir=Path("/tmp"),
        targets=(
            TargetConfig(name="a", tables=("events", "metrics")),
            TargetConfig(name="b", tables=("audit_log",), inject_outbox_seq=False),
        ),
    )
    stmts = cfg.schema_sql()
    # 2 tables × 2 stmts each = 4
    assert len(stmts) == 4
    assert stmts[0] == "ALTER TABLE events ADD COLUMN outbox_seq INTEGER NOT NULL DEFAULT 0"
    assert stmts[1] == (
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_events_outbox_seq "
        "ON events (outbox_seq) WHERE outbox_seq != 0"
    )
    assert stmts[2] == "ALTER TABLE metrics ADD COLUMN outbox_seq INTEGER NOT NULL DEFAULT 0"
    assert stmts[3] == (
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_metrics_outbox_seq "
        "ON metrics (outbox_seq) WHERE outbox_seq != 0"
    )


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


def test_drop_schema_sql_generates_drop_index_and_alter():
    """drop_schema_sql returns DROP INDEX + ALTER TABLE per inject_outbox_seq=False table."""
    cfg = OutboxConfig(
        db_dir=Path("/tmp"),
        targets=(
            TargetConfig(name="a", tables=("events", "metrics")),
            TargetConfig(name="b", tables=("audit_log",), inject_outbox_seq=False),
        ),
    )
    stmts = cfg.drop_schema_sql()
    # 1 table × 2 stmts = 2
    assert len(stmts) == 2
    assert stmts[0] == "DROP INDEX IF EXISTS idx_audit_log_outbox_seq"
    assert stmts[1] == "ALTER TABLE audit_log DROP COLUMN outbox_seq"


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


# ── Per-table inject_outbox_seq ─────────────────────────────────────────────


def test_should_inject_seq_bool_true():
    """inject_outbox_seq=True → all tables inject."""
    t = TargetConfig(name="a", tables=("x", "y"), inject_outbox_seq=True)
    assert t.should_inject_seq("x") is True
    assert t.should_inject_seq("y") is True


def test_should_inject_seq_bool_false():
    """inject_outbox_seq=False → no tables inject."""
    t = TargetConfig(name="a", tables=("x", "y"), inject_outbox_seq=False)
    assert t.should_inject_seq("x") is False
    assert t.should_inject_seq("y") is False


def test_should_inject_seq_frozenset():
    """inject_outbox_seq=frozenset → only named tables inject."""
    t = TargetConfig(
        name="a",
        tables=("events", "metrics", "raw_log"),
        inject_outbox_seq=frozenset({"events", "metrics"}),
    )
    assert t.should_inject_seq("events") is True
    assert t.should_inject_seq("metrics") is True
    assert t.should_inject_seq("raw_log") is False


def test_schema_sql_per_table_frozenset():
    """schema_sql respects per-table frozenset inject_outbox_seq."""
    cfg = OutboxConfig(
        db_dir=Path("/tmp"),
        targets=(
            TargetConfig(
                name="a",
                tables=("events", "raw_log"),
                inject_outbox_seq=frozenset({"events"}),
            ),
        ),
    )
    stmts = cfg.schema_sql()
    # 1 table × 2 stmts (ALTER + CREATE INDEX) = 2
    assert len(stmts) == 2
    assert "events" in stmts[0]
    assert "events" in stmts[1]


def test_drop_schema_sql_per_table_frozenset():
    """drop_schema_sql respects per-table frozenset inject_outbox_seq."""
    cfg = OutboxConfig(
        db_dir=Path("/tmp"),
        targets=(
            TargetConfig(
                name="a",
                tables=("events", "raw_log"),
                inject_outbox_seq=frozenset({"events"}),
            ),
        ),
    )
    stmts = cfg.drop_schema_sql()
    # 1 table × 2 stmts (DROP INDEX + ALTER) = 2
    assert len(stmts) == 2
    assert "raw_log" in stmts[0]
    assert "raw_log" in stmts[1]


def test_target_config_batch_size_default():
    """batch_size defaults to 500."""
    t = TargetConfig(name="a", tables=("t",))
    assert t.batch_size == 500


def test_target_config_batch_size_custom():
    """batch_size can be set per target."""
    t = TargetConfig(name="a", tables=("t",), batch_size=100)
    assert t.batch_size == 100


def test_target_config_db_dir():
    """db_dir can be set per target."""
    t = TargetConfig(name="a", tables=("t",), db_dir=Path("/custom"))
    assert t.db_dir == Path("/custom")


# ── retain_log_days ────────────────────────────────────────────────────────


def test_target_config_retain_default():
    """retain_log_days defaults to 30."""
    t = TargetConfig(name="a", tables=("t",))
    assert t.retain_log_days == 30


def test_target_config_retain_custom():
    """retain_log_days can be set per target."""
    t = TargetConfig(name="a", tables=("t",), retain_log_days=14)
    assert t.retain_log_days == 14


def test_get_retain_days_default():
    """get_retain_days returns target default when no per-table override."""
    t = TargetConfig(name="a", tables=("events", "metrics"), retain_log_days=14)
    assert t.get_retain_days("events") == 14
    assert t.get_retain_days("metrics") == 14


def test_get_retain_days_per_table_override():
    """get_retain_days returns per-table override when configured."""
    t = TargetConfig(
        name="a",
        tables=("events", "api_calls", "raw_log"),
        retain_log_days=30,
        table_retain_overrides=(("api_calls", 7), ("raw_log", 14)),
    )
    assert t.get_retain_days("events") == 30      # target default
    assert t.get_retain_days("api_calls") == 7     # override
    assert t.get_retain_days("raw_log") == 14      # override


def test_outbox_config_retain_default():
    """OutboxConfig retain_log_days defaults to 30."""
    cfg = OutboxConfig(db_dir=Path("/tmp"))
    assert cfg.retain_log_days == 30
