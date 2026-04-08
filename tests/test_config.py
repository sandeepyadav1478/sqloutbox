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
    assert cfg.flush_interval == 15.0
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
