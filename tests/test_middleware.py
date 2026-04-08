"""Tests for SQLMiddleware — hot-path enqueue to local SQLite."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sqloutbox import SQLMiddleware, OutboxConfig, Outbox, shared_outbox, clear_registry


class StubMiddleware(SQLMiddleware):
    """Concrete middleware for testing."""

    def __init__(self, db_dir: Path):
        self._config = OutboxConfig(db_dir=db_dir)


class CustomSourceMiddleware(SQLMiddleware):
    """Middleware with custom _source."""

    def __init__(self, db_dir: Path):
        self._config = OutboxConfig(db_dir=db_dir)

    @property
    def _source(self) -> str:
        return "custom-source"


@pytest.fixture(autouse=True)
def _clean_registry():
    """Clear the shared_outbox registry between tests."""
    yield
    clear_registry()


def test_push_enqueues_row(tmp_path):
    """_push() writes one row to the outbox."""
    mw = StubMiddleware(db_dir=tmp_path)
    mw._push("events", "INSERT INTO events (id) VALUES (?)", [42])

    outbox = shared_outbox(db_path=tmp_path / "events.db", namespace="events")
    rows = outbox.fetch_unsynced()
    assert len(rows) == 1
    assert rows[0].tag == "INSERT INTO events (id) VALUES (?)"
    assert json.loads(rows[0].payload.decode()) == [42]


def test_push_source_default(tmp_path):
    """Default _source is the class name."""
    mw = StubMiddleware(db_dir=tmp_path)
    mw._push("events", "INSERT INTO events (id) VALUES (?)", [1])

    outbox = shared_outbox(db_path=tmp_path / "events.db", namespace="events")
    rows = outbox.fetch_unsynced()
    assert rows[0].source == "StubMiddleware"


def test_push_source_custom(tmp_path):
    """Custom _source is used when overridden."""
    mw = CustomSourceMiddleware(db_dir=tmp_path)
    mw._push("events", "INSERT INTO events (id) VALUES (?)", [1])

    outbox = shared_outbox(db_path=tmp_path / "events.db", namespace="events")
    rows = outbox.fetch_unsynced()
    assert rows[0].source == "custom-source"


def test_push_many_enqueues_batch(tmp_path):
    """_push_many() writes multiple rows in one transaction."""
    mw = StubMiddleware(db_dir=tmp_path)
    stmts = [
        ("INSERT INTO events (id) VALUES (?)", [1]),
        ("INSERT INTO events (id) VALUES (?)", [2]),
        ("INSERT INTO events (id) VALUES (?)", [3]),
    ]
    mw._push_many("events", stmts)

    outbox = shared_outbox(db_path=tmp_path / "events.db", namespace="events")
    rows = outbox.fetch_unsynced()
    assert len(rows) == 3


def test_push_many_empty_noop(tmp_path):
    """_push_many() with empty list does nothing."""
    mw = StubMiddleware(db_dir=tmp_path)
    mw._push_many("events", [])
    # No file should be created since we didn't push anything
    # (shared_outbox is never called)


def test_push_creates_db_dir(tmp_path):
    """_push() creates db_dir if it doesn't exist."""
    db_dir = tmp_path / "nested" / "deep" / "dir"
    mw = StubMiddleware(db_dir=db_dir)
    mw._push("events", "INSERT INTO events (id) VALUES (?)", [1])

    assert db_dir.exists()
    assert (db_dir / "events.db").exists()


def test_push_multiple_tables(tmp_path):
    """_push() creates separate DB files per table."""
    mw = StubMiddleware(db_dir=tmp_path)
    mw._push("events", "INSERT INTO events (id) VALUES (?)", [1])
    mw._push("metrics", "INSERT INTO metrics (val) VALUES (?)", [99])

    assert (tmp_path / "events.db").exists()
    assert (tmp_path / "metrics.db").exists()

    ob_events = shared_outbox(db_path=tmp_path / "events.db", namespace="events")
    ob_metrics = shared_outbox(db_path=tmp_path / "metrics.db", namespace="metrics")
    assert ob_events.pending_count() == 1
    assert ob_metrics.pending_count() == 1
