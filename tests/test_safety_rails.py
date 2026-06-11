"""WS-3 safety rails: backpressure, grammar guard, var-limit chunking."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from sqloutbox._outbox import Outbox
from sqloutbox.exceptions import QueueFullError


# ── Backpressure (D2) ─────────────────────────────────────────────────────────


def test_enqueue_unbounded_never_raises(tmp_path: Path):
    """Default max_pending=None: enqueue never raises even with many rows."""
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    for i in range(50):
        seq = ob.enqueue("INSERT INTO evt (a) VALUES (?)", json.dumps([i]).encode())
        assert seq is not None
    assert ob.pending_count() == 50


def test_enqueue_raises_queue_full_at_cap(tmp_path: Path):
    """max_pending set: enqueue raises QueueFullError once pending >= cap."""
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt", max_pending=3)
    for i in range(3):
        assert ob.enqueue("INSERT INTO evt (a) VALUES (?)",
                          json.dumps([i]).encode()) is not None
    # Now pending_count() == 3 == max_pending → the next enqueue must raise.
    with pytest.raises(QueueFullError) as ei:
        ob.enqueue("INSERT INTO evt (a) VALUES (?)", json.dumps([99]).encode())
    assert ei.value.namespace == "evt"
    assert ei.value.max_pending == 3
    # The rejected row was NOT inserted — still exactly 3 pending.
    assert ob.pending_count() == 3


def test_enqueue_cap_reopens_after_drain(tmp_path: Path):
    """Once rows are marked+deleted, pending drops and enqueue accepts again."""
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt", max_pending=2)
    s1 = ob.enqueue("INSERT INTO evt (a) VALUES (?)", json.dumps([1]).encode())
    ob.enqueue("INSERT INTO evt (a) VALUES (?)", json.dumps([2]).encode())
    with pytest.raises(QueueFullError):
        ob.enqueue("INSERT INTO evt (a) VALUES (?)", json.dumps([3]).encode())
    # Drain one row → pending falls to 1 → enqueue accepts again.
    ob.mark_synced([s1])
    ob.delete_synced([s1])
    assert ob.pending_count() == 1
    assert ob.enqueue("INSERT INTO evt (a) VALUES (?)",
                      json.dumps([3]).encode()) is not None
