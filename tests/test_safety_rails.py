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


# ── inject_outbox_seq grammar guard (D3) ──────────────────────────────────────

from sqloutbox.exceptions import UnsupportedStatementError
from sqloutbox.sync import inject_outbox_seq


def test_guard_accepts_basic_insert():
    """Supported single-row INSERT still transforms correctly."""
    sql, args = inject_outbox_seq(
        "INSERT INTO orders (id, amount) VALUES (?, ?)", [1, 9.99], outbox_seq=100,
    )
    assert "INSERT OR IGNORE INTO" in sql
    assert "id, amount, outbox_seq" in sql
    assert args == [1, 9.99, 100]


def test_guard_accepts_update_with_where():
    """Supported UPDATE … WHERE still transforms correctly."""
    sql, args = inject_outbox_seq(
        "UPDATE orders SET status=?, amount=? WHERE id=?", ["x", 9.99, 42],
        outbox_seq=100,
    )
    assert "outbox_seq = ?" in sql
    assert args == ["x", 9.99, 100, 42]


def test_guard_accepts_insert_with_literal_containing_paren_and_qmark():
    """A string literal containing ')' / '?' / WHERE does NOT corrupt the rewrite."""
    # The literal "a)?WHERE" contains every structural char the naive scanner keys on.
    sql, args = inject_outbox_seq(
        "INSERT INTO t (label, n) VALUES ('a)?WHERE', ?)", ["a)?WHERE", 5],
        outbox_seq=7,
    )
    # outbox_seq column appended to the real column list, placeholder to real VALUES.
    assert "label, n, outbox_seq" in sql
    assert sql.rstrip().endswith(", ?)")
    assert args == ["a)?WHERE", 5, 7]


def test_guard_rejects_insert_select():
    """INSERT … SELECT has no VALUES list → rejected, never rewritten."""
    with pytest.raises(UnsupportedStatementError):
        inject_outbox_seq(
            "INSERT INTO t (a, b) SELECT a, b FROM other", [], outbox_seq=1,
        )


def test_guard_rejects_multirow_values():
    """INSERT … VALUES (…),(…) is multi-row → rejected."""
    with pytest.raises(UnsupportedStatementError):
        inject_outbox_seq(
            "INSERT INTO t (a) VALUES (?), (?)", [1, 2], outbox_seq=1,
        )


def test_guard_rejects_qmark_inside_literal_for_update():
    """An UPDATE whose only '?' is inside a literal is ambiguous → rejected."""
    # SET clause has NO real placeholder (the '?' is inside the literal),
    # so the structural scan finds zero SET args — reject rather than guess.
    with pytest.raises(UnsupportedStatementError):
        inject_outbox_seq(
            "UPDATE t SET note='why?' WHERE id=?", [5], outbox_seq=1,
        )


def test_guard_rejects_unknown_statement():
    """Neither INSERT nor UPDATE → rejected (no more silent passthrough)."""
    with pytest.raises(UnsupportedStatementError):
        inject_outbox_seq("DELETE FROM t WHERE id=?", [1], outbox_seq=1)


def test_guard_rejects_insert_without_values():
    """INSERT with no VALUES keyword at all → rejected."""
    with pytest.raises(UnsupportedStatementError):
        inject_outbox_seq("INSERT INTO t DEFAULT VALUES", [], outbox_seq=1)
