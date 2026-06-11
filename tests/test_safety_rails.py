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


# ── SQLite variable-limit chunking (F025) ─────────────────────────────────────

import sqlite3

import sqloutbox._outbox as _outbox_mod
from sqloutbox._schema import thread_conn as _real_thread_conn


def _enqueue_n(ob: Outbox, n: int) -> list[int]:
    seqs: list[int] = []
    for i in range(n):
        s = ob.enqueue("INSERT INTO big (a) VALUES (?)", json.dumps([i]).encode())
        assert s is not None
        seqs.append(s)
    return seqs


def test_chunked_helper_splits_at_var_chunk():
    """_chunked() splits a seq list into <=_VAR_CHUNK pieces, in order, losing
    nothing. This is the PRIMARY red gate: it is host- and Python-version-
    independent (no reliance on the host's SQLite variable limit, which on modern
    builds is 32766+ and on this host is 500000 — far above the historical 999)."""
    # Local import: pre-implementation _VAR_CHUNK/_chunked do not exist, so this
    # raises ImportError → THIS test fails (red) WITHOUT breaking collection of
    # the other tests in the file (a module-top import would fail the whole file).
    from sqloutbox._outbox import _VAR_CHUNK, _chunked

    assert _VAR_CHUNK <= 999          # stays under the historical SQLite default
    seqs = list(range(1000))
    chunks = _chunked(seqs)
    assert chunks                      # non-empty
    assert all(len(c) <= _VAR_CHUNK for c in chunks)
    assert [x for c in chunks for x in c] == seqs   # order + completeness preserved


@pytest.fixture
def _cap_vars_999(monkeypatch):
    """Pin SQLITE_LIMIT_VARIABLE_NUMBER=999 on the connections mark_synced /
    delete_synced open, so a >999-placeholder IN(...) genuinely raises
    'too many SQL variables' — reproducing the historical default regardless of
    the host's SQLite build. enqueue() uses the persistent write connection
    (self._write_conn), NOT thread_conn, so this cap does not affect row insertion."""
    def _capped(db_path):
        conn = _real_thread_conn(db_path)
        conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999)
        return conn
    # _outbox.py uses the module-global name `thread_conn`; patch it there.
    monkeypatch.setattr(_outbox_mod, "thread_conn", _capped)


@pytest.mark.skipif(
    not hasattr(sqlite3.Connection, "setlimit"),
    reason="Connection.setlimit requires Python 3.11+; cannot pin the var limit",
)
def test_mark_synced_chunks_over_var_limit(_cap_vars_999, tmp_path: Path):
    """mark_synced over 1000 seqs does not raise 'too many SQL variables' even
    when the connection's variable limit is pinned to 999 (pre-chunking the single
    IN (?x1000) would raise sqlite3.OperationalError)."""
    ob = Outbox(db_path=tmp_path / "big.db", namespace="big")
    seqs = _enqueue_n(ob, 1000)
    ob.mark_synced(seqs)
    ob.delete_synced(seqs)
    assert ob.pending_count() == 0


@pytest.mark.skipif(
    not hasattr(sqlite3.Connection, "setlimit"),
    reason="Connection.setlimit requires Python 3.11+; cannot pin the var limit",
)
def test_delete_synced_chunks_over_var_limit(_cap_vars_999, tmp_path: Path):
    """delete_synced over 1000 seqs chunks its SELECT and DELETE safely under the
    999 cap."""
    ob = Outbox(db_path=tmp_path / "big.db", namespace="big")
    seqs = _enqueue_n(ob, 1000)
    ob.mark_synced(seqs)
    ob.delete_synced(seqs)
    assert ob.pending_count() == 0
    # Re-deleting the same (now absent) seqs is a no-op, not an error.
    ob.delete_synced(seqs)
    assert ob.pending_count() == 0


def test_mark_delete_synced_chunk_boundary_correct(tmp_path: Path):
    """Sizes spanning the 900-chunk boundary sync + delete EVERY row (guards against
    an off-by-one in the chunking). Functional correctness; runs on every host."""
    for n in (899, 900, 901, 1801):
        ob = Outbox(db_path=tmp_path / f"b{n}.db", namespace="b")
        seqs = _enqueue_n(ob, n)
        ob.mark_synced(seqs)
        ob.delete_synced(seqs)
        assert ob.pending_count() == 0


# ── Watermark ownership contract (doc + library-boundary assertion) ───────────

import sqloutbox
from sqloutbox import OutboxConfig


def test_library_owns_max_pending_not_the_watermark(tmp_path: Path):
    """max_pending is library config; the 80% watermark is NOT a library symbol.

    The library reports the number (depth / max_pending). The stop-producing
    threshold lives in the PRODUCING APPLICATION, so sqloutbox must not export
    a STOP_WATERMARK_PCT constant or any auto-resume control.
    """
    cfg = OutboxConfig(db_dir=tmp_path, max_pending=1000)
    assert cfg.max_pending == 1000                 # library owns the hard cap
    # The watermark percentage is NOT a library-owned symbol.
    assert not hasattr(sqloutbox, "STOP_WATERMARK_PCT")
    assert "STOP_WATERMARK_PCT" not in getattr(sqloutbox, "__all__", [])


def test_readme_documents_watermark_as_producer_policy():
    """README states the watermark is a producing-app policy with no auto-resume."""
    readme = (Path(__file__).resolve().parent.parent / "README.md").read_text()
    assert "max_pending" in readme
    # The doc must make the ownership + no-auto-resume contract explicit.
    assert "80%" in readme
    assert "no auto-resume" in readme.lower() or "manual" in readme.lower()
