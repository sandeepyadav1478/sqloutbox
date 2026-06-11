"""WS-6: read-only health() signal + health_all() free function."""
from __future__ import annotations

from pathlib import Path

from sqloutbox._models import NamespaceHealth
from sqloutbox._outbox import Outbox


def test_health_empty_namespace(tmp_path: Path):
    """A fresh, empty namespace reports a clean, not-stuck health."""
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    h = ob.health()
    assert isinstance(h, NamespaceHealth)
    assert h.namespace == "evt"
    assert h.depth == 0
    assert h.head_attempts == 0
    assert h.is_stuck is False
    assert h.last_error is None
    assert h.last_error_class is None
    assert h.last_attempt_at is None
    # capacity_pct is None because max_pending is not known to a bare Outbox.
    assert h.capacity_pct is None


def test_health_depth_counts_unsynced(tmp_path: Path):
    """depth == number of undelivered rows in this namespace."""
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")
    ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[2]")
    ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[3]")
    h = ob.health()
    assert h.depth == 3
    assert h.head_attempts == 0
    assert h.is_stuck is False


def test_health_reflects_stuck_head(tmp_path: Path):
    """When the head row has attempts>0, health reports it as stuck.

    Simulate the drain having recorded a failed attempt on the head by writing
    the retry columns directly (the same columns Plan 2's drain writes). This
    test does NOT depend on the drain — it asserts health() reads the columns.
    """
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    head_seq = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")
    ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[2]")
    # Stamp the head with a failed-attempt state on whatever retry columns exist.
    conn = ob._write_conn
    cols = {r[1] for r in conn.execute("PRAGMA table_info(outbox_queue)")}
    if "attempts" in cols:
        conn.execute(
            "UPDATE outbox_queue SET attempts=3, last_attempt_at='2026-06-11T00:00:00+00:00', "
            "last_error='connection refused', last_error_class='TRANSIENT' WHERE seq=?",
            [head_seq],
        )
        conn.commit()
        h = ob.health()
        assert h.head_attempts == 3
        assert h.is_stuck is True
        assert h.last_error == "connection refused"
        assert h.last_error_class == "TRANSIENT"
        assert h.last_attempt_at == "2026-06-11T00:00:00+00:00"
    else:
        # Plan 2 not yet applied — columns absent. health() must degrade to
        # "not stuck" without raising (defensive read).
        h = ob.health()
        assert h.head_attempts == 0
        assert h.is_stuck is False


def test_health_head_is_lowest_seq_unsynced(tmp_path: Path):
    """head_attempts comes from the LOWEST-seq unsynced row, not any other row."""
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    s1 = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")  # head
    s2 = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[2]")
    conn = ob._write_conn
    cols = {r[1] for r in conn.execute("PRAGMA table_info(outbox_queue)")}
    if "attempts" not in cols:
        return  # Plan 2 not applied — nothing to assert here
    # Put attempts on the NON-head row only; head stays at 0.
    conn.execute("UPDATE outbox_queue SET attempts=9 WHERE seq=?", [s2])
    conn.commit()
    h = ob.health()
    assert h.head_attempts == 0      # head (s1) is clean
    assert h.is_stuck is False
    assert s1 < s2


def test_health_is_read_only_no_mutation(tmp_path: Path):
    """CONTROL-DIRECTION INVARIANT: health() must NEVER mutate DB state.

    Snapshot every row before and after calling health() many times; nothing
    changes. (The signal is a pull — the library exposes, never writes.)
    """
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")
    ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[2]")

    def snapshot() -> list[tuple]:
        with ob._write_conn:  # reuse the open conn; read full table state
            return ob._write_conn.execute(
                "SELECT seq, synced, payload FROM outbox_queue ORDER BY seq"
            ).fetchall()

    before = snapshot()
    for _ in range(25):
        ob.health()
    after = snapshot()
    assert before == after


def test_health_never_calls_back_into_app(tmp_path: Path):
    """CONTROL-DIRECTION INVARIANT: health() takes no app callable and imports
    no app module. We assert the signature exposes only plain scalar params
    (no callbacks/app objects to push through) and the returned object is plain
    data (a frozen dataclass of ints/strs/None)."""
    import inspect

    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    sig = inspect.signature(ob.health)
    # The only parameter is the optional scalar `max_pending` (used purely to
    # derive capacity_pct). There is NO parameter that could carry a callback
    # or an app object back into the library — the control-direction invariant.
    params = [p for p in sig.parameters.values()]
    assert all(p.name == "max_pending" for p in params), \
        f"health() must expose no callback/app params, got {[p.name for p in params]}"
    # max_pending, if present, must be an optional scalar (annotation 'int | None').
    for p in params:
        assert p.default is None, "max_pending must default to None (library owns no threshold)"
    h = ob.health()
    # Every field is a plain scalar or None — no callables, no app objects.
    for value in (h.namespace, h.depth, h.head_attempts, h.is_stuck,
                  h.last_error, h.last_error_class, h.last_attempt_at,
                  h.capacity_pct):
        assert value is None or isinstance(value, (int, float, str, bool))
