# WS-6 + WS-7: Health Signal, status CLI, Observability & OSS Packaging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the read-only `health()` / `health_all()` signal, a `sqloutbox status` CLI, log hygiene (log-once-on-transition for stuck/degraded namespaces), and the OSS packaging + honest-contract README that makes 0.5.0 safe to publish.

**Architecture:** `health()` is a *pure read* on a single `Outbox` (one namespace) returning a frozen `NamespaceHealth` dataclass built from `pending_count()` + the four retry columns Plan 2 added (`attempts`, `last_attempt_at`, `last_error`, `last_error_class`). `health_all(db_dir)` is a module-level free function that globs `{db_dir}/*.db` and emits one `NamespaceHealth` per distinct namespace. The control-direction invariant is absolute: the library computes and exposes numbers; it never calls back into the consuming app, never pauses anything, never mutates the DB during a read. The `status` CLI and a producer-side 80% watermark are *consumers* of this signal — the 80% threshold lives in the producing app, never in library config. WS-7 turns the now-honest delivery semantics (head-hold, at-least-once, auto-dead-letter) into a TRUE-contract README plus packaging fixes (`tomli`, CHANGELOG, quality gates).

**Tech Stack:** Python 3.10+ stdlib only (sqlite3, asyncio, json, logging); pytest + pytest-asyncio.

**Spec:** Implements `docs/specs/2026-06-11-durable-ordered-retry-and-health-signal.md` §3.4 (the read-only `health()` signal — fields `depth`, `head_attempts`, `is_stuck`, `last_error`, `last_error_class`, `last_attempt_at`; and the `health_all(db_dir)` free function) and §4 (control-direction invariant); `docs/specs/2026-06-11-standalone-hardening-design.md` §4.2 (`capacity_pct = depth / max_pending`, `None` when `max_pending` unset — derived arithmetic, not policy) and §5.3 (writerless-target WARN); and the WS-6 + WS-7 tables in `docs/diagnosis/2026-06-11-remediation-roadmap.md` (F020, F040, F041, F042 / F021, F022, F043, F044, F070).

**Recommended execution order:** This is **Plan 6 (WS-6 + WS-7)** and runs **LAST** in the locked sequence: Plan 1 WS-0 (done) → Plan 3 WS-3 → Plan 2 WS-1+2 → Plan 4 WS-4 → Plan 5 WS-5 → **Plan 6 WS-6+7**. By the time you execute this plan, earlier plans have already changed shared files. **Assume these exist and DO NOT redefine them:**
- `src/sqloutbox/exceptions.py` — `SqloutboxError`, `ConfigError`, `QueueFullError`, `UnsupportedStatementError`, `ChainIntegrityError` (created by Plan 3 / WS-3).
- `OutboxConfig.max_attempts: int | None = 10`, `OutboxConfig.max_pending: int | None = None`, `OutboxConfig.max_batch_bytes: int | None = None` with `__post_init__` validation on both frozen dataclasses (created by Plan 3 / WS-3).
- `outbox_queue` retry columns `attempts INTEGER NOT NULL DEFAULT 0`, `last_attempt_at TEXT`, `last_error TEXT`, `last_error_class TEXT` (created by Plan 2 / WS-2 as idempotent ALTERs in `_schema.py`).
- `outbox_dead_log` table and `Outbox.dead_letter(seq, reason)` / `Outbox.replay(seq)` (created by Plan 2 / WS-2).
- The `dead-letter {list,show,replay}` and `skip` CLI subparsers (created by Plan 2 / WS-2) — this plan ADDS the `status` subparser alongside them, it does not touch theirs.
- `busy_timeout=30000` on `open_write_conn` + `thread_conn` (added by Plan 1 / WS-0).
- The L1 per-row guard and L2 per-unit `try/except` in `sync.py:_worker_loop` and the L3 observe-drain-task in `_runner.py` (added by Plan 1 / WS-0).

When this plan references the `attempts`/`last_attempt_at`/`last_error`/`last_error_class` columns it relies on Plan 2 having run. **Task 1 below also tolerates their absence** (defensive `PRAGMA table_info` check) so the plan's own tests pass even if executed against a tree where only Plan 1 has landed — but in the locked order Plan 2 has already added them.

> **PREREQUISITE — editable install (do this once before Task 1).** This repo uses a `src/` layout and `import sqloutbox` resolves to whatever is on `sys.path`. If `sqloutbox` is currently installed as a *non-editable* copy in site-packages (verify with `python -c "import sqloutbox; print(sqloutbox.__file__)"` — if the path is under `site-packages/` rather than this repo's `src/`, it is non-editable), then **every source edit in this plan would be invisible to the tests** and the packaging tests (Task 6/7) would resolve the wrong repo root. Fix it once up front:
> ```bash
> pip install -e ".[dev]"     # editable install of THIS checkout; picks up src/ edits live
> python -c "import sqloutbox, pathlib; assert 'site-packages' not in sqloutbox.__file__, sqloutbox.__file__; print('editable OK:', sqloutbox.__file__)"
> ```
> After this, `python -m pytest` reflects your `src/` edits without reinstalling. (Task 6 adds `ruff`/`mypy` to the `[dev]` extra; re-run `pip install -e ".[dev]"` after Task 6 if you installed before it landed, so the quality-gate tools are present.)

---

## File Structure

| File | Responsibility | Create/Modify |
|------|----------------|---------------|
| `src/sqloutbox/_models.py` | Add the frozen `NamespaceHealth` dataclass. | Modify |
| `src/sqloutbox/_outbox.py` | Add `Outbox.health()` (read-only) + the module-level `health_all(db_dir)` free function. | Modify |
| `src/sqloutbox/__init__.py` | Export `NamespaceHealth`, `health_all`. | Modify |
| `src/sqloutbox/sync.py` | Log hygiene: log-once-on-transition for a stuck namespace; WARN-once for a writerless/degraded target. | Modify |
| `src/sqloutbox/cli.py` | Add the `status` subcommand (`cmd_status` + subparser + dispatch). | Modify |
| `tests/test_health.py` | All WS-6 `health()` / `health_all()` / read-only-invariant tests. | Create |
| `tests/test_status_cli.py` | `status` CLI round-trip test. | Create |
| `tests/test_log_hygiene.py` | Log-once-on-transition + writerless WARN tests. | Create |
| `tests/test_packaging.py` | WS-7 packaging assertions (py.typed exported, types importable, tomli marker). | Create |
| `pyproject.toml` | WS-7 packaging: keep `tomli` conditional core dep for <3.11; bump version. | Modify |
| `CHANGELOG.md` | WS-7: new changelog documenting 0.5.0. | Create |
| `CONTRIBUTING.md` | WS-7: document quality gates (ruff/mypy/pytest). | Create |
| `README.md` | WS-7: replace "strict order / never drops" with the TRUE contract — add "Delivery guarantees" + "Limitations" sections. | Modify |

---

## Task 1: `NamespaceHealth` dataclass + `Outbox.health()` read-only signal

Implements spec §3.4 (the per-namespace health fields) and §4.2 (`capacity_pct`). `health()` is a single SELECT against the WAL file — pure read, cross-process safe, never mutates, never calls back into any app. The four retry columns (`attempts`, `last_attempt_at`, `last_error`, `last_error_class`) are added by Plan 2; the helper tolerates their absence so the test passes regardless.

**Files:**
- Modify: `src/sqloutbox/_models.py`
- Modify: `src/sqloutbox/_outbox.py`
- Test: `tests/test_health.py` (Create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_health.py` with:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_health.py -v`
Expected: FAIL at collection — `ImportError: cannot import name 'NamespaceHealth' from 'sqloutbox._models'` (the dataclass and `Outbox.health()` do not exist yet).

- [ ] **Step 3: Add the `NamespaceHealth` dataclass**

In `src/sqloutbox/_models.py`, append after the `QueueRow` class (after line 40):

```python


@dataclass(frozen=True)
class NamespaceHealth:
    """Read-only health snapshot for one outbox namespace.

    Built by ``Outbox.health()`` and the ``health_all()`` free function. This
    is PURE DATA pulled by a consumer — the library never pushes it, never
    calls back into the app, and never mutates state to produce it (the
    control-direction invariant; see the durable-retry spec §3.4 / §4).

    Attributes
    ----------
    namespace:
        The namespace (table) this snapshot describes.
    depth:
        Number of undelivered (``synced = 0``) rows in this namespace.
    head_attempts:
        Consecutive failed delivery attempts of the *current head* (the
        lowest-``seq`` undelivered row). 0 when healthy. With the head-hold
        drain (spec §3.2) only the head's ``attempts`` grows.
    is_stuck:
        ``True`` iff ``head_attempts > 0`` — a convenience boolean.
    last_error:
        Destination error message of the head's last failed attempt, or None.
    last_error_class:
        One of ``TRANSIENT`` | ``DETERMINISTIC`` | ``ALREADY_APPLIED`` |
        ``UNKNOWN`` (spec §3.3), or None until the first failure.
    last_attempt_at:
        ISO-8601 (UTC) timestamp of the head's last attempt, or None.
    capacity_pct:
        Derived convenience: ``depth / max_pending`` (0.0–1.0+), or None when
        ``max_pending`` is unset. Arithmetic, NOT policy — the 80% stop
        watermark lives in the producing app, never in this library
        (hardening spec §4.2).
    """
    namespace:        str
    depth:            int
    head_attempts:    int
    is_stuck:         bool
    last_error:       str | None
    last_error_class: str | None
    last_attempt_at:  str | None
    capacity_pct:     float | None = None
```

- [ ] **Step 4: Add `Outbox.health()` and the `health_all()` free function**

In `src/sqloutbox/_outbox.py`, add `NamespaceHealth` to the model import. Replace:

```python
from sqloutbox._models import QueueRow
```

with:

```python
from sqloutbox._models import NamespaceHealth, QueueRow
```

Then add the `health()` method to the `Outbox` class. Insert it right after `pending_count()` (which currently ends at line 356, just before the `# ── Seeding ──` section header). Insert:

```python

    def health(self, max_pending: int | None = None) -> NamespaceHealth:
        """Return a read-only health snapshot for this namespace.

        PURE READ — a single SELECT against the WAL SQLite file. Never mutates
        a row, never calls back into the consuming app, never imports an app
        module. Safe to call from a different process than the drain (WAL: a
        read never blocks the writer). This is the consumer's *eyes*; the
        consumer pulls it on its own schedule and owns every threshold
        (control-direction invariant — durable-retry spec §3.4 / §4).

        Parameters
        ----------
        max_pending:
            Optional cap used only to derive ``capacity_pct = depth /
            max_pending``. None (default) → ``capacity_pct`` is None. The
            library does NOT own a default; callers that configured
            ``OutboxConfig.max_pending`` pass it in. The 80% stop watermark is
            a PRODUCING-APP threshold, not library config (hardening §4.2).

        Returns
        -------
        NamespaceHealth
        """
        with thread_conn(self.db_path) as conn:
            depth_row = conn.execute(
                "SELECT COUNT(*) FROM outbox_queue "
                "WHERE namespace = ? AND synced = 0",
                [self.namespace],
            ).fetchone()
            depth = depth_row[0] if depth_row else 0

            # The retry columns (attempts/last_attempt_at/last_error/
            # last_error_class) are added by Plan 2 (WS-2). If they are not yet
            # present (e.g. running on a pre-WS-2 file), degrade gracefully to a
            # not-stuck reading instead of raising — health() must never crash.
            cols = {r[1] for r in conn.execute("PRAGMA table_info(outbox_queue)")}
            head_attempts = 0
            last_error: str | None = None
            last_error_class: str | None = None
            last_attempt_at: str | None = None
            if {"attempts", "last_attempt_at", "last_error",
                "last_error_class"} <= cols:
                head = conn.execute(
                    "SELECT attempts, last_attempt_at, last_error, "
                    "last_error_class FROM outbox_queue "
                    "WHERE namespace = ? AND synced = 0 "
                    "ORDER BY seq LIMIT 1",
                    [self.namespace],
                ).fetchone()
                if head is not None:
                    head_attempts    = head[0] or 0
                    last_attempt_at  = head[1]
                    last_error       = head[2]
                    last_error_class = head[3]

        capacity_pct: float | None = None
        if max_pending is not None and max_pending > 0:
            capacity_pct = depth / max_pending

        return NamespaceHealth(
            namespace=self.namespace,
            depth=depth,
            head_attempts=head_attempts,
            is_stuck=head_attempts > 0,
            last_error=last_error,
            last_error_class=last_error_class,
            last_attempt_at=last_attempt_at,
            capacity_pct=capacity_pct,
        )
```

Then add the module-level `health_all()` free function at the very END of `src/sqloutbox/_outbox.py` (after the `_seq_accounted` method, outside the class — same indent level as `class Outbox`):

```python


def health_all(
    db_dir: Path, max_pending: int | None = None
) -> list[NamespaceHealth]:
    """Enumerate health for every namespace under ``db_dir`` (free function).

    NOT a method — it takes a directory, not ``self``. Globs ``{db_dir}/*.db``
    and, for each file, emits one :class:`NamespaceHealth` per distinct
    namespace it contains (``SELECT DISTINCT namespace``). The common layout is
    one file per table (namespace == table), giving one snapshot per file; the
    multiple-namespaces-per-file case (allowed by ``shared_outbox``) is handled.

    PURE READ — same control-direction invariant as :meth:`Outbox.health`.
    Results are sorted by namespace for stable output.

    Parameters
    ----------
    db_dir:
        Directory containing the per-table ``*.db`` outbox files.
    max_pending:
        Forwarded to each ``health()`` for ``capacity_pct`` (None → None).

    Returns
    -------
    list[NamespaceHealth]
        Empty list if ``db_dir`` does not exist or contains no ``*.db`` files.
    """
    if not db_dir.is_dir():
        return []
    out: list[NamespaceHealth] = []
    for db_file in sorted(db_dir.glob("*.db")):
        # Read-only enumerate of namespaces in this file.
        with thread_conn(db_file) as conn:
            names = [
                r[0] for r in conn.execute(
                    "SELECT DISTINCT namespace FROM outbox_queue "
                    "ORDER BY namespace"
                ).fetchall()
            ]
        if not names:
            # File exists but has no rows yet — report the file's stem as an
            # empty namespace so depth=0 is still observable.
            names = [db_file.stem]
        for namespace in names:
            ob = Outbox(db_path=db_file, namespace=namespace)
            out.append(ob.health(max_pending=max_pending))
    out.sort(key=lambda h: h.namespace)
    return out
```

> Note: `thread_conn` and `Path` are already imported at the top of `_outbox.py` (`from pathlib import Path` line 7; `thread_conn` in the `_schema` import block lines 10-15). Do not re-import them. `health_all` constructs an `Outbox(...)` per namespace, which opens a *write* connection in `__init__` — that is unavoidable with the current `Outbox` constructor and is acceptable here (the file already exists; no data is mutated). The truly read-only verify path is Plan 5's concern, not this signal.

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m pytest tests/test_health.py -v`
Expected: PASS (6 passed).

- [ ] **Step 6: Run the full suite**

Run: `python -m pytest -q`
Expected: all green (no regression).

- [ ] **Step 7: Commit**

```bash
git add src/sqloutbox/_models.py src/sqloutbox/_outbox.py tests/test_health.py
git commit -m "feat(observability): NamespaceHealth + read-only Outbox.health() (WS-6, spec 3.4)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: `health_all()` cross-process accuracy + export the signal types

`health_all(db_dir)` was implemented in Task 1; this task locks its multi-file / cross-process behavior with a dedicated test and exports `NamespaceHealth` + `health_all` from the package so downstream consumers can import them.

**Files:**
- Modify: `src/sqloutbox/__init__.py`
- Test: `tests/test_health.py` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_health.py`:

```python
def test_health_all_one_namespace_per_file(tmp_path: Path):
    """health_all returns one snapshot per file (namespace == file stem)."""
    from sqloutbox._outbox import health_all

    Outbox(db_path=tmp_path / "alpha.db", namespace="alpha").enqueue(
        "INSERT INTO alpha (a) VALUES (?)", b"[1]"
    )
    Outbox(db_path=tmp_path / "beta.db", namespace="beta").enqueue(
        "INSERT INTO beta (a) VALUES (?)", b"[1]"
    )
    Outbox(db_path=tmp_path / "beta.db", namespace="beta").enqueue(
        "INSERT INTO beta (a) VALUES (?)", b"[2]"
    )

    healths = health_all(tmp_path)
    by_ns = {h.namespace: h for h in healths}
    assert set(by_ns) == {"alpha", "beta"}
    assert by_ns["alpha"].depth == 1
    assert by_ns["beta"].depth == 2
    # Sorted by namespace.
    assert [h.namespace for h in healths] == ["alpha", "beta"]


def test_health_all_missing_dir_returns_empty(tmp_path: Path):
    """A non-existent db_dir yields an empty list, never raises."""
    from sqloutbox._outbox import health_all

    assert health_all(tmp_path / "does_not_exist") == []


def test_health_all_capacity_pct_when_max_pending_set(tmp_path: Path):
    """capacity_pct = depth / max_pending when passed; None otherwise."""
    from sqloutbox._outbox import health_all

    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    for i in range(4):
        ob.enqueue("INSERT INTO evt (a) VALUES (?)", f"[{i}]".encode())

    # max_pending unset → capacity_pct None.
    h_none = health_all(tmp_path)[0]
    assert h_none.capacity_pct is None

    # max_pending=10, depth=4 → 0.4.
    h_set = health_all(tmp_path, max_pending=10)[0]
    assert h_set.depth == 4
    assert h_set.capacity_pct == 0.4


def test_health_all_cross_process_read_while_writing(tmp_path: Path):
    """A second connection reads correct depth from a WAL DB while the writer
    holds its own connection open (cross-process safety — spec §3.4)."""
    from sqloutbox._outbox import health_all

    # Writer keeps its persistent write connection open (simulates the drain
    # process). A separate health_all() read (simulates the consumer process)
    # sees committed rows without blocking.
    writer_ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    writer_ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")
    writer_ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[2]")

    healths = health_all(tmp_path)            # independent connections
    assert healths[0].depth == 2

    writer_ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[3]")
    assert health_all(tmp_path)[0].depth == 3


def test_namespace_health_and_health_all_exported():
    """Public API: NamespaceHealth and health_all importable from package root."""
    import sqloutbox

    assert hasattr(sqloutbox, "NamespaceHealth")
    assert hasattr(sqloutbox, "health_all")
    assert "NamespaceHealth" in sqloutbox.__all__
    assert "health_all" in sqloutbox.__all__
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_health.py -v -k "health_all or exported"`
Expected: FAIL — the four `health_all` tests PASS (it exists from Task 1), but `test_namespace_health_and_health_all_exported` FAILS (`AssertionError`: `NamespaceHealth`/`health_all` not yet on the package root).

- [ ] **Step 3: Export the signal types**

In `src/sqloutbox/__init__.py`, change the model + outbox imports. Replace:

```python
from sqloutbox._models import QueueRow
from sqloutbox._outbox import Outbox
```

with:

```python
from sqloutbox._models import NamespaceHealth, QueueRow
from sqloutbox._outbox import Outbox, health_all
```

Then in the `__all__` list, in the `# Core queue` block, add `"NamespaceHealth"` and `"health_all"`. Replace:

```python
__all__ = [
    # Core queue
    "Outbox",
    "OutboxWorker",
    "QueueRow",
    "shared_outbox",
    "clear_registry",
```

with:

```python
__all__ = [
    # Core queue
    "Outbox",
    "OutboxWorker",
    "QueueRow",
    "NamespaceHealth",
    "health_all",
    "shared_outbox",
    "clear_registry",
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_health.py -v`
Expected: PASS (all health tests, 11 passed).

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest -q`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/__init__.py tests/test_health.py
git commit -m "feat(observability): export NamespaceHealth + health_all; cross-process tests (WS-6)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: `sqloutbox status` CLI subcommand

Implements WS-6 / F042 (and the spec's `status` overlap with `dead-letter`): per-namespace depth / oldest / stuck, reading `health_all`. Mirrors the existing `verify` CLI's two-mode resolution (`--config` or `--db-dir`).

**Files:**
- Modify: `src/sqloutbox/cli.py`
- Test: `tests/test_status_cli.py` (Create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_status_cli.py` with:

```python
"""WS-6: `sqloutbox status` CLI — per-namespace depth / stuck, read-only."""
from __future__ import annotations

from pathlib import Path

import pytest

from sqloutbox._outbox import Outbox
from sqloutbox.cli import cmd_status, main


def test_status_db_dir_round_trip(tmp_path: Path, capsys):
    """status --db-dir prints one line per namespace with its depth."""
    Outbox(db_path=tmp_path / "alpha.db", namespace="alpha").enqueue(
        "INSERT INTO alpha (a) VALUES (?)", b"[1]"
    )
    ob = Outbox(db_path=tmp_path / "beta.db", namespace="beta")
    ob.enqueue("INSERT INTO beta (a) VALUES (?)", b"[1]")
    ob.enqueue("INSERT INTO beta (a) VALUES (?)", b"[2]")

    with pytest.raises(SystemExit) as ei:
        cmd_status(config_path=None, db_dir_path=tmp_path)
    assert ei.value.code == 0

    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" in out
    # Depths appear (alpha=1, beta=2).
    assert "depth=1" in out
    assert "depth=2" in out


def test_status_empty_dir(tmp_path: Path, capsys):
    """status on a dir with no .db files prints a friendly message, exit 0."""
    with pytest.raises(SystemExit) as ei:
        cmd_status(config_path=None, db_dir_path=tmp_path)
    assert ei.value.code == 0
    out = capsys.readouterr().out
    assert "no" in out.lower()


def test_status_requires_a_source(capsys):
    """status with neither --config nor --db-dir errors out, exit 1."""
    with pytest.raises(SystemExit) as ei:
        cmd_status(config_path=None, db_dir_path=None)
    assert ei.value.code == 1
    err = capsys.readouterr().err
    assert "--config" in err and "--db-dir" in err


def test_status_marks_stuck_namespace(tmp_path: Path, capsys):
    """A namespace whose head has attempts>0 is flagged STUCK in the output."""
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    head = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")
    conn = ob._write_conn
    cols = {r[1] for r in conn.execute("PRAGMA table_info(outbox_queue)")}
    if "attempts" not in cols:
        pytest.skip("retry columns not present (Plan 2 not applied)")
    conn.execute(
        "UPDATE outbox_queue SET attempts=5, last_error_class='TRANSIENT' "
        "WHERE seq=?",
        [head],
    )
    conn.commit()

    with pytest.raises(SystemExit) as ei:
        cmd_status(config_path=None, db_dir_path=tmp_path)
    assert ei.value.code == 0
    out = capsys.readouterr().out
    assert "STUCK" in out
    assert "attempts=5" in out


def test_status_main_dispatch(tmp_path: Path, capsys):
    """`main(['status', '--db-dir', ...])` routes to cmd_status."""
    Outbox(db_path=tmp_path / "evt.db", namespace="evt").enqueue(
        "INSERT INTO evt (a) VALUES (?)", b"[1]"
    )
    with pytest.raises(SystemExit) as ei:
        main(["status", "--db-dir", str(tmp_path)])
    assert ei.value.code == 0
    assert "evt" in capsys.readouterr().out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_status_cli.py -v`
Expected: FAIL at collection — `ImportError: cannot import name 'cmd_status' from 'sqloutbox.cli'`.

- [ ] **Step 3: Add `cmd_status` and wire the subparser + dispatch**

In `src/sqloutbox/cli.py`, add the `cmd_status` function. Insert it right after `cmd_verify` ends (currently line 567, just before the `# ── Entry point ──` section header at line 570):

```python


# ── status command ────────────────────────────────────────────────────────────


def cmd_status(config_path: Path | None, db_dir_path: Path | None) -> None:
    """Print per-namespace queue health (depth / oldest / stuck), read-only.

    Two modes (mirrors ``verify``):

    1. ``--config outbox.toml`` — discover db_dirs from TOML targets
    2. ``--db-dir /path/to/data`` — scan one directory for ``*.db`` files

    Reads the WS-6 ``health_all()`` signal. PURE READ — never mutates state,
    never starts a drain. Exits 0 always when a source is given (status is
    informational); exits 1 only when no source is provided.
    """
    from sqloutbox._outbox import health_all

    db_dirs: list[Path] = []

    if config_path is not None:
        from sqloutbox._runner import load_config_toml
        config, _writers = load_config_toml(config_path)
        seen: set[str] = set()
        for target in config.targets:
            db_dir = target.db_dir or config.db_dir
            key = str(db_dir)
            if key not in seen:
                seen.add(key)
                db_dirs.append(db_dir)

    elif db_dir_path is not None:
        if not db_dir_path.is_dir():
            print(f"error: not a directory: {db_dir_path}", file=sys.stderr)
            sys.exit(1)
        db_dirs.append(db_dir_path)

    else:
        print(
            "error: provide --config <file.toml> or --db-dir <path>\n\n"
            "Usage:\n"
            "  sqloutbox status --config outbox.toml\n"
            "  sqloutbox status --db-dir /path/to/data",
            file=sys.stderr,
        )
        sys.exit(1)

    healths = []
    for db_dir in db_dirs:
        healths.extend(health_all(db_dir))

    if not healths:
        print("no namespaces found — nothing to report")
        sys.exit(0)

    print()
    print("sqloutbox status — per-namespace queue health")
    print("-" * 70)
    total_depth = 0
    stuck_count = 0
    for h in healths:
        total_depth += h.depth
        state = "STUCK" if h.is_stuck else "ok"
        if h.is_stuck:
            stuck_count += 1
        line = (
            f"  {h.namespace:<30s}  {state:<5s}  "
            f"depth={h.depth}  attempts={h.head_attempts}"
        )
        if h.last_error_class:
            line += f"  class={h.last_error_class}"
        print(line)
        if h.is_stuck and h.last_error:
            print(f"  {'':<30s}         last_error={h.last_error}")
    print("-" * 70)
    print(
        f"  {len(healths)} namespace(s)  "
        f"total_depth={total_depth}  stuck={stuck_count}"
    )
    print()
    sys.exit(0)
```

Then register the subparser. In `main()`, after the `p_verify` block (which ends at line 605, just before `args = parser.parse_args(argv)` at line 607), insert:

```python

    p_status = sub.add_parser(
        "status",
        help="show per-namespace queue depth / stuck state (read-only)",
    )
    p_status.add_argument(
        "--config", "-c", type=Path, default=None, dest="status_config",
        help="TOML config file (discover db_dirs from targets)",
    )
    p_status.add_argument(
        "--db-dir", "-d", type=Path, default=None, dest="status_db_dir",
        help="directory to scan for *.db files",
    )
```

Then add dispatch. After the `elif args.command == "verify":` block (which ends at line 618), add:

```python
    elif args.command == "status":
        cmd_status(args.status_config, args.status_db_dir)
```

> Note: `status` uses distinct `dest=` names (`status_config`, `status_db_dir`) so its `--config`/`--db-dir` do not collide with the `verify` subparser's `verify_config`/`db_dir`. argparse subparsers each own their namespace, but distinct dests keep the dispatch unambiguous and match the `verify` precedent.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_status_cli.py -v`
Expected: PASS (5 passed; `test_status_marks_stuck_namespace` PASSES or SKIPS depending on whether Plan 2's retry columns are present).

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest -q`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/cli.py tests/test_status_cli.py
git commit -m "feat(cli): add 'sqloutbox status' — per-namespace depth/stuck, read-only (WS-6, F042)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: Log hygiene — log-once-on-transition for a stuck namespace

Implements WS-6 / F040, F041: a persistently-stuck namespace must NOT WARN every cycle (log spam) — it WARNs once when it *transitions* into the stuck state and stays quiet until it recovers (then logs the recovery once). The current drain logs a `warning` for every failed row on every flush (`sync.py:669`). Add a per-namespace "already warned" set so the WARN fires only on the not-stuck → stuck transition.

**Files:**
- Modify: `src/sqloutbox/sync.py`
- Test: `tests/test_log_hygiene.py` (Create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_log_hygiene.py` with:

```python
"""WS-6: log hygiene — stuck namespace WARNs once-on-transition, not per cycle."""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

import pytest

from sqloutbox._outbox import Outbox
from sqloutbox.config import OutboxConfig, TargetConfig
from sqloutbox.sync import OutboxSyncService


class _AlwaysFailWriter:
    """Writer whose every statement fails — drives a namespace into 'stuck'."""
    def __init__(self) -> None:
        self.calls = 0

    async def write_batch(self, stmts):
        self.calls += 1
        return [{"ok": False, "error": "connection refused"} for _ in stmts]


def _make_failing_service(tmp_path: Path, writer, *, table="evt"):
    # auto_schema=False + inject_outbox_seq=False so run()'s startup
    # _ensure_schema()/_seed_from_remote() do NOT call write_batch (Plan 1
    # lesson #1 — startup writer calls would otherwise pollute behavior).
    cfg = OutboxConfig(
        db_dir=tmp_path,
        targets=(TargetConfig(name="primary", tables=(table,),
                              inject_outbox_seq=False),),
        flush_interval=0.01,
        table_flush_threshold=1,
        table_max_wait=0.0,
        auto_schema=False,
    )
    return OutboxSyncService(config=cfg, writers={"primary": writer})


@pytest.mark.asyncio
async def test_stuck_namespace_warns_once_not_every_cycle(tmp_path: Path, caplog):
    """The 'stuck' transition WARN fires once across many failing cycles."""
    writer = _AlwaysFailWriter()
    svc = _make_failing_service(tmp_path, writer)
    Outbox(db_path=tmp_path / "evt.db", namespace="evt").enqueue(
        "INSERT INTO evt (a) VALUES (?)", json.dumps([1]).encode()
    )

    caplog.set_level(logging.WARNING, logger="sqloutbox.sync")
    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.3)            # many failing cycles elapse
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # The writer was called many times (the drain kept retrying) ...
    assert writer.calls >= 3
    # ... but the once-on-transition "stuck" WARN appears exactly once.
    stuck_warns = [
        r for r in caplog.records
        if "namespace stuck" in r.getMessage()
    ]
    assert len(stuck_warns) == 1
    msg = stuck_warns[0].getMessage()
    assert "evt" in msg          # names the namespace


@pytest.mark.asyncio
async def test_recovery_logs_once(tmp_path: Path, caplog):
    """When a stuck namespace recovers, an INFO 'recovered' fires once."""

    class _FlakyWriter:
        """Fails the first 3 calls, then succeeds — drives stuck → recovered."""
        def __init__(self) -> None:
            self.calls = 0

        async def write_batch(self, stmts):
            self.calls += 1
            if self.calls <= 3:
                return [{"ok": False, "error": "boom"} for _ in stmts]
            return [{"ok": True} for _ in stmts]

    writer = _FlakyWriter()
    svc = _make_failing_service(tmp_path, writer)
    Outbox(db_path=tmp_path / "evt.db", namespace="evt").enqueue(
        "INSERT INTO evt (a) VALUES (?)", json.dumps([1]).encode()
    )

    caplog.set_level(logging.INFO, logger="sqloutbox.sync")
    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.4)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    recovered = [
        r for r in caplog.records
        if "namespace recovered" in r.getMessage()
    ]
    assert len(recovered) == 1
    assert "evt" in recovered[0].getMessage()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_log_hygiene.py -v`
Expected: FAIL — there is no "namespace stuck" / "namespace recovered" log line yet, so `len(stuck_warns) == 1` (and the recovery assertion) fail with `0 != 1`.

- [ ] **Step 3: Add a per-namespace stuck-state tracker and emit once-on-transition**

In `src/sqloutbox/sync.py`, add a stuck-state set to `OutboxSyncService.__init__`. After the line `self._cycle_count = 0` (currently line 210), insert:

```python
        # WS-6 log hygiene: namespaces currently in the "stuck" (failing) state.
        # Used to WARN once on the not-stuck → stuck transition (and INFO once
        # on recovery) instead of spamming a warning every failing cycle (F040).
        self._stuck_namespaces: set[str] = set()
```

Then, in `_flush_to_target`, change the per-row failure handling and add transition logging. The current confirm loop (lines 661-686) is:

```python
        confirmed_by_table: dict[str, list[int]] = defaultdict(list)
        failed_count = 0
        for i, result in enumerate(results):
            table, outbox_seq = stmt_info[i]
            if result["ok"]:
                confirmed_by_table[table].append(outbox_seq)
            else:
                failed_count += 1
                logger.warning(
                    "[outbox_sync] %s write failed for '%s' seq=%d: %s",
                    target_name, table, outbox_seq, result.get("error", ""),
                )

        total_confirmed = 0
        for table, seqs in confirmed_by_table.items():
            outbox = outboxes[table]
            await asyncio.to_thread(outbox.mark_synced, seqs)
            await asyncio.to_thread(outbox.delete_synced, seqs)
            total_confirmed += len(seqs)
            if logger.isEnabledFor(_VERBOSE):
                logger.log(
                    _VERBOSE,
                    "[outbox_sync]   confirmed %s table='%s'  %d rows  "
                    "seqs=%s",
                    target_name, table, len(seqs), seqs[:10],
                )
```

Replace that entire block with:

```python
        confirmed_by_table: dict[str, list[int]] = defaultdict(list)
        failed_tables: set[str] = set()
        failed_count = 0
        for i, result in enumerate(results):
            table, outbox_seq = stmt_info[i]
            # F031: use .get("ok") so a writer that omits the key fails closed
            # (treated as not-ok) instead of raising KeyError.
            if result.get("ok"):
                confirmed_by_table[table].append(outbox_seq)
            else:
                failed_count += 1
                failed_tables.add(table)
                # Demoted to DEBUG: per-row failures are summarised by the
                # once-on-transition WARN below (F040 — no per-cycle spam).
                logger.debug(
                    "[outbox_sync] %s write failed for '%s' seq=%d: %s",
                    target_name, table, outbox_seq, result.get("error", ""),
                )

        # WS-6 log hygiene (F040, F041): WARN once when a namespace enters the
        # stuck state; INFO once when it recovers. The persistent failure is
        # reported with {namespace, seq, target} the first time only.
        for i, result in enumerate(results):
            table, outbox_seq = stmt_info[i]
            if table in failed_tables and table not in self._stuck_namespaces:
                self._stuck_namespaces.add(table)
                logger.warning(
                    "[outbox_sync] namespace stuck: '%s' on target '%s' "
                    "(first failing seq=%d): %s",
                    table, target_name, outbox_seq, result.get("error", ""),
                )

        total_confirmed = 0
        for table, seqs in confirmed_by_table.items():
            outbox = outboxes[table]
            await asyncio.to_thread(outbox.mark_synced, seqs)
            await asyncio.to_thread(outbox.delete_synced, seqs)
            total_confirmed += len(seqs)
            # A table that delivered AND was not in this cycle's failed set has
            # recovered — clear it and log the recovery once.
            if table not in failed_tables and table in self._stuck_namespaces:
                self._stuck_namespaces.discard(table)
                logger.info(
                    "[outbox_sync] namespace recovered: '%s' on target '%s'",
                    table, target_name,
                )
            if logger.isEnabledFor(_VERBOSE):
                logger.log(
                    _VERBOSE,
                    "[outbox_sync]   confirmed %s table='%s'  %d rows  "
                    "seqs=%s",
                    target_name, table, len(seqs), seqs[:10],
                )
```

> Note: the inner `for i, result in enumerate(results)` loop that emits the once-on-transition WARN only logs the *first* failing seq per namespace per transition because the `table not in self._stuck_namespaces` guard adds the table to the set on the first hit, so subsequent failing rows in the same batch are skipped. The recovery branch runs in the confirm loop because a table only appears in `confirmed_by_table` when at least one of its rows delivered — under head-hold (Plan 2) a recovered head delivers, which is exactly the recovery signal.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_log_hygiene.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest -q`
Expected: all green. (If a pre-existing test asserted the old per-row `logger.warning` text on a failed batch, update it to the new DEBUG level / once-on-transition contract — that update is expected per the spec's behavior-change note, not a regression. Locate any such test with `grep -rn "write failed for" tests/`.)

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/sync.py tests/test_log_hygiene.py
git commit -m "feat(observability): log stuck/recovered namespace once-on-transition (WS-6, F040/F041)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: Writerless / degraded target WARN-once at startup (F020)

Implements WS-6 / F020 and hardening §5.3: a target listed in config but with **no writer** is currently silently skipped in `run()` (`sync.py:558-559` and `:307-308`), yet it still appears in the "started" banner. WARN once at construction naming each writerless target so an operator sees the black hole instead of silent loss.

**Files:**
- Modify: `src/sqloutbox/sync.py`
- Test: `tests/test_log_hygiene.py` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_log_hygiene.py`:

```python
def test_writerless_target_warns_at_init(tmp_path: Path, caplog):
    """A target with no matching writer WARNs once at __init__, naming it."""
    caplog.set_level(logging.WARNING, logger="sqloutbox.sync")
    cfg = OutboxConfig(
        db_dir=tmp_path,
        targets=(
            TargetConfig(name="has_writer", tables=("a",), inject_outbox_seq=False),
            TargetConfig(name="no_writer", tables=("b",), inject_outbox_seq=False),
        ),
        auto_schema=False,
    )

    class _W:
        async def write_batch(self, stmts):
            return [{"ok": True} for _ in stmts]

    # Only provide a writer for 'has_writer'.
    OutboxSyncService(config=cfg, writers={"has_writer": _W()})

    warns = [
        r for r in caplog.records
        if "no writer configured" in r.getMessage()
    ]
    assert len(warns) == 1
    msg = warns[0].getMessage()
    assert "no_writer" in msg
    assert "has_writer" not in msg


def test_all_targets_have_writers_no_warn(tmp_path: Path, caplog):
    """When every target has a writer, no writerless WARN is emitted."""
    caplog.set_level(logging.WARNING, logger="sqloutbox.sync")
    cfg = OutboxConfig(
        db_dir=tmp_path,
        targets=(TargetConfig(name="t", tables=("a",), inject_outbox_seq=False),),
        auto_schema=False,
    )

    class _W:
        async def write_batch(self, stmts):
            return [{"ok": True} for _ in stmts]

    OutboxSyncService(config=cfg, writers={"t": _W()})

    warns = [
        r for r in caplog.records
        if "no writer configured" in r.getMessage()
    ]
    assert warns == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_log_hygiene.py -v -k writerless`
Expected: FAIL — no "no writer configured" WARN is emitted at `__init__` yet, so `len(warns) == 1` fails with `0 != 1`.

- [ ] **Step 3: WARN once for writerless targets in `__init__`**

In `src/sqloutbox/sync.py`, at the END of `OutboxSyncService.__init__` (right after the `self._target_outboxes = {...}` build loop completes — currently the loop ends at line 238), add:

```python

        # WS-6 (F020): a target listed in config but with no matching writer is
        # silently skipped by run(); it would still appear in the started
        # banner. WARN once here, naming each writerless target, so the operator
        # sees the black hole rather than silent loss.
        writerless = [t.name for t in config.targets if t.name not in writers]
        if writerless:
            logger.warning(
                "[outbox_sync] no writer configured for target(s): %s "
                "— rows for these targets will NOT be delivered until a writer "
                "is provided",
                writerless,
            )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_log_hygiene.py -v -k writerless`
Expected: PASS (2 passed).

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest -q`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/sync.py tests/test_log_hygiene.py
git commit -m "feat(observability): WARN once for writerless targets at init (WS-6, F020)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: OSS packaging — tomli core dep for <3.11, version bump, py.typed export (WS-7)

Implements WS-7 / F021 + F070. **Decision (justified):** keep `requires-python>=3.10` and make `tomli` a **conditional core dependency** for Python `<3.11` rather than bumping to `>=3.11`. Rationale: TOML config is the *headline feature*; on 3.10 a bare `pip install sqloutbox` must be able to parse it without the user knowing to add an extra. The cost is one tiny pure-Python dependency only on 3.10 (3.11+ uses stdlib `tomllib`, zero deps). Dropping 3.10 would shrink the install base for no engineering benefit. `py.typed` already exists (confirm) — this task adds a test that the marker ships and types import.

**Files:**
- Modify: `pyproject.toml`
- Test: `tests/test_packaging.py` (Create)

- [ ] **Step 1: Confirm `py.typed` exists and write the failing test**

First confirm the marker file is present:

Run: `ls -l src/sqloutbox/py.typed`
Expected: the file exists (0 bytes is correct — PEP 561 only requires presence).

Create `tests/test_packaging.py` with:

```python
"""WS-7: OSS packaging assertions — py.typed, types export, tomli marker."""
from __future__ import annotations

from pathlib import Path

import pytest

import sqloutbox


_PKG_DIR = Path(sqloutbox.__file__).parent


def _find_repo_root() -> Path | None:
    """Walk up from this test file to the dir that holds pyproject.toml.

    Do NOT assume ``_PKG_DIR.parent.parent`` — that only resolves to the repo
    root for a src-layout *editable* install. For a non-editable install the
    package lives in site-packages and that math points nowhere near the repo,
    so the file-reading packaging tests below would fail spuriously. Anchor on
    this test file's own location instead (it always lives under the repo).
    """
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists():
            return parent
    return None


_REPO_ROOT = _find_repo_root()


def test_py_typed_marker_present():
    """PEP 561 marker ships inside the package so downstream gets types."""
    assert (_PKG_DIR / "py.typed").exists()


def test_signal_types_are_importable_and_typed():
    """The WS-6 signal types are exported with annotations (typed API)."""
    from sqloutbox import NamespaceHealth, health_all

    # NamespaceHealth is a dataclass with the contract fields annotated.
    anns = NamespaceHealth.__annotations__
    for field in ("namespace", "depth", "head_attempts", "is_stuck",
                  "last_error", "last_error_class", "last_attempt_at",
                  "capacity_pct"):
        assert field in anns, f"missing annotation: {field}"

    # health_all is annotated (return type present).
    assert "return" in health_all.__annotations__


def test_pyproject_tomli_conditional_core_dep():
    """tomli is a conditional CORE dependency for Python < 3.11 (F021).

    A bare `pip install sqloutbox` on 3.10 must be able to parse TOML without
    the user opting into an extra. We assert the marker lives in [project]
    dependencies, not only in an optional extra.
    """
    if _REPO_ROOT is None:
        pytest.skip("repo root not found (running from a built dist, not a checkout)")
    text = (_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    # The conditional core dependency line.
    assert 'tomli>=2.0; python_version < "3.11"' in text \
        or "tomli>=2.0; python_version < '3.11'" in text
    # And it is in the [project] dependencies array (core), located before the
    # optional-dependencies table.
    deps_marker = "dependencies = ["
    opt_marker = "[project.optional-dependencies]"
    assert deps_marker in text
    assert opt_marker in text
    core_deps_pos = text.index(deps_marker)
    opt_pos = text.index(opt_marker)
    tomli_pos = text.index("tomli>=2.0")
    assert core_deps_pos < tomli_pos < opt_pos, \
        "tomli must appear in [project] dependencies (core), before extras"


def test_version_is_0_5_0():
    """0.5.0 cut — the release that ships WS-0..WS-7 (per both specs §5/§8)."""
    if _REPO_ROOT is None:
        pytest.skip("repo root not found (running from a built dist, not a checkout)")
    text = (_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'version = "0.5.0"' in text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_packaging.py -v`
Expected: FAIL — `test_py_typed_marker_present` and `test_signal_types_are_importable_and_typed` PASS, but `test_pyproject_tomli_conditional_core_dep` FAILS (tomli is currently only in the optional `toml` extra, line 50) and `test_version_is_0_5_0` FAILS (version is `0.4.1`, line 7).

- [ ] **Step 3: Make `tomli` a conditional core dep and bump the version**

In `pyproject.toml`, change the version. Replace:

```toml
version = "0.4.1"
```

with:

```toml
version = "0.5.0"
```

Replace the empty core dependencies line (line 33):

```toml
dependencies = []   # stdlib only — sqlite3, pathlib, dataclasses, logging, asyncio, json, tomllib (3.11+)
```

with:

```toml
dependencies = [
    # stdlib only on 3.11+ (tomllib). On 3.10 the stdlib has no TOML parser,
    # so ship tomli as a CONDITIONAL CORE dependency — TOML config is the
    # headline feature and `pip install sqloutbox` must parse it on 3.10
    # without the user opting into an extra (F021). Zero deps on 3.11+.
    'tomli>=2.0; python_version < "3.11"',
]
```

Then remove the now-redundant `toml` optional extra. Replace:

```toml
[project.optional-dependencies]
toml = ["tomli>=2.0; python_version < '3.11'"]
dev = ["pytest>=7.0", "pytest-asyncio>=0.21"]
```

with:

```toml
[project.optional-dependencies]
dev = ["pytest>=7.0", "pytest-asyncio>=0.21", "ruff>=0.4", "mypy>=1.8"]
```

> Note: `ruff` and `mypy` are added to the `dev` extra so the documented quality gates (Task 8) install in one command. The `_load_tomllib()` helper in `_runner.py:293-308` already prefers stdlib `tomllib` then falls back to `tomli`, so no code change is needed — the dependency move just guarantees `tomli` is present on 3.10.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_packaging.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest -q`
Expected: all green. (Note: `sqloutbox.__version__` reads from installed package metadata via `importlib.metadata`; the test reads `pyproject.toml` text directly, so it does not require a reinstall.)

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml tests/test_packaging.py
git commit -m "build: tomli conditional core dep for py<3.11; bump to 0.5.0 (WS-7, F021/F070)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 7: CHANGELOG.md + CONTRIBUTING.md quality gates (WS-7)

Implements WS-7 / F043 + F044: ship a CHANGELOG documenting the 0.5.0 release and a CONTRIBUTING that documents the quality gates (ruff / mypy / pytest). Doc-only task with one structural test (the files exist and name the gates), so the executor cannot skip it.

**Files:**
- Create: `CHANGELOG.md`
- Create: `CONTRIBUTING.md`
- Test: `tests/test_packaging.py` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_packaging.py`:

```python
def test_changelog_exists_and_documents_0_5_0():
    """CHANGELOG.md ships and has a 0.5.0 entry naming the headline changes."""
    if _REPO_ROOT is None:
        pytest.skip("repo root not found (running from a built dist, not a checkout)")
    cl = _REPO_ROOT / "CHANGELOG.md"
    assert cl.exists()
    text = cl.read_text(encoding="utf-8")
    assert "0.5.0" in text
    # Names the contract-defining behavior changes of this release.
    assert "at-least-once" in text.lower()
    assert "dead-letter" in text.lower() or "dead letter" in text.lower()
    assert "health" in text.lower()


def test_contributing_documents_quality_gates():
    """CONTRIBUTING.md documents the ruff / mypy / pytest gates."""
    if _REPO_ROOT is None:
        pytest.skip("repo root not found (running from a built dist, not a checkout)")
    cg = _REPO_ROOT / "CONTRIBUTING.md"
    assert cg.exists()
    text = cg.read_text(encoding="utf-8").lower()
    assert "ruff" in text
    assert "mypy" in text
    assert "pytest" in text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_packaging.py -v -k "changelog or contributing"`
Expected: FAIL — `CHANGELOG.md` and `CONTRIBUTING.md` do not exist yet (`AssertionError` on `.exists()`).

- [ ] **Step 3: Create CHANGELOG.md**

Create `CHANGELOG.md` with:

```markdown
# Changelog

All notable changes to **sqloutbox** are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] — 2026-06-11

The "honest contract" release. Hardens the library for standalone OSS use and
corrects the delivery semantics the README previously over-promised.

### Added
- **Read-only health signal** — `Outbox.health()` and the module-level
  `health_all(db_dir)` free function return a frozen `NamespaceHealth`
  (`namespace`, `depth`, `head_attempts`, `is_stuck`, `last_error`,
  `last_error_class`, `last_attempt_at`, and derived `capacity_pct`). PURE READ:
  the library never calls back into the consuming app and never mutates state
  to produce the signal (control-direction invariant).
- **`sqloutbox status` CLI** — per-namespace depth / stuck / last-error class,
  read from `health_all`. Two modes: `--config outbox.toml` or `--db-dir PATH`.
- **Auto dead-letter** — after `max_attempts` failed deliveries a row is moved
  (not deleted) to the audited, replayable `outbox_dead_log` table; the
  namespace then advances. `max_attempts=None` restores plateau-forever.
- **Dead-letter / skip CLI** — `dead-letter {list,show,replay}` and `skip`.
- **Opt-in backpressure** — `OutboxConfig.max_pending`; `enqueue()` raises
  `QueueFullError` at the cap. Default unbounded (no behavior change).
- **Typed exceptions** — `SqloutboxError` base with `ConfigError`,
  `QueueFullError`, `UnsupportedStatementError`, `ChainIntegrityError`.
- **Config validation** — `__post_init__` bounds-checks on `OutboxConfig` /
  `TargetConfig`; out-of-range fields raise `ConfigError(field, value, reason)`.
- **Single-drain lock** — `fcntl.flock` on `<db_dir>/.sqloutbox.lock`; a second
  `runservice` on the same `db_dir` exits with a clear error.
- **`py.typed`** marker — downstream type checkers now see sqloutbox types.

### Changed
- **Delivery is now strictly head-of-line ordered.** A failed row holds the
  head and blocks the rows behind it (no more leapfrog), with per-namespace
  exponential backoff (cap `backoff_cap_minutes`, default 64). This is a
  deliberate delivery-semantics change.
- **Honest delivery contract** documented: **at-least-once** (a crash between
  remote write and local delete can redeliver), ordering caveats, and
  idempotency only with `inject_outbox_seq=True`.
- **Log hygiene** — a stuck namespace WARNs once on transition (not every
  cycle); recovery logs once; writerless targets WARN once at startup.
- **Cooperative shutdown** — the confirm step is shielded so routine SIGTERM
  no longer manufactures duplicates.
- **`tomli` is a conditional core dependency** on Python < 3.11 (was an
  optional extra), so TOML config works out of the box on 3.10.
- **Read-only `verify`** — `verify` no longer creates files or migrates
  unrelated SQLite databases.

### Fixed
- Writer results read via `.get("ok")` (fail-closed) instead of `result["ok"]`
  (KeyError) when a writer omits the key.
- `inject_outbox_seq` rejects unsupported SQL shapes (`INSERT…SELECT`,
  multi-row `VALUES`, `?`/`)`/` WHERE ` inside string literals) with
  `UnsupportedStatementError` instead of silently mangling SQL.
- `mark_synced` / `delete_synced` chunk seq lists to ≤900 per statement,
  avoiding the SQLite variable limit on large batches.
- Forked-chain DBs raise `ChainIntegrityError` (not a bare `IntegrityError`);
  read-only `verify` reports the fork without crashing.

## [0.4.1]

- Verbose `verify` output; minor fixes. (Pre-changelog; see git history.)
```

- [ ] **Step 4: Create CONTRIBUTING.md**

Create `CONTRIBUTING.md` with:

```markdown
# Contributing to sqloutbox

Thanks for contributing! sqloutbox is a small, dependency-light library; the
bar is high on correctness (it is a durability primitive) and low on
ceremony.

## Development setup

```bash
git clone https://github.com/sandeepyadav1478/sqloutbox
cd sqloutbox
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"          # installs pytest, pytest-asyncio, ruff, mypy
```

The library itself is **stdlib only** at runtime (plus `tomli` on Python <3.11
for TOML parsing). Do not add runtime dependencies without discussion — the
zero-dependency promise is a feature.

## Quality gates

Every change must pass all three gates before it is merged. Run them locally:

```bash
ruff check src tests          # lint (E, F, I, UP, B rules)
mypy src                      # static types (the package ships py.typed)
python -m pytest -q           # full test suite
```

- **ruff** — linting and import sorting. Fix with `ruff check --fix`.
- **mypy** — static type checking against the public, typed API.
- **pytest** — `asyncio_mode = "auto"` is set in `pyproject.toml`, so
  `@pytest.mark.asyncio` markers are optional (existing tests keep them for
  clarity — match that convention).

## Tests

- Use TDD: write the failing test first, then the implementation.
- Drain/runner tests use a fake `OutboxWriter`. When a test asserts on what was
  delivered, construct the config with `auto_schema=False` AND
  `inject_outbox_seq=False` so the service's startup `_ensure_schema()` /
  `_seed_from_remote()` calls do not pollute the recorded statements.
- Never send a real OS signal in a test (it can interrupt pytest). Capture the
  handler the runner registers and invoke it directly.

## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/)
(`feat:`, `fix:`, `docs:`, `build:`, `test:`, `refactor:`).

## Releasing

1. Update `CHANGELOG.md` with the new version's changes.
2. Bump `version` in `pyproject.toml`.
3. Tag `vX.Y.Z` and push.
```

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m pytest tests/test_packaging.py -v -k "changelog or contributing"`
Expected: PASS (2 passed).

- [ ] **Step 6: Run the full suite**

Run: `python -m pytest -q`
Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add CHANGELOG.md CONTRIBUTING.md tests/test_packaging.py
git commit -m "docs: add CHANGELOG (0.5.0) + CONTRIBUTING quality gates (WS-7, F043/F044)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 8: README honesty rewrite — TRUE delivery contract (WS-7, F022)

Implements WS-7 / F022 (and the doc half of WS-1 — F013/F058/F065). The README currently sells "in strict order" (line 7) and "never silently drops events" (line 257) — both materially false / over-promised before 0.5.0. Rewrite to the TRUE contract: **at-least-once delivery**, ordering caveats, idempotency only with `inject_outbox_seq=True`, auto-dead-letter after `max_attempts` (move-not-delete), and single-drain-per-`db_dir`. This is a **doc-only task with no automated test** — the structural assertion is folded into the review checklist below; the executor must verify each required section exists by reading the file after the edit.

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Soften the two over-promising lines in the intro and "Chain integrity"**

In `README.md`, replace the opening summary (lines 6-8):

```
**Producer** writes SQL events synchronously to a local SQLite file (~150µs,
no network). **Consumer** drains them to N remote databases in strict order,
with singly-linked chain integrity verification on every batch.
```

with:

```
**Producer** writes SQL events synchronously to a local SQLite file (~150µs,
no network). **Consumer** drains them to N remote databases with **at-least-once**
delivery and per-namespace head-of-line ordering, verifying singly-linked chain
integrity on every batch. See [Delivery guarantees](#delivery-guarantees) for
the exact contract.
```

Then in the "Chain integrity" section, replace (lines 254-257):

```
Each outbox row stores `prev_seq` — a backward pointer to the previous row.
Before every delivery, `verify_chain()` validates the chain is unbroken.
A gap blocks delivery and logs an error (never silently drops events).
```

with:

```
Each outbox row stores `prev_seq` — a backward pointer to the previous row.
Before every delivery, `verify_chain()` validates the chain is unbroken.
A *local* chain gap blocks delivery and logs an error. A row the remote can
never accept is retried with backoff and, after `max_attempts`, **moved** (not
deleted) to the audited `outbox_dead_log` table. Nothing is silently dropped —
see [Delivery guarantees](#delivery-guarantees).
```

- [ ] **Step 2: Add the "Delivery guarantees" section**

In `README.md`, insert a new section immediately BEFORE the existing `## Limitations` section (currently line 479). Insert:

```markdown
## Delivery guarantees

Read this before relying on sqloutbox for anything important. These are the
*true* guarantees as of 0.5.0 — stated precisely so there are no surprises.

### At-least-once delivery

sqloutbox is **at-least-once**, not exactly-once. The drain delivers a batch
(`writer.write_batch`), then records the rows as synced and deletes them. If
the process crashes **between** the remote write and the local delete, those
rows are redelivered on restart. This is unavoidable for a durable queue
without a distributed transaction across the two databases.

**Make delivery idempotent** by setting `inject_outbox_seq=True` (the default).
The drain rewrites each INSERT to `INSERT OR IGNORE ... (..., outbox_seq)` and
the remote's partial unique index on `outbox_seq` absorbs the duplicate. Without
`inject_outbox_seq=True`, a redelivery applies the statement again — for a
non-idempotent UPDATE, that may be incorrect. **Idempotency is only guaranteed
with `inject_outbox_seq=True` on idempotent INSERTs.**

### Ordering

Delivery is **strictly head-of-line ordered within a namespace**. When the head
row (lowest unsynced `seq`) fails, it is held and the rows behind it are *not*
delivered until it succeeds, is dead-lettered, or is skipped. Earlier versions
confirmed rows independently, letting a later row leapfrog a failed earlier one
— that is fixed in 0.5.0.

- **No cross-namespace ordering.** Each namespace (table) has its own chain and
  its own backoff clock; they drain independently.
- A persistently failing head retries with **exponential backoff**
  (`2^attempts` minutes, capped at `backoff_cap_minutes`, default 64).

### Poison rows — auto dead-letter (move, not drop)

After `max_attempts` failed deliveries (default 10; set `None` to retry
forever), the head row is **moved atomically** to the `outbox_dead_log` table
with the failure reason, and the namespace advances so it is no longer blocked.
The row is **never lost** — it is quarantined, auditable, and replayable:

```bash
sqloutbox dead-letter list  --config outbox.toml            # what's quarantined
sqloutbox dead-letter show  --config outbox.toml --namespace N --seq S
sqloutbox dead-letter replay --config outbox.toml --namespace N --seq S
sqloutbox skip   --config outbox.toml --namespace N --seq S   # move a stuck head
```

An undecodable payload or an SQL shape `inject_outbox_seq` cannot transform is
dead-lettered immediately (it can never succeed).

### Observing health (pull, never push)

The library exposes a **read-only** signal you poll; it never calls back into
your app, never pauses anything:

```python
from sqloutbox import health_all
from pathlib import Path

for h in health_all(Path("data/myapp")):
    print(h.namespace, h.depth, h.is_stuck, h.head_attempts, h.last_error_class)
```

`NamespaceHealth` fields: `namespace`, `depth`, `head_attempts`, `is_stuck`,
`last_error`, `last_error_class`, `last_attempt_at`, and `capacity_pct`
(`depth / max_pending`, or `None` when `max_pending` is unset). The CLI prints
the same data:

```bash
sqloutbox status --db-dir data/myapp
sqloutbox status --config outbox.toml
```

**Backpressure is your decision, not the library's.** Set `max_pending` to cap
the queue: `enqueue()` then raises `QueueFullError` at the hard wall. To stop
*earlier* (recommended), have your **producing application** poll `health()`
and stop producing at, say, 80% of `max_pending` — that 80% watermark lives in
*your* app, not in library config. The library only reports the number; it
never halts or resumes your producer (an operator restarts it after diagnosing
*why* the queue filled). sqloutbox's own drain never stops — it keeps pulling
the backlog down.

### Single drain per `db_dir`

**Run exactly one drain process per `db_dir`.** `runservice` takes an exclusive
`flock` on `<db_dir>/.sqloutbox.lock` at startup and exits with a clear error if
another drain already holds it. Two drains on one `db_dir` would double-deliver.
Producers (`enqueue`) do **not** take this lock — many producers + one drain is
the supported topology.
```

- [ ] **Step 3: Rewrite the "Limitations" section to be honest and current**

In `README.md`, replace the existing `## Limitations` block (lines 479-485):

```
## Limitations

- **Single process only** — one write connection per SQLite file
- **UTF-8 payloads only** — payload stored as TEXT
- **No TTL/expiry** — rows stay until explicitly deleted
- **No priorities** — strictly FIFO per namespace
- **No cross-namespace ordering** — each namespace is independent
```

with:

```
## Limitations

- **At-least-once, not exactly-once** — a crash between remote write and local
  delete can redeliver. Use `inject_outbox_seq=True` for idempotent absorption
  (see [Delivery guarantees](#delivery-guarantees)).
- **Idempotency only with `inject_outbox_seq=True`** — a non-idempotent UPDATE
  routed without injection may be applied twice on redelivery.
- **One drain per `db_dir`** — enforced by a `flock`; a second `runservice`
  exits. Many producers, one drain.
- **One write connection per SQLite file** — `enqueue()` is single-writer per
  namespace file.
- **UTF-8 payloads only** — payload is stored as TEXT; non-UTF-8 bytes are
  rejected/dead-lettered.
- **No TTL/expiry** — rows stay until delivered, dead-lettered, or skipped.
- **No priorities** — strictly FIFO per namespace, head-of-line held on failure.
- **No cross-namespace ordering** — each namespace drains independently with its
  own backoff clock.
- **`tag` is raw SQL you control** — sqloutbox executes it verbatim at the
  remote. Never put untrusted input in `tag`; parameterise via `args`.
```

- [ ] **Step 4: Update the CLI section to list the new subcommands**

In `README.md`, replace the `## CLI` command block (lines 422-425):

```
sqloutbox runservice [--config FILE]  Start drain from TOML config
sqloutbox init [DIR]                  Scaffold a Python config directory
```

with:

```
sqloutbox runservice [--config FILE]            Start drain from TOML config
sqloutbox init [DIR]                            Scaffold a Python config directory
sqloutbox verify  [--config FILE | --db-dir D]  Offline integrity scan (read-only)
sqloutbox status  [--config FILE | --db-dir D]  Per-namespace depth / stuck (read-only)
sqloutbox dead-letter {list,show,replay} …      Inspect / replay quarantined rows
sqloutbox skip --namespace N --seq S …          Move a stuck head to the dead-letter
```

- [ ] **Step 5: Verify the rewrite (manual review — there is no automated test)**

Run: `python -m pytest -q`
Expected: all green (this task touched only `README.md`; the suite is unaffected).

Then read the file back and confirm every required element is present:

Run: `grep -nE "Delivery guarantees|at-least-once|inject_outbox_seq=True|max_attempts|move|single|flock|Limitations" README.md`
Expected: matches show the new "Delivery guarantees" section (with at-least-once, idempotency-only-with-injection, auto-dead-letter move-not-delete, single-drain-per-db_dir) and the rewritten "Limitations" section. Confirm the old phrases "in strict order" (intro) and "never silently drops events" (chain integrity) are GONE:

Run: `grep -nE "in strict order|never silently drops" README.md`
Expected: NO matches (both removed).

- [ ] **Step 6: Commit**

```bash
git add README.md
git commit -m "docs: rewrite README to the TRUE delivery contract (WS-7, F022)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 9: Final full-suite gate + lint/type check

A final gate proving WS-6 + WS-7 compose cleanly and the documented quality gates pass.

**Files:** none (verification only).

- [ ] **Step 1: Run the full suite**

Run: `python -m pytest -q`
Expected: all green — the 180 baseline tests plus the ~22 added by this plan (test_health.py ~11, test_status_cli.py ~5, test_log_hygiene.py ~4, test_packaging.py ~6). Gate on "all green", NOT a hard count (earlier plans also added tests).

- [ ] **Step 2: Run the documented quality gates**

Run: `ruff check src tests && mypy src && python -m pytest -q`
Expected: ruff clean, mypy clean, pytest green. If `ruff`/`mypy` are not installed, run `pip install -e ".[dev]"` first (Task 6 added them to the `dev` extra). If mypy flags a pre-existing issue unrelated to this plan's files, note it but do not fix unrelated code in this plan — confine fixes to `_models.py`, `_outbox.py`, `sync.py`, `cli.py`, `__init__.py`.

- [ ] **Step 3: Confirm the public API surface**

Run: `python -c "import sqloutbox; print('NamespaceHealth' in sqloutbox.__all__, 'health_all' in sqloutbox.__all__); from sqloutbox import NamespaceHealth, health_all; print('ok')"`
Expected: `True True` then `ok`.

- [ ] **Step 4: Commit (only if Step 2 required a lint/type fix in this plan's files)**

If no changes were needed, skip. Otherwise:

```bash
git add -A
git commit -m "chore: lint/type fixes for WS-6/WS-7 surface

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review notes (for the executor)

- **Spec coverage:** Task 1-2 implement the durable-retry spec §3.4 (`health()` fields `depth`/`head_attempts`/`is_stuck`/`last_error`/`last_error_class`/`last_attempt_at`, plus `health_all`) and the hardening spec §4.2 (`capacity_pct`). Task 3 is the `status` CLI (WS-6 / F042). Task 4-5 are log hygiene (F040/F041/F020). Task 6-8 are WS-7 packaging + the honest README (F021/F070/F022/F043/F044). The control-direction invariant (durable-retry §4) is enforced by `test_health_is_read_only_no_mutation` and `test_health_never_calls_back_into_app`.
- **Field-name assumption to double-check:** the durable-retry spec §3.4 declares `NamespaceHealth` WITH a `namespace` field; the cross-plan CONTRACT brief lists the health fields as `depth, head_attempts, is_stuck, last_error, last_error_class, last_attempt_at` (+ optional `capacity_pct`) without explicitly listing `namespace`. This plan INCLUDES `namespace` because (a) the spec's dataclass has it and (b) `health_all()` is useless without it (you can't tell which namespace each snapshot describes). If the verifier wants the contract read literally as "no `namespace` field," that is a one-line removal — but it would break `health_all` callers. Flagging explicitly.
- **Cross-plan dependency (assume-exists):** `health()` reads the `attempts`/`last_attempt_at`/`last_error`/`last_error_class` columns Plan 2 (WS-2) adds and the `max_pending` config Plan 3 (WS-3) adds. In the locked execution order (Plan 6 runs LAST) both exist. Task 1's `health()` and its tests also tolerate the columns' ABSENCE (a `PRAGMA table_info` guard) so the plan is independently runnable — do NOT remove that guard thinking it is dead code; it is the graceful-degradation path the spec's "never crash" read demands.
- **Plan 1 lesson #1 (writer pollution):** every drain test in `test_log_hygiene.py` builds its config with `auto_schema=False` AND `inject_outbox_seq=False` so startup `_ensure_schema()`/`_seed_from_remote()` do not call `write_batch`. Keep this — removing it makes the once-on-transition assertions wrong.
- **Behavior-change test churn (Task 4):** demoting the per-row failure `logger.warning` to DEBUG may break a pre-existing test asserting that warning text on a partially-failed batch. The roadmap/specs explicitly call this an expected update, not a regression. The grep in Task 4 Step 5 (`grep -rn "write failed for" tests/`) locates any such test; update it to the new once-on-transition contract.
- **Line numbers:** all cited line numbers (`_outbox.py:356`, `_models.py:40`, `_outbox.py:7/10-15`, `cli.py:567/605/618`, `sync.py:210/238/558/661-686`, `__init__.py:61-62`, `pyproject.toml:7/33/49-51`, `README.md:6-8/254-257/422-425/479-485`) were accurate against the branch `feat/durable-ordered-retry-signal` at plan-writing time but EARLIER PLANS WILL HAVE SHIFTED THEM. If a number has drifted, locate the insertion point by the quoted surrounding code, not the number.
- **`.get("ok")` change (Task 4):** the confirm loop now reads `result.get("ok")` instead of `result["ok"]` (F031 — fail-closed if a writer omits the key). This is a small correctness improvement folded into Task 4; if a test relied on `result["ok"]` raising KeyError it should be updated.
- **No real OS signals in tests:** none of this plan's tests send signals; the runner signal path is Plan 1/Plan 4's concern. Keep it that way.
- **Gate on "all green", not a number:** the 180 baseline plus this plan's additions plus earlier plans' additions make any hard count brittle. Use `python -m pytest -q` exit status.
