# WS-0 Resilience Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the sqloutbox drain daemon survive a single bad row, a corrupt/locked DB file, or any per-cycle exception without silently zombifying — and crash *loudly* (non-zero exit) when the worker truly dies, so a supervisor restarts it.

**Architecture:** Three defense layers. (L1) the per-row decode/transform is guarded so one undecodable row is skipped, not fatal. (L2) each `(target → table)` drain unit is wrapped so a corrupt/locked namespace file is isolated and siblings keep draining. (L3) the runner *observes* the drain task and exits non-zero if it dies for any reason other than a clean stop. This is the release-gate workstream from `docs/diagnosis/2026-06-11-remediation-roadmap.md` (findings F001, F002, F026, F028-sibling) and is designed to land BEFORE the drain rework in later plans.

**Tech Stack:** Python 3.10+ stdlib only (`sqlite3`, `asyncio`, `json`, `logging`); `pytest` + `pytest-asyncio` for tests. No new runtime dependencies.

**Spec:** `docs/specs/2026-06-11-standalone-hardening-design.md` §2 (WS-0). Companion to `docs/specs/2026-06-11-durable-ordered-retry-and-health-signal.md`.

**Scope note — what this plan does NOT do:** It does not add the dead-letter table, backoff, head-of-line hold, or `health()` (those are later plans). For an undecodable row, L1 here does **log-and-skip-this-cycle** (the row stays pending and is retried next cycle, logging once). The permanent dead-letter routing for undecodable rows arrives in the WS-2 plan; this plan only guarantees the row cannot *zombify the daemon*. That keeps WS-0 independently shippable.

---

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `src/sqloutbox/sync.py` | Drain loop (`_worker_loop`, `_flush_to_target`). Add L1 per-row guard + L2 per-unit guard. | Modify |
| `src/sqloutbox/_runner.py` | Service runner. Add L3 task observation (loud exit). | Modify |
| `src/sqloutbox/_schema.py` | Connection helpers. Add `busy_timeout` PRAGMA to both connection paths. | Modify |
| `tests/test_resilience.py` | All WS-0 tests (L1/L2/L3, busy_timeout). | Create |

**Why one new test file:** these tests share fixtures (a fake writer, a seeded outbox, a poison-row helper) and all assert the same property — "one fault does not kill the daemon." Keeping them together makes that property auditable in one place.

---

## Task 1: `busy_timeout` on every connection (F028 sibling)

Ordinary two-process lock contention (producer + drain on one file) currently raises `OperationalError: database is locked` after SQLite's 5s default, which (pre-L2) crashes the loop. Setting `busy_timeout` makes contended writes *wait* instead of raising. Do this first — it reduces the rate at which L2 even has to catch a locked-DB error.

**Files:**
- Modify: `src/sqloutbox/_schema.py:77-112`
- Test: `tests/test_resilience.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_resilience.py` with:

```python
"""WS-0 resilience: one fault must not zombify the daemon."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from sqloutbox._schema import open_write_conn, thread_conn


def _busy_timeout(conn: sqlite3.Connection) -> int:
    return conn.execute("PRAGMA busy_timeout").fetchone()[0]


def test_open_write_conn_sets_busy_timeout(tmp_path: Path):
    conn = open_write_conn(tmp_path / "t.db")
    try:
        assert _busy_timeout(conn) == 30000
    finally:
        conn.close()


def test_thread_conn_sets_busy_timeout(tmp_path: Path):
    # thread_conn needs the file to exist first (open_write_conn creates it).
    open_write_conn(tmp_path / "t.db").close()
    conn = thread_conn(tmp_path / "t.db")
    try:
        assert _busy_timeout(conn) == 30000
    finally:
        conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_resilience.py -v -k busy_timeout`
Expected: FAIL — `thread_conn` returns `0` (SQLite default when unset), assert `0 == 30000` fails.

- [ ] **Step 3: Add a shared constant and apply it in both connection helpers**

In `src/sqloutbox/_schema.py`, add the constant after the imports (around line 8):

```python
# Wait up to 30s for a contended write lock before raising "database is locked".
# Two processes (producer + drain) on one WAL file routinely contend briefly;
# without this they raise after SQLite's 5s default and (pre-L2) crash the loop.
_BUSY_TIMEOUT_MS = 30_000
```

In `open_write_conn`, add the PRAGMA right after the `WAL` line (currently line 87):

```python
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA synchronous=NORMAL")
```

Replace `thread_conn` (currently lines 106-112) with:

```python
def thread_conn(db_path: Path) -> sqlite3.Connection:
    """Open a short-lived connection for use inside asyncio.to_thread() calls.

    Each thread-pool task opens and closes its own connection.
    No state is shared — sqlite3 WAL handles concurrent access safely.
    busy_timeout makes a contended write wait rather than raise immediately.
    """
    conn = sqlite3.connect(str(db_path))
    conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
    return conn
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_resilience.py -v -k busy_timeout`
Expected: PASS (2 passed).

- [ ] **Step 5: Run the full suite to confirm no regression**

Run: `python -m pytest -q`
Expected: all existing tests still pass (180 + 2 new = 182 collected, all green).

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/_schema.py tests/test_resilience.py
git commit -m "feat(resilience): set busy_timeout=30s on all connections (WS-0)"
```

---

## Task 2: L1 — guard per-row decode/transform so one bad row can't escape the cycle (F001, F002)

`json.loads(row.payload.decode())` at `sync.py:609` and `inject_outbox_seq(...)` at `:611` are unguarded inside the `for row in rows:` loop. One undecodable payload or untransformable statement throws straight out of `_worker_loop`. Wrap the per-row body so a bad row is logged and skipped *this cycle* (it stays pending; permanent dead-lettering is the WS-2 plan).

**Files:**
- Modify: `src/sqloutbox/sync.py:607-613`
- Test: `tests/test_resilience.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_resilience.py`:

```python
import asyncio
import json
import pytest

from sqloutbox._outbox import Outbox
from sqloutbox.config import OutboxConfig, TargetConfig
from sqloutbox.sync import OutboxSyncService


class _CollectingWriter:
    """Test writer: records every statement it is asked to write, all ok."""
    def __init__(self) -> None:
        self.seen: list[tuple[str, list]] = []

    async def write_batch(self, stmts):
        self.seen.extend(stmts)
        return [{"ok": True} for _ in stmts]


def _make_service(tmp_path: Path, writer, *, table="evt"):
    # auto_schema=False + inject_outbox_seq=False so run() does NOT call
    # write_batch() during _ensure_schema()/_seed_from_remote() at startup —
    # those calls would otherwise pollute writer.seen and break the assertions.
    cfg = OutboxConfig(
        db_dir=tmp_path,
        targets=(TargetConfig(name="primary", tables=(table,),
                              inject_outbox_seq=False),),
        flush_interval=0.01,
        table_flush_threshold=1,
        table_max_wait=0.0,
        auto_schema=False,
    )
    return OutboxSyncService(config=cfg, writers={"primary": writer}), cfg


def _enqueue_raw(db_path: Path, namespace: str, tag: str, payload: bytes):
    """Enqueue a row, then overwrite its payload with raw bytes (bypass JSON)."""
    ob = Outbox(db_path=db_path, namespace=namespace)
    ob.enqueue(tag, b'{}')
    # Corrupt the stored payload directly to simulate a non-JSON row on disk.
    conn = ob._write_conn  # persistent connection
    conn.execute("UPDATE outbox_queue SET payload=?", (payload.decode("latin-1"),))
    conn.commit()


@pytest.mark.asyncio
async def test_undecodable_row_does_not_kill_loop(tmp_path: Path):
    writer = _CollectingWriter()
    svc, cfg = _make_service(tmp_path, writer)
    db_path = tmp_path / "evt.db"
    # One poison row: payload is not valid JSON.
    _enqueue_raw(db_path, "evt", "INSERT INTO evt (a) VALUES (?)", b"not json{{{")

    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.2)            # let several cycles run
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # The loop survived (task did not die on its own before cancel) and the
    # poison row was NOT delivered to the writer.
    assert writer.seen == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_resilience.py::test_undecodable_row_does_not_kill_loop -v`
Expected: FAIL — the `json.loads` raises inside the loop, `svc.run()` faults; the test sees the task finished with a `JSONDecodeError` (the `await task` re-raises it before/without `CancelledError`), so the test errors.

- [ ] **Step 3: Guard the per-row body**

In `src/sqloutbox/sync.py`, replace the per-row loop (currently lines 607-613):

```python
                    for row in rows:
                        sql = row.tag
                        args = json.loads(row.payload.decode())
                        if target.should_inject_seq(table):
                            sql, args = inject_outbox_seq(sql, args, row.seq)
                        all_stmts.append((sql, args))
                        stmt_info.append((table, row.seq))
```

with:

```python
                    for row in rows:
                        try:
                            sql = row.tag
                            args = json.loads(row.payload.decode())
                            if target.should_inject_seq(table):
                                sql, args = inject_outbox_seq(sql, args, row.seq)
                        except Exception as exc:
                            # L1: a single undecodable / untransformable row must
                            # not escape the cycle and zombify the daemon. Log once
                            # and skip it THIS cycle (it stays pending; permanent
                            # dead-letter routing arrives in the WS-2 plan).
                            logger.error(
                                "[outbox_sync] skipping bad row table='%s' seq=%d: %s",
                                table, row.seq, exc,
                            )
                            continue
                        all_stmts.append((sql, args))
                        stmt_info.append((table, row.seq))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_resilience.py::test_undecodable_row_does_not_kill_loop -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/sqloutbox/sync.py tests/test_resilience.py
git commit -m "feat(resilience): L1 guard per-row decode/transform, skip bad row (WS-0, F001/F002)"
```

---

## Task 3: L2 — isolate per-unit DB faults so one corrupt/locked namespace can't halt all delivery (F026)

`pending_count()` (`sync.py:566`), `fetch_unsynced()` (`:584`), and `verify_chain()` (`:596`) run unguarded in the per-table loop. A corrupt or locked file raises and kills `_worker_loop`. Wrap each table's unit so a fault is logged, that table is skipped this cycle, and sibling tables keep draining.

**Files:**
- Modify: `src/sqloutbox/sync.py:565-615` (the `for table, outbox in outboxes.items():` body)
- Test: `tests/test_resilience.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_resilience.py`:

```python
class _BoomOutbox:
    """Stand-in outbox whose reads always raise — simulates a corrupt file."""
    def pending_count(self):
        raise sqlite3.DatabaseError("database disk image is malformed")
    def fetch_unsynced(self):
        raise sqlite3.DatabaseError("database disk image is malformed")


@pytest.mark.asyncio
async def test_corrupt_namespace_isolated_siblings_drain(tmp_path: Path):
    writer = _CollectingWriter()
    # Two tables on one target: 'bad' is corrupt, 'good' is healthy.
    # inject_outbox_seq=False + auto_schema=False keeps startup write_batch
    # calls out of writer.seen (see _make_service note).
    cfg = OutboxConfig(
        db_dir=tmp_path,
        targets=(TargetConfig(name="primary", tables=("bad", "good"),
                              inject_outbox_seq=False),),
        flush_interval=0.01,
        table_flush_threshold=1,
        table_max_wait=0.0,
        auto_schema=False,
    )
    svc = OutboxSyncService(config=cfg, writers={"primary": writer})

    # Seed a healthy row in 'good'.
    Outbox(db_path=tmp_path / "good.db", namespace="good").enqueue(
        "INSERT INTO good (a) VALUES (?)", json.dumps([1]).encode()
    )
    # Replace the 'bad' outbox with one that raises on every read.
    svc._target_outboxes["primary"]["bad"] = _BoomOutbox()

    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # 'good' was delivered despite 'bad' raising every cycle.
    assert any("good" in sql for sql, _ in writer.seen)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_resilience.py::test_corrupt_namespace_isolated_siblings_drain -v`
Expected: FAIL — `_BoomOutbox.pending_count()` raises, kills the loop before `good` is processed; `await task` re-raises `DatabaseError`.

- [ ] **Step 3: Wrap the per-table unit**

> **Prerequisite import:** the `except (sqlite3.DatabaseError, sqlite3.OperationalError)` clause below references the `sqlite3` module, which `sync.py` does NOT currently import (only `_schema.py` and the tests do). `py_compile` will pass without it (it only checks syntax), but the except clause raises `NameError` at runtime. Add `import sqlite3` to `src/sqloutbox/sync.py`'s import block (alphabetically, between `import logging` and `import time`) as part of this step.

In `src/sqloutbox/sync.py`, the inner loop currently begins at line 565 (`for table, outbox in outboxes.items():`) and ends at line 615 (`flushed_tables.append(table)`). Wrap its entire body in a try/except. Replace:

```python
                for table, outbox in outboxes.items():
                    pending = outbox.pending_count()
                    if pending == 0:
                        continue
```

with:

```python
                for table, outbox in outboxes.items():
                  try:
                    pending = outbox.pending_count()
                    if pending == 0:
                        continue
```

and replace the unit's tail:

```python
                    flushed_tables.append(table)

                if all_stmts:
```

with:

```python
                    flushed_tables.append(table)
                  except (sqlite3.DatabaseError, sqlite3.OperationalError) as exc:
                    # L2: isolate a corrupt/locked namespace. Skip it THIS cycle;
                    # sibling tables keep draining. (Transient lock → retries next
                    # cycle; structural corruption → keeps logging until repaired.)
                    logger.error(
                        "[outbox_sync] table='%s' skipped this cycle (db error): %s",
                        table, exc,
                    )
                    continue
                  except Exception as exc:
                    # Any other per-table fault must not escape the loop either.
                    logger.exception(
                        "[outbox_sync] table='%s' skipped this cycle (unexpected): %s",
                        table, exc,
                    )
                    continue

                if all_stmts:
```

> Note on indentation: the `try:` adds one indent level *inside* the `for` body. Keep the existing body lines at their current indentation under the `try` (Python allows the `try`/`except` to align with the loop variable as shown — the `try:` is indented 18 spaces, body stays at 20). After editing, run `python -m py_compile src/sqloutbox/sync.py` to confirm valid syntax before testing.

- [ ] **Step 4: Verify syntax, then run the test**

Run: `python -m py_compile src/sqloutbox/sync.py && python -m pytest tests/test_resilience.py::test_corrupt_namespace_isolated_siblings_drain -v`
Expected: compile OK, test PASS.

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest -q`
Expected: all green (no regression from the wrap).

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/sync.py tests/test_resilience.py
git commit -m "feat(resilience): L2 isolate per-namespace DB faults, siblings drain (WS-0, F026)"
```

---

## Task 4: L3 — runner observes the drain task and exits non-zero on unexpected death (F001)

`run_service_main` does `task = create_task(svc.run()); await stop.wait()` (`_runner.py:591-592`). If `svc.run()` faults, nothing observes it — the process lives on as a zombie. Make the runner await *either* the stop event *or* the task, and if the task finished on its own (a fault), log critically and raise `SystemExit(1)` so a supervisor restarts.

**Files:**
- Modify: `src/sqloutbox/_runner.py:591-598`
- Test: `tests/test_resilience.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_resilience.py`:

```python
from sqloutbox import _runner


@pytest.mark.asyncio
async def test_runner_exits_nonzero_when_worker_dies(monkeypatch, tmp_path: Path):
    """If the drain task faults, the runner must raise SystemExit(1), not hang."""

    class _DyingService:
        async def run(self):
            raise RuntimeError("worker exploded")

    # Patch the pieces run_service_main needs so we exercise ONLY the task-watch logic.
    monkeypatch.setattr(
        _runner, "load_config_toml",
        lambda _p: (_FakeConfig(), {}),
    )
    monkeypatch.setattr(
        "sqloutbox.sync.OutboxSyncService",
        lambda **_kw: _DyingService(),
    )

    with pytest.raises(SystemExit) as ei:
        await asyncio.wait_for(_runner.run_service_main(tmp_path / "outbox.toml"), timeout=2.0)
    assert ei.value.code == 1


class _FakeConfig:
    """Minimal stand-in for the loaded config (only what run_service_main reads)."""
    flush_interval = 1.0
    table_flush_threshold = 15
    table_max_wait = 6.0
    db_dir = Path("/tmp")
    targets = ()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_resilience.py::test_runner_exits_nonzero_when_worker_dies -v`
Expected: FAIL — current runner blocks on `await stop.wait()` forever; `asyncio.wait_for` raises `TimeoutError` (not `SystemExit`), so the test fails.

- [ ] **Step 3: Replace the task-await block with task observation**

In `src/sqloutbox/_runner.py`, replace lines 591-598:

```python
    task = loop.create_task(svc.run(), name="sqloutbox.drain")
    await stop.wait()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    logger.info("stopped")
```

with:

```python
    task = loop.create_task(svc.run(), name="sqloutbox.drain")
    stop_task = loop.create_task(stop.wait(), name="sqloutbox.stop")

    done, _pending = await asyncio.wait(
        {task, stop_task}, return_when=asyncio.FIRST_COMPLETED,
    )

    if task in done:
        # The drain exited on its own — this is always a fault (the worker loop
        # is an infinite loop; it only returns/raises on error). Surface it
        # LOUDLY so a supervisor (systemd Restart=on-failure) restarts us,
        # instead of lingering as a zombie with a dead worker.
        stop_task.cancel()
        exc = task.exception()
        logger.critical("drain worker exited unexpectedly: %r", exc)
        raise SystemExit(1)

    # Normal path: a stop signal arrived. Cancel the drain and shut down cleanly.
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    logger.info("stopped")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_resilience.py::test_runner_exits_nonzero_when_worker_dies -v`
Expected: PASS.

- [ ] **Step 5: Add a test that the normal stop path still works**

Append to `tests/test_resilience.py`:

We do NOT send a real OS signal (that can interrupt pytest itself and is
flaky). Instead we capture the signal handlers the runner registers on the
loop and invoke the stop handler directly — fully deterministic.

```python
@pytest.mark.asyncio
async def test_runner_clean_stop_does_not_raise(monkeypatch, tmp_path: Path):
    """A normal stop signal shuts down cleanly (no SystemExit)."""
    import signal as _signal

    started = asyncio.Event()

    class _LiveService:
        async def run(self):
            started.set()
            while True:
                await asyncio.sleep(0.01)

    monkeypatch.setattr(_runner, "load_config_toml", lambda _p: (_FakeConfig(), {}))
    monkeypatch.setattr("sqloutbox.sync.OutboxSyncService", lambda **_kw: _LiveService())

    # Capture the handlers the runner registers, without touching process-wide
    # signal state. Shadows the bound method on this loop instance only.
    loop = asyncio.get_running_loop()
    handlers: dict[int, object] = {}

    def _capture(sig, cb, *a):
        handlers[sig] = cb

    monkeypatch.setattr(loop, "add_signal_handler", _capture)

    runner_task = asyncio.create_task(_runner.run_service_main(tmp_path / "outbox.toml"))
    await started.wait()                 # service is running, handlers registered
    handlers[_signal.SIGINT]()           # fire the stop handler the runner set
    # Completes WITHOUT SystemExit (clean-stop path), within the timeout.
    await asyncio.wait_for(runner_task, timeout=2.0)
```

- [ ] **Step 6: Run both runner tests**

Run: `python -m pytest tests/test_resilience.py -v -k runner`
Expected: PASS (2 passed).

- [ ] **Step 7: Run the full suite**

Run: `python -m pytest -q`
Expected: all green.

- [ ] **Step 8: Commit**

```bash
git add src/sqloutbox/_runner.py tests/test_resilience.py
git commit -m "feat(resilience): L3 observe drain task, exit non-zero on worker death (WS-0, F001)"
```

---

## Task 5: Integration test — bad row + healthy row, daemon stays up and delivers the good one

A single end-to-end test proving the three layers compose: a poison row and a healthy row in different namespaces; after several cycles the daemon is still running and the healthy row was delivered.

**Files:**
- Test: `tests/test_resilience.py`

- [ ] **Step 1: Write the test**

Append to `tests/test_resilience.py`:

```python
@pytest.mark.asyncio
async def test_poison_and_healthy_coexist(tmp_path: Path):
    writer = _CollectingWriter()
    cfg = OutboxConfig(
        db_dir=tmp_path,
        targets=(TargetConfig(name="primary", tables=("poison", "healthy"),
                              inject_outbox_seq=False),),
        flush_interval=0.01,
        table_flush_threshold=1,
        table_max_wait=0.0,
        auto_schema=False,
    )
    svc = OutboxSyncService(config=cfg, writers={"primary": writer})

    _enqueue_raw(tmp_path / "poison.db", "poison",
                 "INSERT INTO poison (a) VALUES (?)", b"}{ not json")
    Outbox(db_path=tmp_path / "healthy.db", namespace="healthy").enqueue(
        "INSERT INTO healthy (a) VALUES (?)", json.dumps([7]).encode()
    )

    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.25)
    assert not task.done()                       # daemon still alive
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    delivered = [sql for sql, _ in writer.seen]
    assert any("healthy" in s for s in delivered)
    assert not any("poison" in s for s in delivered)
```

- [ ] **Step 2: Run the test**

Run: `python -m pytest tests/test_resilience.py::test_poison_and_healthy_coexist -v`
Expected: PASS.

- [ ] **Step 3: Run the full suite one final time**

Run: `python -m pytest -q`
Expected: all green (180 original + ~7 new resilience tests).

- [ ] **Step 4: Commit**

```bash
git add tests/test_resilience.py
git commit -m "test(resilience): integration — poison + healthy coexist, daemon survives (WS-0)"
```

---

## Self-Review notes (for the executor)

- **Spec coverage:** This plan implements spec §2 (WS-0) layers L1 (Task 2), L2 (Task 3), L3 (Task 4), and the `busy_timeout` sibling (Task 1). It deliberately does NOT implement dead-letter/backoff/health — those are later plans, and the scope note at the top says so.
- **Cross-plan dependency:** Task 2's "skip this cycle" for undecodable rows is a *temporary* behavior; the WS-2 plan upgrades it to permanent dead-letter routing. Do not delete the L1 guard when that lands — extend it.
- **Indentation risk (Task 3):** the per-table try/except wrap changes indentation of a real loop body. The `py_compile` check in Step 4 is mandatory, not optional.
- **Verify against real source line numbers:** line numbers cited (`sync.py:565/607/617`, `_runner.py:591`) were accurate at plan-writing time against commit on branch `feat/durable-ordered-retry-signal`. If they have drifted, locate by the quoted code, not the number.
- **Test count:** the suite baseline is 180 (`pytest --collect-only`); this plan adds ~7 tests. Use "all green", not a hard number, as the gate.
