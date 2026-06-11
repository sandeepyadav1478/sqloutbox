# WS-4: Single-Drain Lock, Cooperative Shutdown & Per-Target Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Guarantee exactly one drain per `db_dir` (advisory `fcntl.flock`), make routine SIGTERM finish the current cycle and confirm an already-delivered batch under `asyncio.shield` (no SIGTERM-manufactured duplicates), and ensure one bad/missing writer can neither halt sibling targets nor silently black-hole rows.

**Architecture:** Three independent lifecycle/concurrency hardenings on the standalone daemon. (1) `run_service_main` acquires an exclusive non-blocking `fcntl.flock` on `<db_dir>/.sqloutbox.lock` at startup and holds the file handle for the process lifetime — a second drain on the same dir exits(1) with a clear message; producers never take this lock. (2) `OutboxSyncService` gains a cooperative `request_stop()` that the worker checks at the **top** of each cycle (no new cycle if set), and the confirm step (`mark_synced` + `delete_synced` for an already-delivered batch) runs under `asyncio.shield` so it completes once `write_batch` returned ok; the runner sets the cooperative stop **before** falling back to `task.cancel()`. (3) The per-target drain body is already wrapped by WS-0 Layer 2; this plan adds writerless-target **fail-fast at `__init__`** (validation) so a missing writer surfaces loudly instead of silently dropping rows.

**Tech Stack:** Python 3.10+ stdlib only (sqlite3, asyncio, json, logging; plus `fcntl` — Unix-only); pytest + pytest-asyncio.

**Spec:** `docs/specs/2026-06-11-standalone-hardening-design.md` §5 — §5.1 (one drain per `db_dir`, flock), §5.2 (cooperative shutdown), §5.3 (per-target fault isolation + writerless-target fail-fast). Companion: `docs/specs/2026-06-11-durable-ordered-retry-and-health-signal.md`.

**Recommended execution order:** Per the cross-plan CONTRACT this is **Plan 4 — WS-4**, scheduled after Plan 1 (WS-0, done), Plan 3 (WS-3), and Plan 2 (WS-1+2). It must build on the following changes earlier plans already made to the shared files:
- **WS-0 (Plan 1, already merged) changed `_runner.py:591-598`** — replaced the old `task = create_task(...); await stop.wait(); task.cancel()` block with an *observe-the-drain-task* shape: `task = loop.create_task(svc.run(), name="sqloutbox.drain")`, `stop_task = loop.create_task(stop.wait(), ...)`, `await asyncio.wait({task, stop_task}, return_when=FIRST_COMPLETED)`, then `if task in done: ... raise SystemExit(1)` (worker death = loud non-zero exit) and an `else:` clean-stop branch that does `task.cancel()`. **WS-4 builds on THAT shape** — it adds the flock at startup and replaces the clean-stop branch's bare `task.cancel()` with a cooperative `svc.request_stop()` + bounded wait, falling back to `cancel()`. Do NOT re-introduce the old fire-and-forget block.
- **WS-0 (Plan 1) also wrapped the per-row body (L1, `sync.py:607-613`) and the per-table unit (L2, `sync.py:565-615`) in `_worker_loop`, and added `busy_timeout=30000` in `_schema.py`.** WS-4 §5.3 fault isolation **already exists via that L2 wrap** — this plan does NOT duplicate it; it only adds the *writerless-target fail-fast at `__init__`* that L2 does not cover. Do not re-wrap the per-target loop.
- This plan does NOT add the dead-letter table, backoff, `health()`, config validation, typed exceptions, or the grammar guard — those are owned by Plans 2/3/5/6 per the CONTRACT. WS-4 touches only `_runner.py`, `sync.py` (`OutboxSyncService.__init__` + `_worker_loop` top-of-cycle), and a new `tests/test_lifecycle.py`.

---

## File Structure

| File | Responsibility | Create/Modify |
|------|----------------|---------------|
| `src/sqloutbox/_runner.py` | Service runner. Acquire `fcntl.flock` single-drain lock at startup (held for process lifetime); replace clean-stop branch with cooperative `request_stop()` + bounded wait then cancel fallback. | Modify |
| `src/sqloutbox/sync.py` | `OutboxSyncService`: add `request_stop()` + `_stopping` event in `__init__`; check `_stopping` at the TOP of each `_worker_loop` cycle; run the confirm step (`mark_synced` + `delete_synced`) under `asyncio.shield` in `_flush_to_target`. Validate every target has a writer in `__init__` (writerless = fail-fast). | Modify |
| `tests/test_lifecycle.py` | All WS-4 tests: flock single-drain (A), cooperative shutdown + shielded confirm (B), per-target fault isolation + writerless fail-fast (C). | Create |

**Why one new test file:** WS-4's three concerns share a small set of fixtures (a recording fake writer with `auto_schema=False` + `inject_outbox_seq=False` to keep startup writes out of the recording, a tmp `db_dir`, and a runner monkeypatch harness). Keeping them in one `tests/test_lifecycle.py` makes the "one drain, clean stop, no sibling halt" property auditable in one place and mirrors the WS-0 `tests/test_resilience.py` layout.

---

## Task 1: Single-drain `fcntl.flock` lock at runservice startup (§5.1, F007/F010/F019)

`run_service_main` (`_runner.py:545`) currently starts the drain with no mutual exclusion — a double `runservice`, a blue-green overlap, or a k8s `maxSurge>0` rollout puts two drains on the same `db_dir`, double-delivering rows (especially for `inject_outbox_seq=False` targets that have no `INSERT OR IGNORE` dedupe). Acquire an exclusive non-blocking advisory lock on `<db_dir>/.sqloutbox.lock` at startup, hold the file handle for the whole process lifetime, and exit(1) with a clear message if another drain already holds it. Producers (enqueue) do NOT take this lock — only the drain.

**Files:**
- Modify: `src/sqloutbox/_runner.py` (imports near line 72-85; new helper + call site inside `run_service_main`, around line 555 after `config, writers = load_config_toml(...)`)
- Test: `tests/test_lifecycle.py` (Create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_lifecycle.py` with:

```python
"""WS-4 lifecycle: single-drain lock, cooperative shutdown, per-target isolation."""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from sqloutbox import _runner


def test_acquire_single_drain_lock_then_second_fails(tmp_path: Path):
    """First acquisition succeeds and holds; a second on the same db_dir fails."""
    handle1 = _runner.acquire_single_drain_lock(tmp_path)
    assert handle1 is not None
    # The lock file was created in the db_dir.
    assert (tmp_path / ".sqloutbox.lock").exists()

    # A second acquisition on the SAME dir must raise SystemExit(1) with a
    # clear message (the first handle is still open / lock still held).
    with pytest.raises(SystemExit) as ei:
        _runner.acquire_single_drain_lock(tmp_path)
    assert ei.value.code == 1

    # Releasing the first handle frees the lock so a later drain can re-acquire.
    _runner.release_single_drain_lock(handle1)
    handle2 = _runner.acquire_single_drain_lock(tmp_path)
    assert handle2 is not None
    _runner.release_single_drain_lock(handle2)


def test_distinct_db_dirs_do_not_contend(tmp_path: Path):
    """Two different db_dirs each get their own lock — no contention."""
    a = _runner.acquire_single_drain_lock(tmp_path / "a")
    b = _runner.acquire_single_drain_lock(tmp_path / "b")
    assert a is not None and b is not None
    _runner.release_single_drain_lock(a)
    _runner.release_single_drain_lock(b)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_lifecycle.py -v -k drain_lock`
Expected: FAIL at collection/attribute access — `AttributeError: module 'sqloutbox._runner' has no attribute 'acquire_single_drain_lock'` (the helpers do not exist yet).

- [ ] **Step 3: Add the flock helpers and the lock constant**

In `src/sqloutbox/_runner.py`, add `fcntl` to the imports. The current import block (around lines 74-85) is:

```python
import asyncio
import importlib
import json
import logging
import os
import re
import signal
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
```

Replace it with (adds a guarded `fcntl` import — `fcntl` is Unix-only; supported platforms are macOS + Ubuntu, but never crash the import on a non-POSIX host):

```python
import asyncio
import importlib
import json
import logging
import os
import re
import signal
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

try:
    import fcntl  # Unix-only (macOS + Ubuntu are the supported platforms)
except ImportError:  # pragma: no cover — Windows / non-POSIX
    fcntl = None  # type: ignore[assignment]
```

Then, immediately after the `DEFAULT_CONFIG_FILE = "outbox.toml"` line (currently line 89), add the lock filename constant and the two helper functions:

```python
# Advisory single-drain lock. Held for the process lifetime so exactly one
# `sqloutbox runservice` drains a given db_dir. Producers (enqueue) never take
# this lock — only the drain service. Prevents double-delivery from an
# accidental double-start, blue-green overlap, or k8s maxSurge>0.
LOCK_FILENAME = ".sqloutbox.lock"


def acquire_single_drain_lock(db_dir: Path):
    """Acquire an exclusive, non-blocking advisory lock on ``<db_dir>/.sqloutbox.lock``.

    Returns the open file handle (KEEP it for the process lifetime — closing it
    releases the lock). If another drain already holds the lock, logs a clear
    message and raises ``SystemExit(1)``.

    On non-POSIX platforms (no ``fcntl``) this is a no-op that returns ``None``
    and WARNs once — single-drain enforcement is unavailable there.
    """
    db_dir.mkdir(parents=True, exist_ok=True)
    lock_path = db_dir / LOCK_FILENAME

    if fcntl is None:  # pragma: no cover — Windows / non-POSIX
        logger.warning(
            "fcntl unavailable on this platform — single-drain lock NOT enforced "
            "for %s. Ensure exactly one drain runs per db_dir by other means.",
            db_dir,
        )
        return None

    # Open (or create) the lock file and keep the handle open. flock is tied to
    # the open file description, so the handle must outlive this function.
    handle = open(lock_path, "w")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        logger.critical(
            "another drain is already running on %s "
            "(lock held: %s) — refusing to start a second drain",
            db_dir, lock_path,
        )
        raise SystemExit(1)
    logger.info("acquired single-drain lock: %s", lock_path)
    return handle


def release_single_drain_lock(handle) -> None:
    """Release the single-drain lock by closing its file handle (no-op if None)."""
    if handle is None:
        return
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        handle.close()
    except Exception:
        pass
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_lifecycle.py -v -k drain_lock`
Expected: PASS (`test_acquire_single_drain_lock_then_second_fails`). Also run the sibling: `python -m pytest tests/test_lifecycle.py -v -k db_dirs` → PASS.

- [ ] **Step 5: Wire the lock into `run_service_main` (held for process lifetime, released on exit)**

In `src/sqloutbox/_runner.py`, `run_service_main` currently begins:

```python
    from sqloutbox.sync import OutboxSyncService

    config, writers = load_config_toml(config_path)
    svc = OutboxSyncService(config=config, writers=writers)
```

Replace those four lines with (acquire the lock right after config load, before constructing the service, and wrap the whole run in try/finally so the lock is released on any exit):

```python
    from sqloutbox.sync import OutboxSyncService

    config, writers = load_config_toml(config_path)

    # WS-4 §5.1: exactly one drain per db_dir. Held for the process lifetime;
    # released in the finally below. A second drain on the same dir exits(1).
    lock_handle = acquire_single_drain_lock(config.db_dir)
    try:
        svc = OutboxSyncService(config=config, writers=writers)
        await _run_service_body(config_path, config, svc)
    finally:
        release_single_drain_lock(lock_handle)
```

Now extract everything that previously followed (the logging banner, signal handlers, and the observe-drain-task block that WS-0 installed) into a new coroutine `_run_service_body`. Find the WS-0 block that currently runs from the logging banner through the clean-stop branch (the lines from `logger.info("config=%s ...")` down through the final `logger.info("stopped")`). Move that entire body verbatim into a new function defined immediately above `run_service_main`:

```python
async def _run_service_body(
    config_path: Path, config: Any, svc: Any,
) -> None:
    """Banner + signal handlers + observe-the-drain-task loop (WS-0 shape).

    Split out of run_service_main so the single-drain lock (WS-4) can wrap the
    whole run in try/finally without re-indenting the WS-0 task-watch logic.
    """
    logger.info(
        "config=%s  poll=%.1fs  threshold=%d  max_wait=%.1fs",
        config_path,
        config.flush_interval, config.table_flush_threshold,
        config.table_max_wait,
    )
    for target in config.targets:
        db_dir = target.db_dir or config.db_dir
        db_files = [f"{table}.db" for table in target.tables]
        logger.info(
            "  target '%s'  db_dir=%s  batch=%d  .db files: %s",
            target.name, db_dir, target.batch_size, db_files,
        )

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def _on_signal(*_: object) -> None:
        logger.info("shutdown signal received")
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    # SIGUSR1 triggers integrity verification (Unix only)
    if hasattr(signal, "SIGUSR1"):
        def _on_verify(*_: object) -> None:
            logger.info("SIGUSR1 received — requesting integrity verification")
            svc._verify_requested.set()

        loop.add_signal_handler(signal.SIGUSR1, _on_verify)

    task = loop.create_task(svc.run(), name="sqloutbox.drain")
    stop_task = loop.create_task(stop.wait(), name="sqloutbox.stop")

    done, _pending = await asyncio.wait(
        {task, stop_task}, return_when=asyncio.FIRST_COMPLETED,
    )

    if task in done:
        # The drain exited on its own — always a fault (the worker loop is
        # infinite; it only returns/raises on error). Surface it LOUDLY so a
        # supervisor restarts us instead of lingering as a zombie.
        stop_task.cancel()
        exc = task.exception()
        logger.critical("drain worker exited unexpectedly: %r", exc)
        raise SystemExit(1)

    # Normal path: a stop signal arrived. WS-4 §5.2: ask the worker to stop
    # cooperatively (finish the current cycle + shielded confirm), wait briefly,
    # then cancel as a backstop if it does not return in time.
    svc.request_stop()
    try:
        await asyncio.wait_for(task, timeout=config.flush_interval + 5.0)
    except asyncio.TimeoutError:
        logger.warning(
            "drain did not stop within grace period — cancelling",
        )
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    except asyncio.CancelledError:
        pass
    logger.info("stopped")
```

> Note: `_run_service_body` references `svc.request_stop()` (added to `OutboxSyncService` in Task 2 below). For the grace-period timeout it reads `config.flush_interval` (NOT `svc._flush_interval`) — `config` is already a parameter here, the banner above already uses `config.flush_interval`, and crucially the WS-0 `_LiveService` test fake (Task 2 Step 5) and the `_FakeConfig` fake both expose `flush_interval` but NOT a private `_flush_interval`, so reading the public config value keeps both runner tests green. If Task 2 has not yet been implemented when this step runs, the runner-clean-stop test (Task 2, Step 5) will be the one that exercises `request_stop()`. The flock tests in this task do not call `request_stop()`. Run `python -m py_compile src/sqloutbox/_runner.py` after editing to confirm valid syntax.

- [ ] **Step 6: Verify syntax and run the runner regression tests**

Run: `python -m py_compile src/sqloutbox/_runner.py && python -m pytest tests/test_runner.py tests/test_resilience.py -q`
Expected: compile OK; all existing runner + WS-0 resilience tests still green (the WS-0 `test_runner_exits_nonzero_when_worker_dies` and `test_runner_clean_stop_does_not_raise` still pass because the observe-task shape is preserved inside `_run_service_body`, and `_FakeConfig` must now also expose `db_dir`).
>
> **POLLUTION FIX REQUIRED (do in Task 2 Step 5, same `test_resilience.py` edit):** the WS-0 `_FakeConfig` defines `db_dir = Path("/tmp")`. Now that `run_service_main` calls `acquire_single_drain_lock(config.db_dir)`, both WS-0 runner tests would take a REAL exclusive `flock` on the shared host path `/tmp/.sqloutbox.lock`. That is cross-test pollution (xdist contention, a leftover handle from a crashed run blocking the next, and writing into a shared system dir). The `finally: release_single_drain_lock(...)` in `run_service_main` releases it within one test, but the shared path is still wrong. Repoint `_FakeConfig.db_dir` to an isolated per-test dir. Because both runner tests already receive the `tmp_path` fixture, set it on the instance inside each test before constructing — e.g. in each test add `cfg = _FakeConfig(); cfg.db_dir = tmp_path` and make the `load_config_toml` monkeypatch return `(cfg, {})` — OR change the class default to a unique temp dir (`db_dir = Path(tempfile.mkdtemp())`). Do NOT leave it pointing at `/tmp`.

> If `test_runner_clean_stop_does_not_raise` (from WS-0) now fails because it expected a bare `task.cancel()` and the body now calls `svc.request_stop()` on a `_LiveService` that has no such method, that is expected at this point — it is fixed when Task 2 adds `request_stop()` to the real service and the WS-0 fake gains the method. Defer that failure to Task 2 Step 5; do NOT weaken the runner here. If you are running tasks strictly in order, run only `tests/test_lifecycle.py -k drain_lock` and `tests/test_runner.py` (the config-loader tests, which do not invoke `run_service_main`) for this task's green gate, and complete the full-suite gate after Task 2.

- [ ] **Step 7: Commit**

```bash
git add src/sqloutbox/_runner.py tests/test_lifecycle.py
git commit -m "feat(lifecycle): single-drain fcntl.flock per db_dir at runservice startup (WS-4, F007/F010/F019)"
```

---

## Task 2: Cooperative shutdown — stop at top of cycle + shielded confirm (§5.2, F018/F055)

Today (post-WS-0) a SIGTERM cancels the drain task, which injects `CancelledError` at the next `await` — possibly **between** `write_batch()` (delivery succeeded) and `delete_synced()` (local cleanup), causing redelivery on restart. WS-4 makes shutdown cooperative: the worker checks a `_stopping` flag at the **top** of each cycle (no new cycle once set), and the confirm step (`mark_synced` + `delete_synced` for an *already-delivered* batch) runs under `asyncio.shield` so it always completes once `write_batch` returned ok. At-least-once stays the honest guarantee (a crash can still land in the gap) — the goal is that **routine SIGTERM no longer manufactures duplicates.**

**Files:**
- Modify: `src/sqloutbox/sync.py` — `OutboxSyncService.__init__` (around line 210, add `_stopping`); add `request_stop()` method; `_worker_loop` top-of-cycle check (around line 528-529); `_flush_to_target` confirm step (lines 674-679, wrap in `asyncio.shield`)
- Test: `tests/test_lifecycle.py`
- Test (fix WS-0 fake): `tests/test_resilience.py` (`_LiveService` gains a no-op `request_stop`)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_lifecycle.py`:

```python
import json

from sqloutbox._outbox import Outbox
from sqloutbox.config import OutboxConfig, TargetConfig
from sqloutbox.sync import OutboxSyncService


class _SlowConfirmWriter:
    """Writer that delivers instantly but lets us assert the confirm completes.

    write_batch records what it delivered and returns ok for everything.
    """
    def __init__(self) -> None:
        self.delivered: list[tuple[str, list]] = []

    async def write_batch(self, stmts):
        self.delivered.extend(stmts)
        return [{"ok": True} for _ in stmts]


def _make_recording_service(tmp_path: Path, writer, *, table: str):
    # auto_schema=False + inject_outbox_seq=False so run() does NOT call
    # write_batch() during _ensure_schema()/_seed_from_remote() at startup —
    # those startup calls would otherwise pollute writer.delivered.
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


@pytest.mark.asyncio
async def test_request_stop_ends_loop_at_top_of_cycle(tmp_path: Path):
    """request_stop() makes the worker return cleanly (no new cycle, no error)."""
    writer = _SlowConfirmWriter()
    svc, _cfg = _make_recording_service(tmp_path, writer, table="evt")

    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.1)        # let it run a few cycles
    svc.request_stop()
    # The worker returns on its own (top-of-cycle check) — NOT cancelled.
    await asyncio.wait_for(task, timeout=2.0)
    assert task.done() and task.exception() is None


@pytest.mark.asyncio
async def test_confirm_completes_then_no_redelivery_after_restart(tmp_path: Path):
    """Stop set after write returns ok → confirm (mark+delete) still runs →
    the row is gone from the local queue → a restarted drain re-delivers nothing."""
    writer = _SlowConfirmWriter()
    svc, cfg = _make_recording_service(tmp_path, writer, table="evt")

    # One healthy row to deliver.
    Outbox(db_path=tmp_path / "evt.db", namespace="evt").enqueue(
        "INSERT INTO evt (a) VALUES (?)", json.dumps([1]).encode()
    )

    task = asyncio.create_task(svc.run())
    # Wait until the row was delivered (write_batch ran) AND confirmed (row
    # deleted from the local queue), then stop.
    for _ in range(200):
        await asyncio.sleep(0.01)
        if writer.delivered and Outbox(
            db_path=tmp_path / "evt.db", namespace="evt"
        ).pending_count() == 0:
            break
    svc.request_stop()
    await asyncio.wait_for(task, timeout=2.0)

    # Delivered exactly once and the local queue is empty (confirm completed).
    assert len([s for s, _ in writer.delivered if "evt" in s]) == 1
    assert Outbox(db_path=tmp_path / "evt.db", namespace="evt").pending_count() == 0

    # Simulate a restart: a fresh drain over the same dir delivers NOTHING new
    # (the row was deleted by the confirm before shutdown — no redelivery).
    writer2 = _SlowConfirmWriter()
    svc2, _ = _make_recording_service(tmp_path, writer2, table="evt")
    task2 = asyncio.create_task(svc2.run())
    await asyncio.sleep(0.1)
    svc2.request_stop()
    await asyncio.wait_for(task2, timeout=2.0)
    assert writer2.delivered == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_lifecycle.py -v -k "request_stop or redelivery"`
Expected: FAIL — `AttributeError: 'OutboxSyncService' object has no attribute 'request_stop'` (the method does not exist yet).

- [ ] **Step 3: Add `_stopping` + `request_stop()`, check it at the top of the cycle, and shield the confirm**

In `src/sqloutbox/sync.py`, `OutboxSyncService.__init__` currently ends its first block with:

```python
        self._config = config
        self._writers = writers
        self._flush_interval = config.flush_interval
        self._cycle_count = 0
```

Replace that with (add the cooperative-stop flag):

```python
        self._config = config
        self._writers = writers
        self._flush_interval = config.flush_interval
        self._cycle_count = 0

        # WS-4 §5.2: cooperative shutdown. request_stop() sets this; the worker
        # checks it at the TOP of each cycle and returns cleanly (no new cycle).
        self._stopping = asyncio.Event()
```

Next, add the `request_stop()` method. Place it immediately after the `run()` method (after the line `await self._worker_loop()`, around line 280) and before the `# ── Schema setup ──` section comment:

```python
    def request_stop(self) -> None:
        """Ask the worker to stop after the current cycle finishes.

        WS-4 §5.2: cooperative shutdown. The worker checks this flag at the TOP
        of each cycle and returns cleanly — it never starts a new cycle once set.
        An in-flight cycle's confirm step (mark_synced + delete_synced for an
        already-delivered batch) runs under asyncio.shield so it completes even
        if a cancel arrives. At-least-once is still the honest guarantee; this
        only prevents ROUTINE SIGTERM from manufacturing duplicates.
        """
        self._stopping.set()
```

Now make `_worker_loop` honor it. The loop currently starts (around line 528):

```python
        while True:
            await asyncio.sleep(self._flush_interval)
```

Replace those two lines with (check at the TOP of the cycle — before sleeping into a new cycle):

```python
        while True:
            if self._stopping.is_set():
                logger.info(
                    "[outbox_sync] stop requested — worker loop exiting cleanly",
                )
                return
            await asyncio.sleep(self._flush_interval)
```

Finally, shield the confirm step in `_flush_to_target`. The confirm loop currently reads (lines 674-679):

```python
        total_confirmed = 0
        for table, seqs in confirmed_by_table.items():
            outbox = outboxes[table]
            await asyncio.to_thread(outbox.mark_synced, seqs)
            await asyncio.to_thread(outbox.delete_synced, seqs)
            total_confirmed += len(seqs)
```

Replace it with (wrap the per-table mark+delete in `asyncio.shield` so a cancel during shutdown cannot interrupt the confirm of an already-delivered batch):

```python
        total_confirmed = 0
        for table, seqs in confirmed_by_table.items():
            outbox = outboxes[table]
            # WS-4 §5.2: write_batch already returned ok for these seqs — the
            # remote has the rows. Confirm locally under shield so a shutdown
            # cancel cannot land BETWEEN delivery and local cleanup (which would
            # redeliver on restart). Shield protects the await from cancellation.
            async def _confirm(ob=outbox, ss=seqs):
                await asyncio.to_thread(ob.mark_synced, ss)
                await asyncio.to_thread(ob.delete_synced, ss)

            await asyncio.shield(_confirm())
            total_confirmed += len(seqs)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_lifecycle.py -v -k "request_stop or redelivery"`
Expected: PASS (2 passed).

- [ ] **Step 5: Fix the WS-0 runner fake so the clean-stop test still passes**

The WS-0 `tests/test_resilience.py::test_runner_clean_stop_does_not_raise` uses a `_LiveService` stub that the runner now calls `request_stop()` on (via `_run_service_body`). Give the WS-0 fake a no-op `request_stop` so the cooperative path works. In `tests/test_resilience.py`, locate the `_LiveService` class inside `test_runner_clean_stop_does_not_raise`:

```python
    class _LiveService:
        async def run(self):
            started.set()
            while True:
                await asyncio.sleep(0.01)
```

Replace it with (add `request_stop` that ends the `run()` loop, mirroring the real service's cooperative stop):

```python
    class _LiveService:
        def __init__(self):
            self._stop = False

        def request_stop(self):
            self._stop = True

        async def run(self):
            started.set()
            while not self._stop:
                await asyncio.sleep(0.01)
```

> Why: `_run_service_body`'s clean-stop branch now does `svc.request_stop()` then `await asyncio.wait_for(task, ...)`. The fake's `run()` must return when `request_stop()` is called so the wait completes without the cancel backstop. The runner test asserts no `SystemExit` and that it finishes within the timeout — both hold once the fake honors `request_stop()`.

Also in `tests/test_resilience.py`, repoint the WS-0 `_FakeConfig.db_dir` off the shared host path `/tmp` (flagged in Task 1 Step 6). `run_service_main` now calls `acquire_single_drain_lock(config.db_dir)`, so a class-level `db_dir = Path("/tmp")` makes BOTH runner tests take a real exclusive `flock` on `/tmp/.sqloutbox.lock` — a shared-path collision risk. Locate the `_FakeConfig` class (WS-0 defined it as):

```python
class _FakeConfig:
    """Minimal stand-in for the loaded config (only what run_service_main reads)."""
    flush_interval = 1.0
    table_flush_threshold = 15
    table_max_wait = 6.0
    db_dir = Path("/tmp")
    targets = ()
```

Give it an isolated, unique lock dir instead of `/tmp` (add `import tempfile` at the top of `test_resilience.py` if not already present):

```python
class _FakeConfig:
    """Minimal stand-in for the loaded config (only what run_service_main reads)."""
    flush_interval = 1.0
    table_flush_threshold = 15
    table_max_wait = 6.0
    db_dir = Path(tempfile.mkdtemp(prefix="sqloutbox-faketest-"))
    targets = ()
```

Each `run_service_main` call then locks `<unique tmpdir>/.sqloutbox.lock`, released by the runner's `finally` — no contention, no writes into a shared system dir. (`_FakeConfig` exposes `flush_interval` but no `_flush_interval`; the runner reads `config.flush_interval` for its grace timeout, so this fake needs no `_flush_interval` attribute.)

- [ ] **Step 6: Run the WS-0 runner tests + the full suite**

Run: `python -m pytest tests/test_resilience.py -v -k runner && python -m pytest -q`
Expected: both WS-0 runner tests PASS; full suite all green (180 original + WS-0 tests + new WS-4 tests — gate on "all green", not a number).

- [ ] **Step 7: Commit**

```bash
git add src/sqloutbox/sync.py tests/test_lifecycle.py tests/test_resilience.py
git commit -m "feat(lifecycle): cooperative shutdown — stop at top of cycle, shield confirm (WS-4, F018/F055)"
```

---

## Task 3: Writerless-target fail-fast at `__init__` (§5.3, F020)

Per-target fault isolation (one bad writer cannot halt sibling targets) is **already provided** by the WS-0 Layer-2 per-table wrap in `_worker_loop` — this plan does NOT duplicate it. What WS-0 does NOT cover is a target configured with **no writer at all**: today `_worker_loop` silently does `if not writer: continue` (`sync.py:557-559`), so a misconfigured target black-holes every row with no signal. WS-4 §5.3 makes a writerless target **fail fast at `__init__`** so the misconfiguration surfaces loudly at startup instead of silently dropping data forever. (We add a small `test_corrupt_writer_isolated_siblings_drain` proving the WS-0 sibling-isolation property holds end-to-end through this path, so the §5.3 guarantee is auditable in `test_lifecycle.py` too.)

**Files:**
- Modify: `src/sqloutbox/sync.py` — `OutboxSyncService.__init__` (add writer-presence validation after `self._writers = writers`, before building `_target_outboxes`)
- Test: `tests/test_lifecycle.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_lifecycle.py`:

```python
import sqlite3


def test_writerless_target_fails_fast_at_init(tmp_path: Path):
    """A target with no matching writer raises ValueError at construction —
    it never silently black-holes rows."""
    cfg = OutboxConfig(
        db_dir=tmp_path,
        targets=(
            TargetConfig(name="primary", tables=("evt",), inject_outbox_seq=False),
            TargetConfig(name="missing", tables=("other",), inject_outbox_seq=False),
        ),
        auto_schema=False,
    )
    with pytest.raises(ValueError) as ei:
        OutboxSyncService(config=cfg, writers={"primary": _SlowConfirmWriter()})
    # The error names the offending target so the misconfiguration is obvious.
    assert "missing" in str(ei.value)


def test_all_targets_have_writers_constructs_ok(tmp_path: Path):
    """When every target has a writer, construction succeeds (no false positive)."""
    cfg = OutboxConfig(
        db_dir=tmp_path,
        targets=(TargetConfig(name="primary", tables=("evt",),
                              inject_outbox_seq=False),),
        auto_schema=False,
    )
    svc = OutboxSyncService(
        config=cfg, writers={"primary": _SlowConfirmWriter()},
    )
    assert svc is not None


class _ExplodingWriter:
    """Writer whose write_batch always raises — simulates a broken target."""
    async def write_batch(self, stmts):
        raise RuntimeError("writer exploded")


@pytest.mark.asyncio
async def test_broken_writer_isolated_siblings_drain(tmp_path: Path):
    """One target whose writer always raises must not halt a sibling target.

    (Proves the WS-0 Layer-2 isolation property holds through the per-target
    flush boundary — the broken target's write failure is caught and the
    healthy sibling still delivers.)"""
    good = _SlowConfirmWriter()
    bad = _ExplodingWriter()
    cfg = OutboxConfig(
        db_dir=tmp_path,
        targets=(
            TargetConfig(name="broken", tables=("bad",), inject_outbox_seq=False),
            TargetConfig(name="healthy", tables=("good",), inject_outbox_seq=False),
        ),
        flush_interval=0.01,
        table_flush_threshold=1,
        table_max_wait=0.0,
        auto_schema=False,
    )
    svc = OutboxSyncService(
        config=cfg, writers={"broken": bad, "healthy": good},
    )

    Outbox(db_path=tmp_path / "bad.db", namespace="bad").enqueue(
        "INSERT INTO bad (a) VALUES (?)", json.dumps([1]).encode()
    )
    Outbox(db_path=tmp_path / "good.db", namespace="good").enqueue(
        "INSERT INTO good (a) VALUES (?)", json.dumps([2]).encode()
    )

    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.2)
    svc.request_stop()
    await asyncio.wait_for(task, timeout=2.0)

    # Healthy sibling delivered despite the broken target raising every cycle.
    assert any("good" in s for s, _ in good.delivered)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_lifecycle.py -v -k "writerless or all_targets or broken_writer"`
Expected: FAIL — `test_writerless_target_fails_fast_at_init` does NOT raise (today `__init__` accepts a writerless target; the worker silently `continue`s on it), so `pytest.raises(ValueError)` fails. The other two may already pass (sibling isolation is WS-0; construction with full writers is fine).

- [ ] **Step 3: Validate writer presence in `__init__`**

In `src/sqloutbox/sync.py`, `OutboxSyncService.__init__` currently has (after the WS-4 `_stopping` addition from Task 2):

```python
        # WS-4 §5.2: cooperative shutdown. request_stop() sets this; the worker
        # checks it at the TOP of each cycle and returns cleanly (no new cycle).
        self._stopping = asyncio.Event()

        # Verification support — request_verify() sets the event,
        # worker loop checks it between drain cycles.
        self._verify_requested = asyncio.Event()
```

Insert the writer-presence validation between the `_stopping` line and the verification-support block:

```python
        # WS-4 §5.2: cooperative shutdown. request_stop() sets this; the worker
        # checks it at the TOP of each cycle and returns cleanly (no new cycle).
        self._stopping = asyncio.Event()

        # WS-4 §5.3: fail fast on a writerless target. Without a writer the
        # worker would silently `continue` past it forever — every row to that
        # target black-holed with no signal. Surface the misconfiguration at
        # construction instead. (Empty targets is fine — middleware-only use.)
        missing = [t.name for t in config.targets if t.name not in writers]
        if missing:
            raise ValueError(
                "OutboxSyncService: no writer provided for target(s) "
                f"{missing}. Every config.targets entry must have a matching "
                f"key in `writers`. Provided writers: {sorted(writers)}."
            )

        # Verification support — request_verify() sets the event,
        # worker loop checks it between drain cycles.
        self._verify_requested = asyncio.Event()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_lifecycle.py -v -k "writerless or all_targets or broken_writer"`
Expected: PASS (3 passed).

- [ ] **Step 5: Run the full suite — check for fixtures that relied on writerless targets**

Run: `python -m pytest -q`
Expected: all green. If any existing test constructs `OutboxSyncService` with a target that has no matching writer (relying on the old silent-skip), it will now raise `ValueError` at construction. That is the intended behavior change (§5.3). Inspect any failure: if a real test legitimately needs a target with no writer, it was depending on the silent black-hole — update that test to provide a writer (or drop the writerless target from its config), and note it in the commit. Do NOT relax the validation. Per the WS-0 lesson, a fake writer asserting on delivered statements must use `auto_schema=False` + `inject_outbox_seq=False`; those fixtures already pass a writer for every target.

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/sync.py tests/test_lifecycle.py
git commit -m "feat(lifecycle): writerless target fails fast at __init__ (WS-4, F020)"
```

---

## Task 4: Full-suite green + non-POSIX guard documentation pass

A final consolidation: confirm the full suite is green and that the `fcntl`-unavailable path is exercised/documented so the supported-platforms note (macOS + Ubuntu only; Windows = no enforcement) is honest.

**Files:**
- Test: `tests/test_lifecycle.py`

- [ ] **Step 1: Write the non-POSIX guard test**

Append to `tests/test_lifecycle.py`:

```python
def test_acquire_lock_noop_when_fcntl_unavailable(tmp_path: Path, monkeypatch):
    """On a platform without fcntl, the lock is a no-op (returns None) and WARNs —
    single-drain is not enforced, matching the documented Windows behavior."""
    monkeypatch.setattr(_runner, "fcntl", None)
    handle = _runner.acquire_single_drain_lock(tmp_path)
    assert handle is None
    # release on None is a safe no-op
    _runner.release_single_drain_lock(handle)
```

- [ ] **Step 2: Run the test**

Run: `python -m pytest tests/test_lifecycle.py::test_acquire_lock_noop_when_fcntl_unavailable -v`
Expected: PASS.

- [ ] **Step 3: Run the entire WS-4 file, then the full suite**

Run: `python -m pytest tests/test_lifecycle.py -v`
Expected: all WS-4 tests PASS.

Run: `python -m pytest -q`
Expected: all green (180 original + WS-0 resilience tests + WS-4 lifecycle tests). Gate on "all green", not an exact number.

- [ ] **Step 4: Commit**

```bash
git add tests/test_lifecycle.py
git commit -m "test(lifecycle): non-POSIX flock no-op guard; WS-4 suite green (WS-4)"
```

---

## Self-Review notes (for the executor)

- **Spec coverage:** This plan implements spec §5 (WS-4): §5.1 single-drain flock (Task 1), §5.2 cooperative shutdown + shielded confirm (Task 2), §5.3 writerless-target fail-fast (Task 3) plus a non-POSIX guard pass (Task 4). It does NOT implement dead-letter, backoff, `health()`, config validation, typed exceptions, the grammar guard, or read-only verify — those are Plans 2/3/5/6 per the CONTRACT. Do not add them here.
- **Build on WS-0, do not duplicate:** the per-target/per-table fault isolation (§5.3 first bullet) is the WS-0 Layer-2 wrap already in `_worker_loop`; this plan adds ONLY the writerless-target fail-fast at `__init__`. `busy_timeout` is already added by WS-0 — do not re-add. The runner's observe-drain-task + `SystemExit(1)` shape is from WS-0 — Task 1 moves it verbatim into `_run_service_body` and Task 2 swaps the clean-stop branch's bare `cancel()` for cooperative `request_stop()` + bounded wait + cancel-backstop. Do not reintroduce the old fire-and-forget `await stop.wait(); task.cancel()` block.
- **Cross-task ordering inside this plan:** Task 1 wires `_run_service_body` to call `svc.request_stop()`, which Task 2 adds to the service. If you execute strictly task-by-task, Task 1's runner-clean-stop gate is deferred to Task 2 Step 5 (which also fixes the WS-0 `_LiveService` fake). Task 1's own green gate is the flock tests + the config-loader tests in `test_runner.py` (those never call `run_service_main`). This is flagged in Task 1 Step 6.
- **Honest guarantee:** at-least-once is preserved — a hard crash between `write_batch` ok and the shielded confirm can still redeliver. The shield only defends against the SHUTDOWN cancel path, which is the routine-SIGTERM duplicate source. The Task 2 redelivery test asserts no duplicate after a *graceful* stop, not after a crash.
- **No flaky signal tests:** Task 2 drives the cooperative stop via `svc.request_stop()` directly (deterministic). It never sends a real OS signal. The runner clean-stop path is exercised by the WS-0 capture-the-handler pattern in `test_resilience.py` (untouched except the fake gaining `request_stop`).
- **Fixture hygiene (WS-0 lesson 1):** every recording-writer fixture in `test_lifecycle.py` uses `auto_schema=False` + `TargetConfig(..., inject_outbox_seq=False)` so `_ensure_schema()`/`_seed_from_remote()` do not call `write_batch()` at startup and pollute `writer.delivered`. Keep that or the redelivery/sibling assertions become wrong.
- **Symbol grounding:** `acquire_single_drain_lock` / `release_single_drain_lock` / `LOCK_FILENAME` / `_run_service_body` are NEW in this plan (Task 1). `request_stop` / `_stopping` are NEW on `OutboxSyncService` (Task 2). The following are verified against the CURRENT source tree (head `51771a4`): `_flush_interval` (sync.py:209), `_verify_requested` (sync.py:214), `_target_outboxes` (sync.py:221), `Outbox.pending_count`/`enqueue` (_outbox.py:348/68), `config.db_dir`/`config.flush_interval`/`config.auto_schema` (config.py:184/187/190), `TargetConfig.inject_outbox_seq` (config.py:106), `run_service_main`/`load_config_toml`/`OutboxSyncService(config=,writers=)` (existing constructor signature). **`tests/test_resilience.py` and its `_FakeConfig`/`_LiveService` fakes, the observe-drain-task shape in `_runner.py:591-598`, the L1/L2 `_worker_loop` wraps, and `_schema.py` `busy_timeout` do NOT yet exist in the current source — they are CREATED BY Plan 1 (WS-0), which runs BEFORE this plan per the execution order. This plan builds on those WS-0 artifacts; verify WS-0 is merged before executing WS-4.** `fcntl` is guarded-imported (Unix-only).
- **Verify against real line numbers:** cited numbers (`_runner.py:545/572/591`, `sync.py:202/209/210/214/528/557/674`) were accurate at plan-writing time on branch state with `git log` head `51771a4`. If they have drifted, locate by the quoted code, not the number.
