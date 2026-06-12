# WS-3 Safety Rails Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lay the foundation safety rails for the standalone hardening release — a typed exception hierarchy, fail-at-construction config validation, an opt-in `QueueFullError` backpressure cap, a string-literal-aware `inject_outbox_seq` grammar guard, and SQLite variable-limit chunking — so every later workstream (dead-letter, single-drain, verify) builds on typed errors and validated config.

**Architecture:** A new `exceptions.py` module defines the `SqloutboxError` tree (`ConfigError`, `QueueFullError`, `UnsupportedStatementError`, `ChainIntegrityError`) and is re-exported from the package root. The two frozen dataclasses in `config.py` gain `max_attempts` / `max_pending` / `max_batch_bytes` fields plus `__post_init__` validation that raises `ConfigError(field, value, reason)` before any object escapes the constructor. `Outbox.enqueue()` gains an opt-in cap that raises `QueueFullError` only when `max_pending` is set (default path unchanged, never raises). `inject_outbox_seq()` gains a conservative lexer that accepts ONLY single-row `INSERT … VALUES (?,…)` and `UPDATE … SET …=? WHERE …`, rejecting everything else with `UnsupportedStatementError`. `mark_synced` / `delete_synced` chunk their seq lists to ≤900 per `IN(...)` statement.

**Tech Stack:** Python 3.10+ stdlib only (sqlite3, asyncio, json, logging); pytest + pytest-asyncio.

**Spec:** `docs/specs/2026-06-11-standalone-hardening-design.md` §4 — §4.1 config validation (L159-174), §4.2 opt-in backpressure D2 + the 80% stop-producing watermark DOC (L176-229), §4.3 `inject_outbox_seq` grammar guard D3 (L231-245), §4.4 SQLite variable-limit chunking + the typed-exception hierarchy (L247-258). Companion to `docs/specs/2026-06-11-durable-ordered-retry-and-health-signal.md`.

**Recommended execution order:** This is **Plan 3 (WS-3)**, run RIGHT AFTER Plan 1 (WS-0). Per the locked cross-plan contract the order is: WS-0 → **WS-3 (this plan)** → WS-1+2 → WS-4 → WS-5 → WS-6+7. WS-3 is the FOUNDATION plan: it creates the typed-exception hierarchy and the new config fields that WS-2 (dead-letter) and WS-5 (forked-chain) consume. **What Plan 1 (WS-0) already changed in shared files before this plan runs:** in `src/sqloutbox/_schema.py` it added a `_BUSY_TIMEOUT_MS = 30_000` constant and `PRAGMA busy_timeout=30000` to both `open_write_conn()` and `thread_conn()` — **do NOT re-add `busy_timeout`**. In `src/sqloutbox/sync.py` it wrapped the per-row decode/transform body (L1) and the per-table drain unit (L2) in `_worker_loop`. In `src/sqloutbox/_runner.py` it made the drain task observable (L3, `SystemExit(1)` on worker death). It also created `tests/test_resilience.py`. **Build on those; do not duplicate them.** If line numbers below have drifted because WS-0 inserted lines, locate the code by the quoted snippet, not the number.

> **Baseline gate:** the suite is **180 tests** at the WS-0 plan's stated baseline; WS-0 adds ~7. Do NOT gate on a hard number — gate on **"all green"**. Confirm with `python -m pytest --collect-only -q` before you start so you know your local starting count.

> **`asyncio_mode = "auto"`** is set in `pyproject.toml`, so `@pytest.mark.asyncio` markers are optional but harmless. The existing `tests/test_sync.py` uses async tests with no marker; this plan keeps `@pytest.mark.asyncio` on async tests to match `tests/test_resilience.py` (created by WS-0). Both styles work.

---

## File Structure

| File | Responsibility | Create/Modify |
|------|----------------|---------------|
| `src/sqloutbox/exceptions.py` | New typed exception hierarchy: `SqloutboxError` + `ConfigError`, `QueueFullError`, `UnsupportedStatementError`, `ChainIntegrityError`. | Create |
| `src/sqloutbox/__init__.py` | Re-export the five new exception classes from the package root. | Modify |
| `src/sqloutbox/config.py` | Add `max_attempts` / `max_pending` / `max_batch_bytes` to BOTH frozen dataclasses + `__post_init__` validation raising `ConfigError`. | Modify |
| `src/sqloutbox/_outbox.py` | Add opt-in `max_pending` cap to `Outbox.__init__` + `QueueFullError` gate in `enqueue()`; chunk `mark_synced` / `delete_synced` seq lists to ≤900. | Modify |
| `src/sqloutbox/sync.py` | Add the conservative string-literal-aware grammar guard to `inject_outbox_seq()`. | Modify |
| `README.md` | Document the supported `inject_outbox_seq` grammar (Writer class section) and the 80% stop-producing watermark as a PRODUCING-APPLICATION policy (Limitations/Architecture). | Modify |
| `tests/test_exceptions.py` | Tests for the exception hierarchy + attribute payloads (Task 1). | Create |
| `tests/test_config.py` | Add validation tests for each out-of-range field (Task 2). | Modify |
| `tests/test_safety_rails.py` | Tests for backpressure (Task 3), grammar guard (Task 4), var-limit chunking (Task 5), watermark docstring (Task 6). | Create |

---

## Task 1: Typed exception hierarchy

Create the `SqloutboxError` tree exactly as the CONTRACT and spec §4.4 (L251-258) require. `ConfigError` carries `(field, value, reason)`; `QueueFullError` carries `(namespace, max_pending)`. `UnsupportedStatementError` and `ChainIntegrityError` are plain `SqloutboxError` subclasses (their callers — Plan 5 for `ChainIntegrityError`, this plan's Task 4 for `UnsupportedStatementError` — construct them with a message string). Export all five from the package root so later plans `from sqloutbox.exceptions import …` or `from sqloutbox import …` either way.

**Files:**
- Create: `src/sqloutbox/exceptions.py`
- Modify: `src/sqloutbox/__init__.py`
- Test: `tests/test_exceptions.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_exceptions.py` with:

```python
"""WS-3: typed exception hierarchy."""
from __future__ import annotations

import pytest

from sqloutbox.exceptions import (
    ChainIntegrityError,
    ConfigError,
    QueueFullError,
    SqloutboxError,
    UnsupportedStatementError,
)


def test_all_subclass_base():
    """Every typed error is a SqloutboxError (callers can catch one base)."""
    for cls in (ConfigError, QueueFullError, UnsupportedStatementError, ChainIntegrityError):
        assert issubclass(cls, SqloutboxError)
    assert issubclass(SqloutboxError, Exception)


def test_config_error_carries_field_value_reason():
    """ConfigError exposes field, value, reason and a readable message."""
    err = ConfigError(field="batch_size", value=0, reason="must be >= 1")
    assert err.field == "batch_size"
    assert err.value == 0
    assert err.reason == "must be >= 1"
    msg = str(err)
    assert "batch_size" in msg
    assert "must be >= 1" in msg
    assert "0" in msg


def test_queue_full_error_carries_namespace_and_cap():
    """QueueFullError exposes namespace + max_pending and a readable message."""
    err = QueueFullError(namespace="events", max_pending=1000)
    assert err.namespace == "events"
    assert err.max_pending == 1000
    msg = str(err)
    assert "events" in msg
    assert "1000" in msg


def test_unsupported_statement_error_is_message_only():
    """UnsupportedStatementError carries a plain message."""
    err = UnsupportedStatementError("INSERT ... SELECT is not supported")
    assert "SELECT" in str(err)
    assert isinstance(err, SqloutboxError)


def test_chain_integrity_error_is_message_only():
    """ChainIntegrityError carries a plain message (raised by WS-5)."""
    err = ChainIntegrityError("duplicate prev_seq=5")
    assert "prev_seq=5" in str(err)
    assert isinstance(err, SqloutboxError)


def test_exported_from_package_root():
    """The five exceptions are importable from the top-level package."""
    import sqloutbox

    assert sqloutbox.SqloutboxError is SqloutboxError
    assert sqloutbox.ConfigError is ConfigError
    assert sqloutbox.QueueFullError is QueueFullError
    assert sqloutbox.UnsupportedStatementError is UnsupportedStatementError
    assert sqloutbox.ChainIntegrityError is ChainIntegrityError


def test_config_error_raisable_and_catchable_as_base():
    """A ConfigError can be caught as SqloutboxError."""
    with pytest.raises(SqloutboxError):
        raise ConfigError(field="x", value=-1, reason="bad")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_exceptions.py -v`
Expected: FAIL at collection — `ModuleNotFoundError: No module named 'sqloutbox.exceptions'` (the module does not exist yet), so the whole file errors.

- [ ] **Step 3: Create the exceptions module**

Create `src/sqloutbox/exceptions.py` with:

```python
"""Typed exception hierarchy for sqloutbox.

Replaces leaking bare ``sqlite3.*`` / ``RuntimeError`` at the library boundary
so callers can catch a single ``SqloutboxError`` base or discriminate on the
specific subclass. See design spec §4.4.

Hierarchy::

    SqloutboxError
      ├─ ConfigError                (invalid OutboxConfig/TargetConfig field)
      ├─ QueueFullError             (enqueue() at the opt-in max_pending cap)
      ├─ UnsupportedStatementError  (inject_outbox_seq grammar reject)
      └─ ChainIntegrityError        (forked chain / gap — raised by WS-5)
"""

from __future__ import annotations


class SqloutboxError(Exception):
    """Base class for all sqloutbox-raised errors."""


class ConfigError(SqloutboxError):
    """An OutboxConfig / TargetConfig field failed validation.

    Raised from the dataclass ``__post_init__`` so a misconfiguration fails at
    construction time, not in production.

    Attributes
    ----------
    field:
        Name of the offending config field (e.g. ``"batch_size"``).
    value:
        The rejected value.
    reason:
        Human-readable constraint that was violated (e.g. ``"must be >= 1"``).
    """

    def __init__(self, field: str, value: object, reason: str) -> None:
        self.field = field
        self.value = value
        self.reason = reason
        super().__init__(f"invalid config: {field}={value!r} — {reason}")


class QueueFullError(SqloutboxError):
    """``enqueue()`` was called while the namespace is at its ``max_pending`` cap.

    Only raised when the opt-in ``max_pending`` bound is set on the config.
    With the default (``max_pending=None``) ``enqueue()`` never raises this.

    Attributes
    ----------
    namespace:
        The outbox namespace that is full.
    max_pending:
        The configured cap that was reached.
    """

    def __init__(self, namespace: str, max_pending: int) -> None:
        self.namespace = namespace
        self.max_pending = max_pending
        super().__init__(
            f"outbox namespace {namespace!r} is full "
            f"(pending >= max_pending={max_pending}); enqueue rejected"
        )


class UnsupportedStatementError(SqloutboxError):
    """``inject_outbox_seq`` was given a statement it cannot safely transform.

    The grammar guard accepts ONLY single-row ``INSERT INTO t (cols) VALUES (?,…)``
    and ``UPDATE t SET c=? WHERE …``. Anything else (INSERT…SELECT, multi-row
    VALUES, a ``?`` / ``)`` / ``WHERE`` inside a string literal) is rejected here
    instead of being silently rewritten into wrong SQL.
    """


class ChainIntegrityError(SqloutboxError):
    """The singly-linked chain is forked or has a gap (raised by WS-5).

    Defined here in WS-3 so the whole hierarchy lands in one place; the
    forked-chain migration guard that raises it is added by the WS-5 plan.
    """
```

- [ ] **Step 4: Export the new exceptions from the package root**

In `src/sqloutbox/__init__.py`, add an import after the existing `from sqloutbox._verify import …` line (currently line 69):

```python
from sqloutbox.exceptions import (
    ChainIntegrityError,
    ConfigError,
    QueueFullError,
    SqloutboxError,
    UnsupportedStatementError,
)
```

Then add the five names to the `__all__` list (currently ends at line 92 with `"verify_all",`). Insert a new group before the closing `]`:

```python
    # Verification
    "TableVerifyResult",
    "VerifyResult",
    "verify_outbox",
    "verify_all",
    # Exceptions
    "SqloutboxError",
    "ConfigError",
    "QueueFullError",
    "UnsupportedStatementError",
    "ChainIntegrityError",
]
```

(The first four lines under `# Verification` already exist — keep them; only add the `# Exceptions` block before the closing `]`.)

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m pytest tests/test_exceptions.py -v`
Expected: PASS (7 passed).

- [ ] **Step 6: Run the full suite to confirm no regression**

Run: `python -m pytest -q`
Expected: all green (baseline + 7 new exception tests).

- [ ] **Step 7: Commit**

```bash
git add src/sqloutbox/exceptions.py src/sqloutbox/__init__.py tests/test_exceptions.py
git commit -m "feat(safety): typed exception hierarchy SqloutboxError + 4 subclasses (WS-3)"
```

---

## Task 2: Config fields + `__post_init__` validation on both frozen dataclasses

Add `max_attempts` / `max_pending` / `max_batch_bytes` to BOTH `TargetConfig` and `OutboxConfig` (CONTRACT + spec §8 L368-371), and add `__post_init__` validation that raises `ConfigError(field, value, reason)` for every bound in spec §4.1 (L161-171). Both dataclasses are `@dataclass(frozen=True)`, so `__post_init__` must raise BEFORE any normalization; the spec notes `object.__setattr__` only if you must normalize — we do NOT need to normalize anything here, so the method is pure validation. `TargetConfig` does not own `flush_interval` / `table_*` / `cleanup_every` (those are `OutboxConfig`-only), so its `__post_init__` validates only the fields it actually has: `batch_size`, `retain_log_days`, `max_pending`, `max_attempts`, plus each per-table override day count.

> **Field placement (frozen dataclass ordering rule):** all three new fields have defaults, and every existing field on each dataclass also has a default EXCEPT `OutboxConfig.db_dir` and `TargetConfig.name`/`tables` (which come first). Append the new fields at the END of each dataclass's field block so no non-default field follows a default field.

**Files:**
- Modify: `src/sqloutbox/config.py`
- Test: `tests/test_config.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_config.py`:

```python
# ── WS-3: new fields + __post_init__ validation ──────────────────────────────

from sqloutbox.exceptions import ConfigError


def test_new_fields_defaults_outbox():
    """OutboxConfig gains max_attempts=10, max_pending=None, max_batch_bytes=None."""
    cfg = OutboxConfig(db_dir=Path("/tmp"))
    assert cfg.max_attempts == 10
    assert cfg.max_pending is None
    assert cfg.max_batch_bytes is None


def test_new_fields_defaults_target():
    """TargetConfig gains the same three fields with the same defaults."""
    t = TargetConfig(name="a", tables=("t",))
    assert t.max_attempts == 10
    assert t.max_pending is None
    assert t.max_batch_bytes is None


def test_new_fields_settable():
    """The three new fields accept explicit values on both dataclasses."""
    cfg = OutboxConfig(db_dir=Path("/tmp"), max_attempts=None, max_pending=1000,
                       max_batch_bytes=1_048_576)
    assert cfg.max_attempts is None
    assert cfg.max_pending == 1000
    assert cfg.max_batch_bytes == 1_048_576

    t = TargetConfig(name="a", tables=("t",), max_attempts=None, max_pending=500,
                     max_batch_bytes=2048)
    assert t.max_attempts is None
    assert t.max_pending == 500
    assert t.max_batch_bytes == 2048


@pytest.mark.parametrize(
    "kwargs, bad_field",
    [
        ({"batch_size": 0}, "batch_size"),
        ({"flush_interval": 0}, "flush_interval"),
        ({"flush_interval": -1.0}, "flush_interval"),
        ({"table_flush_threshold": 0}, "table_flush_threshold"),
        ({"table_max_wait": -0.1}, "table_max_wait"),
        ({"cleanup_every": 0}, "cleanup_every"),
        ({"retain_log_days": -1}, "retain_log_days"),
        ({"max_pending": 0}, "max_pending"),
        ({"max_attempts": 0}, "max_attempts"),
    ],
)
def test_outbox_config_validation_rejects(kwargs, bad_field):
    """Each out-of-range OutboxConfig field raises ConfigError naming that field."""
    with pytest.raises(ConfigError) as ei:
        OutboxConfig(db_dir=Path("/tmp"), **kwargs)
    assert ei.value.field == bad_field


def test_outbox_config_validation_accepts_boundaries():
    """The smallest legal values construct fine (boundary check)."""
    cfg = OutboxConfig(
        db_dir=Path("/tmp"),
        batch_size=1,
        flush_interval=0.001,
        table_flush_threshold=1,
        table_max_wait=0.0,
        cleanup_every=1,
        retain_log_days=0,
        max_pending=1,
        max_attempts=1,
    )
    assert cfg.batch_size == 1
    assert cfg.table_max_wait == 0.0
    assert cfg.retain_log_days == 0


def test_outbox_config_none_disables_caps():
    """max_pending=None and max_attempts=None are valid (opt-out)."""
    cfg = OutboxConfig(db_dir=Path("/tmp"), max_pending=None, max_attempts=None)
    assert cfg.max_pending is None
    assert cfg.max_attempts is None


@pytest.mark.parametrize(
    "kwargs, bad_field",
    [
        ({"batch_size": 0}, "batch_size"),
        ({"retain_log_days": -1}, "retain_log_days"),
        ({"max_pending": 0}, "max_pending"),
        ({"max_attempts": 0}, "max_attempts"),
    ],
)
def test_target_config_validation_rejects(kwargs, bad_field):
    """Each out-of-range TargetConfig field raises ConfigError naming that field."""
    with pytest.raises(ConfigError) as ei:
        TargetConfig(name="a", tables=("t",), **kwargs)
    assert ei.value.field == bad_field


def test_target_config_validation_rejects_bad_override_days():
    """A negative per-table retain override is rejected, naming retain_log_days."""
    with pytest.raises(ConfigError) as ei:
        TargetConfig(name="a", tables=("t",),
                     table_retain_overrides=(("t", -5),))
    assert ei.value.field == "retain_log_days"


def test_target_config_validation_accepts_boundaries():
    """Smallest legal TargetConfig values construct fine."""
    t = TargetConfig(name="a", tables=("t",), batch_size=1, retain_log_days=0,
                     max_pending=1, max_attempts=1)
    assert t.batch_size == 1
    assert t.retain_log_days == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_config.py -v -k "WS3 or new_fields or validation or boundaries or none_disables or override_days" 2>/dev/null || python -m pytest tests/test_config.py -v`
Expected: FAIL — `TargetConfig`/`OutboxConfig` have no `max_attempts` attribute (`AttributeError` / `TypeError: unexpected keyword argument`) and no `__post_init__`, so the new field and validation tests fail.

- [ ] **Step 3: Add the fields + `__post_init__` to `TargetConfig`**

In `src/sqloutbox/config.py`, locate the `TargetConfig` field block. After the existing last field `table_retain_overrides: tuple[tuple[str, int], ...] = ()` (currently line 114), add the three new fields:

```python
    table_retain_overrides: tuple[tuple[str, int], ...] = ()
    # WS-3 safety-rail fields (mirror OutboxConfig; opt-in / opt-out):
    #   max_attempts:    D1 auto-dead-letter threshold (None = plateau-forever)
    #   max_pending:     D2 opt-in backpressure cap (None = unbounded)
    #   max_batch_bytes: optional per-target memory bound (None = unbounded)
    max_attempts: int | None = 10
    max_pending: int | None = None
    max_batch_bytes: int | None = None
```

Then add a `__post_init__` method to `TargetConfig`. Place it immediately after that field block and BEFORE the existing `def should_inject_seq(self, table: str) -> bool:` method (currently line 116):

```python
    def __post_init__(self) -> None:
        """Validate fields at construction (frozen dataclass — raise before use).

        Raises ConfigError(field, value, reason) on the first violation so a
        misconfiguration fails loudly at startup, not in production.
        """
        from sqloutbox.exceptions import ConfigError

        if self.batch_size < 1:
            raise ConfigError("batch_size", self.batch_size, "must be >= 1")
        if self.retain_log_days < 0:
            raise ConfigError("retain_log_days", self.retain_log_days,
                              "must be >= 0 (negative computes a future cutoff)")
        if self.max_pending is not None and self.max_pending < 1:
            raise ConfigError("max_pending", self.max_pending,
                              "must be None or >= 1")
        if self.max_attempts is not None and self.max_attempts < 1:
            raise ConfigError("max_attempts", self.max_attempts,
                              "must be None or >= 1")
        for _name, _days in self.table_retain_overrides:
            if _days < 0:
                raise ConfigError("retain_log_days", _days,
                                  f"per-table override for {_name!r} must be >= 0")
```

> The import of `ConfigError` is done lazily inside `__post_init__` to avoid any import cycle (`exceptions.py` imports nothing from `config.py`, so a top-level import would also be safe — but the lazy import keeps `config.py`'s module-load surface unchanged and is the conservative choice).

- [ ] **Step 4: Add the fields + `__post_init__` to `OutboxConfig`**

In `src/sqloutbox/config.py`, locate the `OutboxConfig` field block. After the existing last field `retain_log_days: int = 30` (currently line 192), add:

```python
    retain_log_days: int = 30
    # WS-3 safety-rail fields (spec §8):
    #   max_attempts:    D1 auto-dead-letter threshold (None = plateau-forever)
    #   max_pending:     D2 opt-in backpressure cap (None = unbounded, default)
    #   max_batch_bytes: optional memory bound (None = unbounded)
    max_attempts: int | None = 10
    max_pending: int | None = None
    max_batch_bytes: int | None = None
```

Then add a `__post_init__` method to `OutboxConfig`, placed immediately after that field block and BEFORE the existing `def tables_for_target(self, name: str)` method (currently line 194):

```python
    def __post_init__(self) -> None:
        """Validate tuning fields at construction (frozen dataclass).

        Raises ConfigError(field, value, reason) on the first violation — a
        clear, field-named error instead of a modulo-by-zero or future-cutoff
        surprise in production.
        """
        from sqloutbox.exceptions import ConfigError

        if self.batch_size < 1:
            raise ConfigError("batch_size", self.batch_size, "must be >= 1")
        if self.flush_interval <= 0:
            raise ConfigError("flush_interval", self.flush_interval, "must be > 0")
        if self.table_flush_threshold < 1:
            raise ConfigError("table_flush_threshold", self.table_flush_threshold,
                              "must be >= 1")
        if self.table_max_wait < 0:
            raise ConfigError("table_max_wait", self.table_max_wait, "must be >= 0")
        if self.cleanup_every < 1:
            raise ConfigError("cleanup_every", self.cleanup_every,
                              "must be >= 1 (prevents modulo-by-zero / never-prune)")
        if self.retain_log_days < 0:
            raise ConfigError("retain_log_days", self.retain_log_days,
                              "must be >= 0 (negative computes a future cutoff "
                              "→ would wipe the audit log)")
        if self.max_pending is not None and self.max_pending < 1:
            raise ConfigError("max_pending", self.max_pending,
                              "must be None or >= 1")
        if self.max_attempts is not None and self.max_attempts < 1:
            raise ConfigError("max_attempts", self.max_attempts,
                              "must be None or >= 1")
```

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m pytest tests/test_config.py -v`
Expected: PASS — all existing config tests still pass (defaults `batch_size=500`, `flush_interval=1.0`, etc. are all in range) plus the new validation tests.

- [ ] **Step 6: Run the full suite to confirm no regression**

Run: `python -m pytest -q`
Expected: all green. (Watch for any existing test or fixture that constructs a config with an out-of-range value — there is none in the baseline suite; `test_resilience.py` from WS-0 uses `flush_interval=0.01`, `table_flush_threshold=1`, `table_max_wait=0.0`, all in range.)

- [ ] **Step 7: Commit**

```bash
git add src/sqloutbox/config.py tests/test_config.py
git commit -m "feat(safety): config validation + max_attempts/max_pending/max_batch_bytes (WS-3)"
```

---

## Task 3: Opt-in backpressure — `QueueFullError` in `enqueue()`

Per CONTRACT and spec §4.2 (L202-207): `enqueue()` raises `QueueFullError(namespace, max_pending)` ONLY when `max_pending` is set and `pending_count() >= max_pending`; the default (`max_pending=None`) NEVER raises and the existing fast INSERT path is unchanged. `Outbox.__init__` does not currently receive any cap, so add a `max_pending: int | None = None` parameter to `Outbox.__init__` (defaulted so every existing caller — `_outbox` direct use in tests, `sync.py`'s `Outbox(...)` construction at L231 — keeps working unchanged), store it on `self.max_pending`, and gate `enqueue()` on it before the `BEGIN IMMEDIATE`.

> **Why the gate goes before `BEGIN IMMEDIATE`, not inside the try:** `enqueue()`'s existing `except Exception` block swallows errors and returns `None` ("never raises — drops with WARNING"). `QueueFullError` must propagate to the caller (D2: "the caller decides"), so the check must sit OUTSIDE that try/except. `pending_count()` opens its own short-lived `thread_conn` and does not touch `self._write_conn`, so calling it before `BEGIN IMMEDIATE` is safe.

**Files:**
- Modify: `src/sqloutbox/_outbox.py`
- Test: `tests/test_safety_rails.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_safety_rails.py` with:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_safety_rails.py -v -k "enqueue"`
(Quote the `-k` expression so the shell does not split it. The three backpressure tests are all named `test_enqueue_*`. Or simply `python -m pytest tests/test_safety_rails.py -v` — the grammar/chunking tests are added in later tasks and will be absent for now.)
Expected: FAIL — `Outbox.__init__()` got an unexpected keyword argument `max_pending` (the parameter does not exist yet).

- [ ] **Step 3: Add the `max_pending` parameter and the gate**

In `src/sqloutbox/_outbox.py`, change the `Outbox.__init__` signature (currently lines 50-64) to add the `max_pending` parameter and store it. Replace:

```python
    def __init__(
        self,
        db_path: Path,
        namespace: str,
        retain_log_days: int = DEFAULT_RETAIN_LOG_DAYS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        cleanup_every: int = DEFAULT_CLEANUP_EVERY,
    ) -> None:
        self.db_path         = db_path
        self.namespace       = namespace
        self.retain_log_days = retain_log_days
        self.batch_size      = batch_size
        self.cleanup_every   = cleanup_every
        # Persistent write connection — used exclusively by enqueue() from one thread
        self._write_conn = open_write_conn(db_path)
```

with:

```python
    def __init__(
        self,
        db_path: Path,
        namespace: str,
        retain_log_days: int = DEFAULT_RETAIN_LOG_DAYS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        cleanup_every: int = DEFAULT_CLEANUP_EVERY,
        max_pending: int | None = None,
    ) -> None:
        self.db_path         = db_path
        self.namespace       = namespace
        self.retain_log_days = retain_log_days
        self.batch_size      = batch_size
        self.cleanup_every   = cleanup_every
        # WS-3 D2: opt-in backpressure cap. None = unbounded (default, fast path
        # unchanged). When set, enqueue() raises QueueFullError at the cap.
        self.max_pending     = max_pending
        # Persistent write connection — used exclusively by enqueue() from one thread
        self._write_conn = open_write_conn(db_path)
```

Then add the import of `QueueFullError`. At the top of the file the existing imports are `from sqloutbox._models import QueueRow` (line 9) and `from sqloutbox._schema import (...)` (line 10-15). Add, after the `_schema` import block (after line 15):

```python
from sqloutbox.exceptions import QueueFullError
```

Finally, add the cap gate at the very start of `enqueue()` — INSIDE the method but BEFORE the `try:` (currently line 89). Replace the opening of `enqueue`:

```python
        source:
            Identity of the middleware that produced this row
            (e.g. "SchedulerMiddleware"). Used for debugging and analytics.
        """
        try:
            # BEGIN IMMEDIATE acquires the write lock before the SELECT so no
```

with:

```python
        source:
            Identity of the middleware that produced this row
            (e.g. "SchedulerMiddleware"). Used for debugging and analytics.

        Raises
        ------
        QueueFullError
            Only when ``max_pending`` is set and the namespace is already at
            the cap. With the default (``max_pending=None``) this never raises;
            the fast INSERT path below is unchanged.
        """
        # WS-3 D2: opt-in hard backstop. Checked BEFORE the try/except (which
        # swallows-and-returns-None) so QueueFullError propagates to the caller.
        if self.max_pending is not None and self.pending_count() >= self.max_pending:
            raise QueueFullError(self.namespace, self.max_pending)
        try:
            # BEGIN IMMEDIATE acquires the write lock before the SELECT so no
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_safety_rails.py -v`
Expected: PASS for the three backpressure tests (3 passed; grammar/chunking tests are added in Tasks 4-6).

- [ ] **Step 5: Run the full suite to confirm no regression**

Run: `python -m pytest -q`
Expected: all green. The new `max_pending` parameter has a default of `None`, so `sync.py`'s `Outbox(...)` construction (L231) and every existing test that builds an `Outbox` keep the unbounded default — no behavior change.

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/_outbox.py tests/test_safety_rails.py
git commit -m "feat(safety): opt-in QueueFullError backpressure cap in enqueue() (WS-3, D2)"
```

---

## Task 4: `inject_outbox_seq` grammar guard

Per CONTRACT and spec §4.3 (L231-245): replace the silent fall-through behavior of `inject_outbox_seq()` with a conservative, string-literal-aware lexer that accepts ONLY single-row `INSERT INTO t (cols) VALUES (?,…)` and `UPDATE t SET c=? WHERE …`, and rejects everything else by raising `UnsupportedStatementError` (NEVER a silent rewrite). Specifically reject: INSERT…SELECT (no `VALUES`), multi-row VALUES (`(…),(…)`), the current "unknown statement type" fall-through (neither INSERT nor UPDATE), and any statement where a `?`, `)`, or the keyword ` WHERE ` appears INSIDE a quoted string literal (which would make the current naive `find`/`rfind`/`count("?")` offsets wrong).

The guard is implemented as two helpers added above `inject_outbox_seq`: `_mask_string_literals(sql)` blanks out the contents of single- and double-quoted string literals (handling SQL's doubled-quote escape `''`) so the structural scanners (`find`, `rfind`, `count`) operate on a literal-free "skeleton"; and `_assert_supported(sql, masked)` raises `UnsupportedStatementError` for every rejected shape. The existing transform logic is unchanged EXCEPT it now uses `masked` for all index/count operations while applying the edits to the original `s` — so a literal containing a structural character no longer corrupts the output.

**Files:**
- Modify: `src/sqloutbox/sync.py`
- Test: `tests/test_safety_rails.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_safety_rails.py`:

```python
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
```

> Note: `test_guard_rejects_qmark_inside_literal_for_update` and `test_guard_rejects_insert_without_values` document the conservative stance — when the literal-masked skeleton has zero real placeholders in a position the transform depends on, the guard rejects rather than producing wrong SQL.

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_safety_rails.py -v -k guard`
Expected: FAIL — the current `inject_outbox_seq` silently passes through unknown statements (returns `s, list(args) + [outbox_seq]`) and naively rewrites the others, so the four `rejects` tests fail (no exception raised) and `test_guard_accepts_insert_with_literal_containing_paren_and_qmark` fails (the naive `rfind(")")` lands inside the literal, corrupting the SQL).

- [ ] **Step 3: Add the lexer helpers and rewrite `inject_outbox_seq` to use them**

In `src/sqloutbox/sync.py`, first add the import of `UnsupportedStatementError`. The existing imports end with `from sqloutbox.config import OutboxConfig, TargetConfig` (line 54). Add after it:

```python
from sqloutbox.exceptions import UnsupportedStatementError
```

Then, immediately ABOVE the `def inject_outbox_seq(` definition (currently line 107), add the two helper functions:

```python
def _mask_string_literals(sql: str) -> str:
    """Return ``sql`` with the *contents* of quoted string literals blanked.

    Replaces every character inside a single- or double-quoted literal with a
    space, leaving the quote characters and all structural SQL outside the
    literals intact. SQL's doubled-quote escape ('' inside a '…' literal, and
    "" inside a "…") is handled — a doubled quote does NOT close the literal.

    The result (the "skeleton") has the SAME LENGTH and SAME structural-char
    positions as the input, so index/count scans (find, rfind, count('?'))
    on the skeleton map 1:1 back onto the original string — but a '?' / ')' /
    'WHERE' that lived inside a literal is now a space and cannot mislead them.
    """
    out: list[str] = []
    i = 0
    n = len(sql)
    quote = ""  # "" when outside a literal; "'" or '"' when inside one
    while i < n:
        ch = sql[i]
        if quote:
            if ch == quote:
                # A doubled quote ('' or "") is an escape — stays inside the literal.
                if i + 1 < n and sql[i + 1] == quote:
                    out.append(quote)
                    out.append(quote)
                    i += 2
                    continue
                out.append(quote)   # closing quote
                quote = ""
                i += 1
                continue
            out.append(" ")          # blank the literal content
            i += 1
            continue
        if ch == "'" or ch == '"':
            quote = ch
            out.append(ch)           # opening quote
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _assert_supported(sql: str, masked: str) -> None:
    """Raise UnsupportedStatementError unless ``sql`` is a safe INSERT/UPDATE.

    ``masked`` is ``_mask_string_literals(sql)`` — structural scanning runs on
    it so a ')' / '?' / WHERE inside a string literal cannot fool the checks.

    Supported (and ONLY these):
        INSERT INTO t (cols) VALUES (?, …)     -- single-row, explicit columns
        UPDATE t SET c=? [, …] WHERE …          -- at least one real SET arg
    """
    upper = masked.upper().strip()

    if upper.startswith("INSERT"):
        vi = upper.find(") VALUES")
        if vi == -1:
            raise UnsupportedStatementError(
                f"INSERT must be single-row 'INSERT INTO t (cols) VALUES (...)'; "
                f"got: {sql!r}"
            )
        after = upper[vi + len(") VALUES"):]
        # Multi-row VALUES: a second '(' opens after the first VALUES group closes.
        first_close = after.find(")")
        if first_close != -1 and "(" in after[first_close + 1:]:
            raise UnsupportedStatementError(
                f"multi-row VALUES is not supported (one row per statement); "
                f"got: {sql!r}"
            )
        return

    if upper.startswith("UPDATE"):
        where_idx = upper.find(" WHERE ")
        set_part = masked[:where_idx] if where_idx != -1 else masked
        # The transform inserts ', outbox_seq = ?' after the real SET args. If
        # the skeleton has no real '?' in SET, the original only had a literal
        # '?' — ambiguous; reject rather than emit wrong SQL.
        if set_part.count("?") < 1:
            raise UnsupportedStatementError(
                f"UPDATE SET clause has no bind placeholder (a '?' inside a "
                f"string literal does not count); got: {sql!r}"
            )
        return

    raise UnsupportedStatementError(
        f"only single-row INSERT … VALUES (…) and UPDATE … SET …=? are "
        f"supported by inject_outbox_seq; got: {sql!r}"
    )
```

Now rewrite the BODY of `inject_outbox_seq` so it validates first, then performs all structural scans on the masked skeleton while editing the original string. Replace the entire body (currently lines 142-175, from `s = sql.strip()` through the final `return s, list(args) + [outbox_seq]`):

```python
    s = sql.strip()
    upper = s.upper()

    if upper.startswith("INSERT"):
        # Convert INSERT INTO → INSERT OR IGNORE INTO
        if upper.startswith("INSERT INTO"):
            s = "INSERT OR IGNORE INTO" + s[len("INSERT INTO"):]
        # Insert outbox_seq column before ) VALUES
        vi = s.upper().find(") VALUES")
        if vi != -1:
            s = s[:vi] + ", outbox_seq" + s[vi:]
        # Insert ? placeholder before last )
        lp = s.rfind(")")
        if lp != -1:
            s = s[:lp] + ", ?" + s[lp:]
        return s, list(args) + [outbox_seq]

    if upper.startswith("UPDATE"):
        where_idx = upper.find(" WHERE ")
        if where_idx != -1:
            # Count ? placeholders in SET clause (before WHERE)
            set_part = s[:where_idx]
            n_set_args = set_part.count("?")
            # Inject outbox_seq=? before WHERE
            s = s[:where_idx] + ", outbox_seq = ?" + s[where_idx:]
            new_args = list(args)
            new_args.insert(n_set_args, outbox_seq)
            return s, new_args
        # No WHERE clause — append to SET
        s = s + ", outbox_seq = ?"
        return s, list(args) + [outbox_seq]

    # Unknown statement type — return unchanged
    return s, list(args) + [outbox_seq]
```

with:

```python
    s = sql.strip()
    # Mask string-literal contents so a ')' / '?' / WHERE inside a literal cannot
    # mislead the structural scans below, then reject any unsupported shape.
    masked = _mask_string_literals(s)
    _assert_supported(s, masked)

    upper = masked.upper()

    if upper.startswith("INSERT"):
        # Convert INSERT INTO → INSERT OR IGNORE INTO (operate on the original s;
        # the prefix length is identical in s and the skeleton).
        if upper.startswith("INSERT INTO"):
            s = "INSERT OR IGNORE INTO" + s[len("INSERT INTO"):]
            masked = "INSERT OR IGNORE INTO" + masked[len("INSERT INTO"):]
        upper = masked.upper()
        # Insert outbox_seq column before ') VALUES' (index from the skeleton).
        vi = upper.find(") VALUES")
        s = s[:vi] + ", outbox_seq" + s[vi:]
        masked = masked[:vi] + ", outbox_seq" + masked[vi:]
        # Insert '?' placeholder before the LAST ')' of the VALUES group. On the
        # skeleton the only ')' chars are structural, so rfind is now safe.
        lp = masked.rfind(")")
        s = s[:lp] + ", ?" + s[lp:]
        return s, list(args) + [outbox_seq]

    # UPDATE (the only other shape _assert_supported lets through).
    where_idx = upper.find(" WHERE ")
    if where_idx != -1:
        # Count real '?' placeholders in the SET clause via the skeleton.
        n_set_args = masked[:where_idx].count("?")
        s = s[:where_idx] + ", outbox_seq = ?" + s[where_idx:]
        new_args = list(args)
        new_args.insert(n_set_args, outbox_seq)
        return s, new_args
    # No WHERE — append outbox_seq to the SET clause.
    s = s + ", outbox_seq = ?"
    return s, list(args) + [outbox_seq]
```

> Two correctness notes for the executor: (1) `_assert_supported` guarantees an INSERT has a `") VALUES"` and an UPDATE has at least one real SET `?`, so the `vi == -1` / `lp == -1` defensive branches in the old code are no longer reachable and are removed. (2) Every structural index (`vi`, `lp`, `where_idx`, the SET `?` count) is taken from `masked`, then applied to `s` — both strings are kept in lock-step (same edits, same lengths) so the indices stay valid on the original.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_safety_rails.py -v -k guard`
Expected: PASS (8 guard tests).

- [ ] **Step 5: Run the existing `inject_outbox_seq` tests + full suite**

Run: `python -m pytest tests/test_sync.py -v -k inject`
Expected: PASS — all existing `test_inject_outbox_seq_*` tests still pass. They all use supported shapes:
- `INSERT INTO orders (id, amount) VALUES (?, ?)` ✔
- `insert into events (id) values (?)` (lowercase) ✔
- `UPDATE orders SET status=?, amount=? WHERE id=?` ✔
- `UPDATE config SET value=?` (no WHERE, one real `?`) ✔
- `update events set val=? where id=?` ✔

Then run the full suite:
Run: `python -m pytest -q`
Expected: all green.

> If `test_inject_outbox_seq_update_without_where` fails: it sends `"UPDATE config SET value=?"` with one real placeholder — `_assert_supported`'s UPDATE branch requires `set_part.count("?") >= 1`, which holds (the whole masked string is the SET part, with one `?`). It must pass. If `test_undecodable_row_does_not_kill_loop` (WS-0) interacts: that test uses `inject_outbox_seq=False`, so the guard is never invoked there.

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/sync.py tests/test_safety_rails.py
git commit -m "feat(safety): string-literal-aware inject_outbox_seq grammar guard (WS-3, D3)"
```

---

## Task 5: SQLite variable-limit chunking in `mark_synced` / `delete_synced`

Per CONTRACT and spec §4.4 (L249): `mark_synced` and `delete_synced` build `IN (?,?,…)` over `seqs`. With `batch_size` > ~999 this trips SQLite's `SQLITE_MAX_VARIABLE_NUMBER` ("too many SQL variables"). Chunk the seq lists to **≤900 per statement**. The existing logic (the synced-flag verification in `delete_synced`, the sync_log INSERT, the actual UPDATE/DELETE) is preserved — only the `IN(...)` statements are now executed per ≤900-seq chunk inside the same connection/transaction.

Add a module-level constant `_VAR_CHUNK = 900` and a tiny helper `_chunked(seqs, size)` that yields slices. `mark_synced` chunks its single UPDATE. `delete_synced` chunks both its candidate SELECT (Step 1) and its final DELETE (Step 3); the sync_log `executemany` (Step 2) is NOT a variable-limit risk (it binds 3 vars per row, executed row-by-row by `executemany`) so it is left intact.

**Files:**
- Modify: `src/sqloutbox/_outbox.py`
- Test: `tests/test_safety_rails.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_safety_rails.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_safety_rails.py -v -k chunk`
Expected: FAIL — at minimum `test_chunked_helper_splits_at_var_chunk` fails with `ImportError: cannot import name '_chunked'` (the helper does not exist yet), and on Python 3.11+ `test_mark_synced_chunks_over_var_limit` / `test_delete_synced_chunks_over_var_limit` fail with `sqlite3.OperationalError: too many SQL variables` (the `_cap_vars_999` fixture pins the limit to 999 so the 1000-placeholder `IN (?,…)` genuinely overflows regardless of the host's real `SQLITE_MAX_VARIABLE_NUMBER`). `test_mark_delete_synced_chunk_boundary_correct` may pass pre-implementation on hosts whose real limit exceeds 1801 — it is a correctness guard, not a red gate.

> **Why this differs from a naive "bind 1000 vars and expect a raise" test:** modern SQLite (3.32+, 2020) raised `SQLITE_MAX_VARIABLE_NUMBER` from 999 to 32766, and some builds report far higher (this dev host: 500000). A test that just binds 1000 placeholders never overflows on such a host, so it cannot serve as a TDD red gate. The `conn.setlimit(...999)` fixture reproduces the historical limit deterministically; the `_chunked` helper test gives a host- and version-independent red gate so coverage holds even on Python 3.10 (no `setlimit`).

- [ ] **Step 3: Add the chunk constant + helper and chunk the IN(...) statements**

In `src/sqloutbox/_outbox.py`, add the constant and helper after the existing module constants (after `DEFAULT_CLEANUP_EVERY = 500`, currently line 21):

```python
DEFAULT_CLEANUP_EVERY   = 500

# SQLite's SQLITE_MAX_VARIABLE_NUMBER is 999 on many builds. Chunk seq lists
# bound into IN (?,?,…) statements to stay safely below it. (WS-3, F025.)
_VAR_CHUNK = 900


def _chunked(seqs: list[int], size: int = _VAR_CHUNK) -> list[list[int]]:
    """Split a seq list into consecutive chunks of at most ``size`` items."""
    return [seqs[i:i + size] for i in range(0, len(seqs), size)]
```

Now replace `mark_synced` (currently lines 260-272). Replace:

```python
    def mark_synced(self, seqs: list[int]) -> None:
        """Mark rows as confirmed delivered. Does NOT delete them yet.

        Opens its own connection — safe to call from any thread.
        """
        if not seqs:
            return
        with thread_conn(self.db_path) as conn:
            conn.execute(
                f"UPDATE outbox_queue SET synced = 1 "
                f"WHERE seq IN ({placeholders(len(seqs))})",
                seqs,
            )
```

with:

```python
    def mark_synced(self, seqs: list[int]) -> None:
        """Mark rows as confirmed delivered. Does NOT delete them yet.

        Chunks the seq list to <= _VAR_CHUNK per statement so a large
        batch_size cannot trip SQLITE_MAX_VARIABLE_NUMBER.

        Opens its own connection — safe to call from any thread.
        """
        if not seqs:
            return
        with thread_conn(self.db_path) as conn:
            for chunk in _chunked(seqs):
                conn.execute(
                    f"UPDATE outbox_queue SET synced = 1 "
                    f"WHERE seq IN ({placeholders(len(chunk))})",
                    chunk,
                )
```

Now chunk the two `IN(...)` statements inside `delete_synced` (currently lines 274-328). Replace the body Step 1 (the candidate SELECT, currently lines 290-297):

```python
        with thread_conn(self.db_path) as conn:
            # Step 1: Batch-fetch all candidate rows — verify each is synced
            rows_data = conn.execute(
                f"SELECT seq, synced FROM outbox_queue "
                f"WHERE seq IN ({placeholders(len(seqs))})",
                seqs,
            ).fetchall()
            by_seq = {r[0]: bool(r[1]) for r in rows_data}
```

with:

```python
        with thread_conn(self.db_path) as conn:
            # Step 1: Batch-fetch all candidate rows — verify each is synced.
            # Chunked to stay under SQLITE_MAX_VARIABLE_NUMBER.
            by_seq: dict[int, bool] = {}
            for chunk in _chunked(seqs):
                rows_data = conn.execute(
                    f"SELECT seq, synced FROM outbox_queue "
                    f"WHERE seq IN ({placeholders(len(chunk))})",
                    chunk,
                ).fetchall()
                for r in rows_data:
                    by_seq[r[0]] = bool(r[1])
```

Then replace the final DELETE (currently lines 321-325):

```python
            conn.execute(
                f"DELETE FROM outbox_queue "
                f"WHERE seq IN ({placeholders(len(safe))})",
                safe,
            )
```

with:

```python
            for chunk in _chunked(safe):
                conn.execute(
                    f"DELETE FROM outbox_queue "
                    f"WHERE seq IN ({placeholders(len(chunk))})",
                    chunk,
                )
```

> The `executemany` for `outbox_sync_log` (Step 2, currently lines 316-320) is left UNCHANGED — `executemany` runs one statement per row, so its 3-vars-per-row binding never hits the variable limit.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_safety_rails.py -v -k chunk`
Expected: PASS — `test_chunked_helper_splits_at_var_chunk` and `test_mark_delete_synced_chunk_boundary_correct` pass on every host; `test_mark_synced_chunks_over_var_limit` and `test_delete_synced_chunks_over_var_limit` pass on Python 3.11+ (and are SKIPPED, not failed, on 3.10 where `Connection.setlimit` is unavailable). No test fails.

- [ ] **Step 5: Run the full suite to confirm no regression**

Run: `python -m pytest -q`
Expected: all green. Existing small-batch `mark_synced` / `delete_synced` callers produce a single chunk (≤900), so their behavior is identical.

- [ ] **Step 6: Commit**

```bash
git add src/sqloutbox/_outbox.py tests/test_safety_rails.py
git commit -m "feat(safety): chunk mark_synced/delete_synced IN(...) to <=900 vars (WS-3, F025)"
```

---

## Task 6: Document the 80% stop-producing watermark as a producing-application policy

Per CONTRACT and spec §4.2 (L176-229) + §10 (L419): the **80% `STOP_WATERMARK_PCT`** is a **PRODUCING-APPLICATION** threshold — NOT library config. The library only reports the depth (and, in Plan 6, `capacity_pct`); it never owns "80", never pushes, never pauses, and there is **no auto-resume** (restart is a manual operator action). This task documents that contract in two places — the README and a module docstring on `QueueFullError` callers' config field — so an OSS user reading the code or the docs understands the boundary. **No `health()` / `capacity_pct` implementation here — that is Plan 6.** This task is documentation + one assertion test that the library exposes `max_pending` but no watermark constant.

**Files:**
- Modify: `README.md`
- Test: `tests/test_safety_rails.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_safety_rails.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_safety_rails.py -v -k watermark`
Expected: FAIL — `test_library_owns_max_pending_not_the_watermark` passes (no such symbol exists, good), but `test_readme_documents_watermark_as_producer_policy` FAILS because the README does not yet mention `max_pending` / `80%` / the no-auto-resume contract.

- [ ] **Step 3: Document the backpressure model + watermark in the README**

In `README.md`, add a new subsection under the **Architecture** section. Locate the `### Idempotent delivery` heading (currently around line 262) and insert a new subsection immediately BEFORE it (i.e. after the `### Chain integrity` subsection ends, before `### Idempotent delivery`):

```markdown
### Backpressure (`max_pending`) and the stop-producing watermark

`sqloutbox` is **unbounded by default** — `enqueue()` never raises and the
queue grows until the drain catches up. `health().depth` (per namespace)
surfaces the backlog so you can monitor it.

To bound the queue, set `max_pending` on the config. It is a **two-tier,
pull-based** model — the library only ever *reports a number*; it never calls
back into your app, never pauses it, and never resumes it:

| Tier | Who acts | Trigger | Action |
|------|----------|---------|--------|
| **Stop watermark (80%)** | your **producing application** | `depth >= 80% * max_pending` (polled) | the producer stops enqueuing |
| **Hard cap (100%)** | the **library** | `enqueue()` while `pending >= max_pending` | raises `QueueFullError(namespace, max_pending)` |

The **80% `STOP_WATERMARK_PCT` is a producing-application policy, not library
config** — the library does not own "80". Your producer polls `health().depth`
(Plan 6 adds a derived `capacity_pct = depth / max_pending` convenience) and
stops enqueuing at its own threshold; the `QueueFullError` hard cap is the
library backstop for a bare producer that does not poll.

**There is no auto-resume — deliberately.** A fast-rising backlog is a
*symptom*: the cause may be a slow/down remote (the drain will clear it) OR a
**bug in the producer itself** flooding wrong messages. Auto-resuming would
re-arm a faulty producer. So once the producer stops, an **operator restarts
it manually** after diagnosing why the queue filled (and may quarantine the
already-queued bad rows via the dead-letter CLI). sqloutbox's drain service
**never stops or starts** — it keeps draining the backlog down throughout.
```

- [ ] **Step 4: Document the supported `inject_outbox_seq` grammar in the README**

This satisfies spec §4.3's "Document the exact supported grammar in the README writer guide" (L245). In `README.md`, locate the `### Writer class` subsection (currently around line 200). Add a new subsection immediately AFTER it (before `### Programmatic TOML loading`):

```markdown
### Supported SQL grammar for `inject_outbox_seq`

When a target has `inject_outbox_seq` enabled, the drain rewrites each row's
SQL to carry the `outbox_seq` column. The rewrite uses a conservative,
string-literal-aware lexer that accepts **only** two shapes:

```sql
-- single-row INSERT with an explicit column list:
INSERT INTO t (c1, c2) VALUES (?, ?)
    -- → INSERT OR IGNORE INTO t (c1, c2, outbox_seq) VALUES (?, ?, ?)

-- UPDATE with at least one real bind placeholder in SET:
UPDATE t SET c1=?, c2=? WHERE id=?
    -- → UPDATE t SET c1=?, c2=?, outbox_seq = ? WHERE id=?
```

Everything else is **rejected loudly** with `UnsupportedStatementError`
(never silently rewritten):

- `INSERT … SELECT …` (no `VALUES` list)
- multi-row `INSERT … VALUES (…), (…)`
- a `?`, `)`, or `WHERE` that appears **inside a quoted string literal**
- an `UPDATE` whose only `?` is inside a literal (no real SET placeholder)

If you must deliver an unsupported shape, route its table to a target with
`inject_outbox_seq=False` (delivered verbatim, no rewrite).
```

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m pytest tests/test_safety_rails.py -v -k watermark`
Expected: PASS (2 watermark tests — the README now contains `max_pending`, `80%`, and "no auto-resume").

- [ ] **Step 6: Run the full suite to confirm no regression**

Run: `python -m pytest -q`
Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add README.md tests/test_safety_rails.py
git commit -m "docs(safety): backpressure watermark contract + inject grammar guide (WS-3)"
```

---

## Self-Review notes (for the executor)

- **Spec coverage:** Task 1 = §4.4 typed-exception hierarchy (L251-258); Task 2 = §4.1 config validation (L161-171); Task 3 = §4.2 / D2 opt-in `QueueFullError` (L202-207); Task 4 = §4.3 / D3 grammar guard (L233-245); Task 5 = §4.4 var-limit chunking (L249); Task 6 = §4.2 the 80% stop-producing watermark DOC (L176-229) + §4.3's "document the grammar in the README" (L245). `ChainIntegrityError` is *defined* here (CONTRACT says WS-3 owns the whole hierarchy) but *raised* by Plan 5 — do not add a raiser for it in this plan.
- **Cross-plan contract — do not redefine names owned elsewhere:** `outbox_dead_log`, `dead_letter()`, `replay()`, the `_seq_accounted` extension, the dead-letter CLI, and the retry/backoff columns are **Plan 2 (WS-2)**; the flock single-drain, cooperative shutdown, and read-only open path are **Plan 4/5**; `health()` / `capacity_pct` is **Plan 6**. This plan must NOT create any of them — it only creates the exceptions + config fields they consume.
- **WS-0 already touched shared files:** `_schema.py` already has `busy_timeout` (do not re-add); `sync.py` already has the L1/L2 guards in `_worker_loop` (the Task 4 changes are to the module-level `inject_outbox_seq` function, a different region, no conflict); `_runner.py` already has L3 (untouched here). `tests/test_resilience.py` already exists (untouched here).
- **`Outbox.__init__` signature change is additive:** `max_pending` is a new keyword-only-in-practice parameter with default `None`, so `sync.py`'s `Outbox(db_path=…, namespace=…, batch_size=…, retain_log_days=…)` construction (sync.py:230-237) and all existing tests keep working unchanged. A FOLLOW-UP (not in this plan's scope, belongs to whoever wires config→Outbox) would pass `max_pending=target.max_pending` there; this plan only makes the cap *available*.
- **Frozen-dataclass `__post_init__`:** both methods are pure validation (no `object.__setattr__` needed — nothing is normalized). They raise BEFORE any field is mutated, which is the only safe option on a frozen dataclass.
- **Grammar guard `s`/`masked` lock-step:** the single trickiest part is keeping `s` (edited original) and `masked` (edited skeleton) the SAME LENGTH through each edit so structural indices stay valid. Every edit in Task 4 Step 3 applies the identical slice surgery to both. After editing, run `python -m py_compile src/sqloutbox/sync.py` before testing.
- **Line numbers may have drifted** because WS-0 inserted lines into `sync.py`, `_schema.py`, `_runner.py`, and `__init__.py` may have been touched. ALWAYS locate by the quoted code snippet, not the cited line number. Cited numbers were accurate against the on-disk source at plan-writing time (pre-WS-0-execution).
- **Gate on "all green", not a count:** baseline is 180; WS-0 adds ~7; this plan adds ~33 tests (7 exceptions + ~10 config + ~3 backpressure + 8 grammar + 3 chunk + 2 watermark). Confirm your local starting count with `python -m pytest --collect-only -q` before Task 1.
- **One known interaction to watch:** Task 2 adds `__post_init__` validation to `OutboxConfig`/`TargetConfig`. Before committing Task 2, grep the test suite and `_runner.py` TOML loader for any config constructed with `flush_interval=0`, `batch_size=0`, `cleanup_every=0`, or negative retain — there are none in the baseline, but a config built by a TOML fixture with `flush_interval = 0` would now raise `ConfigError`. If `python -m pytest -q` surfaces such a case, it is a real latent bug the validation caught (per the "never silently skip broken tests" rule, flag it — do not loosen the bound).
