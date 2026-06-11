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
