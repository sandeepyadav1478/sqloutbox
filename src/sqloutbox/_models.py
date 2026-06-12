"""Data models for sqloutbox."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class QueueRow:
    """One row read from outbox_queue, ready for delivery.

    Attributes
    ----------
    seq:
        Monotonically increasing row ID (AUTOINCREMENT). Unique across all
        namespaces in the shared DB file.

    tag:
        Caller-defined event type label (e.g. "user.created", "order.paid").
        Used by the consumer to route the payload to the correct handler.

    payload:
        Raw bytes — format chosen entirely by the caller (UTF-8 JSON, msgpack,
        protobuf, plain text, etc.). sqloutbox does not interpret this field.

    prev_seq:
        seq of the predecessor row in this namespace. NULL for the very first
        row ever inserted. Immutable after insert — never updated.

        Successor rows are found on demand via:
            SELECT seq FROM outbox_queue WHERE prev_seq = this_seq
        Storing next_seq was considered but rejected: it requires an UPDATE
        on every INSERT, doubling write operations without adding correctness.
    """
    seq:      int
    tag:      str
    payload:  bytes
    prev_seq: int | None
    source:   str = ""   # middleware that produced this row (e.g. "SchedulerMiddleware")
    # WS-1/WS-2 retry tracking (persisted on outbox_queue). Defaulted so existing
    # callers that build QueueRow with 5 args keep working.
    attempts:         int = 0
    last_attempt_at:  str | None = None   # ISO-8601 UTC, NULL until first attempt
    last_error:       str | None = None   # destination error of the last failed attempt
    last_error_class: str | None = None   # TRANSIENT|DETERMINISTIC|ALREADY_APPLIED|UNKNOWN


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


@dataclass(frozen=True)
class DeadRow:
    """One row read from outbox_dead_log — a quarantined, replayable event.

    Mirrors the outbox_dead_log table columns (see _schema.py::_CREATE_DEAD_LOG).
    A row lands here only via Outbox.dead_letter(); it is never lost, only moved.
    """
    seq:              int
    namespace:        str
    tag:              str
    payload:          bytes
    prev_seq:         int | None
    source:           str | None
    attempts:         int
    last_error:       str | None
    last_error_class: str | None
    dead_lettered_at: str
    reason:           str
