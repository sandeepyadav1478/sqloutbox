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
