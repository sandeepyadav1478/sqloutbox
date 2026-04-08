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
