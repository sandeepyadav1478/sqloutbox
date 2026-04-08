"""OutboxWorker — full sync pipeline: serialize → enqueue → deliver → clean.

sqloutbox owns the ENTIRE outbox lifecycle:
    _push()         — serialize event to bytes + enqueue to local SQLite
    _push_batch()   — serialize N events + enqueue in one SQLite transaction
    _encode()       — JSON serialization (overridable: inject fields, add metadata)
    _worker_loop()  — fetch → verify_chain → decode → _remote_write → mark → delete
    _decode()       — reconstruct domain object from JSON dict (abstract)
    _remote_write() — deliver typed events to destination (abstract)

The subclass only decides:
    1. Which type to reconstruct: implement _decode(tag, data) → event object
    2. Where to write it: implement _remote_write(batch) → confirmed seqs
    3. How to configure: flush_interval, cleanup_every
    4. (Optional) Override _encode() to add fields before encoding

Usage
-----
    from sqloutbox import Outbox, OutboxWorker, _from_dict
    from dataclasses import dataclass

    @dataclass
    class OrderEvent:
        order_id: int
        status:   str

    class OrderWorker(OutboxWorker):

        def _decode(self, tag, data):
            return _from_dict(OrderEvent, data)

        async def _remote_write(self, batch):
            # batch: list of (outbox_seq, tag, OrderEvent)
            confirmed = []
            for seq, tag, event in batch:
                if await send_to_api(event.order_id, event.status):
                    confirmed.append(seq)
            return confirmed

    outbox = Outbox(db_path=Path("events.db"), namespace="orders")
    worker = OrderWorker(outbox=outbox, flush_interval=5.0)

    # Hot path (~150µs, synchronous):
    worker._push("order.paid", OrderEvent(order_id=42, status="paid"))

Worker semantics
----------------
- Starts lazily on first _push() call — zero overhead until first event.
- On each cycle:
    1. fetch_unsynced()   — read pending rows
    2. verify_chain()     — check singly-linked chain integrity
    3. json.loads()       — bytes → dict
    4. _decode()          — dict → typed event (abstract, subclass implements)
    5. _remote_write()    — deliver typed events (abstract, subclass implements)
    6. mark_synced()      — flag confirmed rows
    7. delete_synced()    — remove from queue + write to sync_log
- Chain gap → ERROR logged, sync blocked until resolved.
- Corrupt row (decode failure) → force-confirmed + deleted, ERROR logged.
  One bad row never permanently blocks delivery of subsequent rows.
- _remote_write() returning [] → all rows retry next cycle.
- _remote_write() raising → all rows retry next cycle.
- prune_sync_log() runs every cleanup_every cycles.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
from abc import ABC, abstractmethod
from typing import Any

from sqloutbox._outbox import Outbox

logger = logging.getLogger(__name__)


def _from_dict(cls: type, data: dict) -> Any:
    """Reconstruct a dataclass from a JSON dict, version-tolerantly.

    Extra keys in data (newer serialized version) → ignored.
    Missing keys (older serialized version) → dataclass default used.
    Safe across deploys that add, remove, or reorder fields.

    Example:
        event = _from_dict(OrderEvent, {"order_id": 42, "status": "paid"})
    """
    known = {f.name for f in dataclasses.fields(cls)}
    return cls(**{k: v for k, v in data.items() if k in known})


class OutboxWorker(ABC):
    """Full outbox sync pipeline — serialize, queue, deliver, clean.

    sqloutbox owns the entire pipeline from event object to confirmed remote
    delivery. The subclass only defines what to decode and where to write.

    Parameters
    ----------
    outbox:
        The Outbox instance to use. Caller owns creation and lifetime.

    flush_interval:
        Seconds between worker poll cycles. Default: 5.0.

    cleanup_every:
        Run prune_sync_log() every N cycles. Default: 500.
    """

    def __init__(
        self,
        outbox:         Outbox,
        flush_interval: float = 5.0,
        cleanup_every:  int   = 500,
    ) -> None:
        self._outbox         = outbox
        self._flush_interval = flush_interval
        self._cleanup_every  = cleanup_every
        self._task: asyncio.Task | None = None
        self._cycle_count    = 0

    # ── Hot path ─────────────────────────────────────────────────────────────

    def _push(self, tag: str, event: Any) -> None:
        """Serialize one event and enqueue it. Main hot-path entry point.

        _encode() converts the event to bytes (default: JSON via
        dataclasses.asdict()). Override _encode() to inject fields before
        encoding (e.g. a deployment env tag).

        Cost: one _encode() call + one SQLite INSERT (~150µs). No network.
        """
        payload = self._encode(event)
        if payload is not None:
            self._outbox.enqueue(tag, payload)
            self._ensure_worker()

    def _push_batch(self, items: list[tuple[str, Any]]) -> None:
        """Serialize and enqueue multiple events in one SQLite transaction.

        All items are encoded and committed together — one commit regardless
        of batch size. Use instead of calling _push() in a loop for high-volume
        bursts (e.g. 200 rejection events from one poll cycle).
        """
        encoded: list[tuple[str, bytes]] = []
        for tag, event in items:
            payload = self._encode(event)
            if payload is not None:
                encoded.append((tag, payload))
        if encoded:
            self._outbox.enqueue_batch(encoded)
            self._ensure_worker()

    def _encode(self, event: Any) -> bytes | None:
        """Serialize an event to bytes for storage in the outbox.

        Default: dataclasses.asdict(event) → JSON UTF-8 bytes.
        Override to inject metadata or add fields before encoding.
        Return None to silently drop the event.
        """
        try:
            return json.dumps(dataclasses.asdict(event)).encode()
        except Exception as exc:
            logger.warning(
                "outbox[%s]: encode failed — event dropped: %s",
                self._outbox.namespace, exc,
            )
            return None

    # ── Worker management ────────────────────────────────────────────────────

    def _ensure_worker(self) -> None:
        """Start the background worker task if not already running."""
        if self._task is None or self._task.done():
            try:
                loop = asyncio.get_running_loop()
                self._task = loop.create_task(
                    self._worker_loop(),
                    name=f"outbox.worker.{type(self).__name__}",
                )
            except RuntimeError:
                pass  # No running event loop (sync test context)

    # ── Background worker ────────────────────────────────────────────────────

    async def _worker_loop(self) -> None:
        """Full delivery cycle: fetch → verify → decode → remote_write → clean."""
        while True:
            await asyncio.sleep(self._flush_interval)
            self._cycle_count += 1

            # Step 1: Fetch pending rows
            rows = await asyncio.to_thread(self._outbox.fetch_unsynced)
            if not rows:
                if self._cycle_count % self._cleanup_every == 0:
                    await asyncio.to_thread(self._outbox.prune_sync_log)
                continue

            # Step 2: Chain integrity — must pass before any I/O
            chain_ok, gap_seqs = await asyncio.to_thread(self._outbox.verify_chain, rows)
            if not chain_ok:
                logger.error(
                    "outbox[%s]: sync blocked — chain gap at seq(s) %s. "
                    "See recovery SQL in sqloutbox README.",
                    self._outbox.namespace, gap_seqs,
                )
                continue

            # Step 3: Decode — bytes → dict → typed event
            # Corrupt rows are force-confirmed so they never permanently block delivery.
            typed_batch:     list[tuple[int, str, Any]] = []
            force_confirmed: list[int]                  = []
            for row in rows:
                try:
                    data  = json.loads(row.payload.decode())
                    event = self._decode(row.tag, data)
                    typed_batch.append((row.seq, row.tag, event))
                except Exception as exc:
                    logger.error(
                        "outbox[%s]: corrupt row seq=%d tag=%s — force-deleting: %s",
                        self._outbox.namespace, row.seq, row.tag, exc,
                    )
                    force_confirmed.append(row.seq)

            # Step 4: Remote write — deliver typed events to destination
            confirmed: list[int] = []
            if typed_batch:
                try:
                    confirmed = await self._remote_write(typed_batch)
                except Exception as exc:
                    logger.warning(
                        "outbox[%s]: remote write failed (%d events) — will retry: %s",
                        self._outbox.namespace, len(typed_batch), exc,
                    )
                    # Still clean up corrupt rows even when remote write fails
                    if force_confirmed:
                        await asyncio.to_thread(self._outbox.mark_synced, force_confirmed)
                        await asyncio.to_thread(self._outbox.delete_synced, force_confirmed)
                    continue

            # Step 5: Mark + delete all confirmed rows
            all_confirmed = confirmed + force_confirmed
            if all_confirmed:
                await asyncio.to_thread(self._outbox.mark_synced, all_confirmed)
                await asyncio.to_thread(self._outbox.delete_synced, all_confirmed)
                logger.debug(
                    "outbox[%s]: delivered %d events (seq %d–%d)",
                    self._outbox.namespace, len(all_confirmed),
                    min(all_confirmed), max(all_confirmed),
                )

            if self._cycle_count % self._cleanup_every == 0:
                await asyncio.to_thread(self._outbox.prune_sync_log)

    # ── Abstract — subclass defines these two ────────────────────────────────

    @abstractmethod
    def _decode(self, tag: str, data: dict) -> Any:
        """Reconstruct a typed event object from its JSON dict and tag.

        Raise on unknown tag or construction failure — the row is force-deleted.
        Use _from_dict() for version-tolerant dataclass reconstruction:

            _CLASSES = {"R": RejectionEvent, "M": MarketSnapshotEvent}

            def _decode(self, tag, data):
                return _from_dict(_CLASSES[tag], data)
        """
        ...

    @abstractmethod
    async def _remote_write(self, batch: list[tuple[int, str, Any]]) -> list[int]:
        """Deliver a batch of typed events to the remote destination.

        Parameters
        ----------
        batch:
            List of (outbox_seq, tag, event_object) in insertion order.
            event_object is whatever _decode() returned.

        Returns
        -------
        list[int]
            outbox_seq values confirmed as successfully delivered.
            Only these seqs are marked synced and deleted.
            Return [] to retry all. Raise to retry all.
        """
        ...
