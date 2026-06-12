"""WS-6: log hygiene — stuck namespace WARNs once-on-transition, not per retry."""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

import pytest

import sqloutbox.sync
from sqloutbox._outbox import Outbox
from sqloutbox.config import OutboxConfig, TargetConfig
from sqloutbox.sync import OutboxSyncService


class _AlwaysFailWriter:
    """Every statement fails — drives a namespace into 'stuck'."""
    def __init__(self) -> None:
        self.calls = 0

    async def write_batch(self, stmts):
        self.calls += 1
        return [{"ok": False, "error": "connection refused"} for _ in stmts]


def _make_failing_service(tmp_path: Path, writer, *, table="evt", max_attempts=None):
    # auto_schema=False + inject_outbox_seq=False so startup _ensure_schema()/
    # _seed_from_remote() do NOT call write_batch. max_attempts=None so the
    # stuck head is never dead-lettered (we want it to STAY stuck across cycles).
    cfg = OutboxConfig(
        db_dir=tmp_path,
        targets=(TargetConfig(name="primary", tables=(table,),
                              inject_outbox_seq=False),),
        flush_interval=0.01,
        table_flush_threshold=1,
        table_max_wait=0.0,
        max_attempts=max_attempts,
        auto_schema=False,
    )
    return OutboxSyncService(config=cfg, writers={"primary": writer})


@pytest.mark.asyncio
async def test_stuck_namespace_warns_once_not_every_cycle(tmp_path: Path, caplog, monkeypatch):
    """The once-on-transition 'stuck' WARN fires once across many failing retries."""
    # Neutralize the WS-1 backoff gate so the head is re-fetched and re-attempted
    # every cycle (otherwise backoff defers the 2nd attempt by 2+ minutes).
    monkeypatch.setattr(sqloutbox.sync, "_backoff_eligible", lambda *a, **k: True)

    writer = _AlwaysFailWriter()
    svc = _make_failing_service(tmp_path, writer)
    Outbox(db_path=tmp_path / "evt.db", namespace="evt").enqueue(
        "INSERT INTO evt (a) VALUES (?)", json.dumps([1]).encode()
    )

    caplog.set_level(logging.WARNING, logger="sqloutbox.sync")
    task = asyncio.create_task(svc.run())
    await asyncio.sleep(0.3)            # many failing cycles elapse
    svc.request_stop()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # The writer was retried many times ...
    assert writer.calls >= 3, f"expected repeated retries, got {writer.calls}"
    # ... but the once-on-transition 'namespace stuck' WARN appears exactly once.
    stuck_warns = [r for r in caplog.records if "namespace stuck" in r.getMessage()]
    assert len(stuck_warns) == 1, f"expected 1 stuck WARN, got {len(stuck_warns)}"
    assert "evt" in stuck_warns[0].getMessage()


@pytest.mark.asyncio
async def test_recovery_logs_once(tmp_path: Path, caplog, monkeypatch):
    """When a stuck namespace recovers, an INFO 'recovered' fires once."""
    monkeypatch.setattr(sqloutbox.sync, "_backoff_eligible", lambda *a, **k: True)

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
    svc.request_stop()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    recovered = [r for r in caplog.records if "namespace recovered" in r.getMessage()]
    assert len(recovered) == 1, f"expected 1 recovered INFO, got {len(recovered)}"
    assert "evt" in recovered[0].getMessage()
