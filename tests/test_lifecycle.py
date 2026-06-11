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
