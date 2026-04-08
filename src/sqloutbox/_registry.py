"""shared_outbox() — one write connection per (db_path, namespace).

When multiple callers write to the same SQLite file and namespace (e.g.
SchedulerMiddleware and InvestorMiddleware both write to loan_rejections.db),
this registry returns the SAME Outbox instance, guaranteeing:

    - ONE persistent write connection per file (no concurrent-writer serialization cost)
    - No second caller waits on BEGIN IMMEDIATE held by the first
    - All writes from any caller are serialized through one connection

Usage
-----
    from sqloutbox import shared_outbox

    # Both calls return the same Outbox instance
    ob1 = shared_outbox(db_path=Path("events.db"), namespace="orders")
    ob2 = shared_outbox(db_path=Path("events.db"), namespace="orders")
    assert ob1 is ob2   # True — same object, same connection

Thread safety
-------------
The registry dict is protected by a threading.Lock for safe use from
multiple threads. Python dict operations are GIL-protected but double-check
logic (check-then-set) requires an explicit lock to be truly thread-safe.
"""

from __future__ import annotations

import threading
from pathlib import Path

from sqloutbox._outbox import Outbox

_lock: threading.Lock = threading.Lock()
_registry: dict[tuple[str, str], Outbox] = {}


def shared_outbox(
    db_path:   Path,
    namespace: str,
    **kwargs,
) -> Outbox:
    """Return (or create) the shared Outbox for (db_path, namespace).

    Guarantees exactly ONE Outbox instance — and therefore ONE write
    connection — per (db_path, namespace) pair. Subsequent calls with the
    same arguments return the cached instance.

    Parameters
    ----------
    db_path:
        Path to the SQLite file.

    namespace:
        Partition key within the file (e.g. table name, service name).

    **kwargs:
        Forwarded to Outbox() on first creation: batch_size, retain_log_days,
        cleanup_every. Ignored on cache hits.
    """
    key = (str(db_path.resolve()), namespace)
    # Fast path — no lock if already cached (common case after startup)
    if key in _registry:
        return _registry[key]
    with _lock:
        # Double-check under lock
        if key not in _registry:
            _registry[key] = Outbox(db_path=db_path, namespace=namespace, **kwargs)
        return _registry[key]


def clear_registry() -> None:
    """Remove all cached instances. Intended for testing only."""
    with _lock:
        _registry.clear()
