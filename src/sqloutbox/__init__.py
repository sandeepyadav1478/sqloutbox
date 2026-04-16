"""sqloutbox — config-driven SQLite transactional outbox service.

Zero external dependencies (stdlib only). Designed for single-process
deployments with asyncio.

Pattern
-------
**Producer** writes SQL events synchronously to a local SQLite file (~150µs,
no network). **Consumer** drains them to N remote databases in strict order,
with singly-linked chain integrity verification on every batch.

Quick start (TOML — recommended)::

    # outbox.toml
    [app.myapp]
    db_dir = "data/myapp"

    [app.myapp.db.primary]
    writer_class = "myapp.writers:TursoWriter"
    tables = ["orders", "payments"]

    [app.myapp.db.primary.connection]
    db_url  = "${DB_URL}"
    db_token = "${DB_TOKEN}"

    # Run:
    sqloutbox runservice --config outbox.toml

Quick start (Python)::

    sqloutbox init                  # scaffold config + runner
    python outbox/run_service.py    # start the drain service

Config features
---------------
- Multi-app isolation: ``[app.NAME]`` sections with independent db_dir/tuning
- Per-table inject_outbox_seq: ``True``, ``False``, or ``frozenset`` of names
- ``${VAR}`` interpolation in TOML string values
- Credential loading: os.environ > .env file > Doppler API (fail-fast)
- Per-app tuning: batch_size, flush_interval, thresholds per ``[app.NAME]``
- Target naming: ``{app_name}.{db_name}`` for cross-app uniqueness

Modules
-------
    sqloutbox.config      OutboxConfig, TargetConfig (frozen dataclasses)
    sqloutbox.middleware   SQLMiddleware base class (hot-path producer)
    sqloutbox.sync        OutboxSyncService, OutboxWriter, inject_outbox_seq
    sqloutbox.cli         CLI: init (scaffold) + runservice (TOML drain)
    sqloutbox._runner     TOML config loader, .env/Doppler, service runner
    sqloutbox._outbox     Outbox class (low-level SQLite queue)
    sqloutbox._worker     OutboxWorker abstract base
    sqloutbox._models     QueueRow dataclass
    sqloutbox._schema     SQL schema definitions, WAL setup
    sqloutbox._registry   shared_outbox() singleton registry
"""

from importlib.metadata import version as _pkg_version

__version__ = _pkg_version("sqloutbox")

from sqloutbox._models import QueueRow
from sqloutbox._outbox import Outbox
from sqloutbox._registry import shared_outbox, clear_registry
from sqloutbox._worker import OutboxWorker
from sqloutbox.config import OutboxConfig, TargetConfig
from sqloutbox.middleware import SQLMiddleware
from sqloutbox.sync import OutboxSyncService, OutboxWriter, inject_outbox_seq
from sqloutbox._runner import load_config_toml
from sqloutbox._verify import TableVerifyResult, VerifyResult, verify_all, verify_outbox

__all__ = [
    # Core queue
    "Outbox",
    "OutboxWorker",
    "QueueRow",
    "shared_outbox",
    "clear_registry",
    # Configuration
    "OutboxConfig",
    "TargetConfig",
    "load_config_toml",
    # SQL middleware
    "SQLMiddleware",
    # Sync service
    "OutboxSyncService",
    "OutboxWriter",
    "inject_outbox_seq",
    # Verification
    "TableVerifyResult",
    "VerifyResult",
    "verify_outbox",
    "verify_all",
]
