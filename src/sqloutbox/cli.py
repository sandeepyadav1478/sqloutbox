"""sqloutbox CLI — scaffold a config directory for the drain service.

Usage::

    sqloutbox init [DIR]      Scaffold a config directory (default: ./outbox)

The scaffolded directory contains everything needed to run the service:

    outbox_config.py    OutboxConfig + create_writers() factory
    run_service.py      Entry point — run this to start the drain service
    logging.json        Python logging dict config
    data/               SQLite outbox files (created at runtime)

Start the service::

    python outbox/run_service.py

Or in a systemd unit::

    ExecStart=/usr/bin/python /home/app/outbox/run_service.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_DEFAULT_DIR = "outbox"


# ── Templates ────────────────────────────────────────────────────────────────


_CONFIG_TEMPLATE = '''\
"""sqloutbox outbox configuration.

This file is imported by run_service.py. It must export two names:

    config          An OutboxConfig instance (targets, tuning, paths).
    create_writers  A callable returning {target_name: OutboxWriter}.

Start the service:
    python outbox/run_service.py

Multi-database routing
----------------------
Each target represents ONE remote database. Different tables can be routed
to different databases by defining multiple targets, each with its own
database credentials and writer:

    target "analytics"  →  analytics DB  →  tables: events, metrics, api_calls
    target "billing"    →  billing DB    →  tables: invoices, payments

The sync service collects ALL pending statements for a target into ONE
writer.write_batch() call per cycle, minimising HTTP round-trips per DB.
"""

import os
from pathlib import Path

from sqloutbox import OutboxConfig, TargetConfig

# ── Paths ────────────────────────────────────────────────────────────────────

# Where per-table SQLite outbox files live (one .db per table).
# Created automatically at first write. Shared between producer and consumer.
DB_DIR = Path(__file__).parent / "data"


# ── Database 1 ───────────────────────────────────────────────────────────────
#
# Each target ties a set of tables to a specific remote database.
# The target name must match a key in create_writers() below.
#
# TargetConfig fields:
#
#   name               Unique label — matches a key in the writers dict.
#
#   tables             Tuple of outbox namespaces routed to this database.
#                      Each table gets its own SQLite file: {DB_DIR}/{table}.db.
#
#   inject_outbox_seq  (default: True)
#                      When True, the sync service rewrites every INSERT:
#                        INSERT INTO t (a, b) VALUES (?, ?)
#                      Into:
#                        INSERT OR IGNORE INTO t (a, b, outbox_seq) VALUES (?, ?, ?)
#                      This makes delivery idempotent — if the same row was already
#                      written (crash between remote write and local delete), the
#                      re-attempt silently succeeds via INSERT OR IGNORE.
#                      Set to False for UPDATE statements or when the remote
#                      table has no outbox_seq column.

ANALYTICS_DB_URL   = os.environ.get("ANALYTICS_DB_URL", "")
ANALYTICS_DB_TOKEN = os.environ.get("ANALYTICS_DB_TOKEN", "")

_ANALYTICS = TargetConfig(
    name="analytics",
    tables=(                     # ← tables that live in the analytics database
        "events",
        "metrics",
        "api_calls",
    ),
    inject_outbox_seq=True,
)


# ── Database 2 (optional — uncomment to enable multi-DB routing) ─────────────
#
# Route a different set of tables to a second database.
# Each table belongs to exactly ONE target — no overlaps.

# BILLING_DB_URL   = os.environ.get("BILLING_DB_URL", "")
# BILLING_DB_TOKEN = os.environ.get("BILLING_DB_TOKEN", "")
#
# _BILLING = TargetConfig(
#     name="billing",
#     tables=(                   # ← tables that live in the billing database
#         "invoices",
#         "payments",
#     ),
#     inject_outbox_seq=False,   # these tables have no outbox_seq column
# )


# ── OutboxConfig ─────────────────────────────────────────────────────────────
#
#   db_dir           Directory for SQLite outbox files (required).
#
#   targets          Tuple of TargetConfig entries (see above).
#                    An empty tuple is valid for middleware-only use (no sync).
#
#   batch_size       Max rows fetched per table per sync cycle. Higher values
#                    reduce round-trips but increase memory and remote batch
#                    size. Default: 500.
#
#   flush_interval   Seconds between drain cycles. Lower = less delivery lag,
#                    more CPU. Default: 15.0.
#
#   cleanup_every    Run prune_sync_log() every N cycles to trim the audit
#                    trail (outbox_sync_log table in each SQLite file).
#                    Default: 500 (= every ~2 hours at 15s interval).
#
#   retain_log_days  Days to keep delivered-row records in the outbox_sync_log
#                    table before pruning. These records are used for chain
#                    gap verification — after pruning, the chain gap checker
#                    cannot distinguish "delivered and pruned" from "lost".
#                    Default: 7.

config = OutboxConfig(
    db_dir=DB_DIR,
    targets=(
        _ANALYTICS,
        # _BILLING,             # ← uncomment for multi-DB
    ),
    batch_size=500,
    flush_interval=15.0,
    cleanup_every=500,
    retain_log_days=7,
)


# ── Writers ──────────────────────────────────────────────────────────────────
#
# One writer per target (= one writer per remote database).
# Each writer must implement the OutboxWriter protocol:
#
#   class OutboxWriter(Protocol):
#       async def write_batch(
#           self, stmts: list[tuple[str, list]]
#       ) -> list[dict]:
#           """Send SQL statements to the remote database.
#
#           Parameters:
#               stmts: List of (sql_string, bind_args) tuples.
#
#           Returns:
#               One result dict per statement, in order:
#                 {"ok": True,  "rows_affected": N}   -- confirmed
#                 {"ok": False, "error": "message"}    -- failed (retried next cycle)
#           """
#           ...
#
# The sync service calls write_batch() once per target per cycle with ALL
# pending statements for that target. This minimises HTTP round-trips.
#
# sqloutbox never imports httpx, requests, or any external HTTP library.
# Your writer is the only bridge to the network.


class TursoWriter:
    """Turso / libSQL HTTP pipeline writer.

    Sends all statements in ONE HTTP request. Both "new insert" and
    "INSERT OR IGNORE duplicate" count as confirmed — data IS in the DB.
    """

    def __init__(self, db_url: str, db_token: str) -> None:
        self._pipeline_url = f"{db_url}/v2/pipeline"
        self._token = db_token
        self._client = None

    def _http(self):
        if self._client is None:
            import httpx
            self._client = httpx.AsyncClient(
                headers={"Authorization": f"Bearer {self._token}"},
                timeout=30.0,
            )
        return self._client

    async def write_batch(self, stmts: list[tuple[str, list]]) -> list[dict]:
        if not stmts:
            return []
        pipeline = [
            {"type": "execute", "stmt": {"sql": sql, "args": args}}
            for sql, args in stmts
        ]
        resp = await self._http().post(
            self._pipeline_url, json={"requests": pipeline},
        )
        resp.raise_for_status()
        results = []
        for entry in resp.json()["results"]:
            if entry.get("type") == "error":
                results.append({
                    "ok": False, "rows_affected": 0,
                    "error": entry.get("error", {}).get("message", ""),
                })
            else:
                r = entry["response"]["result"]
                results.append({"ok": True, "rows_affected": r.get("rows_affected", 0)})
        return results


def create_writers() -> dict:
    """Create one writer per target — each connects to its own database.

    The dict keys must match the target names defined above.

    Returns:
        {"analytics": writer_for_analytics_db, "billing": writer_for_billing_db, ...}
    """
    writers = {
        # analytics target → analytics database
        "analytics": TursoWriter(ANALYTICS_DB_URL, ANALYTICS_DB_TOKEN),
    }

    # Uncomment for multi-DB routing:
    # writers["billing"] = TursoWriter(BILLING_DB_URL, BILLING_DB_TOKEN)

    return writers
'''


_SERVICE_TEMPLATE = '''\
"""sqloutbox drain service — entry point.

Loads outbox_config.py from the same directory, sets up logging,
and runs the OutboxSyncService forever.

Start:
    python {dir}/run_service.py

Stop:
    SIGTERM (systemd) or Ctrl+C

Systemd unit example:

    [Unit]
    Description=sqloutbox drain service
    After=network-online.target

    [Service]
    Type=simple
    ExecStart=/usr/bin/python /path/to/{dir}/run_service.py
    Restart=on-failure
    RestartSec=30
    StandardOutput=append:/var/log/sqloutbox.log
    StandardError=append:/var/log/sqloutbox.log

    [Install]
    WantedBy=multi-user.target
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import logging.config
import signal
import sys
from pathlib import Path

_DIR = Path(__file__).parent

# ── Logging setup ────────────────────────────────────────────────────────────

_logging_path = _DIR / "logging.json"
if _logging_path.exists():
    with open(_logging_path) as _f:
        logging.config.dictConfig(json.load(_f))
else:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )

logger = logging.getLogger("sqloutbox.service")


# ── Load config ──────────────────────────────────────────────────────────────

def _load_config():
    config_path = _DIR / "outbox_config.py"
    if not config_path.exists():
        logger.error("outbox_config.py not found in %s", _DIR)
        sys.exit(1)

    spec = importlib.util.spec_from_file_location("outbox_config", str(config_path))
    if spec is None or spec.loader is None:
        logger.error("could not load %s", config_path)
        sys.exit(1)

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    if not hasattr(mod, "config"):
        logger.error("outbox_config.py must define a 'config' variable (OutboxConfig)")
        sys.exit(1)
    if not hasattr(mod, "create_writers"):
        logger.error("outbox_config.py must define a 'create_writers()' function")
        sys.exit(1)

    return mod.config, mod.create_writers()


# ── Main ─────────────────────────────────────────────────────────────────────

async def _run() -> None:
    from sqloutbox import OutboxSyncService

    config, writers = _load_config()
    svc = OutboxSyncService(config=config, writers=writers)

    logger.info(
        "started  db_dir=%s  targets=%s  flush_interval=%.0fs",
        config.db_dir,
        [t.name for t in config.targets],
        config.flush_interval,
    )
    await svc.run()


async def main() -> None:
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def _on_signal(*_: object) -> None:
        logger.info("shutdown signal received")
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    task = loop.create_task(_run(), name="sqloutbox.drain")
    await stop.wait()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    logger.info("stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
'''


_LOGGING_TEMPLATE = """\
{
    "version": 1,
    "disable_existing_loggers": false,
    "formatters": {
        "standard": {
            "format": "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
            "datefmt": "%H:%M:%S"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "stream": "ext://sys.stdout"
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["console"]
    }
}
"""


# ── Init command ─────────────────────────────────────────────────────────────


def cmd_init(config_dir: Path) -> None:
    """Scaffold a config directory with all files needed to run the service."""
    if config_dir.exists() and (config_dir / "outbox_config.py").exists():
        print(f"{config_dir}/outbox_config.py already exists — skipping.",
              file=sys.stderr)
        sys.exit(1)

    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "data").mkdir(exist_ok=True)

    (config_dir / "outbox_config.py").write_text(_CONFIG_TEMPLATE)
    (config_dir / "run_service.py").write_text(
        _SERVICE_TEMPLATE.replace("{dir}", config_dir.name),
    )

    logging_path = config_dir / "logging.json"
    if not logging_path.exists():
        logging_path.write_text(_LOGGING_TEMPLATE)

    print(f"Created {config_dir}/ with:")
    print(f"  outbox_config.py   config + writer factory")
    print(f"  run_service.py     entry point (signal handling, logging)")
    print(f"  logging.json       logging config")
    print(f"  data/              SQLite outbox files (runtime)")
    print()
    print(f"Next steps:")
    print(f"  1. Edit {config_dir}/outbox_config.py — define your targets and writers")
    print(f"  2. Run:  python {config_dir}/run_service.py")


# ── Entry point ──────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> None:
    """CLI entry point — called by ``sqloutbox`` and ``python -m sqloutbox``."""
    parser = argparse.ArgumentParser(
        prog="sqloutbox",
        description="SQLite transactional outbox — config-driven drain service",
    )
    sub = parser.add_subparsers(dest="command")

    p_init = sub.add_parser("init", help="scaffold a config directory")
    p_init.add_argument("dir", nargs="?", default=_DEFAULT_DIR,
                        help="config directory (default: ./outbox)")

    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    if args.command == "init":
        cmd_init(Path(args.dir))
