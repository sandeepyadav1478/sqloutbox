"""sqloutbox CLI — scaffold config and run the drain service.

Usage::

    sqloutbox init [DIR]                  Scaffold a config directory (Python)
    sqloutbox runservice [--config FILE]  Start drain service from TOML config

Quick start (TOML — recommended)::

    # 1. Create outbox.toml (manually or copy from docs)
    # 2. Run the service:
    sqloutbox runservice                      # reads ./outbox.toml
    sqloutbox runservice --config my.toml     # reads custom file

Quick start (Python — for complex bootstrapping)::

    sqloutbox init [DIR]                 # scaffold Python config + runner
    python outbox/run_service.py         # start the service

Systemd unit::

    ExecStart=/usr/bin/python -m sqloutbox runservice --config /path/to/outbox.toml
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

_DEFAULT_DIR = "outbox"


# ── Templates ────────────────────────────────────────────────────────────────


_CONFIG_TEMPLATE = '''\
"""sqloutbox outbox configuration (Python mode).

This file is imported by run_service.py. It must export two names:

    config          An OutboxConfig instance (targets, tuning, paths).
    create_writers  A callable returning {target_name: OutboxWriter}.

Start the service:
    python outbox/run_service.py

Alternative: use TOML config mode instead (recommended for most projects):
    sqloutbox runservice --config outbox.toml

Multi-database routing
----------------------
Each target represents ONE remote database. Different tables can be routed
to different databases by defining multiple targets, each with its own
database credentials and writer:

    target "analytics"  →  analytics DB  →  tables: events, metrics, api_calls
    target "billing"    →  billing DB    →  tables: invoices, payments

The sync service collects ALL pending statements for a target into ONE
writer.write_batch() call per cycle, minimising HTTP round-trips per DB.

inject_outbox_seq control
-------------------------
Controls which tables get ``outbox_seq`` column injection for idempotent delivery.
Can be set at three levels of granularity:

    True                                — all tables in this target get injection
    False                               — no tables get injection
    frozenset({"events", "metrics"})    — only named tables get injection

Per-table control example (mixed):
    TargetConfig(
        name="analytics",
        tables=("events", "metrics", "raw_log"),
        inject_outbox_seq=frozenset({"events", "metrics"}),  # raw_log excluded
    )
"""

import os
from pathlib import Path

from sqloutbox import OutboxConfig, TargetConfig

# ── Paths ────────────────────────────────────────────────────────────────────

# Where per-table SQLite outbox files live (one .db per table).
# Created automatically at first write. Shared between producer and consumer.
DB_DIR = Path(__file__).parent / "data"


# ── Target: analytics ────────────────────────────────────────────────────────
#
# Each target ties a set of tables to a specific remote database.
# The target name must match a key in create_writers() below.

ANALYTICS_DB_URL   = os.environ.get("ANALYTICS_DB_URL", "")
ANALYTICS_DB_TOKEN = os.environ.get("ANALYTICS_DB_TOKEN", "")

_ANALYTICS = TargetConfig(
    name="analytics",
    tables=(                     # ← tables that live in the analytics database
        "events",
        "metrics",
        "api_calls",
    ),
    inject_outbox_seq=True,      # all tables get outbox_seq injection
    batch_size=500,              # per-target batch size override
)


# ── Target: billing (optional — uncomment to enable multi-DB routing) ────────

# BILLING_DB_URL   = os.environ.get("BILLING_DB_URL", "")
# BILLING_DB_TOKEN = os.environ.get("BILLING_DB_TOKEN", "")
#
# _BILLING = TargetConfig(
#     name="billing",
#     tables=(
#         "invoices",
#         "payments",
#     ),
#     inject_outbox_seq=False,   # these tables have no outbox_seq column
# )


# ── OutboxConfig ─────────────────────────────────────────────────────────────

config = OutboxConfig(
    db_dir=DB_DIR,
    targets=(
        _ANALYTICS,
        # _BILLING,             # ← uncomment for multi-DB
    ),
    batch_size=500,              # default for targets that don't override
    flush_interval=1.0,          # seconds between round-robin scans
    table_flush_threshold=15,    # pending rows to trigger immediate flush
    table_max_wait=6.0,          # max seconds before forcing flush
    auto_schema=True,            # auto ADD/DROP outbox_seq on remote tables
    cleanup_every=500,           # prune audit trail every N cycles
    retain_log_days=7,           # days to keep outbox_sync_log records
)


# ── Writers ──────────────────────────────────────────────────────────────────
#
# One writer per target. Must implement the OutboxWriter protocol:
#
#   async def write_batch(self, stmts: list[tuple[str, list]]) -> list[dict]:
#       """Return [{"ok": True, "rows_affected": N}] per stmt."""


class TursoWriter:
    """Turso / libSQL HTTP pipeline writer."""

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
                result = {"ok": True, "rows_affected": r.get("rows_affected", 0)}
                # Include rows for SELECT queries (used by seed_from_remote)
                if "rows" in r:
                    result["rows"] = [
                        [col.get("value") for col in row]
                        for row in r["rows"]
                    ]
                results.append(result)
        return results


def create_writers() -> dict:
    """Create one writer per target — keys must match target names."""
    writers = {
        "analytics": TursoWriter(ANALYTICS_DB_URL, ANALYTICS_DB_TOKEN),
    }
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
        "started  db_dir=%s  targets=%s  poll=%.1fs  threshold=%d  max_wait=%.1fs",
        config.db_dir,
        [t.name for t in config.targets],
        config.flush_interval,
        config.table_flush_threshold,
        config.table_max_wait,
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
    print()
    print(f"  Alternative: use TOML config mode (recommended for most projects):")
    print(f"    sqloutbox runservice --config outbox.toml")
    print(f"    See: https://github.com/sandeepyadav1478/sqloutbox#toml-config")


# ── runservice command ───────────────────────────────────────────────────────


def cmd_runservice(config_path: Path | None) -> None:
    """Start the outbox drain service from a TOML config file.

    Resolution order:
        1. ``--config <file>`` → use that file
        2. No flag → look for ``outbox.toml`` in cwd
        3. Neither exists → error with usage hint
    """
    from sqloutbox._runner import DEFAULT_CONFIG_FILE, run_service_main

    if config_path is None:
        config_path = Path(DEFAULT_CONFIG_FILE)

    if not config_path.exists():
        if config_path.name == DEFAULT_CONFIG_FILE:
            print(
                f"error: config file not found: {config_path}\n\n"
                f"Provide a config file:\n"
                f"  sqloutbox runservice --config <file.toml>\n\n"
                f"Or create '{DEFAULT_CONFIG_FILE}' in the current directory.\n"
                f"See: https://github.com/sandeepyadav1478/sqloutbox#toml-config",
                file=sys.stderr,
            )
        else:
            print(
                f"error: config file not found: {config_path}",
                file=sys.stderr,
            )
        sys.exit(1)

    # Setup logging (respects LOG_LEVEL env var)
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        asyncio.run(run_service_main(config_path))
    except KeyboardInterrupt:
        pass


# ── verify command ──────────────────────────────────────────────────────────


def cmd_verify(config_path: Path | None, db_dir_path: Path | None) -> None:
    """Run offline integrity verification on outbox ``.db`` files.

    Two modes:

    1. ``--config outbox.toml`` — discover ``.db`` files from TOML config
    2. ``--db-dir /path/to/data`` — scan directory for ``*.db`` files

    Prints a structured report and exits with code 0 (all OK) or 1 (failures).
    """
    from sqloutbox._outbox import Outbox
    from sqloutbox._verify import verify_all

    outboxes: dict[str, Outbox] = {}

    if config_path is not None:
        from sqloutbox._runner import load_config_toml
        config, _writers = load_config_toml(config_path)
        for target in config.targets:
            db_dir = target.db_dir or config.db_dir
            for table in target.tables:
                db_path = db_dir / f"{table}.db"
                if db_path.exists():
                    outboxes[f"{target.name}.{table}"] = Outbox(
                        db_path=db_path, namespace=table,
                    )
                else:
                    print(f"  skip {table}.db — file not found at {db_path}")

    elif db_dir_path is not None:
        if not db_dir_path.is_dir():
            print(f"error: not a directory: {db_dir_path}", file=sys.stderr)
            sys.exit(1)
        for db_file in sorted(db_dir_path.glob("*.db")):
            name = db_file.stem
            outboxes[name] = Outbox(db_path=db_file, namespace=name)

    else:
        print(
            "error: provide --config <file.toml> or --db-dir <path>\n\n"
            "Usage:\n"
            "  sqloutbox verify --config outbox.toml\n"
            "  sqloutbox verify --db-dir /path/to/data",
            file=sys.stderr,
        )
        sys.exit(1)

    if not outboxes:
        print("no .db files found — nothing to verify")
        sys.exit(0)

    # Run verification
    result = verify_all(outboxes)

    # Print report
    print()
    print("sqloutbox verify — integrity scan")
    print("-" * 70)
    for t in result.tables:
        chain_str = "ok" if t.chain_ok else f"GAPS{list(t.chain_gaps)}"
        seq_str = "ok" if t.seq_continuous else "GAPS"
        ts_str = "ok" if t.timestamps_monotonic else "DRIFT"
        status = "OK" if t.ok else "FAIL"
        print(
            f"  {t.table:<30s}  {status:<4s}  "
            f"pending={t.pending_count}  rows={t.total_rows}  "
            f"chain={chain_str}  seq={seq_str}  ts={ts_str}"
        )
        if t.errors:
            for err in t.errors:
                print(f"    ! {err}")
    print("-" * 70)
    passed = sum(1 for t in result.tables if t.ok)
    total = len(result.tables)
    print(
        f"  Result: {passed}/{total} passed  "
        f"duration={result.duration_ms:.0f}ms"
    )
    if not result.ok:
        failed = [t.table for t in result.tables if not t.ok]
        print(f"  Failed: {', '.join(failed)}")
    print()

    sys.exit(0 if result.ok else 1)


# ── Entry point ──────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> None:
    """CLI entry point — called by ``sqloutbox`` and ``python -m sqloutbox``."""
    parser = argparse.ArgumentParser(
        prog="sqloutbox",
        description="SQLite transactional outbox — config-driven drain service",
    )
    sub = parser.add_subparsers(dest="command")

    p_init = sub.add_parser("init", help="scaffold a Python config directory")
    p_init.add_argument("dir", nargs="?", default=_DEFAULT_DIR,
                        help="config directory (default: ./outbox)")

    p_run = sub.add_parser(
        "runservice",
        help="start drain service from TOML config",
    )
    p_run.add_argument(
        "--config", "-c", type=Path, default=None,
        help="TOML config file (default: ./outbox.toml)",
    )

    p_verify = sub.add_parser(
        "verify",
        help="run integrity verification on outbox .db files",
    )
    p_verify.add_argument(
        "--config", "-c", type=Path, default=None, dest="verify_config",
        help="TOML config file (discover .db files from targets)",
    )
    p_verify.add_argument(
        "--db-dir", "-d", type=Path, default=None,
        help="directory to scan for *.db files",
    )

    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    if args.command == "init":
        cmd_init(Path(args.dir))
    elif args.command == "runservice":
        cmd_runservice(args.config)
    elif args.command == "verify":
        cmd_verify(args.verify_config, args.db_dir)
