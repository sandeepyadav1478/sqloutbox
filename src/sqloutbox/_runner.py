"""TOML config loader and service runner for ``sqloutbox runservice``.

Loads an ``outbox.toml`` config file, dynamically imports writer classes,
and runs the OutboxSyncService drain loop with signal handling.

Environment loading
-------------------
Before parsing the TOML, the loader resolves secrets in this order:

1. **os.environ** — already-set env vars (systemd, shell, CI) take highest priority
2. **.env file** — from config file's directory; adds vars NOT already in os.environ
3. **Doppler** — if ``DOPPLER_TOKEN`` is present after steps 1+2, secrets are
   fetched from the Doppler API; adds vars NOT already set

``${VAR_NAME}`` in TOML string values is then replaced with the resolved value.
Missing variables raise an error at startup (fail fast, not at first write).

TOML format
-----------
Each ``[app.*]`` section defines one application.  Apps are isolated —
they have their own ``db_dir``, tuning, and database targets.

::

    # ── App: pulseview ──────────────────────────────────────
    [app.pulseview]
    db_dir = "data/pulseview"          # outbox .db files for this app
    batch_size = 500                   # per-app tuning (all optional)
    flush_interval = 15.0
    auto_schema = true

    # Database: analytics (Turso)
    # .db files: loan_rejections.db, market_snapshots.db
    [app.pulseview.db.analytics]
    writer_class = "myapp.writers:TursoWriter"
    inject_outbox_seq = true           # default for all tables below
    tables = [
        "loan_rejections",
        "market_snapshots",
    ]

    [app.pulseview.db.analytics.connection]
    db_url  = "${PULSEVIEW_TURSO_URL}"
    db_token = "${PULSEVIEW_TURSO_TOKEN}"

    # ── App: autopulse ──────────────────────────────────────
    [app.autopulse]
    db_dir = "data/autopulse"

    [app.autopulse.db.main]
    writer_class = "myapp.writers:TursoWriter"
    inject_outbox_seq = false
    tables = ["wallet_transactions", "loan_placements"]

    [app.autopulse.db.main.connection]
    db_url  = "${TURSO_URL}"
    db_token = "${TURSO_AUTH_TOKEN}"


Per-table overrides
-------------------
The db-level ``inject_outbox_seq`` and ``retain_log_days`` are defaults
for all tables.  Override per table using inline tables::

    tables = [
        "events",                                         # uses db defaults
        { name = "raw_log", inject_outbox_seq = false },  # override inject
        { name = "api_calls", retain_log_days = 7 },      # override retention
    ]
"""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
import re
import signal
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

logger = logging.getLogger("sqloutbox.runner")

DEFAULT_CONFIG_FILE = "outbox.toml"

_ENV_RE = re.compile(r"\$\{([^}]+)\}")

# Keys in [app.NAME] that are tuning params (not db_dir or db sections)
_APP_TUNING_KEYS = frozenset({
    "batch_size", "flush_interval", "table_flush_threshold", "table_max_wait",
    "auto_schema", "cleanup_every", "retain_log_days",
})


# ── Environment loading ─────────────────────────────────────────────────────


def _load_env_file(path: Path) -> dict[str, str]:
    """Parse a ``.env`` file into a dict.

    Supports ``KEY=VALUE``, ``KEY="VALUE"``, ``KEY='VALUE'``,
    blank lines, and ``#`` comments.  Does NOT handle multi-line values
    or shell expansions — keep it simple.
    """
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        # Strip surrounding quotes
        if (
            len(value) >= 2
            and value[0] == value[-1]
            and value[0] in ('"', "'")
        ):
            value = value[1:-1]
        if key:
            result[key] = value
    return result


def _load_doppler(token: str) -> dict[str, str]:
    """Fetch secrets from Doppler API using a service token.

    The service token is scoped to a specific project + config on Doppler,
    so no extra parameters are needed.

    Security:
    - HTTPS only (hardcoded URL)
    - Token never logged (masked to first 8 chars in warnings)
    - 10s timeout to prevent hanging
    - SSL verification via system CA bundle (default urllib behaviour)

    Returns {KEY: VALUE} dict.  Raises on HTTP or JSON errors.
    """
    url = (
        "https://api.doppler.com/v3/configs/config/secrets/download"
        "?format=json"
    )
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())

    # Doppler returns {KEY: VALUE} when format=json
    if not isinstance(data, dict):
        raise ValueError(
            f"Unexpected Doppler response type: {type(data).__name__}"
        )
    return {k: str(v) for k, v in data.items()}


def _mask_token(token: str) -> str:
    """Mask a token for safe logging — show first 8 chars only."""
    if len(token) <= 8:
        return "***"
    return token[:8] + "***"


def _prepare_env(config_path: Path) -> None:
    """Load .env file and Doppler secrets into ``os.environ``.

    Priority (highest first):
    1. Already-set os.environ (systemd, shell, CI)
    2. .env file values (from config file's directory)
    3. Doppler secrets (if DOPPLER_TOKEN available after 1+2)

    Uses ``os.environ.setdefault()`` — never overwrites existing vars.
    """
    # ── .env file ────────────────────────────────────────────────────
    env_path = config_path.parent / ".env"
    if env_path.exists():
        env_vars = _load_env_file(env_path)
        loaded = 0
        for k, v in env_vars.items():
            if k not in os.environ:
                os.environ[k] = v
                loaded += 1
        if loaded:
            logger.info(
                "loaded %d vars from %s (%d already set)",
                loaded, env_path, len(env_vars) - loaded,
            )

    # ── Doppler secrets ──────────────────────────────────────────────
    doppler_token = os.environ.get("DOPPLER_TOKEN")
    if not doppler_token:
        return

    logger.info("DOPPLER_TOKEN found — fetching secrets from Doppler...")
    try:
        secrets = _load_doppler(doppler_token)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(
            f"Doppler secrets fetch failed (HTTP {exc.code}) for token "
            f"{_mask_token(doppler_token)}. DOPPLER_TOKEN is configured — "
            f"the service cannot start without Doppler secrets. "
            f"Fix the token or remove DOPPLER_TOKEN to skip Doppler."
        ) from exc
    except Exception as exc:
        raise RuntimeError(
            f"Doppler secrets fetch failed for token "
            f"{_mask_token(doppler_token)}: {exc}. "
            f"DOPPLER_TOKEN is configured — the service cannot start "
            f"without Doppler secrets. "
            f"Fix the connection or remove DOPPLER_TOKEN to skip Doppler."
        ) from exc

    loaded = 0
    for k, v in secrets.items():
        if k not in os.environ:
            os.environ[k] = v
            loaded += 1
    logger.info(
        "loaded %d secrets from Doppler (%d already set)",
        loaded, len(secrets) - loaded,
    )


# ── Helpers ──────────────────────────────────────────────────────────────────


def _interpolate_env(value: str) -> str:
    """Replace ``${VAR}`` patterns with environment variable values.

    Raises ``EnvironmentError`` if a referenced variable is not set.
    """

    def _replace(m: re.Match) -> str:
        var = m.group(1)
        val = os.environ.get(var)
        if val is None:
            raise EnvironmentError(
                f"Environment variable ${{{var}}} is not set "
                f"(referenced in outbox config)"
            )
        return val

    return _ENV_RE.sub(_replace, value)


def _interpolate_dict(d: dict[str, Any]) -> dict[str, Any]:
    """Recursively interpolate ``${VAR}`` in all string values of a dict."""
    result: dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, str):
            result[k] = _interpolate_env(v)
        elif isinstance(v, dict):
            result[k] = _interpolate_dict(v)
        else:
            result[k] = v
    return result


def _import_class(dotted_path: str) -> type:
    """Import a class from a ``module.path:ClassName`` string.

    Raises ``ValueError`` for malformed paths, ``ImportError`` for missing
    modules, ``AttributeError`` for missing class names.
    """
    if ":" not in dotted_path:
        raise ValueError(
            f"writer_class must be 'module.path:ClassName', got: {dotted_path!r}"
        )
    module_path, class_name = dotted_path.rsplit(":", 1)
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise ImportError(
            f"Could not import writer module '{module_path}': {exc}"
        ) from exc

    cls = getattr(module, class_name, None)
    if cls is None:
        raise AttributeError(
            f"Module '{module_path}' has no attribute '{class_name}'"
        )
    return cls


def _load_tomllib():
    """Return the tomllib module (stdlib 3.11+ or tomli backport)."""
    try:
        import tomllib
        return tomllib
    except ModuleNotFoundError:
        pass
    try:
        import tomli as tomllib  # type: ignore[no-redef]
        return tomllib
    except ModuleNotFoundError:
        raise RuntimeError(
            "TOML support requires Python 3.11+ (tomllib) or "
            "'pip install tomli' for Python 3.10. "
            "Install with: pip install tomli"
        )


# ── Table parsing ────────────────────────────────────────────────────────────


def _parse_tables(
    raw_tables: list,
    db_default_inject: bool,
    db_default_retain: int,
    app_name: str,
    db_name: str,
) -> tuple[tuple[str, ...], bool | frozenset[str], tuple[tuple[str, int], ...]]:
    """Parse a tables list that may contain strings and/or inline dicts.

    Returns (table_names, inject_outbox_seq, table_retain_overrides) where:
    - inject_outbox_seq: True/False/frozenset
    - table_retain_overrides: tuple of (name, days) for tables with explicit
      retain_log_days (only those that differ from db_default_retain)
    """
    table_names: list[str] = []
    inject_tables: set[str] = set()
    retain_overrides: list[tuple[str, int]] = []

    for item in raw_tables:
        if isinstance(item, str):
            table_names.append(item)
            if db_default_inject:
                inject_tables.add(item)
        elif isinstance(item, dict):
            name = item.get("name")
            if not name:
                raise ValueError(
                    f"[app.{app_name}.db.{db_name}] table entry "
                    f"missing 'name': {item}"
                )
            table_names.append(name)
            table_inject = item.get("inject_outbox_seq", db_default_inject)
            if table_inject:
                inject_tables.add(name)
            # Per-table retain_log_days override
            if "retain_log_days" in item:
                retain_overrides.append((name, item["retain_log_days"]))
        else:
            raise ValueError(
                f"[app.{app_name}.db.{db_name}] tables must be strings "
                f"or {{name = ..., inject_outbox_seq = ...}}, got: {item!r}"
            )

    if not table_names:
        raise ValueError(
            f"[app.{app_name}.db.{db_name}] 'tables' must not be empty"
        )

    # Simplify: bool for all-or-nothing, frozenset for mixed
    if inject_tables == set(table_names):
        inject: bool | frozenset[str] = True
    elif not inject_tables:
        inject = False
    else:
        inject = frozenset(inject_tables)

    return tuple(table_names), inject, tuple(retain_overrides)


# ── Config loader ────────────────────────────────────────────────────────────


def load_config_toml(
    config_path: Path,
) -> tuple[Any, dict[str, Any]]:
    """Parse an outbox TOML config file into OutboxConfig + writers dict.

    Before parsing, loads ``.env`` and Doppler secrets into ``os.environ``
    (see ``_prepare_env``).

    The TOML format uses a hierarchical ``app → db`` structure::

        [app.myapp]
        db_dir = "data/myapp"
        batch_size = 500

        [app.myapp.db.primary]
        writer_class = "myapp:Writer"
        tables = ["events"]

        [app.myapp.db.primary.connection]
        url = "${DB_URL}"

    Parameters
    ----------
    config_path:
        Path to the TOML config file.

    Returns
    -------
    (OutboxConfig, writers)
        Ready to pass to ``OutboxSyncService(config=config, writers=writers)``.

    Raises
    ------
    FileNotFoundError
        Config file does not exist.
    ValueError
        Missing required fields in the config.
    EnvironmentError
        Referenced ``${VAR}`` not set in environment.
    ImportError
        Writer class module cannot be imported.
    """
    from sqloutbox.config import OutboxConfig, TargetConfig

    tomllib = _load_tomllib()

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    # ── Load .env + Doppler before parsing TOML ─────────────────────
    _prepare_env(config_path)

    with open(config_path, "rb") as f:
        raw = tomllib.load(f)

    # ── [app.*] sections ─────────────────────────────────────────────
    apps = raw.get("app", {})
    if not apps:
        raise ValueError(
            "At least one [app.*] section is required in config"
        )

    targets: list[TargetConfig] = []
    writers: dict[str, Any] = {}
    first_db_dir: Path | None = None
    first_app_tuning: dict[str, Any] | None = None
    seen_target_names: dict[str, str] = {}  # target_name -> app_name

    for app_name, app_cfg in apps.items():
        # ── App-level: db_dir (required) ─────────────────────────────
        db_dir_str = app_cfg.get("db_dir")
        if not db_dir_str:
            raise ValueError(
                f"[app.{app_name}].db_dir is required"
            )

        app_db_dir = Path(db_dir_str)
        if not app_db_dir.is_absolute():
            app_db_dir = config_path.parent / app_db_dir

        if first_db_dir is None:
            first_db_dir = app_db_dir

        # ── App-level tuning (optional, applied to targets) ──────────
        app_tuning = {
            k: app_cfg[k] for k in _APP_TUNING_KEYS if k in app_cfg
        }
        if first_app_tuning is None:
            first_app_tuning = app_tuning

        app_batch_size = app_tuning.get("batch_size", 500)

        # ── [app.NAME.db.*] sections ────────────────────────────────
        dbs = app_cfg.get("db", {})
        if not dbs:
            raise ValueError(
                f"[app.{app_name}] must have at least one "
                f"[app.{app_name}.db.*] section"
            )

        for db_name, db_cfg in dbs.items():
            target_name = f"{app_name}.{db_name}"

            # Uniqueness check
            if target_name in seen_target_names:
                raise ValueError(
                    f"Duplicate target '{target_name}' in config"
                )
            seen_target_names[target_name] = app_name

            # writer_class (required)
            writer_class_path = db_cfg.get("writer_class")
            if not writer_class_path:
                raise ValueError(
                    f"[app.{app_name}.db.{db_name}] missing 'writer_class'"
                )

            # tables (required) — strings or inline dicts
            raw_tables = db_cfg.get("tables")
            if not raw_tables:
                raise ValueError(
                    f"[app.{app_name}.db.{db_name}] missing 'tables'"
                )

            db_default_inject = db_cfg.get("inject_outbox_seq", True)
            db_retain = db_cfg.get("retain_log_days", 30)
            table_names, inject_seq, retain_overrides = _parse_tables(
                raw_tables, db_default_inject, db_retain,
                app_name, db_name,
            )

            targets.append(TargetConfig(
                name=target_name,
                tables=table_names,
                inject_outbox_seq=inject_seq,
                db_dir=app_db_dir,
                batch_size=app_batch_size,
                retain_log_days=db_retain,
                table_retain_overrides=retain_overrides,
            ))

            # ── Writer for this target ───────────────────────────────
            writer_cls = _import_class(writer_class_path)
            raw_conn = db_cfg.get("connection", {})
            conn_args = _interpolate_dict(raw_conn)
            writers[target_name] = writer_cls(**conn_args)

    # ── Build OutboxConfig ───────────────────────────────────────────
    # Service-level tuning comes from the first app's settings.
    tuning = first_app_tuning or {}

    config = OutboxConfig(
        db_dir=first_db_dir or config_path.parent,
        targets=tuple(targets),
        batch_size=tuning.get("batch_size", 500),
        flush_interval=tuning.get("flush_interval", 1.0),
        table_flush_threshold=tuning.get("table_flush_threshold", 15),
        table_max_wait=tuning.get("table_max_wait", 6.0),
        auto_schema=tuning.get("auto_schema", True),
        cleanup_every=tuning.get("cleanup_every", 500),
        retain_log_days=tuning.get("retain_log_days", 30),
    )

    return config, writers


# ── Service runner ───────────────────────────────────────────────────────────


async def _run_service(config_path: Path) -> None:
    """Load config and run the OutboxSyncService forever."""
    from sqloutbox.sync import OutboxSyncService

    config, writers = load_config_toml(config_path)

    svc = OutboxSyncService(config=config, writers=writers)

    logger.info(
        "config=%s  poll=%.1fs  threshold=%d  max_wait=%.1fs",
        config_path,
        config.flush_interval, config.table_flush_threshold,
        config.table_max_wait,
    )
    for target in config.targets:
        db_dir = target.db_dir or config.db_dir
        db_files = [f"{table}.db" for table in target.tables]
        logger.info(
            "  target '%s'  db_dir=%s  batch=%d  .db files: %s",
            target.name, db_dir, target.batch_size, db_files,
        )
    await svc.run()


async def run_service_main(config_path: Path) -> None:
    """Entry point with signal handling.  Runs until SIGTERM / SIGINT."""
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def _on_signal(*_: object) -> None:
        logger.info("shutdown signal received")
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    task = loop.create_task(_run_service(config_path), name="sqloutbox.drain")
    await stop.wait()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    logger.info("stopped")
