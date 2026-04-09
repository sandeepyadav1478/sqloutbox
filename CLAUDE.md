# CLAUDE.md — sqloutbox

## Commands

```bash
# Run all tests (146 passing)
python -m pytest tests/ -v

# Run a single test file
python -m pytest tests/test_runner.py -v

# Run with short tracebacks on first failure
python -m pytest tests/ -x --tb=short
```

## Architecture

sqloutbox is a standalone SQLite transactional outbox service. Zero external
dependencies (stdlib only). Apps are consumers — they register targets via
config, provide transport writers, sqloutbox handles the rest.

### Two roles

| Role | Class | Latency |
|------|-------|---------|
| **Producer** | `SQLMiddleware` | ~150µs sync (local SQLite INSERT) |
| **Consumer** | `OutboxSyncService` | async, batched drain to remote DBs |

### Module map

```
src/sqloutbox/
├── __init__.py       Public API (all exports)
├── config.py         OutboxConfig, TargetConfig (frozen dataclasses)
├── middleware.py      SQLMiddleware base class (hot-path producer)
├── sync.py           OutboxSyncService drain daemon + inject_outbox_seq
├── cli.py            CLI: init (scaffold) + runservice (TOML drain)
├── _runner.py        TOML config loader, .env/Doppler, service runner
├── _outbox.py        Outbox class (low-level SQLite queue)
├── _worker.py        OutboxWorker abstract base
├── _models.py        QueueRow dataclass
├── _schema.py        SQL schema definitions, WAL setup
└── _registry.py      shared_outbox() singleton registry
```

### Key patterns

- **Chain integrity**: each row stores `prev_seq`; `verify_chain()` blocks delivery on gaps
- **Idempotent delivery**: `inject_outbox_seq` transforms INSERT → INSERT OR IGNORE with outbox_seq
- **Remote-seeded seq**: on startup, `_seed_from_remote()` queries `MAX(outbox_seq)` from remote DB to seed local AUTOINCREMENT — prevents seq collision on machine migration
- **Remote schema**: `outbox_seq INTEGER NOT NULL DEFAULT 0` + partial unique index `WHERE outbox_seq != 0`
- **Per-table injection**: `TargetConfig.inject_outbox_seq` accepts `True`, `False`, or `frozenset[str]`
- **Multi-app isolation**: TOML config uses `[app.NAME]` sections, each with own `db_dir`
- **Credential loading**: os.environ > .env file > Doppler API (strict priority, fail-fast on Doppler errors)

### TOML config structure

```
[app.NAME]                          → app-level: db_dir, tuning params
[app.NAME.db.DB_NAME]               → target: writer_class, tables, inject_outbox_seq
[app.NAME.db.DB_NAME.connection]    → writer kwargs: ${VAR} interpolated from env
```

Target names: `{app_name}.{db_name}` (e.g., `pulseview.analytics`).

### Config defaults

| Parameter | Default |
|-----------|---------|
| `batch_size` | 500 |
| `flush_interval` | 1.0 |
| `table_flush_threshold` | 15 |
| `table_max_wait` | 6.0 |
| `auto_schema` | True |
| `cleanup_every` | 500 |
| `retain_log_days` | 30 |

## Test suite

161 tests across 6 files:

| File | Tests | Coverage |
|------|-------|----------|
| `test_runner.py` | 62 | TOML config loader, .env, Doppler, multi-app, per-table inject, retain |
| `test_sqloutbox.py` | 35 | Core Outbox queue, chain integrity, sync_log, seed_sequence |
| `test_config.py` | 29 | OutboxConfig, TargetConfig, schema_sql, per-table frozenset, retain |
| `test_sync.py` | 19 | OutboxSyncService drain loop, writer mock, multi-target, seed_from_remote |
| `test_cli.py` | 9 | CLI commands, argument parsing |
| `test_middleware.py` | 7 | SQLMiddleware _push(), _push_many() |

## Critical invariants

- sqloutbox has ZERO runtime dependencies (stdlib only)
- `tomllib` (Python 3.11+) or `tomli` backport for TOML support
- Writer classes are dynamically imported — sqloutbox never imports httpx/requests
- `os.environ.setdefault()` pattern — higher-priority env sources never overwritten
- Doppler failure is fatal when `DOPPLER_TOKEN` is configured (RuntimeError)
- Chain gaps block delivery — never silently drop events
- On startup, `_seed_from_remote()` queries `MAX(outbox_seq)` per table from the remote DB and seeds the local `sqlite_sequence` — prevents seq collision when migrating to a new machine
