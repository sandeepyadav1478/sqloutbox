# sqloutbox

A durable, config-driven SQLite transactional outbox for Python. Zero external
dependencies (stdlib only). Designed for single-process deployments with asyncio.

**Producer** writes SQL events synchronously to a local SQLite file (~150us,
no network). **Consumer** drains them to N remote databases in strict order,
with singly-linked chain integrity verification on every batch.

## Installation

```bash
pip install sqloutbox
```

## How it works

```
Your app (hot path, sync)          sqloutbox (background, async)
─────────────────────────          ────────────────────────────
SQLMiddleware._push()              OutboxSyncService._worker_loop()
  → SQLite INSERT (~150us)           → fetch_unsynced()
  → returns immediately              → verify_chain()
                                     → writer.write_batch()  ← you provide this
                                     → mark_synced() + delete_synced()
```

sqloutbox owns the entire lifecycle — queue, config, middleware base, sync
daemon. Your app only provides:

1. **Config** — which tables go to which remote DBs (`OutboxConfig` + `TargetConfig`)
2. **Transport** — how to write to your DB (`OutboxWriter` protocol)

## Quick start

```bash
pip install sqloutbox

# 1. Scaffold a config directory
sqloutbox init

# 2. Edit the config — define your targets and writer
vim outbox/outbox_config.py

# 3. Start the drain service
python outbox/run_service.py
```

### What `sqloutbox init` creates

```
outbox/
├── outbox_config.py   # OutboxConfig + create_writers() factory
├── run_service.py     # Entry point — start the drain service
├── logging.json       # Python logging dict config
└── data/              # SQLite outbox files (created at runtime)
```

### The config file

`outbox/outbox_config.py` is a plain Python file with two required exports:

```python
from pathlib import Path
from sqloutbox import OutboxConfig, TargetConfig

DB_DIR = Path(__file__).parent / "data"

config = OutboxConfig(
    db_dir=DB_DIR,
    targets=(
        TargetConfig(name="primary", tables=("orders", "payments")),
    ),
    batch_size=500,       # max rows per table per sync cycle
    flush_interval=15.0,  # seconds between drain cycles
    retain_log_days=7,    # days to keep outbox_sync_log audit trail
)

def create_writers() -> dict:
    from myapp.writers import TursoWriter
    return {"primary": TursoWriter(url="libsql://...", token="...")}
```

Each writer must implement the `OutboxWriter` protocol — one async method:

```python
class TursoWriter:
    async def write_batch(self, stmts: list[tuple[str, list]]) -> list[dict]:
        # Send SQL to your remote database (Turso, SQLite, etc.)
        results = []
        for sql, args in stmts:
            await self.db.execute(sql, args)
            results.append({"ok": True, "rows_affected": 1})
        return results
```

### The entry point

`outbox/run_service.py` is a production-ready script with signal handling and
logging. Run it directly or reference it in a systemd unit:

```ini
[Service]
Type=simple
ExecStart=/usr/bin/python /home/app/outbox/run_service.py
Restart=on-failure
RestartSec=30
StandardOutput=append:/var/log/sqloutbox.log
StandardError=append:/var/log/sqloutbox.log
```

### Producing events (your app's hot path)

Subclass `SQLMiddleware` and call `_push()`. Point `db_dir` at the same
directory the drain service reads from:

```python
from sqloutbox import SQLMiddleware, OutboxConfig

class OrderMiddleware(SQLMiddleware):
    def __init__(self, db_dir: Path):
        self._config = OutboxConfig(db_dir=db_dir)

    def push_order(self, order_id: int, amount: float):
        self._push(
            "orders",
            "INSERT INTO orders (id, amount) VALUES (?, ?)",
            [order_id, amount],
        )

# On the hot path (~150us, no network, no await):
mw = OrderMiddleware(db_dir=Path("./outbox/data"))
mw.push_order(42, 99.99)
```

### Programmatic start (without entry point)

If you prefer to control the event loop yourself:

```python
import asyncio
from sqloutbox import OutboxSyncService

async def main():
    svc = OutboxSyncService(config=config, writers={"primary": MyWriter()})
    await svc.run()  # runs forever

asyncio.run(main())
```

## Architecture

### Two roles, two processes

| Role | Class | Process | Latency |
|------|-------|---------|---------|
| **Producer** | `SQLMiddleware` | Your app (hot path) | ~150us sync |
| **Consumer** | `OutboxSyncService` | Background worker | async, batched |

The producer and consumer share the same `OutboxConfig.db_dir` — they
communicate through SQLite files on disk. Run them in separate processes
or in the same process (producer on the main thread, consumer on the event
loop).

### Multi-target routing

Different tables can be delivered to different remote databases:

```python
config = OutboxConfig(
    db_dir=Path("./outbox"),
    targets=(
        TargetConfig(name="analytics", tables=("events", "metrics")),
        TargetConfig(name="billing", tables=("invoices",)),
    ),
)
```

The sync service collects ALL pending statements for a target into ONE
`writer.write_batch()` call per cycle, minimising round-trips.

### Chain integrity

Each outbox row stores `prev_seq` — a backward pointer to the previous row.
Before every delivery, `verify_chain()` validates the chain is unbroken.
A gap blocks delivery and logs an error (never silently drops events).

After delivery, rows are recorded in `outbox_sync_log` so future gap checks
can confirm the row was delivered, not lost.

### Idempotent delivery

When `inject_outbox_seq=True` (default), the sync service transforms:

```sql
INSERT INTO orders (id, amount) VALUES (?, ?)
```

Into:

```sql
INSERT OR IGNORE INTO orders (id, amount, outbox_seq) VALUES (?, ?, ?)
```

If a row was already written to the remote DB (e.g. crash between remote write
and local delete), the re-delivery silently succeeds via `INSERT OR IGNORE`.

## API Reference

### `OutboxConfig`

Frozen dataclass — immutable, safe to share across threads.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `db_dir` | required | Directory for per-table SQLite outbox files |
| `targets` | `()` | Tuple of `TargetConfig` entries |
| `batch_size` | `500` | Max rows fetched per table per sync cycle |
| `flush_interval` | `15.0` | Seconds between sync cycles |
| `cleanup_every` | `500` | Prune sync_log every N cycles |
| `retain_log_days` | `7` | Days to keep audit trail |

Methods: `tables_for_target(name)`, `target_for_table(table)`, `all_tables()`.

### `TargetConfig`

Frozen dataclass — one remote database target.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `name` | required | Label (must match key in `writers` dict) |
| `tables` | required | Tuple of table names routed here |
| `inject_outbox_seq` | `True` | Append outbox_seq for idempotent delivery |

### `SQLMiddleware`

Base class for hot-path producers. Subclass must set `self._config` before
calling `_push()` or `_push_many()`.

| Method | Description |
|--------|-------------|
| `_push(table, sql, args)` | Enqueue one SQL statement (~150us) |
| `_push_many(table, stmts)` | Enqueue multiple in one SQLite transaction |
| `_source` (property) | Identity label on outbox rows (default: class name) |
| `_outbox(table)` | Get the shared `Outbox` instance for a table |

### `OutboxSyncService`

Multi-target drain daemon. Reads local outbox files, delivers to N remote DBs.

| Method | Description |
|--------|-------------|
| `await run()` | Start the drain loop (runs forever) |
| `pending_count()` | Dict of `{table: pending_rows}` |
| `total_pending()` | Total pending rows across all targets |

### `OutboxWriter` (Protocol)

Your app implements this — the only interface sqloutbox needs from you:

```python
class OutboxWriter(Protocol):
    async def write_batch(
        self, stmts: list[tuple[str, list]]
    ) -> list[dict]:
        """Send SQL to remote DB. Return [{"ok": bool, ...}] per stmt."""
        ...
```

### `Outbox` (low-level)

Direct access to the SQLite queue — use this if you don't need `SQLMiddleware`
or `OutboxSyncService`:

| Method | Description |
|--------|-------------|
| `enqueue(tag, payload)` | Insert one event into the chain |
| `enqueue_batch(items)` | Insert N events in one transaction |
| `fetch_unsynced(limit)` | Read pending rows in order |
| `verify_chain(rows)` | Check prev_seq integrity → `(ok, gaps)` |
| `mark_synced(seqs)` | Flag rows as delivered |
| `delete_synced(seqs)` | Remove delivered rows + audit log |
| `pending_count()` | Count unsynced rows |

### `inject_outbox_seq(sql, args, seq)`

Standalone helper — transforms an INSERT into an idempotent
`INSERT OR IGNORE` with `outbox_seq` appended.

## SQLite schema

Two tables, created automatically per outbox file:

```sql
outbox_queue       (seq, namespace, tag, payload, prev_seq, synced, created_at)
outbox_sync_log    (seq, namespace, synced_at)  -- audit trail
```

WAL mode + `synchronous=NORMAL` — safe on OS crashes, ~3x faster than FULL.

## Recovery

```sql
-- View pending rows
SELECT seq, namespace, tag, created_at, prev_seq
FROM outbox_queue WHERE synced = 0 ORDER BY namespace, seq;

-- Find chain gaps
SELECT q.seq, q.namespace, q.prev_seq FROM outbox_queue q
WHERE q.prev_seq IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM outbox_queue    WHERE seq = q.prev_seq)
  AND NOT EXISTS (SELECT 1 FROM outbox_sync_log WHERE seq = q.prev_seq);

-- Force-skip a lost row (accept the data loss):
INSERT OR IGNORE INTO outbox_sync_log (seq, namespace, synced_at)
VALUES (<lost_seq>, '<namespace>', datetime('now'));
```

## Limitations

- **Single process only** — one write connection per SQLite file
- **UTF-8 payloads only** — payload stored as TEXT
- **No TTL/expiry** — rows stay until explicitly deleted
- **No priorities** — strictly FIFO per namespace
- **No cross-namespace ordering** — each namespace is independent

## License

MIT
