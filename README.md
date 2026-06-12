# sqloutbox

A durable, config-driven SQLite transactional outbox for Python. Zero external
dependencies (stdlib only). Designed for single-process deployments with asyncio.

**Producer** writes SQL events synchronously to a local SQLite file (~150µs,
no network). **Consumer** drains them to N remote databases with **at-least-once**
delivery and per-namespace head-of-line ordering, verifying singly-linked chain
integrity on every batch. See [Delivery guarantees](#delivery-guarantees) for
the exact contract.

## Installation

```bash
pip install sqloutbox
```

## How it works

```
Your app (hot path, sync)          sqloutbox (background, async)
─────────────────────────          ────────────────────────────
SQLMiddleware._push()              OutboxSyncService._worker_loop()
  → SQLite INSERT (~150µs)           → fetch_unsynced()
  → returns immediately              → verify_chain()
                                     → writer.write_batch()  ← you provide this
                                     → mark_synced() + delete_synced()
```

sqloutbox owns the entire lifecycle — queue, config, middleware base, sync
daemon. Your app only provides:

1. **Config** — which tables go to which remote DBs (`OutboxConfig` + `TargetConfig`)
2. **Transport** — how to write to your DB (`OutboxWriter` protocol)

## Quick start

### TOML config (recommended)

Create an `outbox.toml` in your project root:

```toml
# ── App: myapp ─────────────────────────────────────────────
[app.myapp]
db_dir = "data/myapp"          # per-table SQLite outbox files
batch_size = 500               # tuning (all optional)
flush_interval = 1.0
auto_schema = true

# Database: primary
[app.myapp.db.primary]
writer_class = "myapp.writers:TursoWriter"
tables = ["orders", "payments"]

[app.myapp.db.primary.connection]
db_url  = "${PRIMARY_DB_URL}"
db_token = "${PRIMARY_DB_TOKEN}"
```

Run the service:

```bash
sqloutbox runservice                      # reads ./outbox.toml
sqloutbox runservice --config my.toml     # custom file path
```

### Python config (for complex bootstrapping)

```bash
sqloutbox init                  # scaffold Python config + runner
vim outbox/outbox_config.py     # define targets and writers
python outbox/run_service.py    # start the drain service
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

# On the hot path (~150µs, no network, no await):
mw = OrderMiddleware(db_dir=Path("./data/myapp"))
mw.push_order(42, 99.99)
```

## TOML config

The TOML config uses a hierarchical `app → db → connection` structure.
Each app is isolated — its own `db_dir`, tuning params, and database targets.

### Multi-app example

```toml
# ── App: pulseview ─────────────────────────────────────────
[app.pulseview]
db_dir = "data/pulseview"          # outbox .db files for this app
batch_size = 500                   # per-app tuning (all optional)
flush_interval = 1.0
auto_schema = true

# Database: analytics (Turso)
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

# ── App: autopulse ─────────────────────────────────────────
[app.autopulse]
db_dir = "data/autopulse"

[app.autopulse.db.main]
writer_class = "myapp.writers:TursoWriter"
inject_outbox_seq = false
tables = ["wallet_transactions", "loan_placements"]

[app.autopulse.db.main.connection]
db_url  = "${TURSO_URL}"
db_token = "${TURSO_AUTH_TOKEN}"
```

### Per-table overrides

The db-level `inject_outbox_seq` and `retain_log_days` are defaults for all
tables. Override per table using inline tables:

```toml
[app.myapp.db.mixed]
writer_class = "myapp.writers:TursoWriter"
inject_outbox_seq = true           # default: all tables inject
retain_log_days = 30               # default: 30 days audit trail

tables = [
    "events",                                         # uses db defaults
    "metrics",                                        # uses db defaults
    { name = "raw_log", inject_outbox_seq = false },  # override: no injection
    { name = "api_calls", retain_log_days = 7 },      # override: 7 days retention
]
```

### Environment variable interpolation

`${VAR_NAME}` in TOML string values is replaced with the resolved env value.
Missing variables raise an error at startup (fail fast, not at first write).

### Credential loading priority

Before parsing the TOML, the loader resolves secrets in this order:

1. **`os.environ`** — already-set env vars (systemd, shell, CI) take highest priority
2. **`.env` file** — from config file's directory; adds vars NOT already in os.environ
3. **Doppler** — if `DOPPLER_TOKEN` is present after steps 1+2, secrets are
   fetched from the Doppler API; adds vars NOT already set

Uses `os.environ.setdefault()` — higher-priority sources are never overwritten.

If `DOPPLER_TOKEN` is configured but the Doppler fetch fails, the service
**halts with a clear error** (not silently continues). If you don't use
Doppler, simply don't set `DOPPLER_TOKEN`.

### App-level tuning

Each `[app.NAME]` section can set tuning params. These apply to all targets
within that app:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `db_dir` | required | Directory for SQLite outbox files |
| `batch_size` | `500` | Max rows per table per sync cycle |
| `flush_interval` | `1.0` | Seconds between round-robin scans |
| `table_flush_threshold` | `15` | Pending rows to trigger immediate flush |
| `table_max_wait` | `6.0` | Max seconds before forcing flush |
| `auto_schema` | `true` | Auto ADD/DROP outbox_seq on startup |
| `cleanup_every` | `500` | Prune audit trail every N cycles |
| `retain_log_days` | `30` | Days to keep outbox_sync_log records |

### Target naming

Targets are named `{app_name}.{db_name}` (e.g., `pulseview.analytics`,
`autopulse.main`). This ensures uniqueness across apps and matches the key
in the `writers` dict.

### Writer class

`writer_class` is a `module.path:ClassName` string. The class is dynamically
imported and instantiated with the `[connection]` section as keyword arguments:

```toml
[app.myapp.db.primary]
writer_class = "myapp.writers:TursoWriter"

[app.myapp.db.primary.connection]
db_url  = "${DB_URL}"
db_token = "${DB_TOKEN}"
```

This calls `TursoWriter(db_url="...", db_token="...")`.

### Supported SQL grammar for `inject_outbox_seq`

When a target has `inject_outbox_seq` enabled, the drain rewrites each row's
SQL to carry the `outbox_seq` column. The rewrite uses a conservative,
string-literal-aware lexer that accepts **only** two shapes:

```sql
-- single-row INSERT with an explicit column list:
INSERT INTO t (c1, c2) VALUES (?, ?)
    -- → INSERT OR IGNORE INTO t (c1, c2, outbox_seq) VALUES (?, ?, ?)

-- UPDATE with at least one real bind placeholder in SET:
UPDATE t SET c1=?, c2=? WHERE id=?
    -- → UPDATE t SET c1=?, c2=?, outbox_seq = ? WHERE id=?
```

Everything else is **rejected loudly** with `UnsupportedStatementError`
(never silently rewritten):

- `INSERT … SELECT …` (no `VALUES` list)
- multi-row `INSERT … VALUES (…), (…)`
- a `?`, `)`, or `WHERE` that appears **inside a quoted string literal**
- an `UPDATE` whose only `?` is inside a literal (no real SET placeholder)

If you must deliver an unsupported shape, route its table to a target with
`inject_outbox_seq=False` (delivered verbatim, no rewrite).

### Programmatic TOML loading

```python
from sqloutbox import load_config_toml
from pathlib import Path

config, writers = load_config_toml(Path("outbox.toml"))
```

## Architecture

### Two roles, two processes

| Role | Class | Process | Latency |
|------|-------|---------|---------|
| **Producer** | `SQLMiddleware` | Your app (hot path) | ~150µs sync |
| **Consumer** | `OutboxSyncService` | Background worker | async, batched |

The producer and consumer share the same `db_dir` — they communicate through
SQLite files on disk. Run them in separate processes or in the same process
(producer on the main thread, consumer on the event loop).

### Multi-target routing

Different tables can be delivered to different remote databases:

```python
config = OutboxConfig(
    db_dir=Path("./outbox"),
    targets=(
        TargetConfig(name="analytics", tables=("events", "metrics")),
        TargetConfig(name="billing", tables=("invoices",),
                     inject_outbox_seq=False),
    ),
)
```

### Chain integrity

Each outbox row stores `prev_seq` — a backward pointer to the previous row.
Before every delivery, `verify_chain()` validates the chain is unbroken.
A *local* chain gap blocks delivery and logs an error. A row the remote can
never accept is retried with backoff and, after `max_attempts`, **moved** (not
deleted) to the audited `outbox_dead_log` table. Nothing is silently dropped —
see [Delivery guarantees](#delivery-guarantees).

After delivery, rows are recorded in `outbox_sync_log` so future gap checks
can confirm the row was delivered, not lost.

### Backpressure (`max_pending`) and the stop-producing watermark

`sqloutbox` is **unbounded by default** — `enqueue()` never raises and the
queue grows until the drain catches up. `health().depth` (per namespace)
surfaces the backlog so you can monitor it.

To bound the queue, set `max_pending` on the config. It is a **two-tier,
pull-based** model — the library only ever *reports a number*; it never calls
back into your app, never pauses it, and never resumes it:

| Tier | Who acts | Trigger | Action |
|------|----------|---------|--------|
| **Stop watermark (80%)** | your **producing application** | `depth >= 80% * max_pending` (polled) | the producer stops enqueuing |
| **Hard cap (100%)** | the **library** | `enqueue()` while `pending >= max_pending` | raises `QueueFullError(namespace, max_pending)` |

The **80% `STOP_WATERMARK_PCT` is a producing-application policy, not library
config** — the library does not own "80". Your producer polls `health().depth`
(Plan 6 adds a derived `capacity_pct = depth / max_pending` convenience) and
stops enqueuing at its own threshold; the `QueueFullError` hard cap is the
library backstop for a bare producer that does not poll.

**There is no auto-resume — deliberately.** A fast-rising backlog is a
*symptom*: the cause may be a slow/down remote (the drain will clear it) OR a
**bug in the producer itself** flooding wrong messages. Auto-resuming would
re-arm a faulty producer. So once the producer stops, an **operator restarts
it manually** after diagnosing why the queue filled (and may quarantine the
already-queued bad rows via the dead-letter CLI). sqloutbox's drain service
**never stops or starts** — it keeps draining the backlog down throughout.

### Idempotent delivery

When `inject_outbox_seq=True` (default), the sync service transforms:

```sql
INSERT INTO orders (id, amount) VALUES (?, ?)
→ INSERT OR IGNORE INTO orders (id, amount, outbox_seq) VALUES (?, ?, ?)
```

If a row was already written to the remote DB (e.g. crash between remote write
and local delete), the re-delivery silently succeeds via `INSERT OR IGNORE`.

The remote table must have `outbox_seq INTEGER NOT NULL DEFAULT 0` with a
partial unique index:

```sql
ALTER TABLE orders ADD COLUMN outbox_seq INTEGER NOT NULL DEFAULT 0;
CREATE UNIQUE INDEX idx_orders_outbox_seq ON orders (outbox_seq) WHERE outbox_seq != 0;
```

`DEFAULT 0` means rows inserted directly (bypassing the outbox) get `outbox_seq=0`.
The partial index allows unlimited 0-value rows while enforcing uniqueness on
delivered rows. Use `config.schema_sql()` to generate this DDL automatically.

### Remote-seeded sequences

On startup, `OutboxSyncService` queries each remote table for
`MAX(outbox_seq)` and seeds the local SQLite AUTOINCREMENT counter above
that value. This prevents sequence collisions when migrating to a new
machine — the new deployment continues from where the old one left off
instead of restarting from 1 (which would cause `INSERT OR IGNORE` to
silently drop new events that collide with old `outbox_seq` values in the
remote DB).

The seed step runs once at startup (after schema setup, before the drain
loop). If the remote query fails, the service continues with the local
counter — the next restart will retry.

### Per-table injection control

`inject_outbox_seq` accepts three value types:

| Value | Meaning |
|-------|---------|
| `True` (default) | All tables in this target get injection |
| `False` | No tables get injection |
| `frozenset({"t1", "t2"})` | Only named tables get injection |

```python
# Mixed: events and metrics get outbox_seq, raw_log does not
TargetConfig(
    name="mixed",
    tables=("events", "metrics", "raw_log"),
    inject_outbox_seq=frozenset({"events", "metrics"}),
)
```

## API Reference

### `OutboxConfig`

Frozen dataclass — immutable, safe to share across threads.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `db_dir` | required | Directory for per-table SQLite outbox files |
| `targets` | `()` | Tuple of `TargetConfig` entries |
| `batch_size` | `500` | Max rows fetched per table per sync cycle |
| `flush_interval` | `1.0` | Seconds between sync cycles |
| `table_flush_threshold` | `15` | Pending rows to trigger immediate flush |
| `table_max_wait` | `6.0` | Max seconds before forcing flush |
| `auto_schema` | `True` | Auto-manage outbox_seq column on startup |
| `cleanup_every` | `500` | Prune sync_log every N cycles |
| `retain_log_days` | `30` | Days to keep audit trail |

Methods: `tables_for_target(name)`, `target_for_table(table)`, `all_tables()`,
`schema_sql()`, `drop_schema_sql()`.

### `TargetConfig`

Frozen dataclass — one remote database target.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `name` | required | Label (must match key in `writers` dict) |
| `tables` | required | Tuple of table names routed here |
| `inject_outbox_seq` | `True` | `True`, `False`, or `frozenset` of table names |
| `db_dir` | `None` | Per-target override (used by TOML multi-app) |
| `batch_size` | `500` | Per-target batch size override |
| `retain_log_days` | `30` | Days to keep sync_log (per-target, overridable per-table) |

Methods: `should_inject_seq(table)`, `get_retain_days(table)`.

### `SQLMiddleware`

Base class for hot-path producers. Subclass must set `self._config` before
calling `_push()` or `_push_many()`.

| Method | Description |
|--------|-------------|
| `_push(table, sql, args)` | Enqueue one SQL statement (~150µs) |
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
        """Send SQL to remote DB. Return one result dict per stmt:
            {"ok": True,  "rows_affected": N}           — write confirmed
            {"ok": True,  "rows": [[col, ...], ...]}    — SELECT result
            {"ok": False, "error": "..."}                — failed
        """
        ...
```

### `load_config_toml(config_path)`

Parse a TOML config file into `(OutboxConfig, writers)`. Loads `.env` and
Doppler secrets before interpolating `${VAR}` references.

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
| `seed_sequence(min_seq)` | Advance AUTOINCREMENT counter (for remote-based seeding) |
| `pending_count()` | Count unsynced rows |

### `inject_outbox_seq(sql, args, seq)`

Standalone helper — transforms an INSERT into an idempotent
`INSERT OR IGNORE` with `outbox_seq` appended. Also handles UPDATE statements.

## CLI

```
sqloutbox runservice [--config FILE]            Start drain from TOML config
sqloutbox init [DIR]                            Scaffold a Python config directory
sqloutbox verify  [--config FILE | --db-dir D]  Offline integrity scan (read-only)
sqloutbox status  [--config FILE | --db-dir D]  Per-namespace depth / stuck (read-only)
sqloutbox dead-letter {list,show,replay} …      Inspect / replay quarantined rows
sqloutbox skip --namespace N --seq S …          Move a stuck head to the dead-letter
```

Recommended: create an `outbox.toml` and run `sqloutbox runservice`.
For complex bootstrapping (custom secret loading, logging, etc.),
use `sqloutbox init` to scaffold Python files instead.

### Systemd

```ini
[Unit]
Description=sqloutbox drain service
After=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/python -m sqloutbox runservice --config /path/to/outbox.toml
Restart=on-failure
RestartSec=30
StandardOutput=append:/var/log/sqloutbox.log
StandardError=append:/var/log/sqloutbox.log

[Install]
WantedBy=multi-user.target
```

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

## Delivery guarantees

Read this before relying on sqloutbox for anything important. These are the
*true* guarantees as of 0.5.0 — stated precisely so there are no surprises.

### At-least-once delivery

sqloutbox is **at-least-once**, not exactly-once. The drain delivers a batch
(`writer.write_batch`), then records the rows as synced and deletes them. If
the process crashes **between** the remote write and the local delete, those
rows are redelivered on restart. This is unavoidable for a durable queue
without a distributed transaction across the two databases.

**Make delivery idempotent** by setting `inject_outbox_seq=True` (the default).
The drain rewrites each INSERT to `INSERT OR IGNORE ... (..., outbox_seq)` and
the remote's partial unique index on `outbox_seq` absorbs the duplicate. Without
`inject_outbox_seq=True`, a redelivery applies the statement again — for a
non-idempotent UPDATE, that may be incorrect. **Idempotency is only guaranteed
with `inject_outbox_seq=True` on idempotent INSERTs.**

### Ordering

Delivery is **strictly head-of-line ordered within a namespace**. When the head
row (lowest unsynced `seq`) fails, it is held and the rows behind it are *not*
delivered until it succeeds, is dead-lettered, or is skipped. Earlier versions
confirmed rows independently, letting a later row leapfrog a failed earlier one
— that is fixed in 0.5.0.

- **No cross-namespace ordering.** Each namespace (table) has its own chain and
  its own backoff clock; they drain independently.
- A persistently failing head retries with **exponential backoff**
  (`2^attempts` minutes, capped at `backoff_cap_minutes`, default 64).

### Poison rows — auto dead-letter (move, not drop)

After `max_attempts` failed deliveries (default 10; set `None` to retry
forever), the head row is **moved atomically** to the `outbox_dead_log` table
with the failure reason, and the namespace advances so it is no longer blocked.
The row is **never lost** — it is quarantined, auditable, and replayable:

```bash
sqloutbox dead-letter list  --config outbox.toml            # what's quarantined
sqloutbox dead-letter show  --config outbox.toml --namespace N --seq S
sqloutbox dead-letter replay --config outbox.toml --namespace N --seq S
sqloutbox skip   --config outbox.toml --namespace N --seq S   # move a stuck head
```

An undecodable payload or an SQL shape `inject_outbox_seq` cannot transform is
dead-lettered immediately (it can never succeed).

### Observing health (pull, never push)

The library exposes a **read-only** signal you poll; it never calls back into
your app, never pauses anything:

```python
from sqloutbox import health_all
from pathlib import Path

for h in health_all(Path("data/myapp")):
    print(h.namespace, h.depth, h.is_stuck, h.head_attempts, h.last_error_class)
```

`NamespaceHealth` fields: `namespace`, `depth`, `head_attempts`, `is_stuck`,
`last_error`, `last_error_class`, `last_attempt_at`, and `capacity_pct`
(`depth / max_pending`, or `None` when `max_pending` is unset). The CLI prints
the same data:

```bash
sqloutbox status --db-dir data/myapp
sqloutbox status --config outbox.toml
```

**Backpressure is your decision, not the library's.** Set `max_pending` to cap
the queue: `enqueue()` then raises `QueueFullError` at the hard wall. To stop
*earlier* (recommended), have your **producing application** poll `health()`
and stop producing at, say, 80% of `max_pending` — that 80% watermark lives in
*your* app, not in library config. The library only reports the number; it
never halts or resumes your producer (an operator restarts it after diagnosing
*why* the queue filled). sqloutbox's own drain never stops — it keeps pulling
the backlog down.

### Single drain per `db_dir`

**Run exactly one drain process per `db_dir`.** `runservice` takes an exclusive
`flock` on `<db_dir>/.sqloutbox.lock` at startup and exits with a clear error if
another drain already holds it. Two drains on one `db_dir` would double-deliver.
Producers (`enqueue`) do **not** take this lock — many producers + one drain is
the supported topology.

## Limitations

- **At-least-once, not exactly-once** — a crash between remote write and local
  delete can redeliver. Use `inject_outbox_seq=True` for idempotent absorption
  (see [Delivery guarantees](#delivery-guarantees)).
- **Idempotency only with `inject_outbox_seq=True`** — a non-idempotent UPDATE
  routed without injection may be applied twice on redelivery.
- **One drain per `db_dir`** — enforced by a `flock`; a second `runservice`
  exits. Many producers, one drain.
- **One write connection per SQLite file** — `enqueue()` is single-writer per
  namespace file.
- **UTF-8 payloads only** — payload is stored as TEXT; non-UTF-8 bytes are
  rejected/dead-lettered.
- **No TTL/expiry** — rows stay until delivered, dead-lettered, or skipped.
- **No priorities** — strictly FIFO per namespace, head-of-line held on failure.
- **No cross-namespace ordering** — each namespace drains independently with its
  own backoff clock.
- **`tag` is raw SQL you control** — sqloutbox executes it verbatim at the
  remote. Never put untrusted input in `tag`; parameterise via `args`.

## License

MIT
