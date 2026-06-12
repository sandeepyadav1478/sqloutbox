# Changelog

All notable changes to **sqloutbox** are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] — 2026-06-12

The "honest contract" release. Hardens the library for standalone OSS use and
corrects the delivery semantics the README previously over-promised.

### Added
- **Read-only health signal** — `Outbox.health()` and the module-level
  `health_all(db_dir)` free function return a frozen `NamespaceHealth`
  (`namespace`, `depth`, `head_attempts`, `is_stuck`, `last_error`,
  `last_error_class`, `last_attempt_at`, and derived `capacity_pct`). PURE READ:
  the library never calls back into the consuming app and never mutates state
  to produce the signal (control-direction invariant).
- **`sqloutbox status` CLI** — per-namespace depth / stuck / last-error class,
  read from `health_all`. Two modes: `--config outbox.toml` or `--db-dir PATH`.
- **Auto dead-letter** — after `max_attempts` failed deliveries a row is moved
  (not deleted) to the audited, replayable `outbox_dead_log` table; the
  namespace then advances. `max_attempts=None` restores plateau-forever.
- **Dead-letter / skip CLI** — `dead-letter {list,show,replay}` and `skip`.
- **Opt-in backpressure** — `OutboxConfig.max_pending`; `enqueue()` raises
  `QueueFullError` at the cap. Default unbounded (no behavior change).
- **Typed exceptions** — `SqloutboxError` base with `ConfigError`,
  `QueueFullError`, `UnsupportedStatementError`, `ChainIntegrityError`.
- **Config validation** — `__post_init__` bounds-checks on `OutboxConfig` /
  `TargetConfig`; out-of-range fields raise `ConfigError(field, value, reason)`.
- **Single-drain lock** — `fcntl.flock` on `<db_dir>/.sqloutbox.lock`; a second
  `runservice` on the same `db_dir` exits with a clear error.
- **`py.typed`** marker — downstream type checkers now see sqloutbox types.

### Changed
- **Delivery is now strictly head-of-line ordered.** A failed row holds the
  head and blocks the rows behind it (no more leapfrog), with per-namespace
  exponential backoff (cap `backoff_cap_minutes`, default 64). This is a
  deliberate delivery-semantics change.
- **Honest delivery contract** documented: **at-least-once** (a crash between
  remote write and local delete can redeliver), ordering caveats, and
  idempotency only with `inject_outbox_seq=True`.
- **Log hygiene** — a stuck namespace WARNs once on transition (not every
  cycle); recovery logs once; writerless targets raise at construction.
- **Cooperative shutdown** — the confirm step is shielded so routine SIGTERM
  no longer manufactures duplicates.
- **`tomli` is a conditional core dependency** on Python < 3.11 (was an
  optional extra), so TOML config works out of the box on 3.10.
- **Read-only `verify`** — `verify` no longer creates files or migrates
  unrelated SQLite databases.

### Fixed
- Writer results read via `.get("ok")` (fail-closed) instead of `result["ok"]`
  (KeyError) when a writer omits the key.
- `inject_outbox_seq` rejects unsupported SQL shapes with
  `UnsupportedStatementError` instead of silently mangling SQL.
- `mark_synced` / `delete_synced` chunk seq lists to ≤900 per statement,
  avoiding the SQLite variable limit on large batches.
- Forked-chain DBs raise `ChainIntegrityError` (not a bare `IntegrityError`);
  read-only `verify` reports the fork without crashing.
- Producer-side persisted high-water mark prevents fresh-host seq collisions.

## [0.4.1]

- Verbose `verify` output; minor fixes. (Pre-changelog; see git history.)
