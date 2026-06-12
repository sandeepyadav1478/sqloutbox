# Design: standalone hardening — resilience, dead-letter, safety rails, single-drain, safe verify

**Date:** 2026-06-11
**Repo:** `sqloutbox` (library). Companion to [`2026-06-11-durable-ordered-retry-and-health-signal.md`](./2026-06-11-durable-ordered-retry-and-health-signal.md).
**Status:** Design — pending review
**Targets release:** 0.5.0 (same cut as the first spec)

---

## 0. Relationship to the first spec — read this first

The **first spec** designs the *happy-path durability* layer for an **embedded consumer** (autopulse imports the library, runs its own poll loop, reads `health()`, owns the pause): head-of-line hold, per-namespace exponential backoff, error classification, and a read-only `health()` signal. Its control model is **pull**: the library exposes, the consumer decides.

This spec designs the *standalone-service* layer — what the library must do when it is run as a **bare `sqloutbox runservice` daemon with no smart consumer reading the signal**. A third-party OSS user is exactly this case: they `pip install`, write an `outbox.toml`, and run the daemon. Nobody is polling `health()` and pausing the app on their behalf.

This produces **two deliberate revisions** to the first spec's "locked decisions." They are called out in §1 and reconciled in §7 — the first spec's §9 must be amended so the two do not contradict.

```
        EMBEDDED CONSUMER (first spec)             STANDALONE DAEMON (this spec)
        ───────────────────────────────           ────────────────────────────────
        autopulse imports sqloutbox                `sqloutbox runservice --config …`
        runs its own poll + reads health()         no smart consumer; nobody reads health()
        pauses the app at ITS threshold            library needs built-in safety valves
        library never abandons a row               library auto-dead-letters after N (move-not-delete)
        no policy thresholds in library            library owns max_attempts / max_pending defaults
        ▲                                          ▲
        └── pull model preserved when ─────────────┘
            max_attempts=None (opt back into plateau-forever)
```

The single knob that bridges the two models: **`max_attempts`**. Finite (default) = standalone auto-dead-letter. `None` = first-spec plateau-forever, consumer owns policy. autopulse sets `None`.

---

## 1. Scope & the three locked decisions

**In scope (this spec):**
- **WS-0 Resilience:** no single bad row / transient DB error may zombify the daemon; a true fault must crash *loudly* so a supervisor restarts.
- **WS-2 Dead-letter + escape hatch:** auto-dead-letter after N attempts; operator CLI to inspect/skip/replay.
- **WS-3 Safety rails:** config validation, opt-in queue bound, `inject_outbox_seq` grammar guard, SQLite variable-limit chunking, typed errors.
- **WS-4 Single-drain & shutdown:** one-drain-per-`db_dir` lock; cooperative shutdown; per-target fault isolation.
- **WS-5 Schema/verify correctness:** truly read-only `verify`; crash-safe migrations (forked-chain); producer-side seed.

**Out of scope (covered by the first spec):** head-of-line hold, exponential backoff timing, error classification, the `health()` signal.

**Three decisions locked with the maintainer (2026-06-11):**

| # | Decision | Revises first spec? |
|---|----------|---------------------|
| **D1 — Poison rows** | **Auto dead-letter after N attempts.** After `max_attempts` failed deliveries the library moves the row to `outbox_dead_log` (audited, replayable), WARNs + emits a metric, and advances so the namespace unblocks. | **YES** — revises "library never abandons a row" + "no policy thresholds in library". Reconciled in §7 (move-not-delete preserves *never lose data*; `max_attempts=None` restores plateau-forever). |
| **D2 — Backpressure** | **Bounded, raise on full (opt-in).** Default = unbounded + observable (no behavior change). If `max_pending` is set, `enqueue()` raises typed `QueueFullError` at the cap so the caller decides. Never silently drops; hot path stays fast (just raises). | **Partially** — revises "enqueue never raises" *only when `max_pending` is set*. Default contract unchanged. |
| **D3 — `inject_outbox_seq`** | **Validate grammar + reject loudly.** Restrict to a strict supported grammar; reject unsupported shapes at the boundary with a typed error instead of silently mangling SQL. | No (the first spec doesn't touch this). |

---

## 2. WS-0 · Resilience — one bad row must not zombify the daemon 🔴

### 2.1 The two blockers (from diagnosis F001, F002, F026)

```
TODAY:
  _worker_loop (sync.py:528)  while True:
     … fetch_unsynced / verify_chain / json.loads(payload) / prune …   ← ANY raise here
        propagates out of run() → drain Task faults
  _runner.py:591-592   task = create_task(svc.run());  await stop.wait()
                        └─ task exception NEVER observed → process stays alive (ZOMBIE)
                           systemd sees it "running"; all delivery stopped forever.
```

Two independent failures compound: (a) the loop has no try/except around non-writer work, and (b) the runner never observes the task, so even a real crash is silent.

### 2.2 Target design — defense in depth at three layers

```
LAYER 1 (per-row):    decode/transform guarded → one bad row is dead-lettered (§3) or skipped,
                      never escapes the cycle.
LAYER 2 (per-unit):   each (target → table) drain unit wrapped in try/except (sqlite3.DatabaseError,
                      OperationalError, and broad Exception): log with {target, table, seq}, mark that
                      unit degraded for this cycle, CONTINUE to the next unit. One corrupt/locked file
                      isolates to its own namespace; siblings keep draining.
LAYER 3 (task watch): runner observes the drain task. If it exits for ANY reason other than a clean
                      stop, the process exits NON-ZERO so the supervisor restarts it.
```

```python
# _runner.py — make the worker observable instead of fire-and-forget
task = loop.create_task(svc.run(), name="sqloutbox.drain")
done, _ = await asyncio.wait({task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
if task in done:                       # worker exited on its own → that's a fault
    exc = task.exception()
    logger.critical("drain worker exited unexpectedly: %r", exc)
    raise SystemExit(1)                # loud exit → supervisor restarts (no zombie)
```

**Transient vs structural (F026).** Inside Layer 2, distinguish:
- `OperationalError: database is locked` → transient: retry the unit next cycle (no escalation).
- `DatabaseError: database disk image is malformed` → structural: quarantine that namespace (skip it, WARN once per transition, surface in `health()`), keep other namespaces alive.

**`busy_timeout` (F028 lock-contention sibling).** `thread_conn()` (`_schema.py:106`) and `open_write_conn()` set `PRAGMA busy_timeout=30000` so ordinary two-process lock contention waits instead of raising. (Reconciles with WS-4 single-drain: even the legitimate producer+drain split on one file no longer trips a 5s default.)

---

## 3. WS-2 · Dead-letter + operator escape hatch 🟠

### 3.1 Schema — a new audited dead-letter table (additive, idempotent)

```sql
-- _schema.py, idempotent-create (same pattern as outbox_sync_log)
CREATE TABLE IF NOT EXISTS outbox_dead_log (
    seq              INTEGER NOT NULL,          -- original outbox_queue.seq
    namespace        TEXT    NOT NULL,
    tag              TEXT    NOT NULL,          -- the SQL (preserved verbatim)
    payload          TEXT    NOT NULL,          -- the args (preserved verbatim)
    prev_seq         INTEGER,
    source           TEXT,
    attempts         INTEGER NOT NULL,
    last_error       TEXT,
    last_error_class TEXT,
    dead_lettered_at TEXT    NOT NULL,          -- ISO-8601 UTC
    reason           TEXT    NOT NULL,          -- 'max_attempts' | 'manual_skip' | 'undecodable'
    PRIMARY KEY (namespace, seq)
);
```

**Move-not-delete:** dead-lettering is an atomic transaction — `INSERT INTO outbox_dead_log … ; DELETE FROM outbox_queue WHERE seq=?` — so the row is never *lost*, only relocated to an audited, replayable store. This is the reconciliation with "never drop" (§7).

### 3.2 Auto dead-letter (D1)

```
DRAIN head fails (after the first spec's backoff/classification):
  attempts += 1
  if max_attempts is not None and attempts >= max_attempts:
        move head → outbox_dead_log (reason='max_attempts')      ← atomic, audited
        WARN  "dead-lettered ns=%s seq=%s after %d attempts: %s"
        emit metric  sqloutbox_dead_lettered_total{namespace}
        advance head → namespace unblocks
  else:
        hold head, apply backoff (first spec §3.2)
```

`max_attempts` default: **a finite N (proposed 10)**; the value re-uses the same "10 attempts" the first spec's *consumer* used as its pause threshold — but here the **library** owns it. `max_attempts=None` disables auto-dead-letter entirely (plateau-forever; first-spec embedded-consumer mode). **Undecodable payload (F002)** is dead-lettered immediately with `reason='undecodable'` (it can never succeed — attempts gate is moot).

### 3.3 Operator escape hatch — CLI (new subcommands)

```
sqloutbox dead-letter list   --config … [--namespace N]      # what's quarantined + why
sqloutbox dead-letter show   --config … --namespace N --seq S
sqloutbox dead-letter replay --config … --namespace N --seq S   # re-enqueue at the TAIL, re-stitched
sqloutbox skip   --config … --namespace N --seq S            # manual: move a stuck head → dead_log
sqloutbox status --config …                                  # per-namespace depth / oldest / stuck (WS-6 overlap)
```

**Chain re-stitching invariant:** `skip`/auto-dead-letter must leave the chain verifiable. When the head (seq S, prev_seq P) is removed, its successor's `prev_seq` still points at S. Because `_seq_accounted` (`_outbox.py:414`) treats a row present in `outbox_sync_log` OR `outbox_dead_log` as accounted, **dead-lettering writes the seq into an accounted store** → the successor's chain check passes. (Requires `_seq_accounted` to also consult `outbox_dead_log` — small change.) `replay` re-enqueues with a fresh seq at the tail (new chain link), never reusing the old seq.

---

## 4. WS-3 · Safety rails 🟠

### 4.1 Config validation (F012, F035) — fail at construction, not in prod

```python
# config.py — __post_init__ on both frozen dataclasses (object.__setattr__ for any normalization)
#   batch_size            >= 1
#   flush_interval        >  0
#   table_flush_threshold >= 1
#   table_max_wait        >= 0
#   cleanup_every         >= 1          (prevents modulo-by-zero / never-prune)
#   retain_log_days       >= 0          (negative would compute a FUTURE cutoff → wipe the audit log)
#   max_pending           is None or >= 1
#   max_attempts          is None or >= 1
# Violation → raise ConfigError(field, value, reason)  — typed (see §4.4)
```

A frozen dataclass *looks* validated but isn't; `__post_init__` closes that gap with a clear, field-named error.

### 4.2 Opt-in backpressure (D2, F027) + the 80% stop-producing watermark

Backpressure is a **two-tier, pull-based** model on the *same* `health().depth` signal the first spec already exposes — there is **no library push**. This is the exact mechanism the first spec used for the depth-300 ceiling: the library reports `depth`; the **consumer polls and decides**. The library never calls into, pauses, or signals the app (control-direction invariant, first spec §4, preserved).

```
            SAME health().depth, POLLED by the message-PRODUCING application (PULL):

  depth ≥ stop_watermark (default 80% of max_pending)
        → the PRODUCING APPLICATION stops producing (halts its own enqueue / pauses itself)
        → the 20% headroom absorbs in-flight writes + poll lag before the hard wall
        → it STAYS STOPPED — NO auto-resume. An OPERATOR restarts the producer only AFTER
          diagnosing WHY the queue filled: the cause may be the PRODUCER ITSELF (a bug
          flooding wrong messages), not just a slow remote. Auto-resuming a faulty producer
          would re-arm it to keep emitting bad data. Restart is a human decision.

  depth ≥ 100% (max_pending)  [LIBRARY hard backstop — for a producer that does NOT poll]
        → enqueue() raises QueueFullError(namespace, max_pending)   ← D2, caller decides

  head_attempts ≥ retry ceiling  [first spec §6 — DURABLY STUCK, a different trigger]
        → producing application halts + alerts a human (something is wrong, not mere backlog)

  NOTE: sqloutbox's own DRAIN service NEVER stops/starts — it keeps draining throughout, pulling
        the backlog down. Only the message-PRODUCING application halts, and its restart is a
        HUMAN action. The library only reports the number; it never halts or resumes the producer.
```

```python
# enqueue() — the LIBRARY hard backstop only (unchanged default):
enqueue():
  if max_pending is not None and pending_count() >= max_pending:
        raise QueueFullError(namespace, max_pending)     ← only when max_pending set
  … existing fast INSERT …                               ← default (max_pending=None): never raises

# 80% watermark — PRODUCING-APP policy, polled (mirrors first spec §6 depth-ceiling check).
# Runs in the message-PRODUCING application, NOT in sqloutbox's drain service.
def producer_should_keep_going() -> bool:
    for h in health_all(OPS_DIR):
        if h.depth >= STOP_WATERMARK_PCT * max_pending:   # consumer knows max_pending (it set it)
            stop_producing(h.namespace)        # the producing app halts ITSELF; no auto-resume
            alert(f"producer halted: '{h.namespace}' at {h.depth}/{max_pending} — operator must restart")
            return False
    return True
# NOTE: no resume branch. Restart of the producer is a manual/operator action once the
# backlog has drained. sqloutbox neither stops nor resumes it.
```

**Why this layering (not one line at 80%):**
- The **80% stop watermark** is *backpressure on the producer*: the producing application stops enqueuing so the queue never reaches the hard wall. It **stays stopped** — restart is an explicit operator action after a human diagnoses *why* the queue filled. **No auto-resume, deliberately:** a fast-rising backlog is a SYMPTOM whose cause may be the **producer itself** — a producer bug emitting a flood of *wrong* messages drives depth up just as a slow/down remote does. Auto-resuming would blindly re-arm a malfunctioning producer to keep generating bad data. The operator must first determine whether the cause is downstream (remote outage → drain will clear it) or upstream (producer is faulty → must be fixed, and the already-queued bad messages may need quarantine/skip via the dead-letter CLI, §3.3) before restarting. The drain keeps running and pulling the backlog down the entire time regardless.
- The **100% `QueueFullError`** is the *library backstop* for a bare producer that does **not** poll `health()`. It guarantees the cap holds even with no watermark logic.
- The **durably-stuck trigger (head_attempts ≥ ceiling, first spec §6)** is a different concern — delivery is *failing*, not merely backed up; it halts the producer and pages a human regardless of depth.

**Threshold ownership (matches the depth-300 precedent):** `STOP_WATERMARK_PCT` is a **producing-application** threshold — the library does not own "80". The library's only addition is a pure convenience: `health()` MAY expose a derived `capacity_pct = depth / max_pending` (arithmetic, not policy) so the producer needn't recompute it. `max_pending` itself stays a library config (D2). Default unbounded (no behavior change); `health().depth` surfaces the backlog regardless. Docs state plainly: *unbounded by default — set `max_pending` and have your PRODUCER poll `health()` to apply a stop-producing watermark, or monitor depth + disk yourself.*

**Per-namespace vs app-wide:** the watermark is evaluated **per namespace** (each has its own `depth`), but the *stop-producing* action is the producing app's choice — autopulse, a single daemon, will likely stop app-wide when any namespace breaches 80% (same global-stop logic as the first spec's §3.2 reconciliation); a multi-tenant standalone user may stop only the hot namespace's producer. Either way, **sqloutbox is not involved in the stop or the restart** — it only reports the depth.

### 4.3 `inject_outbox_seq` grammar guard (D3, F030, F009)

```
SUPPORTED (transform applied):
    INSERT INTO t (c1, c2)         VALUES (?, ?)         -- single-row, explicit cols
    UPDATE t SET c1=?, c2=?        WHERE …               -- no '?' or keyword inside string literals

REJECTED LOUDLY (raise UnsupportedStatementError, NEVER silently rewrite):
    INSERT … SELECT …                                    -- no VALUES list
    INSERT … VALUES (…),(…)                              -- multi-row
    any statement with '?' or ')' or ' WHERE ' inside a quoted string literal
    UPDATE whose SET-clause '?' count is ambiguous under literal scan
```

Detection is a *conservative* lexer pass (string-literal aware) that recognizes ONLY the two safe shapes and rejects everything else — it does not try to parse arbitrary SQL. The error fires at the drain boundary the first time the row is processed, naming the namespace/seq, and the row is dead-lettered with `reason='unsupported_stmt'` (it can never be transformed safely). Document the exact supported grammar in the README writer guide.

### 4.4 SQLite variable-limit chunking (F025) & typed errors

- `mark_synced` / `delete_synced` build `IN (?,?,…)` over `seqs`; with `batch_size` > ~999 this trips `SQLITE_MAX_VARIABLE_NUMBER`. Chunk seq lists to **≤ 900 per statement**.
- Optional `max_batch_bytes` so large payloads bound memory, not just row count.
- **Typed exception hierarchy** (replaces leaking bare `sqlite3.*` / `RuntimeError`):
  ```
  SqloutboxError
    ├─ ConfigError
    ├─ QueueFullError
    ├─ UnsupportedStatementError
    └─ ChainIntegrityError       (forked chain / gap, see WS-5)
  ```

---

## 5. WS-4 · Single-drain & lifecycle 🟠

### 5.1 One drain per `db_dir` (F007, F010, F019)

```
runservice startup:
  acquire fcntl.flock(LOCK_EX|LOCK_NB) on  <db_dir>/.sqloutbox.lock
  held?  → raise / exit(1): "another drain is already running on <db_dir>"
  ok?    → hold for process lifetime; released on exit
```

Stops double-delivery from accidental double-start / blue-green overlap / k8s `maxSurge>0`. Producers (enqueue) do **not** take this lock — only the drain. Documented: *exactly one drain per `db_dir`.* (Belt-and-suspenders for `inject_outbox_seq=False` targets that have no `INSERT OR IGNORE` dedupe.)

### 5.2 Cooperative shutdown (F018, F055)

```
TODAY:  stop set → task.cancel() → CancelledError injected at the NEXT await,
        possibly BETWEEN write_batch() and delete_synced() → redelivery.

TARGET: worker checks `stop` at the TOP of each cycle (no new cycle if set);
        the confirm step (mark_synced + delete_synced for an already-delivered batch)
        runs under asyncio.shield so it completes once write_batch returned ok.
        → graceful "finish current cycle then stop" matches the documented promise.
```

At-least-once is still the honest guarantee (crash can always land in the gap) — but routine SIGTERM no longer manufactures duplicates.

### 5.3 Per-target fault isolation (F008, F020)

- Each target's drain wrapped (WS-0 Layer 2) so one bad writer cannot halt sibling targets.
- `OutboxSyncService.__init__` validates every `config.targets` entry has a writer; a writerless target **fails fast** (or WARNs once and is excluded from the "started" banner) instead of silently black-holing (F020).

---

## 6. WS-5 · Schema / verify correctness 🟠

### 6.1 Truly read-only verify (F005, F050)

```
TODAY:  verify → Outbox(__init__) → open_write_conn() → mkdir + CREATE TABLE + PRAGMA WAL
        + ALTER + CREATE UNIQUE INDEX + commit … on EVERY globbed *.db
        ⇒ "read-only" verify CREATES files and MIGRATES unrelated SQLite files.

TARGET: a dedicated read-only open path:
        sqlite3.connect(f"file:{path}?mode=ro", uri=True)   — no mkdir, no DDL, no PRAGMA writes
        missing file → report "not an outbox DB", do not create it.
        Separate "construct (may migrate)" from "inspect (must not)".
```

### 6.2 Crash-safe migration / forked chain (F006, F029)

```
_MIGRATE_PREV_SEQ_UNIQUE (CREATE UNIQUE INDEX …)  is currently UNGUARDED (_schema.py:99-101),
unlike the sibling _MIGRATE_ADD_SOURCE which is wrapped (95-98).
A forked-chain DB → IntegrityError → crashes __init__ (producer hot path!) AND verify.

TARGET: wrap it like its sibling. On IntegrityError:
        raise ChainIntegrityError naming the duplicate prev_seq rows + recovery pointer,
        BUT let read-only verify still open and REPORT the fork (don't crash the diagnostic tool).
```

### 6.3 Producer-side seed (F004)

```
Fresh host, populated remote: producer assigns seq=1,2,3 BEFORE the drain's one-time
_seed_from_remote runs → those collide with the remote's existing outbox_seq 1..N →
INSERT OR IGNORE silently drops them → silent data loss.

TARGET (pick at planning): either
  (a) producer lazily seeds its local AUTOINCREMENT from a persisted high-water mark, or
  (b) producer refuses to assign seqs below a recorded floor, or
  (c) document loudly that _seed_from_remote MUST complete before first producer write
      on a fresh-vs-populated-remote host.
Recommend (a): correctness without an ordering constraint on startup.
```

---

## 7. Reconciliation with the first spec (the contradiction, resolved)

The first spec's §9 locked: *"library never abandons a row"* and *"no policy thresholds in the library."* D1 (auto dead-letter at `max_attempts`) appears to break both. It does not, once framed correctly:

```
"never DROP / never LOSE data"   ← the DEEP invariant.  PRESERVED.
   dead-letter = atomic move to outbox_dead_log (audited, replayable). Nothing is lost.

"never ABANDON a row in the delivery queue without a human"  ← the SHALLOW rule.  REVISED.
   standalone daemons have no human/consumer reading health(); a finite max_attempts is
   the built-in safety valve. The row is quarantined, not destroyed, and replayable.

"no policy thresholds in the library"   ← REVISED for standalone mode.
   max_attempts / max_pending are library-owned thresholds, but BOTH are opt-out:
   max_attempts=None + max_pending=None  ⇒  exact first-spec behavior (embedded consumer).
```

**Action required:** the first spec's §9 "Decisions locked" must gain a cross-reference and a softened bullet:
- ~~"library never abandons a row"~~ → "library never *loses* a row; in standalone mode it may *quarantine* to an audited dead-letter after `max_attempts` (default finite; set `None` to plateau-forever)."
- ~~"no policy thresholds in the library"~~ → "no policy thresholds **for app-pause** (consumer owns those); the library owns only its self-protection defaults `max_attempts` / `max_pending`, both opt-out."

This amendment is a **separate, explicit edit** to the first spec (not silently folded in) so the two specs are consistent before either is implemented.

---

## 8. Public API & config additions (summary)

```python
# config.py — OutboxConfig / TargetConfig gain (all with __post_init__ validation):
max_attempts:    int | None = 10      # D1: auto-dead-letter threshold; None = plateau-forever
max_pending:     int | None = None    # D2: opt-in backpressure cap; None = unbounded (default)
max_batch_bytes: int | None = None    # F025: optional memory bound

# health() MAY expose a derived convenience field (arithmetic, not policy):
#   capacity_pct: float | None   # depth / max_pending, or None if max_pending unset
# The 80% stop watermark is a PRODUCING-APPLICATION threshold (like the first spec's depth-300),
# NOT library config — the library never owns "80" and never halts/resumes the producer.

# exceptions.py (new): SqloutboxError + ConfigError, QueueFullError,
#                      UnsupportedStatementError, ChainIntegrityError

# _outbox.py: enqueue() may raise QueueFullError (only when max_pending set);
#             dead_letter(seq, reason), replay(seq); _seq_accounted also consults outbox_dead_log
# cli.py: new subcommands  dead-letter {list,show,replay},  skip,  status
# _schema.py: outbox_dead_log table; guard _MIGRATE_PREV_SEQ_UNIQUE; busy_timeout; read-only open path
# _runner.py: flock single-drain; observe drain task (loud exit); cooperative shutdown
```

**Compatibility:** all schema changes additive + idempotent. The *behavior* changes (auto-dead-letter default, grammar rejection, single-drain lock, cooperative shutdown, read-only verify) are deliberate and ship in **0.5.0** alongside the first spec's drain rework. No public *signature* breaks; `enqueue` only raises under opt-in `max_pending`.

---

## 9. Testing (library)

| Area | Test asserts |
|---|---|
| Resilience | one undecodable row → dead-lettered + cycle continues; corrupt namespace file → isolated, siblings drain; worker fault → process exits non-zero (no zombie) |
| Dead-letter | head fails `max_attempts`× → moved to `outbox_dead_log`, namespace advances; `max_attempts=None` → never auto-dead-letters (plateau); move is atomic (crash mid-move loses nothing) |
| Chain after skip | skip/dead-letter head → successor's `verify_chain` still passes via `outbox_dead_log`; replay re-enqueues at tail with new seq |
| CLI | `dead-letter list/show/replay`, `skip`, `status` round-trip |
| Config validation | each out-of-range field → `ConfigError(field)`; valid config constructs |
| Backpressure | `max_pending` set + at cap → `QueueFullError`; unset → never raises |
| 80% watermark (producer-poll) | `health()` reports `capacity_pct = depth/max_pending` accurately; `capacity_pct` is `None` when `max_pending` unset; library never calls back / pushes / halts / resumes (pure read); the stop decision + the (manual) restart live entirely in the producing app — sqloutbox state is identical whether the producer is running or stopped |
| Grammar guard | supported INSERT/UPDATE transform correctly; INSERT…SELECT / multi-row / '?'-in-literal → `UnsupportedStatementError`, never silent rewrite |
| Var-limit | `batch_size` > 999 → mark/delete chunked, no `too many SQL variables` |
| Single-drain | second `runservice` on same `db_dir` → exits with clear error |
| Shutdown | SIGTERM between write and confirm → confirm completes (shield), no redelivery on restart |
| Read-only verify | `verify` on missing path → "not an outbox DB", file NOT created; existing DB unchanged (no WAL switch, no migration) |
| Forked chain | duplicate prev_seq DB → `__init__`/`runservice` raises `ChainIntegrityError` (not bare IntegrityError); read-only `verify` REPORTS the fork without crashing |
| Producer seed | fresh-host-vs-populated-remote → producer seqs do not collide / are not silently dropped |

The full existing suite (180 tests as of `c62f361`) must stay green; behavior-change tests updated per §8.

---

## 10. Decisions locked

- ✅ **D1** auto dead-letter after `max_attempts` (default finite, `None` = plateau-forever) — move-not-delete to `outbox_dead_log`, audited + replayable.
- ✅ **D2** opt-in `max_pending` → `QueueFullError` hard backstop; default unbounded + observable.
- ✅ **80% stop-producing watermark = producing-app polled (PULL), not library push** — same mechanism as the first spec's depth-300 ceiling. The message-PRODUCING application reads `health().depth` (or `capacity_pct`) and stops producing at 80% of `max_pending`. **No auto-resume — restart is a manual operator action.** sqloutbox's drain never stops/starts; the library only reports the number and never halts or resumes the producer. Control-direction invariant preserved.
- ✅ **D3** `inject_outbox_seq` validated grammar; reject unsupported shapes loudly.
- ✅ Resilience is release-blocking: no zombie daemon; loud crash for supervisor restart; per-namespace fault isolation.
- ✅ Single drain per `db_dir` (flock); cooperative shutdown; per-target isolation.
- ✅ Read-only verify; crash-safe forked-chain migration; producer-side seed.
- ✅ Ships in **0.5.0** with the first spec. Additive schema; deliberate behavior changes; no public signature breaks.
- ⬜ Producer-seed mechanism (a/b/c in §6.3) — recommend (a), confirm at planning.
- ⬜ First spec §9 amendment (§7) — apply as a separate explicit edit before implementation.
- ⬜ `max_attempts` default value (proposed 10) — confirm at planning.
- ⬜ Watermark value — `STOP_WATERMARK_PCT` (proposed 80%). No resume watermark (restart is manual/operator). Confirm at planning.
```
