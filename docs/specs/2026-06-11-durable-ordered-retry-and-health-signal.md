# Design: strictly-ordered retry with backoff + read-only health signal

**Date:** 2026-06-11
**Repo:** `sqloutbox` (library). Lead change — a downstream consumer (autopulse) depends on it.
**Status:** Design — pending review

---

## 1. Why this exists

sqloutbox today is a durable, strictly-ordered outbox: producers enqueue locally (~150µs), a drain delivers to remote DBs in `prev_seq` chain order, and a stated invariant is **"chain gaps block delivery — never silently drop."** That ordering + no-drop guarantee is exactly what a consuming app needs when it wants a *reliable* write queue.

A real consumer (autopulse — a single-process financial daemon) wants to retire its hand-rolled failed-write queue (a JSONL file) and use sqloutbox as the **single durable retry primitive** for its operational DB writes. Doing that surfaced three capabilities the library does not yet expose cleanly:

1. **Exponential backoff on a stuck head row.** Today the drain re-attempts a failing row on its normal scan cadence (governed by `table_flush_threshold` / `table_max_wait` — sub-minute), with **no backoff** and **no head-of-line hold**: it confirms each row in a batch independently, so a later row can land while an earlier one keeps failing (see §3.2). A consumer wants a *backoff* (2→4→…→64 min) so a persistently failing destination isn't hammered, **and** strict no-skip so the failing row blocks the ones behind it (preserve order). Both are behavior changes to the drain, not just config — §3.2 spells out the rework.
2. **A read-only health signal.** A consumer needs to *observe* queue health — depth, how many times the head has failed, the last error — to make its **own** policy decisions (alert, pause its own work). The library must expose this as **data the consumer pulls**, never as a callback the library pushes.
3. **Error visibility (classification).** When a row fails, the consumer wants to know *what kind* of failure (transient vs deterministic vs already-applied) so it can react proportionately. The library classifies and reports; it still **never drops**.

**Design tenet (non-negotiable):** sqloutbox has **zero control** over consuming apps. It computes and exposes; the consumer reads and decides. Control flows **app → library** (pull), never **library → app** (push). The library must not import, call back into, or make policy for any app.

---

## 2. Scope

**In scope (this library):**
- `attempts` / `last_attempt_at` / `last_error` / `last_error_class` tracking per queue row (all persisted — see §3.1; the health signal reads them back, possibly cross-process).
- **A drain rework** so head-of-line delivery failure holds the head (stops confirming rows behind it) instead of the current independent per-row confirm. Never skip the head. This is a behavior change, not additive — see §3.2.
- Per-table (per-namespace) exponential backoff on the held head, with a configurable cap, gated into the existing `_worker_loop` scheduler (§3.2).
- Error classification of write failures, surfaced in the signal (no behavior change that drops data).
- A **read-only health signal** accessor: per-namespace `depth`, `head_attempts`, `is_stuck`, `last_error`, `last_attempt_at`.
- One safe auto-advance: a write that fails because the row is **already applied** at the destination (idempotent-insert UNIQUE collision) advances the head — that is *success*, not a skip.

**Out of scope (belongs to the consumer, NOT this library):**
- ❌ Any notion of "pause the app", scheduler control, Discord alerts.
- ❌ Numeric policy thresholds (e.g. "300 rows", "10 attempts"). The library exposes counts; the consumer owns thresholds.
- ❌ A dead-letter table / row-skipping. The library never drops or skips unwritten data.
- ❌ The consumer's migration of its old queue (autopulse's JSONL retirement) — that's an autopulse concern; sketched in §6 only as the motivating example.

---

## 3. Capabilities in detail

### 3.1 Schema — track attempts and the last error

`outbox_queue` gains four columns, added with the existing idempotent-migration pattern (the same `try/except` ALTER wrapper used to add `source` — see `_MIGRATE_ADD_SOURCE` in `_schema.py`):

```python
# _schema.py — applied in open_write_conn(), idempotent
ALTER TABLE outbox_queue ADD COLUMN attempts         INTEGER NOT NULL DEFAULT 0
ALTER TABLE outbox_queue ADD COLUMN last_attempt_at  TEXT     # ISO-8601 (UTC, from now_iso()), NULL until first attempt
ALTER TABLE outbox_queue ADD COLUMN last_error       TEXT     # destination error message of the last failed attempt, NULL until first failure
ALTER TABLE outbox_queue ADD COLUMN last_error_class TEXT     # TRANSIENT|DETERMINISTIC|ALREADY_APPLIED|UNKNOWN (§3.3), NULL until first failure
```

`last_error` / `last_error_class` **must be persisted columns** (not in-memory) because the health signal (§3.4) is a SELECT that may run in a **different process** than the drain — the only durable cross-process channel is the SQLite file.

`attempts` is incremented on each failed delivery of **that specific row**. With the §3.2 head-hold drain, the only row whose `attempts` grows is the current **head** (lowest-`seq` unsynced row) of each namespace — because while a head is stuck the drain stops confirming/advancing the rows behind it, so they are never attempted. (This is *not* true of today's drain, which confirms rows independently; §3.2 is the rework that makes it true.)

### 3.2 Exponential backoff — never skip the head

> ⚠️ **This is a behavior change to the drain, not an additive feature.** Today `_flush_to_target` (`sync.py`) fetches a whole batch per namespace, sends it in one `write_batch`, then **confirms each row independently** by `result["ok"]`: a later row that succeeds is marked-synced + deleted **even if an earlier row in the same batch failed** — i.e. successful rows leapfrog a failed predecessor, and there is no "held head". `verify_chain` only guards *local* queue gaps before send; it does **not** gate confirmation when a row fails *at the writer*. The work below reworks that confirm loop. (Schema columns and the `health()` accessor are additive; the drain semantics are not — see §5.)

**Target drain (post-change), per namespace:**

```
DRAIN per namespace (strict in-order, head-first):
  fetch the head (lowest-seq unsynced row). While a head is stuck (attempts > 0),
  fetch ONLY the head (limit=1) — do not pull rows behind it. Normal batch delivery
  resumes once the namespace is healthy again (head attempts back to 0).

  head delivers? → mark_synced + delete_synced → advance to next head.
                   The new head is a different row, so its attempts is naturally 0
                   (no explicit reset). If the queue empties, the namespace is healthy.
  head FAILS?    → DO NOT skip, DO NOT confirm anything behind it. attempts += 1 on the
                   head row only; set last_attempt_at / last_error / last_error_class (§3.3).
                   next attempt no earlier than: now + min(2^attempts, cap) minutes
                   (cap configurable; default 64).
```

**The two drain edits required (neither is additive):**
1. **Stop the leapfrog.** In `_flush_to_target`, once any row in a namespace's batch fails, do not confirm/delete rows of that namespace that sit *after* the failed row in `seq` order. (Simplest correct form: while a namespace head is stuck, the drain only sends that one head row, so there is nothing behind it to leapfrog.)
2. **Gate the retry on backoff.** The current scheduler in `_worker_loop` decides whether to touch a table purely by `pending >= table_flush_threshold` **OR** `elapsed >= table_max_wait` (sub-minute). A stuck head in minutes-scale backoff would still be re-sent every `table_max_wait` (~6 s) and the backoff would do nothing. So the gate must **suppress** the table:

```python
# in _worker_loop, BEFORE the threshold / max_wait decision for a table:
head = outbox.peek_head()                 # lowest-seq unsynced row (attempts, last_attempt_at)
if head and head.attempts > 0 and head.last_attempt_at is not None:
    delay = timedelta(minutes=min(2 ** head.attempts, cap_minutes))
    next_eligible = parse_iso(head.last_attempt_at) + delay   # UTC, from now_iso()
    if now_utc() < next_eligible:
        continue        # not eligible yet — skip this table this cycle, regardless of threshold/max_wait
```

- **Per-namespace backoff clock (a *drain-timing* property only).** Each namespace (table) has its own outbox file, its own head, its own backoff clock. A stuck head in namespace A never slows the *delivery* of namespace B — B keeps draining on its own schedule. This independence is about **retry timing/throughput inside the library**, nothing more. It is *not* a claim that A can never affect B by any route: see the note below.
- **One backoff clock ≠ one app pause — different layers.** The per-namespace backoff lives entirely in the library (when to retry each head). Whether anything *pauses* is a separate decision the consumer makes by reading the signal (§3.4). The consumer applies its ceilings (e.g. `depth` or `head_attempts`) **per namespace**, but the action it takes when *any one* namespace breaches a ceiling is **global** — autopulse is a single daemon, so it pauses the whole app, not just that table's drain. So: A's *backoff* never delays B (library/timing layer), yet A breaching a ceiling *does* stop B — because the consumer chose to halt globally (consumer/policy layer), not because the backoff clocks are coupled. The two statements are both true and do not contradict.
- **Plateau, don't give up.** At the cap the row keeps retrying at the cap interval forever — the library never abandons a row. (A consumer may *choose* to stop polling / pause itself based on the signal — that's the consumer's call, §3.4.)
- **Backoff resets on success** — the next head starts at 2 min, not where the previous left off.

```python
# config — backoff cap is a library tuning param (NOT a policy threshold)
OutboxConfig(db_dir=..., backoff_cap_minutes=64)   # default 64
```

### 3.3 Error classification — report, never drop

On a failed delivery the library classifies the destination error and stores the class + message on the signal. **Classification changes reporting only — no class authorizes dropping or skipping unwritten data.**

| Class | Detected from | Effect on the row | Drops? |
|---|---|---|---|
| `TRANSIENT` | network, 5xx, timeout, `database is locked` | retry w/ backoff | Never |
| `DETERMINISTIC` | FK fail, NOT NULL, no such column/table, syntax | retry w/ backoff (often clears after a destination migration / a prior row lands) | Never |
| `ALREADY_APPLIED` | UNIQUE collision on an idempotent insert | **advance head** (data provably exists at destination = success) | N/A (present) |
| `UNKNOWN` | unmatched | retry w/ backoff | Never |

> **`ALREADY_APPLIED` is the only class that advances a still-undelivered row, and it is safe precisely because the UNIQUE collision proves the row's key already exists at the destination.** It assumes idempotent inserts (the consumer's `INSERT OR IGNORE` style). A non-idempotent UPDATE must NOT be routed expecting auto-advance. The library exposes the class; whether a consumer relies on auto-advance is documented behavior, not silent magic.

### 3.4 Read-only health signal — the consumer's eyes

The library exposes a passive, read-only view of per-namespace health. **No callbacks. The consumer reads it on its own schedule.**

```python
@dataclass(frozen=True)
class NamespaceHealth:
    namespace:       str
    depth:           int            # undelivered rows in this namespace
    head_attempts:   int            # consecutive failures of the current head (0 if healthy)
    is_stuck:        bool           # head_attempts > 0
    last_error:      str | None     # destination error message of the last failed attempt
    last_error_class: str | None    # TRANSIENT | DETERMINISTIC | ALREADY_APPLIED | UNKNOWN
    last_attempt_at: str | None     # ISO-8601

# Public API (read-only — a SELECT against the WAL SQLite file):
class Outbox:
    def health(self) -> NamespaceHealth: ...      # method on a single Outbox (one namespace)

# module-level free function (NOT a method) — see §5:
def health_all(db_dir: Path) -> list[NamespaceHealth]: ...   # enumerate every namespace under a db_dir
```

`health_all(db_dir)` is a **free function, not a method** — it takes a directory, not `self`. Namespace discovery: glob `{db_dir}/*.db`, and for each file emit one `NamespaceHealth` per distinct `namespace` it contains (`SELECT DISTINCT namespace`). The consumer layout in §6 is one file per table (namespace == table), so this is one `NamespaceHealth` per file in practice, but the multi-namespace-per-file case (allowed by `shared_outbox`) is handled.

**Cross-process safe.** The outbox is WAL-mode SQLite (concurrent reads never block writes). So a consumer can read `health()` from a **different process** than the one running the drain — no IPC, no shared memory. This is what lets a consumer's main loop poll health while the drain runs in-process or as a separate service. (This is also why §3.1 persists `last_error`/`last_error_class` as columns: a cross-process SELECT can only return data that lives in the file.)

---

## 4. Control-direction invariant (the heart of the design)

```
   ┌──────────── sqloutbox (library) ────────────┐     ┌────────── consumer (e.g. autopulse) ──────────┐
   │ • maintains queue state in local SQLite       │     │ • READS health() when IT chooses              │
   │ • COMPUTES: depth, head_attempts, is_stuck,   │ ──► │ • applies ITS OWN thresholds & policy         │
   │   last_error, last_error_class                │ pull│ • DECIDES what to do (alert, pause itself, …) │
   │ • EXPOSES it read-only                        │     │ • the library is never told what the          │
   │ • does NOT decide / pause / call back / import│     │   thresholds are                              │
   └────────────────────────────────────────────────┘     └────────────────────────────────────────────────┘
```

- The library has **no knowledge** of consumer concepts ("pause", "scheduler", "Discord") or consumer numbers ("300", "10").
- The library never invokes consumer code. It returns plain data.
- This preserves sqloutbox's existing invariants: **zero runtime dependencies, never imports httpx/requests/etc.** The signal is ints/strings from SQLite.

---

## 5. Public API additions (summary)

```python
# config.py
OutboxConfig(..., backoff_cap_minutes: int = 64)

# _models.py
@dataclass(frozen=True)
class NamespaceHealth: ...           # see §3.4

# _outbox.py (single namespace)
class Outbox:
    def health(self) -> NamespaceHealth: ...
    def peek_head(self) -> QueueRow | None: ...   # lowest-seq unsynced row (for the §3.2 backoff gate)

# sync.py / a small helper for a db_dir of namespaces
def health_all(db_dir: Path) -> list[NamespaceHealth]: ...   # free function — takes a dir, not self

# __init__.py exports: NamespaceHealth, health_all
```

**What stays compatible vs what changes:**
- *Additive (no breaking changes):* the four new columns (idempotent-migrated), `NamespaceHealth`, `Outbox.health()`, `Outbox.peek_head()`, the `health_all()` free function, and `OutboxConfig.backoff_cap_minutes`. Public signatures of `enqueue`, `verify`, `shared_outbox` are unchanged.
- *Behavior change (not additive):* the drain's confirm path (`_flush_to_target` / `_worker_loop`) — head-of-line hold + backoff gate (§3.2). Existing callers' APIs don't change, but the *delivery semantics* do (a failed row now blocks rows behind it instead of letting them leapfrog). This is intended and is the core of the release.

Version bump **0.4.1 → 0.5.0** (new feature + a deliberate drain-semantics change; minor-version bump because no public signature breaks).

---

## 6. Example — how a consumer uses this (illustrative; autopulse)

This section is **illustrative**: it shows the library doing its job and the consumer owning all policy. None of this code lives in sqloutbox.

```python
# ── consumer side (autopulse), pseudo-code ──────────────────────────────────
# Producer: durable write. Healthy → direct; degraded → enqueue (preserve order).
async def write(table, sql, args):
    ob = shared_outbox(db_path=OPS_DIR / f"{table}.db", namespace=table)
    if ob.health().depth == 0:
        try:
            await turso.execute(sql, args)        # direct, awaited (read-after-write stays correct)
            return
        except Exception:
            ob.enqueue(sql, json.dumps(args).encode(), source="autopulse")  # queue on failure
    else:
        ob.enqueue(sql, json.dumps(args).encode(), source="autopulse")      # backlog exists → queue

# Consumer polls the SIGNAL on its own schedule and owns the thresholds.
# Ceilings are checked PER namespace; the pause action is GLOBAL (one daemon).
# ANY single namespace breaching either ceiling halts the whole app — there is
# no per-table partial pause. (See §3.2: backoff clocks are independent, the
# pause is the one deliberate global coupling.)
async def at_poll_start():
    for h in health_all(OPS_DIR):                 # READ — library decides nothing
        if h.depth > OUTBOX_DEPTH_CEILING or h.head_attempts >= OUTBOX_RETRY_CEILING:
            scheduler.pause()                     # GLOBAL app pause — consumer policy, NOT the library
            await bot.send_alert(
                f"⚠️ outbox '{h.namespace}' stuck: depth={h.depth} "
                f"attempts={h.head_attempts} class={h.last_error_class} err={h.last_error}"
            )
            break                                 # one breach is enough — app is already paused
```

**Notes on the example (consumer concerns, not library):**
- `OUTBOX_DEPTH_CEILING` (e.g. 300) and `OUTBOX_RETRY_CEILING` (e.g. 10) live entirely in the consumer, reloadable via its own config (`!refresh`).
- Whichever ceiling trips first, on *whichever* namespace, → the consumer pauses *itself* **globally** (one daemon, one pause). The per-namespace backoff clocks stayed independent the whole time; the global halt is a consumer policy choice, not a coupling the library introduces. The library just kept reporting `depth`/`head_attempts`.
- The consumer registers the operational outbox as its own app in `outbox.toml` (separate `db_dir` + target), independent of any analytics outbox it already runs.
- The consumer's migration of its legacy queue (e.g. discard SELECTs, quarantine corrupt lines, enqueue valid writes in order) is **its** job — the library only receives well-formed `enqueue()` calls.

---

## 7. Testing (library)

| Test | Asserts |
|---|---|
| `attempts`/`last_attempt_at` migration idempotent | re-running `open_write_conn` on an old DB adds columns once, no error |
| backoff schedule | head failure N× → next-eligible times follow `min(2^n, cap)` (2,4,8,16,32,64,64,…) |
| backoff gate suppresses the table | a head in backoff is NOT re-sent by the `table_max_wait` trigger before `next_eligible` |
| backoff resets on success | head delivers → next head's first retry is 2 min |
| **head-hold (the behavior change)** | batch where head fails but a later row would succeed → the later row is **not** confirmed/deleted (regression test against today's independent per-row confirm) |
| strict no-skip | head fails → rows behind stay undelivered; none leapfrog |
| per-namespace independence | namespace A in backoff doesn't delay namespace B's drain |
| classification | each induced error maps to its class; **no class drops** (row still present unless ALREADY_APPLIED) |
| ALREADY_APPLIED auto-advance | UNIQUE-collision result → head advances; destination row count unchanged (no loss) |
| `health()` accuracy | depth/head_attempts/last_error reflect queue state after seeded successes & failures |
| `health()` read-only | calling it never mutates rows, never calls back, never imports a consumer module |
| `health_all()` cross-process | a second connection reads correct values from a WAL DB while the drain writes |

The entire existing suite must stay green (run `pytest --collect-only` for the live baseline — 180 tests as of commit `c62f361`). Note: the head-hold change (§3.2) alters drain *delivery semantics*, so any existing test that asserts the old independent-per-row-confirm behavior on a partially-failing batch must be updated to the new no-leapfrog contract — that update is expected, not a regression.

---

## 8. Rollout

1. Implement library changes here, on `feat/durable-ordered-retry-signal`. Keep `health()` read-only + dependency-free.
2. PR in **this** repo first (sqloutbox leads — the consumer depends on it). Review, merge, publish **0.5.0**.
3. *Then* the consumer (autopulse) bumps the dependency and wires producer/poll/breaker/migration/config against the published 0.5.0. That work is a **separate PR in the autopulse repo** and is not started until 0.5.0 exists.

---

## 9. Decisions locked

- ✅ sqloutbox **leads**; autopulse is downstream and not touched until 0.5.0 ships.
- ✅ **Pull, not push**: read-only `health()` signal; no callbacks; library never controls/pauses/imports the consumer.
- ✅ **Never-skip head + 2ⁿ backoff (cap configurable, default 64 min)**; plateau-retry forever (library never abandons a row).
- ✅ **Head-hold is a drain behavior change, not additive** — today's drain confirms rows independently (later rows leapfrog a failed head); 0.5.0 holds the head. Schema + `health()` are additive; delivery semantics change.
- ✅ **No policy thresholds in the library** — counts exposed; consumer owns "300"/"10"/pause.
- ✅ **Classification reports, never drops.** `ALREADY_APPLIED` advances (= success, data present); all others retry.
- ✅ **API-compatible, semantics-changing; 0.4.1 → 0.5.0.** No public signature breaks (additive schema + new accessors), but drain delivery semantics change deliberately — hence a minor bump, not a patch.
- ⬜ Exact class-detection substrings (per destination DB error text) — confirm against TursoWriter error messages at implementation.
```
