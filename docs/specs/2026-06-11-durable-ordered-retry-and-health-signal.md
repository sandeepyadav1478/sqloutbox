# Design: strictly-ordered retry with backoff + read-only health signal

**Date:** 2026-06-11
**Repo:** `sqloutbox` (library). Lead change — a downstream consumer (autopulse) depends on it.
**Status:** Design — pending review

---

## 1. Why this exists

sqloutbox today is a durable, strictly-ordered outbox: producers enqueue locally (~150µs), a drain delivers to remote DBs in `prev_seq` chain order, and a stated invariant is **"chain gaps block delivery — never silently drop."** That ordering + no-drop guarantee is exactly what a consuming app needs when it wants a *reliable* write queue.

A real consumer (autopulse — a single-process financial daemon) wants to retire its hand-rolled failed-write queue (a JSONL file) and use sqloutbox as the **single durable retry primitive** for its operational DB writes. Doing that surfaced three capabilities the library does not yet expose cleanly:

1. **Exponential backoff on a stuck head row.** Today the drain retries a failing batch every `flush_interval`. A consumer wants a *backoff* (2→4→…→64 min) so a persistently failing destination isn't hammered, while **never skipping** the head row (preserve order).
2. **A read-only health signal.** A consumer needs to *observe* queue health — depth, how many times the head has failed, the last error — to make its **own** policy decisions (alert, pause its own work). The library must expose this as **data the consumer pulls**, never as a callback the library pushes.
3. **Error visibility (classification).** When a row fails, the consumer wants to know *what kind* of failure (transient vs deterministic vs already-applied) so it can react proportionately. The library classifies and reports; it still **never drops**.

**Design tenet (non-negotiable):** sqloutbox has **zero control** over consuming apps. It computes and exposes; the consumer reads and decides. Control flows **app → library** (pull), never **library → app** (push). The library must not import, call back into, or make policy for any app.

---

## 2. Scope

**In scope (this library):**
- `attempts` / `last_attempt_at` tracking per queue row.
- Per-table (per-namespace) exponential backoff on head-of-line delivery failure, with a configurable cap. Never skip the head.
- Error classification of write failures, surfaced in the signal (no behavior change that drops data).
- A **read-only health signal** accessor: per-namespace `depth`, `head_attempts`, `is_stuck`, `last_error`, `last_attempt_at`.
- One safe auto-advance: a write that fails because the row is **already applied** at the destination (idempotent-insert UNIQUE collision) advances the head — that is *success*, not a skip.

**Out of scope (belongs to the consumer, NOT this library):**
- ❌ Any notion of "pause the app", scheduler control, Discord alerts.
- ❌ Numeric policy thresholds (e.g. "300 rows", "10 attempts"). The library exposes counts; the consumer owns thresholds.
- ❌ A dead-letter table / row-skipping. The library never drops or skips unwritten data.
- ❌ The consumer's migration of its old queue (autopulse's JSONL retirement) — that's an autopulse concern; sketched in §7 only as the motivating example.

---

## 3. Capabilities in detail

### 3.1 Schema — track attempts

`outbox_queue` gains two columns, added with the existing idempotent-migration pattern (mirrors how `source` was added):

```python
# _schema.py — applied in open_write_conn(), idempotent
ALTER TABLE outbox_queue ADD COLUMN attempts        INTEGER NOT NULL DEFAULT 0
ALTER TABLE outbox_queue ADD COLUMN last_attempt_at TEXT     # ISO-8601, NULL until first attempt
```

`attempts` is incremented on each failed delivery of that row; it is **per-row**, but because the drain is strict-FIFO, the only row whose `attempts` grows is the current **head** of each namespace.

### 3.2 Exponential backoff — never skip the head

```
DRAIN per namespace (strict in-order, head-first — existing prev_seq chain):
  head delivers? → mark_synced + delete_synced → advance. If queue empty, backoff state clears.
  head FAILS?    → DO NOT skip. attempts += 1. Hold the head.
                   next attempt no earlier than: now + min(2^attempts, cap) minutes
                   (cap configurable; default 64). Classify + record last_error (§3.3).
```

- **Per-namespace backoff clock.** Each namespace (table) has its own outbox file, its own head, its own backoff. A stuck head in namespace A never delays namespace B.
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
def health(self) -> NamespaceHealth: ...          # on Outbox (single namespace)
def health_all(self) -> list[NamespaceHealth]: ... # across a db_dir
```

**Cross-process safe.** The outbox is WAL-mode SQLite (concurrent reads never block writes). So a consumer can read `health()` from a **different process** than the one running the drain — no IPC, no shared memory. This is what lets a consumer's main loop poll health while the drain runs in-process or as a separate service.

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

# sync.py / a small helper for a db_dir of namespaces
def health_all(db_dir: Path) -> list[NamespaceHealth]: ...

# __init__.py exports: NamespaceHealth, health_all
```

No breaking changes: existing `enqueue`, drain, `verify`, `shared_outbox` keep their signatures. New columns are additive + idempotent-migrated. Version bump **0.4.1 → 0.5.0** (additive feature release).

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

# Consumer polls the SIGNAL on its own schedule and owns the thresholds:
async def at_poll_start():
    for h in health_all(OPS_DIR):                 # READ — library decides nothing
        if h.depth > OUTBOX_DEPTH_CEILING or h.head_attempts >= OUTBOX_RETRY_CEILING:
            scheduler.pause()                     # consumer policy — NOT the library
            await bot.send_alert(
                f"⚠️ outbox '{h.namespace}' stuck: depth={h.depth} "
                f"attempts={h.head_attempts} class={h.last_error_class} err={h.last_error}"
            )
```

**Notes on the example (consumer concerns, not library):**
- `OUTBOX_DEPTH_CEILING` (e.g. 300) and `OUTBOX_RETRY_CEILING` (e.g. 10) live entirely in the consumer, reloadable via its own config (`!refresh`).
- Whichever ceiling trips first → the consumer pauses *itself*. The library just kept reporting `depth`/`head_attempts`.
- The consumer registers the operational outbox as its own app in `outbox.toml` (separate `db_dir` + target), independent of any analytics outbox it already runs.
- The consumer's migration of its legacy queue (e.g. discard SELECTs, quarantine corrupt lines, enqueue valid writes in order) is **its** job — the library only receives well-formed `enqueue()` calls.

---

## 7. Testing (library)

| Test | Asserts |
|---|---|
| `attempts`/`last_attempt_at` migration idempotent | re-running `open_write_conn` on an old DB adds columns once, no error |
| backoff schedule | head failure N× → next-eligible times follow `min(2^n, cap)` (2,4,8,16,32,64,64,…) |
| backoff resets on success | head delivers → next head's first retry is 2 min |
| strict no-skip | head fails → rows behind stay undelivered; none leapfrog |
| per-namespace independence | namespace A in backoff doesn't delay namespace B's drain |
| classification | each induced error maps to its class; **no class drops** (row still present unless ALREADY_APPLIED) |
| ALREADY_APPLIED auto-advance | UNIQUE-collision result → head advances; destination row count unchanged (no loss) |
| `health()` accuracy | depth/head_attempts/last_error reflect queue state after seeded successes & failures |
| `health()` read-only | calling it never mutates rows, never calls back, never imports a consumer module |
| `health_all()` cross-process | a second connection reads correct values from a WAL DB while the drain writes |

Existing 161 tests must stay green (additive change).

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
- ✅ **No policy thresholds in the library** — counts exposed; consumer owns "300"/"10"/pause.
- ✅ **Classification reports, never drops.** `ALREADY_APPLIED` advances (= success, data present); all others retry.
- ✅ Additive, non-breaking; **0.4.1 → 0.5.0**.
- ⬜ Exact class-detection substrings (per destination DB error text) — confirm against TursoWriter error messages at implementation.
```
