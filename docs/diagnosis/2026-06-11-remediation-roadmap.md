# sqloutbox — Remediation Roadmap (from the 2026-06-11 standalone diagnosis)

**Source:** [`2026-06-11-standalone-oss-readiness-diagnosis.md`](./2026-06-11-standalone-oss-readiness-diagnosis.md) — 79 confirmed findings (2 blocker, 30 major, 24 minor, 1 nit, 22 info).
**Purpose:** turn the flat finding list into ordered workstreams with dependencies and a release cut-line, so the implementation plan (writing-plans) has a spine.
**Finding IDs** (F001–F079) are assigned in the diagnosis report's order (severity, then dimension).

---

## TL;DR — the shape of the work

```
                 ┌─────────────────────────────────────────────────────────┐
                 │ WS-0  RESILIENCE  (blockers) — one bad row must not       │
                 │       zombify the daemon. Gates everything else.          │
                 └───────────────────────────┬─────────────────────────────┘
                                             │ (must land first)
        ┌────────────────────┬───────────────┼───────────────┬────────────────────┐
        ▼                    ▼               ▼               ▼                    ▼
  ┌───────────┐      ┌──────────────┐  ┌────────────┐  ┌─────────────┐    ┌──────────────┐
  │ WS-1      │      │ WS-2         │  │ WS-3       │  │ WS-4        │    │ WS-5         │
  │ Honest    │      │ Poison /     │  │ Safety     │  │ Concurrency │    │ Schema /     │
  │ delivery  │◄─────│ retry / DLQ  │  │ rails      │  │ & lifecycle │    │ verify       │
  │ (head-    │ spec │ + escape     │  │ (validate, │  │ (single-    │    │ correctness  │
  │  hold)    │ ties │ hatch CLI)   │  │  bounds,   │  │  drain lock,│    │ (read-only   │
  └─────┬─────┘      └──────────────┘  │  SQL guard)│  │  shutdown)  │    │  verify,     │
        │                              └────────────┘  └─────────────┘    │  migration)  │
        │ delivery contract                                               └──────────────┘
        ▼
  ┌──────────────────────────────────────────────────────────────────────────────────┐
  │ WS-6  OBSERVABILITY  (health signal from spec + CLI inspect + log hygiene)          │
  └──────────────────────────────────────────────────────────────────────────────────┘
  ┌──────────────────────────────────────────────────────────────────────────────────┐
  │ WS-7  OSS PACKAGING & DOCS  (py.typed, CHANGELOG, SECURITY.md, CI, 3.10 TOML, the   │
  │       TRUE-contract README rewrite — consumes outputs of WS-1..WS-5)                │
  └──────────────────────────────────────────────────────────────────────────────────┘
```

**Release cut-lines:**
- **0.5.0 (must-fix to be OSS-safe):** WS-0 entirely, WS-1 (head-hold, which the existing spec already designs), WS-2 core (backoff + DLQ + skip CLI), WS-3 core (config validation, bounds, SQL-guard), WS-4 (single-drain lock + shutdown), WS-5 (read-only verify + safe migration), plus the WS-7 README honesty rewrite. Everything that is *silent data loss, silent corruption, or silent total stall* must be in 0.5.0.
- **0.5.x / 0.6.0 (polish):** the remaining minors + info (metrics hooks, Prometheus, Windows/NFS docs, PII guidance, contributor tooling).

---

## WS-0 · Resilience — *blockers, gate everything* 🔴

The whole service dies silently on one bad row or one transient DB hiccup, and the process stays alive so no supervisor restarts it. Nothing else matters until this is fixed.

| ID | Sev | What | Fix |
|----|-----|------|-----|
| F001 | blocker | Any non-writer exception in `_worker_loop` (verify/fetch/prune/decode) escapes → `run()` dies, `_runner.py:591` never observes it → zombie | Per-cycle + per-target/per-table try/except that logs-and-continues; **make the drain task's death observable** (await task alongside `stop`, exit non-zero) so the supervisor restarts |
| F002 | blocker | `json.loads(payload)` at `sync.py:609` is unguarded and contradicts the documented "opaque bytes" payload contract → one bad row zombifies all delivery | Wrap per-row decode; route undecodable row to dead-letter/skip (ties to WS-2); reconcile the payload contract with `_models.py` |
| F026 | major | Corrupt/locked SQLite file → unguarded reads in the loop → same zombie, non-isolated across namespaces | Wrap each per-table unit in try/except `DatabaseError`/`OperationalError`; isolate one bad file; distinguish transient-lock (retry) from corruption (quarantine + alert) |
| F028 | major | Permanently-failing row → fixed-rate retry storm + log spam (no backoff) | Backoff (delivered by WS-2) |

**Dependency:** F002's "route to dead-letter" needs WS-2's DLQ; ship F002 as *log-and-skip* first if WS-2 lands later, but both should be in 0.5.0.

---

## WS-1 · Honest delivery semantics — head-of-line hold 🟠

The README sells "strict order, never silently drops." The code confirms each row independently, so a later row leapfrogs a failed earlier one — out-of-order, permanently. **This is exactly what the existing spec (`docs/specs/2026-06-11-durable-ordered-retry-and-health-signal.md`) already designs.** This workstream = implement that spec's head-hold + verify the contract.

| ID | Sev | What |
|----|-----|------|
| F011, F014, F015 | major | Per-row independent confirm → leapfrog → durable out-of-order delivery; `verify_chain` masks the gap via `sync_log` |
| F022 | major | README claims "strict order" — materially false under partial-batch failure |
| F013 | major | At-least-once on crash between write and `delete_synced` — real, must be documented |
| F058, F065 | info | Document the TRUE contract (at-least-once, idempotency only with `inject_outbox_seq=True`, UPDATE caveats) |

**Tie to spec:** WS-1 IS the spec's §3.2 head-hold. The spec is already corrected (commit `51771a4`). This workstream implements it. F013/F058/F065 are the doc half — feed WS-7.

---

## WS-2 · Poison handling — backoff, dead-letter, escape hatch 🟠

A row the remote can never accept currently retries forever, blocks its namespace, and has **no supported way to skip it** (CLI is only init/runservice/verify).

| ID | Sev | What | Fix |
|----|-----|------|-----|
| F016, F023, F028 | major | Poison row retried forever, no backoff, no DLQ | Per-row `attempts` + capped exponential backoff (spec §3.2/§3.3) + dead-letter after N |
| F003, F024 | major | No escape hatch to skip/inspect/quarantine a stuck row | `sqloutbox inspect`, `skip --seq`, `dead-letter list/replay` CLI; skip must re-stitch the chain so successors validate |
| F046, F045 | minor | Empty/whitespace tag, empty payload, non-UTF8 bytes at enqueue | Validate at enqueue boundary |

**Tie to spec:** backoff + classification already designed in spec §3.2/§3.3. DLQ + skip-CLI are NEW (not in spec) — add to plan.

---

## WS-3 · Safety rails — validation, bounds, SQL-guard 🟠

A library that *looks* safe (frozen dataclasses, "never raises" enqueue) but validates nothing and rewrites SQL with brittle string ops.

| ID | Sev | What | Fix |
|----|-----|------|-----|
| F012 | major | Zero config validation — `batch_size=0` never drains, negative `retain_log_days` wipes the audit log | `__post_init__` bounds-checks → typed `ConfigError` |
| F030, F009 | major | `inject_outbox_seq` string-surgery corrupts `INSERT…SELECT`, multi-row VALUES, `?`/`)` in literals, UPDATE arg-misalignment | Restrict to a validated grammar + reject unsupported shapes loudly; long-term move seq-injection into the writer |
| F025, F027 | major | Unbounded queue → disk-full DoS; whole batch materialized in memory → OOM; 999-var IN-clause limit | `max_pending_rows` / byte-budget; chunk `mark_synced`/`delete_synced` to ≤900 |
| F035 | minor | Config errors raise bare/loose exceptions | Typed exception hierarchy |
| F075, F076, F077, F078 | info | Trust model for `tag` (raw SQL), PII-at-rest, untrusted TOML, `${VAR}` env resolution | Document the trust boundary (→ WS-7 SECURITY.md) |

---

## WS-4 · Concurrency & lifecycle 🟠

| ID | Sev | What | Fix |
|----|-----|------|-----|
| F007, F010, F019 | major | Two drains on one `db_dir` → duplicate delivery (no lock, no row-claim) | `flock` lockfile / lease at startup; exit clearly if held; document one-drain-per-db_dir |
| F018, F055 | major/minor | SIGTERM mid-flush cancels between write and `delete_synced` → redelivery; README promises graceful finish | Cooperative shutdown: check stop at cycle top; `shield` the confirm step |
| F008 | major | No fault isolation between targets — one bad writer halts all targets | Per-target try/except (overlaps WS-0) |
| F036, F037, F039 | minor | mark→delete crash gap; restart wedge; `add_signal_handler` unsupported on Windows/non-main-thread | Single-txn confirm; signal-handler fallback |
| F038, F059, F066, F067 | minor/info | Long-run connection handling, separate-process doc, startup SIGTERM | Doc + audit |

---

## WS-5 · Schema / verify correctness 🟠

| ID | Sev | What | Fix |
|----|-----|------|-----|
| F005, F050 | major/minor | `verify` is documented read-only but **CREATEs files, forces WAL, runs migrations** on every globbed `*.db` | Open verify connections `mode=ro`; never create/migrate in verify path |
| F006, F029 | major | Forked-chain DB → unguarded `CREATE UNIQUE INDEX` crashes `__init__` AND `verify` | Wrap migration in try/except (like the sibling `ADD COLUMN`); make verify report forks instead of crashing |
| F004 | major | Producer never seeds the seq counter → fresh-host-vs-populated-remote silently drops via `INSERT OR IGNORE` | Producer-side lazy seed / high-water floor, or document the ordering requirement |
| F049, F051 | minor | `db_dir` containing a foreign `<table>.db`; remote `auto_schema` column addition | Detect non-sqloutbox files; document remote schema mutation |
| F034 | minor | Wall-clock step-backward affects backoff timing | Use monotonic where applicable (ties to WS-2 backoff impl) |
| F074 | info | Cross-version upgrade / rolling deploy | `user_version` pragma for skew detection; document |

---

## WS-6 · Observability 🟡

| ID | Sev | What | Fix |
|----|-----|------|-----|
| F020 | major | Writerless target silently skipped, yet listed in startup banner | Fail-fast or WARN-once at init naming writerless targets |
| F040, F041 | minor | Stuck delivery logs every cycle (spam) or never (silent) | Log-once-on-transition; persistent-failure WARN with namespace/seq/target |
| F042 | minor | No way to inspect live queue depth per namespace | `sqloutbox status` CLI (overlaps WS-2 inspect) + the spec's `health()` signal |
| F068, F069 | info | No metrics hooks (Prometheus/StatsD/OTel); verify-while-running | Optional metrics callback; doc |

**Tie to spec:** the read-only `health()` signal (spec §3.4) is the programmatic half of WS-6.

---

## WS-7 · OSS packaging & docs 🟡 (consumes WS-1..WS-5 outputs)

| ID | Sev | What | Fix |
|----|-----|------|-----|
| F021, F063 | major/info | `pip install` on Python 3.10 can't parse TOML (the headline feature); `tomli` only an optional extra | Make `tomli` a conditional core dep, OR bump `requires-python>=3.11` and drop 3.10 |
| F022 → README | major | Rewrite "strict order / never drops" to the TRUE contract (at-least-once, ordering caveats) — until/unless WS-1 ships head-hold | Honest "Delivery guarantees" + "Limitations" sections |
| F043, F044 | minor | No CHANGELOG, no documented quality gates (lint/type/test) | CHANGELOG.md, CONTRIBUTING, document ruff/mypy/pytest |
| F070 | info | No `py.typed` marker → downstream gets no types | Ship `py.typed` |
| F031, F032, F017, F054, F056, F079 | major/minor/info | Writer protocol underspecified: `result["ok"]` KeyError, length/order mismatch, atomicity, list-mutation | `result.get("ok")`; validate `len(results)==len(stmts)`; **fully document the OutboxWriter contract** |
| F052, F053, F060, F061, F062, F071, F072, F073, F033, F048, F047, F057 | minor/info | Doppler resp size, namespace path-traversal, shared_outbox tuning collisions, ENOSPC, WAL growth over long uptime, many-namespace fd use, cleanup gating, default-doc nit | Batch into docs + small guards; mostly 0.5.x |

---

## Recommended sequencing for the implementation plan

1. **WS-0** (resilience) — unblocks safe iteration; nothing ships without it.
2. **WS-1 + WS-2** together — they share the spec's drain rework (head-hold + backoff + classification); implement once.
3. **WS-3, WS-4, WS-5** — parallelizable; independent surfaces (config, concurrency, schema/verify).
4. **WS-6** — health signal + CLI inspect (reuses WS-2's CLI scaffolding).
5. **WS-7** — last, because the README's honest contract depends on what WS-1..WS-5 actually deliver.

**0.5.0 contains:** WS-0, WS-1, WS-2-core, WS-3-core, WS-4, WS-5, WS-7-README. **Deferred to 0.5.x:** the info-tier docs, metrics hooks, and minor hardening that are not silent-loss/corruption/stall.

**Spec coverage check:** the existing spec covers WS-1 (head-hold), WS-2's backoff+classification, and WS-6's `health()`. It does **not** cover: WS-0 resilience, WS-2's DLQ + skip-CLI, WS-3 (validation/SQL-guard/bounds), WS-4 (lock/shutdown), WS-5 (verify/migration/seed), or WS-7 packaging. Those are net-new and must be added to the plan (or a follow-up spec) before implementation.
