# sqloutbox — Standalone / Open-Source Readiness Diagnosis

**Date:** 2026-06-11  
**Scope:** sqloutbox as an independent, third-party-consumable OSS library — NOT as autopulse's adapter.  
**Method:** 12 failure-domain reviewers read the real source; every finding adversarially verified against code; a completeness critic swept for cross-cutting gaps (also verified).  
**Result:** 79 confirmed findings (6 rejected as non-issues). Severity: blocker 2, major 30, minor 24, nit 1, info 22.

> Recurring theme: **autopulse masks almost every one of these** because it runs a single drain, uses idempotent INSERTs with `inject_outbox_seq=True`, emits well-formed SQL, and has operators with DB access. A third party shares none of those assumptions. Each finding's *standalone impact* is the reason it matters for OSS even though prod is fine.


---


## BLOCKERS


### BLOCKER-1 · `lifecycle` · (confidence: high)

**Scenario.** The drain worker loop hits an uncaught exception that is NOT inside the per-target write path — e.g. verify_chain raises (DB locked / disk error), prune_sync_log raises, fetch_unsynced raises, _run_verify raises, json.loads on a malformed payload raises, or any logic bug in _worker_loop itself.


**Current behavior.** Only writer.write_batch() failures are caught (sync.py:651 in _flush_to_target, and _worker.py:231). The outer while-True loop in OutboxSyncService._worker_loop (sync.py:528) has NO try/except around the per-table fetch_unsynced (584), verify_chain (596), json.loads (609), inject_outbox_seq (611), or the cleanup_every prune (627-628). Any exception there propagates out of _worker_loop → out of run() → the drain Task completes with an exception. run_service_main is sitting in `await stop.wait()`, so it never observes the task exception; the process keeps running with a DEAD worker (task done, exception stored but never retrieved).


**Risk.** Silent total stall. The service process stays alive (systemd sees it running, Restart=on-failure never triggers), the drain has stopped permanently, and the outbox grows unbounded. Pending rows are never delivered until a human notices. A malformed payload that is not valid JSON raises json.loads at sync.py:609 OUTSIDE any try → kills the whole drain for ALL targets and tables, not just the offending row.


**Why it matters standalone.** An OSS consumer relies on the daemon to keep draining or to crash loudly so their supervisor restarts it. Here it does neither for non-writer errors — it dies quietly inside a still-alive process. CLAUDE.md/README present write_batch retry as the failure model, implying the loop is resilient; in reality a single bad row's json.loads, or any transient sqlite OperationalError during verify/prune, is fatal to the entire drain with zero external signal.


**Evidence.** sync.py:528-628 (worker loop body has no enclosing try/except; json.loads at 609, verify_chain at 596, fetch_unsynced at 584, prune at 627-628 all unguarded); _runner.py:591-592 (parent awaits stop.wait(), not the task, so task exceptions are never surfaced); OutboxWorker._worker_loop (_worker.py:189-254) is also an unguarded while-True with the same exposure outside its write try-block


**Recommendation.** Wrap each cycle body (or at least each per-target/per-table unit) in try/except that logs and continues, mirroring the write_batch handling. Move json.loads(row.payload) inside a per-row try so one corrupt row is force-skipped (as _worker.py:214-224 already does) instead of killing the loop. Separately, in run_service_main, await the task alongside stop.wait() (asyncio.wait FIRST_COMPLETED) so a dead worker is detected and the process exits non-zero, letting the supervisor restart it.


---


### BLOCKER-2 · `poison-data` · (confidence: high)

**Scenario.** A producer (or a different sqloutbox version / a manually-edited DB / a third-party process writing to the same outbox file) enqueues a row whose payload is not valid JSON — e.g. a raw protobuf/msgpack blob, an empty string, truncated JSON, or text written by code that did NOT go through SQLMiddleware._push (which is the only thing that guarantees json.dumps). The drain reaches that row at the head of its namespace's batch.


**Current behavior.** In _worker_loop the per-row decode is `args = json.loads(row.payload.decode())` (sync.py:609), executed inside the `for row in rows:` loop (607-613), which is inside `for table, outbox in outboxes.items():` (565), inside `for target in self._config.targets:` (554), inside `while True:` (528). There is NO try/except anywhere from line 609 up to the `while True`. A json.JSONDecodeError (or UnicodeDecodeError) propagates straight out of `_worker_loop`, terminating `svc.run()`. The runner does `task = loop.create_task(svc.run())` then `await stop.wait()` (_runner.py:591-592) and never awaits/observes the task, so the drain task dies silently while the process keeps running blocked on stop.wait() forever. The exception only surfaces as an asyncio 'Task exception was never retrieved' warning at GC time.


**Risk.** Total, silent, permanent loss of delivery for EVERY namespace and EVERY target served by this service — not just the poisoned one. One un-decodable payload anywhere converts the daemon into a zombie: process alive, systemd thinks it's healthy (no exit, no crash-loop), pending rows pile up unbounded in all local SQLite files, and nothing is ever delivered to any remote DB again until a human notices and restarts.


**Why it matters standalone.** An OSS consumer has no control over what gets into the queue if they use the raw Outbox.enqueue(tag, payload: bytes) API directly (it accepts arbitrary bytes and only stores them — _outbox.py:68), or if they mix payload encodings, or if a row is corrupted on disk. The drain UNCONDITIONALLY assumes payload is JSON (sync.py:609), but QueueRow/_models.py:22-25 explicitly documents payload as 'Raw bytes — format chosen entirely by the caller (UTF-8 JSON, msgpack, protobuf, plain text)... sqloutbox does not interpret this field.' The consumer is told the field is opaque, yet the shipped drain hard-codes JSON. A non-JSON payload is not exotic misuse — it is the documented contract, and it kills the whole service.


**Evidence.** sync.py:609 (json.loads inside unguarded loop); sync.py:528 (`while True:` with no try/except in body); _runner.py:591-592 (task created but never awaited — `await stop.wait()` blocks forever, exception swallowed)


**Recommendation.** Wrap the per-row decode (and the per-table/per-target body) in try/except so a single bad row cannot escape the cycle. On decode failure, route the row to a dead-letter path (a quarantine table or sync_log with an error marker) and advance past it, OR at minimum log+skip that one row and continue the cycle. Additionally, in _runner.py add a done-callback on the drain task (or `await` it alongside stop) so an unexpected worker-loop exit crashes the process loudly (enabling systemd restart) instead of zombifying. Document explicitly that the bundled SQL drain requires JSON payloads, contradicting the 'opaque bytes' contract in _models.py.


---


## MAJOR


### MAJOR-1 · `completeness` · (confidence: high)

**Scenario.** A poison/persistently-failing row sits at the HEAD of a namespace (lowest unsynced seq) while its chain predecessor was already delivered and recorded in outbox_sync_log. The head row stays pending for longer than retain_log_days (e.g. the remote rejects it forever, or the target is down for weeks). Meanwhile prune_sync_log() runs every cleanup_every cycles and deletes the predecessor's sync_log entry because it is older than the retention window.


**Current behavior.** verify_chain() (used by both OutboxSyncService._worker_loop sync.py:596 and OutboxWorker._worker_loop _worker.py:201) validates the head row's predecessor via _seq_accounted(), which checks ONLY outbox_queue OR outbox_sync_log (_outbox.py:414-421). prune_sync_log() deletes sync_log entries by age with NO check that any still-pending head row references them (_outbox.py:330-346). Once the predecessor's sync_log row is pruned, _seq_accounted returns False, verify_chain reports a chain gap, and the head row (and everything behind it) is blocked from delivery forever — 'Delivery blocked' is logged each cycle.


**Risk.** Permanent, self-inflicted deadlock of an entire namespace's queue (silent drop of all subsequent events' delivery) caused purely by the library's own retention pruning racing its own chain-verification. Reproduced: verify_chain returns (True,[]) before pruning seq=1's sync_log row, then (False,[1]) after — head row seq=2 is permanently blocked.


**Why it matters standalone.** Any third party with a slow/failing destination (the exact case an outbox exists to survive) plus the default 30-day retention will eventually have predecessors pruned out from under stuck head rows. autopulse's targets drain fast so it never hits this; an OSS user with a flaky remote will silently lose all delivery after the retention window with only a recurring ERROR log to show for it.


**Evidence.** /Users/sandeep.yadav/tmp/sqloutbox/src/sqloutbox/_outbox.py:330-346 (prune_sync_log, age-only delete), :414-421 (_seq_accounted checks queue|sync_log), :245 (verify_chain head check); sync.py:596-605 (drain blocks on gap)


**Recommendation.** prune_sync_log() must never delete a sync_log entry that is the prev_seq of any still-pending outbox_queue row in the same namespace (add a NOT EXISTS guard). Alternatively, gate retention on delivered-and-no-pending-successor, or document that retain_log_days must exceed the maximum tolerable backlog age.


---


### MAJOR-2 · `completeness` · (confidence: high)

**Scenario.** Recommended separate-process deployment (README 'Run them in separate processes', README.md:236) on a fresh machine that is re-seeding from a non-empty remote: the producer process (SQLMiddleware / Outbox.enqueue) starts writing rows BEFORE — or independently of — the drain service's startup _seed_from_remote(). The remote already has outbox_seq values 1..N from a previous host.


**Current behavior.** seed_sequence() is called ONLY from OutboxSyncService._seed_from_remote() at the drain service's startup (sync.py:499). The producer side (Outbox.enqueue / SQLMiddleware._push / shared_outbox) never seeds the AUTOINCREMENT counter — confirmed: no caller of seed_sequence exists outside sync.py. So a producer running on a fresh local DB assigns seq=1,2,3..., and inject_outbox_seq stamps those as outbox_seq=1,2,3 (sync.py:611). Those collide with the remote's existing outbox_seq 1..N, and the INSERT OR IGNORE prefix added by inject_outbox_seq (sync.py:148) makes the remote silently drop them.


**Risk.** Silent, permanent data loss: rows enqueued by the producer before the drain seeds get low outbox_seq values that the remote already has, so INSERT OR IGNORE drops them and the drain still marks them delivered (result ok). No error, no gap, no log.


**Why it matters standalone.** autopulse runs producer and drain coupled and rarely rebuilds a host from a populated remote, so it doesn't see this. A third party who follows the README's separate-process guidance and ever rehydrates a fresh node against an existing remote DB will silently lose every event the producer emits in the window before the drain's one-time seed completes — and the producer may start first by design.


**Evidence.** /Users/sandeep.yadav/tmp/sqloutbox/src/sqloutbox/sync.py:435-499 (_seed_from_remote, only seeder), :148 (INSERT OR IGNORE), :611 (inject row.seq as outbox_seq); _outbox.py:360-397 (seed_sequence); middleware.py / _registry.py never seed


**Recommendation.** Seed the local sequence on the producer side too (e.g. lazily in Outbox/shared_outbox from a persisted high-water mark), or have the producer refuse to assign seqs below a recorded floor, or document loudly that the drain's _seed_from_remote must complete before any producer write on a fresh-vs-populated-remote host and that producers do not self-seed.


---


### MAJOR-3 · `completeness` · (confidence: high)

**Scenario.** Any consumer follows the documented 'read-only' verification contract: runs `sqloutbox verify --db-dir <path>` or `sqloutbox verify --config`, or calls `outbox.verify_full()` / `svc.request_verify()` / `verify_all()` against existing .db files — including a path that contains no sqloutbox DBs yet, a fresh directory, or a DB from a future/foreign schema version.


**Current behavior.** _verify.py:18 and :67-68 explicitly promise 'All checks are read-only — they never modify the database.' But every verify path constructs an Outbox (cli.py:482, :494; verify_outbox uses the passed Outbox), and Outbox.__init__ (_outbox.py:64) eagerly calls open_write_conn (_schema.py:77-103) which: (1) CREATEs the .db file if absent (mkdir + connect), (2) runs CREATE TABLE IF NOT EXISTS for outbox_queue/outbox_sync_log, (3) forces a persistent PRAGMA journal_mode=WAL, (4) runs ALTER TABLE ADD COLUMN source, (5) runs CREATE UNIQUE INDEX idx_outbox_prev_unique and commits. Confirmed: constructing an Outbox on a non-existent path creates the file with all three tables and flips it to WAL.


**Risk.** The 'read-only' contract is false. verify materializes empty outbox DBs (so `verify --db-dir` on a typo'd path reports phantom tables it just created), permanently converts inspected DBs to WAL, and applies schema migrations to any *.db it globs — corrupting unrelated databases that merely happen to live in the scanned directory.


**Why it matters standalone.** A third party auditing a production deployment, or scripting `verify` in CI/monitoring against a backup directory, will mutate the very files they intended only to inspect — and `verify --db-dir somedir/*.db` will run migrations against any non-sqloutbox SQLite file in that directory. The documented safety guarantee they relied on does not hold.


**Evidence.** /Users/sandeep.yadav/tmp/sqloutbox/src/sqloutbox/_verify.py:18,:67; _outbox.py:64 (eager open_write_conn in __init__); _schema.py:85-102 (mkdir, CREATE TABLE, PRAGMA WAL, ALTER, CREATE UNIQUE INDEX, commit); cli.py:482,:494 (Outbox per globbed *.db)


**Recommendation.** Open verify connections read-only (sqlite3.connect(f'file:{path}?mode=ro', uri=True)) and skip migrations/PRAGMA in a verify code path; do not create the file if missing (report 'not an outbox DB' instead). Separate construction (which migrates) from inspection (which must not).


---


### MAJOR-4 · `completeness` · (confidence: high)

**Scenario.** A DB created by an older sqloutbox build with a forked chain (two rows sharing prev_seq, from before prev_seq UNIQUE was enforced or from a historical bug) is opened by the current version — either by the hot-path producer's first write (shared_outbox/SQLMiddleware) or by the supposedly read-only `sqloutbox verify`.


**Current behavior.** open_write_conn runs _MIGRATE_PREV_SEQ_UNIQUE = 'CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_prev_unique ON outbox_queue (prev_seq)' (_schema.py:43-46, :101) with NO try/except, unlike the immediately-preceding _MIGRATE_ADD_SOURCE which IS wrapped (_schema.py:95-98). On a forked DB this raises sqlite3.IntegrityError: UNIQUE constraint failed: outbox_queue.prev_seq during Outbox.__init__. Confirmed by reproduction.


**Risk.** The producer crashes on its FIRST enqueue (or even at construction) with an unhandled IntegrityError — the hot path that is documented to 'never raise — drops with WARNING' cannot even be reached because __init__ dies first. The drain crashes at startup. `sqloutbox verify`, which is supposed to DIAGNOSE such corruption read-only, also crashes instead of reporting the fork in its TableVerifyResult.


**Why it matters standalone.** An OSS user upgrading across versions, or recovering a DB that a prior bug forked, gets a hard crash at the worst possible layer (producer construction / startup) and cannot use the built-in verify tool to even understand the problem — the opposite of graceful degradation. autopulse's DBs were always created by recent builds so it never trips the migration.


**Evidence.** /Users/sandeep.yadav/tmp/sqloutbox/src/sqloutbox/_schema.py:95-98 (ADD COLUMN wrapped in try/except) vs :99-101 (CREATE UNIQUE INDEX NOT wrapped); _outbox.py:64 (eager call in __init__). Reproduced: IntegrityError UNIQUE constraint failed: outbox_queue.prev_seq


**Recommendation.** Wrap _MIGRATE_PREV_SEQ_UNIQUE in try/except like _MIGRATE_ADD_SOURCE, log a clear 'forked chain detected — manual repair required' error, and make verify detect/report forks rather than crash. Document the repair path (dedupe prev_seq) in the recovery SQL section.


---


### MAJOR-5 · `completeness` · (confidence: high)

**Scenario.** Two processes touch the same outbox .db file under the README-recommended separate-process layout (producer app + drain service), and the producer holds the BEGIN IMMEDIATE write lock for longer than the default 5-second SQLite busy timeout — e.g. a large enqueue_batch, a slow disk, an ENOSPC retry, or the host briefly paused/swapping.


**Current behavior.** thread_conn() (_schema.py:106-112) opens consumer-side connections (used by fetch_unsynced, mark_synced, delete_synced, prune_sync_log, verify, pending_count) with NO PRAGMA busy_timeout and NO explicit timeout argument — it relies on sqlite3's 5000ms default. mark_synced/delete_synced/prune_sync_log have no try/except (_outbox.py:260-346). When the producer's lock outlasts 5s, the consumer's write raises sqlite3.OperationalError 'database is locked' (reproduced: raised after 5.3s). That propagates out of asyncio.to_thread(outbox.mark_synced/delete_synced) in _flush_to_target (sync.py:677-678), which is awaited with no surrounding try/except in _worker_loop.


**Risk.** A transient lock-contention event (entirely normal for two processes on one SQLite file) crashes the entire drain task — not just one cycle — stopping delivery for ALL tables and ALL targets in that service until a supervisor restarts it. Worse, it can crash AFTER write_batch succeeded remotely but BEFORE delete_synced commits, leaving rows that re-deliver on restart.


**Why it matters standalone.** autopulse's enqueues are ~150µs so contention never reaches 5s; a third party with bigger payloads, slower disks (NFS/network FS, which also weakens WAL locking semantics), or a busier producer will see periodic drain crashes from ordinary lock contention. The library never sets busy_timeout, so the user has no knob and no documentation that this is required.


**Evidence.** /Users/sandeep.yadav/tmp/sqloutbox/src/sqloutbox/_schema.py:106-112 (thread_conn: bare sqlite3.connect, no busy_timeout); _outbox.py:260-346 (mark_synced/delete_synced/prune_sync_log, no error handling); sync.py:677-678 (awaited to_thread, no try/except), worker loop body (no per-target guard). Reproduced 'database is locked' after default 5s.


**Recommendation.** Set PRAGMA busy_timeout (e.g. 30000) on every connection in both open_write_conn and thread_conn, and wrap consumer write ops so a lock timeout retries the cycle instead of killing the loop. Document that WAL is unsafe over network filesystems.


---


### MAJOR-6 · `completeness` · (confidence: high)

**Scenario.** A single OutboxSyncService is configured with multiple targets (a documented multi-DB feature, sync.py:181-200, config example with 'primary' and 'audit'). One target's third-party writer returns a malformed result list (missing 'ok' key, or MORE results than statements), or the consumer hits a per-target SQLite error.


**Current behavior.** _worker_loop iterates targets in a plain for-loop with NO per-target try/except (sync.py:554-625). _flush_to_target accesses result['ok'] (KeyError if absent, sync.py:665) and stmt_info[i] indexed by results length (IndexError if writer returns extra entries, sync.py:663-664). Both raise out of _flush_to_target into the unguarded await at sync.py:619, killing the whole run() task. Confirmed: results longer than stmt_info raises IndexError.


**Risk.** There is zero fault isolation between independent targets sharing one daemon. A single misbehaving writer (KeyError on missing 'ok', or IndexError on a length-mismatched result list) takes down delivery for every other target and table in the service — a cross-tenant/cross-database availability coupling.


**Why it matters standalone.** The confirmed writer-protocol findings note the per-writer failure shapes, and the lifecycle blocker notes uncaught exceptions generally — but neither captures that the multi-target design provides NO blast-radius containment: an OSS user routing 'billing' and 'analytics' through one service reasonably expects a broken analytics writer not to halt billing delivery. It does halt it.


**Evidence.** /Users/sandeep.yadav/tmp/sqloutbox/src/sqloutbox/sync.py:554-625 (target loop, no try/except), :619 (unguarded await _flush_to_target), :663-665 (stmt_info[i] / result['ok']). Reproduced IndexError.


**Recommendation.** Wrap each target's drain (and each table's processing) in try/except that logs and continues to the next target/cycle, so one writer's contract violation degrades only that target. Validate result list length == len(stmts) and presence of 'ok' before indexing, treating violations as 'whole batch failed, retry'.


---


### MAJOR-7 · `completeness` · (confidence: high)

**Scenario.** A producer enqueues an UPDATE statement (a supported transform, inject_outbox_seq sync.py:159-172) to a target with inject_outbox_seq=True, where the SET clause contains a '?' character INSIDE a string literal — e.g. `UPDATE t SET note='huh?', a=? WHERE id=?`.


**Current behavior.** inject_outbox_seq counts '?' placeholders by `set_part.count('?')` over the substring before ' WHERE ' (sync.py:163-164), then inserts the outbox_seq value at that index via new_args.insert(n_set_args, outbox_seq) (sync.py:168). The literal '?' is miscounted as a placeholder, so n_set_args is too high and outbox_seq is inserted at the wrong position. Confirmed: `UPDATE t SET note='huh?', a=? WHERE id=?` with args ['v',5] and outbox_seq 99 produces args ['v',5,99] against placeholders (a=?, outbox_seq=?, id=?) — binding a='v', outbox_seq=5, id=99.


**Risk.** Silent argument misalignment on the remote: the wrong column gets the outbox_seq value, the real WHERE-key gets the outbox_seq, and the intended outbox_seq is lost. The row's idempotency key is corrupted and the UPDATE targets the wrong record — data corruption with no error (the statement is still valid SQL).


**Why it matters standalone.** The confirmed inject findings flag INSERT mangling and '?'/'WHERE'-in-literal for non-single-row INSERTs; this is the distinct UPDATE-path arg-misalignment caused by counting '?' inside SET-clause string literals. Any third party whose UPDATE payloads legitimately contain '?' in a quoted string (common in text data) gets silently corrupted writes. autopulse's statements happen not to embed '?' literals, hiding it.


**Evidence.** /Users/sandeep.yadav/tmp/sqloutbox/src/sqloutbox/sync.py:163-169 (set_part.count('?') string-literal-blind; new_args.insert at miscounted index). Reproduced arg reordering to ['v',5,99].


**Recommendation.** Do not parse SQL with substring/character counting. Require callers to pre-shape statements, or restrict inject_outbox_seq to a strict validated grammar and reject anything it cannot safely transform, or pass outbox_seq as a named parameter the caller pre-declares rather than positionally splicing it.


---


### MAJOR-8 · `concurrency` · (confidence: high)

**Scenario.** Two OutboxSyncService drain processes (or a drain plus an OutboxWorker, or two `sqloutbox runservice` invocations) point at the same db_dir and drain the same namespace concurrently — e.g. an operator accidentally starts the systemd unit twice, runs the CLI manually while the service is active, or a blue/green deploy briefly overlaps.


**Current behavior.** The drain cycle is three independent connections with no cross-statement atomicity: fetch_unsynced() opens its own connection (_outbox.py:208), verify_chain() opens another (_outbox.py:242), then after the network write mark_synced() (_outbox.py:267) and delete_synced() (_outbox.py:290) each open their own. fetch_unsynced selects `WHERE synced = 0 ORDER BY seq LIMIT ?` (_outbox.py:209-215) with no row claiming/locking. Two drains both read synced=0, both see the same seqs, both call writer.write_batch() with the same rows, both mark_synced + delete_synced. The shared_outbox registry (_registry.py) is per-PROCESS only — a second process gets its own Outbox and its own write connection, so there is no in-memory serialization between them.


**Risk.** Duplicate remote delivery. For inject_outbox_seq=True targets the `INSERT OR IGNORE ... outbox_seq` (sync.py:145-157, partial unique index in _add_outbox_seq sync.py:336-340) makes the duplicate INSERT idempotent, so data is not double-written. But for inject_outbox_seq=False targets (a supported, documented config — sync.py:53, README billing example) there is NO idempotency guard: both drains execute the raw INSERT/UPDATE, producing duplicate rows or double-applied UPDATEs on the remote DB. delete_synced is also racy but harmless (both pass the synced=1 check and DELETE); the damage is the duplicate SEND already happened.


**Why it matters standalone.** autopulse runs exactly one drain (one systemd unit) and uses inject_outbox_seq for its analytics targets, so it is protected by INSERT OR IGNORE and never trips this. A third-party user running inject_outbox_seq=False (e.g. delivering to a table they don't want a synthetic column on) gets duplicate inserts the moment two drains overlap, with no protection. Nothing in the code prevents a second drain from starting — there is no PID/flock guard.


**Evidence.** _outbox.py:202-219 (fetch_unsynced — no claim/lock, plain synced=0 select); _registry.py:34-35,61-69 (registry is module-global = per-process, no cross-process coordination); sync.py:650-679 (write_batch then per-row mark_synced/delete_synced); sync.py:145-157 (INSERT OR IGNORE only applied when should_inject_seq is True); README.md:481 documents 'Single process only' but only as a one-line producer-write-connection limitation, not as a delivery-duplication hazard.


**Recommendation.** Add process-level mutual exclusion for draining a given db_dir (flock on a lockfile, or a SQLite advisory claim). At minimum, document loudly that exactly one drain process per db_dir is mandatory and that inject_outbox_seq=False targets have NO duplicate-delivery protection. Consider a claim step (UPDATE ... SET claimed_by=? WHERE synced=0 in a transaction) so concurrent drains partition rows instead of duplicating them.


---


### MAJOR-9 · `concurrency` · (confidence: high)

**Scenario.** Within ONE drain process, a row earlier in a write_batch fails (result['ok'] is False) while a later row in the same batch succeeds — e.g. seq=5 hits a transient row-level remote error but seq=6 (its chain successor) writes fine.


**Current behavior.** _flush_to_target confirms EACH row independently by result['ok'] (sync.py:663-672), grouping confirmed seqs per table, then calls mark_synced + delete_synced only for confirmed seqs (sync.py:674-679). There is NO head-of-line hold: seq=6 is marked synced and DELETED from outbox_queue while seq=5 stays unsynced. seq=6 was the chain successor of seq=5 (prev_seq=5). Next cycle, fetch_unsynced returns seq=5 (still synced=0); verify_chain on [seq=5] checks _seq_accounted(prev_seq of 5): its predecessor is in sync_log so it passes (_outbox.py:245, :414-421); seq=5 is re-sent AFTER 6 was already delivered. sync_log makes every deleted seq 'accounted', so the broken ordering is undetectable by the chain machinery.


**Risk.** Out-of-order delivery and a permanently holey local chain. The remote DB receives seq=6 before seq=5, violating the FIFO-per-namespace contract (README.md:484). For inject_outbox_seq=False UPDATE statements this is a correctness hazard: a later UPDATE applied before an earlier one on the same key leaves the remote in the wrong final state, and the earlier UPDATE re-applied next cycle silently rewinds the row. verify_chain cannot detect this.


**Why it matters standalone.** autopulse's events are mostly idempotent appends (rejections, snapshots) where per-row ordering does not matter, so the violation is invisible to it. A third party using the outbox for ordered state replication — the natural use of UPDATE-carrying statements, which sync.py:159-172 explicitly supports — gets silent state corruption when partial-batch failures occur, and partial failures are normal (per-row remote constraint violations, transient row errors).


**Evidence.** sync.py:661-679 (independent per-row confirm; mark+delete only confirmed seqs; no abort-on-first-failure); _outbox.py:243-251 (verify_chain only checks adjacency within the fetched batch + first row's predecessor via sync_log, not global delivery order); _outbox.py:414-421 (_seq_accounted treats sync_log presence as OK); README.md:484 ('strictly FIFO per namespace') is the contract being broken.


**Recommendation.** Either implement head-of-line blocking (stop confirming at the first failed row in a namespace so ordering is preserved — the spec the prompt references), OR explicitly document that delivery is at-least-once and UNORDERED across partial failures and that ordered/UPDATE use cases are unsupported. The current 'strictly FIFO' docstring is false under partial batch failure.


---


### MAJOR-10 · `config-api` · (confidence: high)

**Scenario.** A naive consumer constructs OutboxConfig(db_dir=..., batch_size=0) or TargetConfig(name='x', tables=('t',), batch_size=-5, retain_log_days=-1), or OutboxConfig(flush_interval=0, table_flush_threshold=0, table_max_wait=-1, cleanup_every=0). This is the single most likely first mistake an arbitrary third party makes.


**Current behavior.** Both config dataclasses are plain @dataclass(frozen=True) with NO __post_init__ and NO validation anywhere. config.py contains zero `raise`, `assert`, or bounds checks (verified by grep). Every nonsensical value is silently accepted and propagated. batch_size=0 means fetch_unsynced returns nothing -> outbox never drains, grows unbounded. cleanup_every=0 / negative drives the modulo-based prune trigger in the drain loop into either never-prune or ZeroDivisionError depending on how N is used. negative retain_log_days makes prune compute a future cutoff and delete all sync_log audit rows immediately. flush_interval=0 busy-spins the drain loop at 100% CPU.


**Risk.** Silent unbounded outbox growth (disk exhaustion / DoS), 100% CPU busy-loop, or wholesale deletion of the local delivery audit trail — all from a config typo, with no error at construction time and no error at startup. The failure surfaces hours later in production as a disk-full or pegged-CPU incident, far from the cause.


**Why it matters standalone.** autopulse only ever passes its own known-good constants, so it never hits this. An arbitrary OSS user typing values into outbox.toml or the Python API has no guardrail — a frozen dataclass *looks* validated/safe but performs no validation, which is a misleading contract for a durability-critical library.


**Evidence.** src/sqloutbox/config.py:47-130 (TargetConfig — no __post_init__), 133-192 (OutboxConfig — no __post_init__); grep for `__post_init__|raise|assert|ValueError` in config.py returns nothing. _runner.py:527-537 builds OutboxConfig directly from TOML ints/floats with no range check either.


**Recommendation.** Add a __post_init__ to both dataclasses validating: batch_size>=1, flush_interval>0, table_flush_threshold>=1, table_max_wait>=0, cleanup_every>=1, retain_log_days>=0; raise a typed sqloutbox.ConfigError (see error-type finding) with the offending field name. Validate per-table retain_overrides too. Frozen dataclasses fully support __post_init__ via object.__setattr__ for normalization.


---


### MAJOR-11 · `crash-durability` · (confidence: high)

**Scenario.** Process is killed (SIGKILL, OOM, power loss, host eviction) AFTER writer.write_batch() returned ok=True for a row but BEFORE the subsequent mark_synced()/delete_synced() commit. On restart the row is still synced=0 in outbox_queue, so the next drain cycle re-fetches it and sends it to the remote again.


**Current behavior.** sync.py _flush_to_target sends the batch (line 650), then only AFTER a successful write does it call mark_synced + delete_synced (sync.py:677-678). There is no pre-write 'in-flight' marker and no transactional coupling between the remote write and the local commit. The remote write succeeding is not durably recorded locally until the two follow-up statements commit. Any crash in the window between writer.write_batch() returning and delete_synced committing leaves the row pending → guaranteed redelivery on restart. This is classic at-least-once delivery.


**Risk.** At-least-once delivery: every delivered row can be sent to the remote a second (or Nth) time after a crash. For tables WITHOUT idempotency (inject_outbox_seq=False), this causes duplicate rows / double-applied writes in the consumer's DB. For UPDATE statements (even with inject_outbox_seq=True), re-delivery re-applies the UPDATE — INSERT OR IGNORE does not de-dup UPDATEs (inject_outbox_seq for UPDATE only appends outbox_seq=? to SET, it does not gate the WHERE — sync.py:159-172), so a non-idempotent UPDATE (e.g. balance = balance + 10) is applied twice.


**Why it matters standalone.** An arbitrary OSS consumer who routes UPDATE statements, or who sets inject_outbox_seq=False (a fully supported, documented config — config.py:64-66, README limitation list), gets silent duplicate application on every crash-after-write. autopulse happens to use INSERT-mostly idempotent tables so it never sees this; a third party doing usage-based billing UPDATEs or audit-log inserts without the outbox_seq column will get data corruption. The README states 'at-least-once' nowhere — the delivery guarantee is undocumented.


**Evidence.** sync.py:650 (write), sync.py:663-672 (per-row ok check), sync.py:677-678 (mark_synced then delete_synced AFTER write); inject_outbox_seq UPDATE path sync.py:159-172; README.md:262-272 documents idempotency only for the INSERT case via INSERT OR IGNORE.


**Recommendation.** Explicitly document the guarantee as AT-LEAST-ONCE in README and the OutboxWriter protocol docstring. State clearly that (a) consumers MUST make their writes idempotent, (b) inject_outbox_seq=True only de-dups INSERTs not UPDATEs, and (c) UPDATE statements must be naturally idempotent (set absolute values, not deltas). Consider an idempotency strategy for UPDATEs (e.g. WHERE outbox_seq < ? guard) or at minimum a prominent warning.


---


### MAJOR-12 · `crash-durability` · (confidence: high)

**Scenario.** Per-row independent confirmation within a batch: a later row in a write_batch result is marked-synced+deleted while an earlier row in the SAME batch failed (result[i]['ok']==False). This breaks the singly-linked chain the design relies on, and the broken state is durable across restart.


**Current behavior.** _flush_to_target iterates results and confirms EACH row independently by result['ok'] (sync.py:663-679). There is no head-of-line hold: if row at seq=N fails but seq=N+1 succeeds, N+1 is added to confirmed_by_table, then mark_synced+delete_synced deletes N+1 from outbox_queue and writes it to sync_log, while N stays pending. On the next cycle, fetch_unsynced returns N (and any later survivors). verify_chain then checks the batch: N's successor relationship is validated against _seq_accounted, which returns True for N+1 because N+1 is now in sync_log (_outbox.py:414-421). So the gap is masked — delivery of N proceeds, but the original FIFO/causal ordering guarantee (N delivered before N+1) has already been violated and is now permanent.


**Risk.** Ordering guarantee violation that is durable and silently self-healing in verify_chain. The README sells 'strictly FIFO per namespace' (README.md:485) and the whole prev_seq chain machinery implies in-order, gapless delivery. In reality a single failed row mid-batch causes out-of-order delivery (N+1 lands at the remote before N), and after restart the chain check is satisfied via sync_log so nothing alarms. For consumers depending on causal order (e.g. 'created' before 'updated' for the same entity in different rows), this produces a wrong final remote state.


**Why it matters standalone.** A third-party consumer reading the README's 'strictly FIFO per namespace' will assume in-order delivery and build on it. The actual behavior delivers later rows ahead of an earlier failed row in the same batch, permanently. This is the single most surprising crash/durability-adjacent behavior for an OSS user and directly contradicts documented semantics. (Context confirms a spec proposes a head-of-line hold but it is NOT implemented.)


**Evidence.** sync.py:663-679 per-row ok handling with NO head-of-line stop; confirmed_by_table built per-row (sync.py:665-666); mark_synced+delete_synced per table (sync.py:677-678). _seq_accounted treats sync_log presence as 'accounted' (_outbox.py:414-421) so a delivered-out-of-order successor masks the gap. README.md:484-485 claims strictly FIFO per namespace.


**Recommendation.** Either implement head-of-line blocking (stop confirming a table's rows at the first failed seq, retry from there next cycle) to deliver on the FIFO promise, OR change the README to state delivery is NOT ordered under partial-batch failure — events within a namespace can be delivered out of order if an earlier event in a batch fails while a later one succeeds. Given the prev_seq chain exists specifically to protect ordering, head-of-line blocking is the intended fix.


---


### MAJOR-13 · `delivery-semantics` · (confidence: high)

**Scenario.** A drain batch for one table contains chain-consecutive rows seq=11,12,13 (prev_seq 10->11->12). writer.write_batch returns [{ok:false},{ok:true},{ok:true}] — the earliest row fails at the destination (e.g. transient FK/constraint/timeout on that one statement) while later rows in the SAME batch succeed.


**Current behavior.** _flush_to_target iterates results independently: seq=11 goes to failed_count, seq=12 and 13 go to confirmed_by_table[table], then mark_synced([12,13]) + delete_synced([12,13]) run while seq=11 remains pending (sync.py:663-679). There is NO head-of-line hold — a later row is committed-as-delivered while an earlier chain predecessor is still undelivered. The destination thus received rows 12 and 13 BEFORE row 11. On the next cycle fetch_unsynced returns only [11]; verify_chain([11]) checks row 11's prev_seq=10 via _seq_accounted (found in sync_log because 10 was delivered earlier) and returns chain_ok=True (_outbox.py:243-258, 414-421), so row 11 is finally re-sent — strictly AFTER 12/13 already landed.


**Risk.** Out-of-order delivery to the destination, silently, despite the advertised 'strict order' contract. For any consumer that depends on intra-namespace ordering (state machines, last-write-wins UPDATEs, append-only event logs replayed in seq order, CDC into another system), the destination observes a temporarily — and across the failure window, durably — reordered stream. The local chain integrity check passes the whole time, so no error surfaces; verify_chain only guards the LOCAL queue, never the delivered order.


**Why it matters standalone.** A third-party adopter reads 'strict order' + 'chain integrity verification on every batch' in the README and reasonably assumes per-namespace FIFO is delivered to the remote DB. autopulse's tables are mostly INSERT-only append logs into analytics, so reordering is benign for them; an arbitrary OSS user doing ordered UPDATEs or feeding an ordered downstream pipeline gets corruption with zero warning. The guarantee the code provides is 'at-least-once, best-effort order, reorders on partial failure', which is materially weaker than advertised.


**Evidence.** src/sqloutbox/sync.py:661-679 (independent per-result confirm, no ordering gate); src/sqloutbox/_outbox.py:221-258 (verify_chain only checks local linkage + sync_log presence of predecessor); src/sqloutbox/_outbox.py:414-421 (_seq_accounted treats sync_log presence as 'fine'); README.md:6-9 ('drains them to N remote databases in strict order') and README.md:253-260 ('verify_chain validates the chain is unbroken ... never silently drops events').


**Recommendation.** Either (a) implement head-of-line blocking: on the first failed result within a table's contiguous run, stop confirming that table at the failed seq and leave all subsequent seqs pending so they re-send after the predecessor lands; or (b) explicitly document the real contract in README/limitations: 'delivery is at-least-once; ordering is NOT preserved across a partial-batch failure — a later row may be delivered before an earlier row that failed. Use inject_outbox_seq + idempotent INSERT OR IGNORE; do not rely on remote-side ordering.' Today neither is true and the README claims the opposite.


---


### MAJOR-14 · `delivery-semantics` · (confidence: high)

**Scenario.** A single row is a 'poison' message — it fails at the writer on every attempt (malformed SQL for the remote dialect, a value that violates a remote CHECK/NOT NULL, an oversized payload, etc.). It is the only pending row, or it sits at the head with its predecessor already in sync_log so verify_chain passes.


**Current behavior.** Every drain cycle: fetch_unsynced returns the row, verify_chain passes, write_batch is called, result['ok'] is False, failed_count increments, nothing is marked/deleted (sync.py:663-672). The row stays pending forever. Because pending_count stays >= 1 and (after table_max_wait, default 6s) the time trigger fires every cycle, the table is re-included on essentially every scan — the service re-sends the same doomed statement to the destination on a fixed cadence indefinitely. There is no attempt counter, no exponential backoff, no dead-letter queue, no cap (grep across sync.py/_outbox.py/_worker.py: only flush_interval sleeps, only 'will retry' logs — sync.py:651-658, _worker.py:60-61,287).


**Risk.** Unbounded retry hammering of the remote DB with a request that can never succeed, plus permanent head-of-line block of everything queued behind it in that namespace. Blast radius: (1) the poison row's namespace never drains past it again — all later rows for that table accumulate forever (disk growth, ever-growing batches); (2) the destination receives a steady stream of identical failing writes (wasted quota/cost on metered DBs like Turso, error-log spam, possible rate-limit/lockout). The only escape is manual operator intervention via the documented 'force-skip' recovery SQL.


**Why it matters standalone.** autopulse controls both ends — its writer and its own well-formed SQL — so a permanently-failing row is unlikely in its deployment. An arbitrary OSS user with a hand-written writer or a remote schema that drifts WILL hit a poison row, and the library gives them an unbounded busy-retry loop that wedges an entire namespace and pounds their remote DB until a human notices and runs raw SQL. For a 'professionally-maintained OSS package', no max-attempts / DLQ / backoff and no mention in Limitations is a real operational gap.


**Evidence.** src/sqloutbox/sync.py:663-672 (failed rows simply not confirmed, no counter); src/sqloutbox/sync.py:528-529 + 570-588 (every scan re-includes any table with pending rows once max_wait elapses); absence of any backoff/attempt/dead-letter logic confirmed by grep over sync.py/_outbox.py/_worker.py; README.md:479-486 'Limitations' lists none of this; README.md:462-477 only offers manual force-skip SQL.


**Recommendation.** Add a per-row failure counter (attempts column or in-memory map keyed by seq) with capped exponential backoff before re-including a failing table, and a dead-letter path (move to outbox_dead_log / set a poisoned flag) after N attempts, with a loud WARNING/metric. At minimum, document explicitly in Limitations that a permanently-failing row causes unbounded retries and head-of-line blocking of its namespace, and that operators must monitor and manually force-skip.


---


### MAJOR-15 · `delivery-semantics` · (confidence: high)

**Scenario.** A user's writer returns a result list whose length or order does not match the statements sent: e.g. it collapses a successful no-op into fewer entries, returns one summary dict for the whole batch, or (for a writer that reorders/parallelizes) returns results in a different order than stmts. This is entirely under third-party control via the OutboxWriter protocol.


**Current behavior.** _flush_to_target does `for i, result in enumerate(results): table, outbox_seq = stmt_info[i]` (sync.py:663-664). If len(results) < len(stmts), the trailing seqs are simply never confirmed (silently treated as pending → retried next cycle, harmless-ish). If len(results) > len(stmts), `stmt_info[i]` raises IndexError, the exception propagates out of _flush_to_target, and since the call is awaited directly in _worker_loop with no try/except around it (sync.py:617-622), the entire drain task crashes — the worker_loop coroutine dies and the service silently stops draining ALL targets. Worst case: if results are merely REORDERED (same length), each result['ok'] is attributed to the WRONG seq — a failed statement's seq gets marked_synced+deleted (permanent data LOSS of an undelivered row) while a succeeded statement's seq is left pending and re-sent (duplicate). No length/identity validation exists.


**Risk.** Silent data loss + duplication (reordered results), or a hard crash of the whole drain daemon that takes down delivery for every target (over-length results), from a contract the writer author can violate without any guard. The protocol docstring says 'one result dict per statement (in order)' but nothing enforces it.


**Why it matters standalone.** autopulse ships its own TursoWriter that returns exactly one ok-dict per stmt in order, so it never trips this. An OSS consumer writing their own writer against the documented Protocol has no compile-time or runtime check that their result list matches; a subtle bug (filtering out no-op rows, batching errors into one dict, async reordering) becomes silent loss/dup or a daemon crash with a bare IndexError and no diagnostic. A standalone library must defend its own invariant rather than trust arbitrary user code.


**Evidence.** src/sqloutbox/sync.py:663-664 (positional zip via stmt_info[i], no len check, no echo of seq in result); src/sqloutbox/sync.py:617-622 (await _flush_to_target with no surrounding try/except in the loop body); the same fragile positional mapping also appears in _add_outbox_seq (sync.py:350-351) and _drop_outbox_seq (sync.py:406) and _seed_from_remote guards only the seed case (sync.py:484 `results[i] if i < len(results)`); OutboxWriter protocol docstring sync.py:94-100 states the contract but it is unvalidated.


**Recommendation.** Validate `len(results) == len(stmts)` immediately after write_batch and raise a clear, named error (or treat the whole batch as failed/retry) on mismatch. Wrap _flush_to_target's body so a malformed-writer error degrades to 'retry this target next cycle' instead of killing the drain loop. Strongly consider making the protocol carry the seq/index back in each result (or have sqloutbox key results by an opaque token) so reordering cannot misattribute ok/fail to the wrong row.


---


### MAJOR-16 · `lifecycle` · (confidence: high)

**Scenario.** Operator stops the service mid-flush. SIGTERM/SIGINT arrives while OutboxSyncService is between writer.write_batch() returning and the subsequent mark_synced()/delete_synced() calls for confirmed rows (sync.py _flush_to_target lines 677-678, each an `await asyncio.to_thread(...)`).


**Current behavior.** run_service_main (_runner.py:592-597) does `await stop.wait()` then `task.cancel()` then awaits the task swallowing CancelledError. The cancel injects CancelledError at the NEXT await point inside the running drain. If the writer already delivered the batch to the remote DB but the task is cancelled before/between `mark_synced` and `delete_synced` complete, those rows are delivered remotely but never marked/deleted locally. On restart they are re-fetched and re-delivered.


**Risk.** At-least-once redelivery of an already-delivered batch on every graceful shutdown that lands in this window. Whether this causes duplicate rows in the remote DB depends entirely on inject_outbox_seq: tables with injection get INSERT OR IGNORE (safe), but tables configured with inject_outbox_seq=False (a first-class, documented option — sync.py:289-296, config TargetConfig) get plain INSERTs replayed → duplicate rows. UPDATEs are idempotent regardless. There is NO drain/grace period: cancellation is immediate at the next await, not 'finish the current cycle'.


**Why it matters standalone.** The module docstring (_runner.py:549) and README systemd section promise 'graceful shutdown (finish current cycle, then stop)'. A third-party consumer reading that contract will assume in-flight batches complete atomically. They do not — cancel is immediate. A consumer who (reasonably) sets inject_outbox_seq=False because their table has no surrogate key will get duplicate remote rows on routine SIGTERM/restart/deploy cycles, with no warning.


**Evidence.** _runner.py:591-598 (create_task, stop.wait, task.cancel, await task except CancelledError); sync.py:650 (write_batch await) then 677-678 (mark_synced/delete_synced awaits — separate await points, cancellable between them); sync.py:289-296 + inject_outbox_seq=False path at 610 (injection is optional, so replay is not always idempotent)


**Recommendation.** Make shutdown cooperative instead of preemptive: have the worker loop check the stop Event at the TOP of each cycle (and skip a new cycle if set) rather than relying on task.cancel(). At minimum, guard the confirm step so that once write_batch() returns, the mark_synced+delete_synced for that batch run to completion uninterrupted (e.g. asyncio.shield, or perform them in a single to_thread that does both). Document explicitly that delivery is at-least-once and that inject_outbox_seq=False tables MUST be idempotent at the destination (unique constraint) to survive shutdown-window redelivery.


---


### MAJOR-17 · `lifecycle` · (confidence: high)

**Scenario.** An operator (or a misconfigured systemd unit / container orchestrator with maxSurge>0) starts a second instance of `sqloutbox runservice` against the same config / same db_dir while the first is still running.


**Current behavior.** There is no PID file, lockfile, flock, or any singleton guard anywhere in _runner.py, sync.py, or _outbox.py. Two processes both open the same outbox .db files. Each process's persistent _write_conn uses BEGIN IMMEDIATE (serialized via WAL), but the drain side uses separate short-lived thread_conn connections. Both drains independently run fetch_unsynced → write_batch → mark_synced → delete_synced.


**Risk.** Double-delivery and lost-update races. Two drains can fetch the same unsynced rows (fetch_unsynced takes no lock and does not claim/lease rows; mark_synced is a separate later step at _outbox.py:260), both send to the remote DB, and both delete. For inject_outbox_seq=True tables the remote INSERT OR IGNORE dedupes; for inject_outbox_seq=False tables it is straight duplicate inserts. _seed_from_remote (sync.py:435) and auto_schema ALTER TABLE (sync.py:319) also run concurrently in both processes. Additionally, default db_dir is resolved relative to cwd (_runner.py:453-454), so two units with different cwds silently point at DIFFERENT db_dirs while sharing one remote — a subtler split-brain.


**Why it matters standalone.** A third-party deploying via systemd plus a manual debug run, or a blue/green deploy that briefly overlaps two instances, or a k8s rollout with maxSurge>0, will silently run two drains with nothing warning them. The fetch-then-mark race means even idempotent tables see duplicate-delivery churn, and non-idempotent tables get duplicated data. The package markets itself as a durable correct outbox; single-drain-per-db_dir is an unstated hard precondition.


**Evidence.** _runner.py:545-598 (no lock acquisition anywhere in startup); _outbox.py:202-219 fetch_unsynced (read-only SELECT WHERE synced=0, no claim/lease); _outbox.py:260-272 mark_synced is a distinct step from fetch; sync.py:228 db_dir.mkdir(exist_ok=True) shares an existing dir with another live process silently


**Recommendation.** Acquire an exclusive OS lock at startup (e.g. fcntl.flock on a lockfile in db_dir, or a lease row) and exit with a clear error if held. Document that exactly one drain process per db_dir is required. Optionally make fetch_unsynced atomically claim rows (a lease column / UPDATE ... RETURNING) so concurrent drains cannot both grab the same rows.


---


### MAJOR-18 · `observability` · (confidence: high)

**Scenario.** A target name listed in config.targets has no matching key in the 'writers' dict (typo in writer key, or a writer that failed to construct and was omitted). The service starts normally.


**Current behavior.** In _ensure_schema (sync.py:306-308), _seed_from_remote (sync.py:457-459), and the main loop (sync.py:557-559), a target with no writer is silently `continue`d with no log line at any level. The service starts, logs 'sync worker started ... targets=[...]' listing ALL configured targets including the writerless one (sync.py:270-277), and then never delivers that target's rows.


**Risk.** Silent permanent non-delivery. Rows for the misconfigured target accumulate in the local outbox forever with zero log indication that the target is being skipped. The startup banner lists the target as if it were active, actively misleading the operator into believing it is wired up.


**Why it matters standalone.** A third-party operator who fat-fingers a writer key or whose writer factory returns a partial dict gets a daemon that looks healthy (clean startup, listed target) but silently black-holes an entire target's data. With the TOML path the writer key always matches (constructed together in _runner.py), but the public OutboxSyncService(config, writers) constructor — the documented Python-API entry point — is fully exposed to this mismatch. There is no validation at construction that every target has a writer.


**Evidence.** src/sqloutbox/sync.py:557-559 (`writer = self._writers.get(target_name); if not writer: continue` — no log); src/sqloutbox/sync.py:306-308 and 457-459 (same silent skip in schema/seed phases); src/sqloutbox/sync.py:270-277 (startup banner lists all targets regardless of writer presence)


**Recommendation.** At OutboxSyncService.__init__, validate that every target in config has a corresponding writer; raise (fail-fast) or at minimum log a WARNING/ERROR once at startup naming each writerless target. If silent-skip is intentional, log it at WARNING per startup (not per cycle) and exclude such targets from the 'started' banner.


---


### MAJOR-19 · `packaging-oss` · (confidence: high)

**Scenario.** A third party reads the README, runs `pip install sqloutbox` on Python 3.10 (which is fully inside the declared `requires-python = ">=3.10"`), creates an `outbox.toml`, and runs `sqloutbox runservice` — exactly the README's headline quickstart.


**Current behavior.** The bare install pulls NO TOML parser. `tomllib` is stdlib only on 3.11+. On 3.10 `_load_tomllib()` falls through to `import tomli`, which is absent (it lives only in the optional `[toml]` extra, and `dependencies = []`), and raises `RuntimeError("TOML support requires Python 3.11+ (tomllib) or 'pip install tomli'...")`. The package's flagship, README-front-and-center 'config-driven TOML' feature is therefore non-functional on a fully-supported Python version straight out of `pip install sqloutbox`.


**Risk.** Broken out-of-box experience / silent capability gap on a declared-supported interpreter. The crash is at service start (runtime), not at install, so a 3.10 user only discovers the missing feature when the daemon refuses to boot in production. The README's `## Installation` block (`pip install sqloutbox`) is incomplete for the very next section's quickstart.


**Why it matters standalone.** An OSS consumer cannot control their interpreter version — Debian 11/Ubuntu 22.04 LTS and many CI matrices still ship 3.10. They will install per the README, get green install, then a RuntimeError in prod. The fix is trivial for the maintainer but invisible to the user because the classifiers and `requires-python` actively assert 3.10 works.


**Evidence.** pyproject.toml:10 (`requires-python = ">=3.10"`), :33 (`dependencies = []`), :49-50 (`[project.optional-dependencies] toml = ["tomli>=2.0; python_version < '3.11'"]`); src/sqloutbox/_runner.py:293-308 (`_load_tomllib` raises RuntimeError when neither tomllib nor tomli is importable); README.md:12-14 (`pip install sqloutbox`) vs :61-62 (`sqloutbox runservice` reads outbox.toml); confirmed wheel METADATA: `Requires-Dist: tomli ...; extra == 'toml'` only.


**Recommendation.** Either (a) bump `requires-python` to `>=3.11` and drop the 3.10 classifier + the `tomli` extra (simplest, since TOML is the headline feature), OR (b) make `tomli` a real conditional core dependency: `dependencies = ["tomli>=2.0; python_version < '3.11'"]` so `pip install sqloutbox` on 3.10 just works. Update README Installation to mention `pip install sqloutbox[toml]` if the extra is retained.


---


### MAJOR-20 · `packaging-oss` · (confidence: high)

**Scenario.** A consumer relies on the README's stated durability/ordering guarantee — 'drains them to N remote databases in strict order' and 'A gap blocks delivery and logs an error (never silently drops events)' — to reason about crash/partial-failure safety, e.g. an event stream where event N+1 must not land before event N.


**Current behavior.** The drain confirms each row in a batch INDEPENDENTLY by `result["ok"]`. In `_flush_to_target`, `confirmed_by_table` collects only the OK seqs, then `mark_synced` + `delete_synced` run per table over exactly those seqs. A higher-seq row whose writer returned `ok:True` is marked-synced and DELETED even though an earlier (lower-seq) row in the same batch returned `ok:False`. `verify_chain` only runs locally before send and does not gate per-row confirmation. So there is NO head-of-line hold: order is NOT preserved across a partial-failure boundary, and the earlier failed row's prev-chain neighbour can be deleted out from under it.


**Risk.** The documented contract is materially stronger than the code. A consumer who trusts 'strict order' / 'never silently drops' can build invariants (e.g. apply-in-order projections, balance ledgers) that the library does not actually uphold under partial batch failure. This is a correctness/expectation mismatch baked into the public docs, the worst kind for an OSS dependency.


**Why it matters standalone.** A standalone user has no access to autopulse's internal knowledge that confirmation is per-row. They read only the README, which promises strict ordering and no silent drops. The drain's actual at-least-once-but-out-of-order-on-partial-failure semantics must be documented as the real guarantee, or the spec's head-of-line hold must ship before the README claims it.


**Evidence.** sync.py:661-679 (`confirmed_by_table` keyed only on `result["ok"]`; `mark_synced`/`delete_synced` per table; `failed_count` merely logged, never gates other rows); README.md:7-8 ('drains them to N remote databases in strict order'), :252-261 ('Chain integrity ... A gap blocks delivery and logs an error (never silently drops events)'); the docs/specs/2026-06-11 file itself acknowledges head-hold is a NOT-YET-IMPLEMENTED behavior change.


**Recommendation.** Until the strictly-ordered-retry spec lands, change README §'How it works'/'Chain integrity' to state the ACTUAL guarantee: at-least-once delivery, per-row independent confirmation, NO head-of-line ordering across partial writer failures, idempotency provided only when `inject_outbox_seq=True`. Add an explicit 'Delivery guarantees' subsection in Limitations. When head-hold ships, bump a minor version and update docs together.


---


### MAJOR-21 · `poison-data` · (confidence: high)

**Scenario.** A valid-JSON payload is delivered but the `tag` (SQL) is malformed, has the wrong number of `?` placeholders for the args array, references a non-existent column, or violates a remote constraint — e.g. a producer bug emits `INSERT INTO t (a,b) VALUES (?)` with 2 args, or the remote schema drifted. This includes inject_outbox_seq mangling: inject_outbox_seq does naive string surgery (sync.py:142-175) — `s.upper().find(') VALUES')` and `s.rfind(')')` — which silently corrupts any INSERT containing a ')' inside a string literal or a function call in VALUES (e.g. `VALUES (?, datetime('now'))`).


**Current behavior.** The drain decodes and appends the (possibly mangled) stmt to the batch with no validation (sync.py:607-613), sends the whole batch via writer.write_batch (sync.py:650), then confirms EACH row independently by `result['ok']` (sync.py:663-672). A failing row is logged as a warning (669-672) and simply left un-confirmed (not added to confirmed_by_table), so it is retried forever on every subsequent cycle. Crucially, because confirmation is per-row, LATER rows in the same batch ARE marked_synced + deleted (677-678) even though an EARLIER row failed — there is no head-of-line hold.


**Risk.** A permanently-poisonous SQL statement (wrong arg count, schema mismatch, mangled by inject_outbox_seq) is retried every cycle forever, burning a full remote round-trip each time and never draining. Worse, the per-row confirmation means a poison row that sits BEHIND already-delivered rows breaks the chain's delete ordering: the poison row stays in the queue at a low seq while its successors are deleted, leaving a permanent 'pending' floor and a queue that grows around the stuck row. verify_chain tolerates this (its _seq_accounted at _outbox.py:414-421 also checks sync_log), but the poison row itself never leaves.


**Why it matters standalone.** A third party's `tag` is THEIR SQL — sqloutbox cannot control its correctness, and inject_outbox_seq (on by default per _runner.py:500 `inject_outbox_seq=True`) actively rewrites it with brittle string ops that break on common SQL (string literals containing parentheses, SQL functions in VALUES, multi-row VALUES, INSERT...SELECT). There is no max-retry, no dead-letter, and no CLI command to purge a stuck row (cli.py only has init/runservice/verify). The OSS user's only recovery is to manually open the SQLite file and DELETE the offending seq — undocumented and dangerous (it can break the chain).


**Evidence.** sync.py:663-679 (per-row independent confirmation, no head-of-line hold; failed rows just warned and skipped); sync.py:142-175 (inject_outbox_seq naive string surgery — find(') VALUES'), rfind(')')); sync.py:650 (whole batch sent, no per-stmt validation)


**Recommendation.** (1) Add a per-row retry counter / dead-letter after N failures so a permanently-failing stmt stops consuming round-trips and is quarantined. (2) Replace inject_outbox_seq's string surgery with a parser-based or opt-in approach, and at minimum document its severe limitations (no string literals with ')', no functions in VALUES, single-row INSERT only). (3) Ship a `sqloutbox purge --seq` / dead-letter CLI command as a supported escape hatch.


---


### MAJOR-22 · `poison-data` · (confidence: high)

**Scenario.** A single corrupt/poison row sits at the HEAD of a namespace (lowest unsynced seq). Either it fails to JSON-decode (finding 1/2 — fatal) or it persistently fails at the remote writer (finding 3 — retried forever). The maintainer / consumer wants to skip past it to unblock the rest of the queue.


**Current behavior.** There is no escape hatch. The CLI exposes only `init`, `runservice`, and `verify` (cli.py:581-618). `verify` is strictly read-only (_verify.py:18 'All checks are read-only — they never modify the database'). There is no purge, skip, requeue, dead-letter, or delete-by-seq command, and no programmatic API to drop a specific row. fetch_unsynced always returns rows `ORDER BY seq LIMIT ?` (_outbox.py:212-213), so the poison row is always re-fetched at the head. mark_synced/delete_synced only act on caller-supplied seqs that the drain never supplies for a failing/poison row.


**Risk.** Permanent operational dead-end. For the JSON/UTF-8 case the whole service is dead until restart, and restart does NOT help (the same row re-decodes on next cycle → re-crashes → effective crash-loop with a zombie process in between). For the SQL-fail case the row blocks its own delivery forever and the only documented recovery (per the verify_chain error message at _outbox.py:254-256, 'See recovery SQL in sqloutbox docs') points at docs the reader must trust exist — there is no in-tool remedy.


**Why it matters standalone.** Autopulse's own operators have the source and Discord tooling to hand-edit Turso/SQLite; an arbitrary OSS consumer running this as a packaged daemon has no equivalent. When (not if) a poison row appears, they need a first-class, safe, supported way to inspect, quarantine, or skip it — surgery on a live WAL SQLite chain by hand risks breaking prev_seq UNIQUE and verify_chain. The absence of any escape hatch is a standalone-readiness gap.


**Evidence.** cli.py:581-618 (only init/runservice/verify subcommands); _verify.py:18 (verify is read-only); _outbox.py:202-219 (fetch_unsynced always head-first, no offset/skip); _outbox.py:254-256 (error tells operator to consult docs for manual recovery SQL)


**Recommendation.** Add supported CLI/API commands: `sqloutbox inspect --table T [--seq N]` (read a row's tag/payload), `sqloutbox skip --table T --seq N` (move to dead-letter and re-stitch the chain so successors validate), and `sqloutbox dead-letter list/replay`. Ensure skip correctly updates the next row's prev_seq (or relaxes verify_chain) so unblocking one row doesn't cascade into a chain-gap error.


---


### MAJOR-23 · `poison-data` · (confidence: high)

**Scenario.** A producer enqueues one or more very large payloads (multi-MB JSON blobs), or simply a large backlog accumulates while a target is down. With batch_size defaulting to 500 in the TOML/runner path (_runner.py:466 `app_batch_size = app_tuning.get('batch_size', 500)`, config default also 500) and no per-payload size cap anywhere.


**Current behavior.** fetch_unsynced loads up to `batch_size` full rows into Python lists at once (_outbox.py:202-219), the drain then decodes ALL of them and builds `all_stmts`/`stmt_info` holding every (sql, args) tuple plus the original QueueRow list simultaneously (sync.py:607-613), and passes the entire list to writer.write_batch in one call (sync.py:650). There is no payload size limit (confirmed: no size guard anywhere in src) and no streaming/chunking — the whole batch is materialized in memory, decoded, and held until the remote round-trip completes.


**Risk.** Memory blow-up / OOM. 500 rows × multi-MB payloads = gigabytes resident, multiplied because each payload exists at least 3× simultaneously (the stored str from fetchall, the .encode() bytes in QueueRow, and the json.loads-decoded Python object), plus the rewritten SQL string. A single oversized payload, or a large batch after an outage, can OOM-kill the daemon. Because batch_size also bounds the SQL `IN (...)` clauses in delete_synced/mark_synced (placeholders(len(seqs)) — _outbox.py:270,323), a very large batch_size can also hit SQLite's default 999-host-parameter limit (SQLITE_MAX_VARIABLE_NUMBER), raising an OperationalError in mark_synced/delete_synced AFTER the remote write already succeeded.


**Why it matters standalone.** An OSS consumer choosing batch_size for throughput has no warning that it interacts with both process RSS (× payload size) and SQLite's parameter limit. There is no byte-budget option (only a row-count limit), so a workload with large rows cannot be tuned safely. The 999-parameter limit in mark_synced/delete_synced is especially nasty: the remote write succeeds, then the local confirm raises, so on retry the rows are re-sent (relying on INSERT OR IGNORE idempotency, which only protects inject_outbox_seq=True INSERT targets).


**Evidence.** _outbox.py:207-219 (fetch_unsynced loads whole batch into memory, no size budget); sync.py:607-613,650 (entire batch decoded and held, single write_batch); _runner.py:466 / config.py:189 (batch_size default 500); _outbox.py:270,323 (IN-clause with placeholders(len(seqs)) — vulnerable to SQLite 999-param limit at large batch_size)


**Recommendation.** Add a byte-budget cap (e.g. max_batch_bytes) to fetch_unsynced/the drain so batches are bounded by size, not just row count; chunk delete_synced/mark_synced seq lists to <=900 per statement to stay under SQLITE_MAX_VARIABLE_NUMBER; document the memory and parameter-limit implications of batch_size for standalone tuning.


---


### MAJOR-24 · `poison-data` · (confidence: high)

**Scenario.** The SQLite outbox file itself becomes corrupted on disk (bad sectors, an OS crash mid-WAL-checkpoint, an incompatible external tool touching the file, or disk-full producing a partial write). Any read against it raises sqlite3.DatabaseError ('database disk image is malformed') or OperationalError ('database is locked' / 'disk I/O error').


**Current behavior.** The drain calls outbox.pending_count() (sync.py:566), outbox.fetch_unsynced() (sync.py:584), outbox.verify_chain() (sync.py:596) — all via thread_conn which opens a fresh sqlite3.connect with NO error handling (_schema.py:106-112, _outbox.py:208/242/350). A DatabaseError from any of these propagates up through the unguarded _worker_loop to kill svc.run() — same zombie outcome as finding 1. Even pending_count() at sync.py:566 (the very first DB touch per table per cycle) is unguarded, so a corrupt file is fatal before any send is attempted. The enqueue side has try/except (_outbox.py:109) so producers degrade gracefully (drop with warning), but the CONSUMER side has none.


**Risk.** One corrupted namespace file takes down delivery for ALL namespaces and ALL targets (single shared worker loop, no per-table isolation). The service zombifies; restart re-opens the same corrupt file and re-crashes — a corruption-driven crash-loop interleaved with zombie states. There is no degrade-and-continue: a transient 'database is locked' (e.g. another process holding a long write lock, or WAL contention) is treated identically to permanent corruption and is equally fatal.


**Why it matters standalone.** Producer-side robustness (enqueue never raises, _outbox.py:76 'Never raises — drops with WARNING') sets an OSS consumer's expectation that the library is crash-resilient. The consumer/drain side violates that expectation: a single bad or transiently-locked file is fatal and non-isolated. A standalone operator running N namespaces expects one bad table to be quarantined, not to silently sink the entire daemon (and every other app/target it serves) with no restart and no alert.


**Evidence.** _schema.py:106-112 (thread_conn — bare sqlite3.connect, no error handling); _outbox.py:208,242,350 (consumer reads have no try/except); sync.py:566,584,596 (all called in the unguarded worker loop); contrast _outbox.py:109-118 (enqueue HAS try/except — asymmetric: producer protected, consumer not)


**Recommendation.** Wrap each per-table drain unit (pending_count → fetch → verify → flush) in try/except sqlite3.DatabaseError/OperationalError: log, mark that table as degraded/skipped for this cycle, and continue to the next table/target so corruption is isolated. Distinguish transient locked-DB (retry next cycle) from structural corruption (quarantine + alert). And, as in finding 1, make the top-level worker-loop exit observable so true crashes restart instead of zombifying.


---


### MAJOR-25 · `resource-limits` · (confidence: high)

**Scenario.** Remote target (writer.write_batch) is down or rejecting for hours/days while producers keep enqueueing. E.g. Turso outage, expired auth token, network partition.


**Current behavior.** enqueue()/enqueue_batch() unconditionally INSERT into outbox_queue (_outbox.py:99-104, 181-186) with NO depth check. The drain loop only deletes rows after write_batch returns result["ok"]==True (sync.py:665-678); on a failed write it returns early (sync.py:651-658) leaving all rows in the queue. There is no max-depth, no high-water mark, no backpressure, and no eviction anywhere in the codebase (grep for max_pending/max_depth/backpressure/drop_oldest returns nothing). The queue grows monotonically until the remote recovers. README documents NO bound on queue depth — only batch_size which caps read-per-cycle, not total stored.


**Risk.** Unbounded disk growth. The SQLite file plus its WAL grow without limit for the entire duration of the outage. On a small VM/container this fills the disk, which then breaks not just sqloutbox but every other process sharing that volume (logs, the app's own DB, the OS). This is a latent DoS triggered by any prolonged downstream outage.


**Why it matters standalone.** autopulse runs on a VM with steady disk and low event volume, so it never notices. An arbitrary OSS consumer with a high-volume table and a flaky remote (or one who simply forgets to run the drain service) gets silent unbounded growth with zero warning logs and no configurable cap. There is no `max_pending` knob to set even if they wanted one.


**Evidence.** src/sqloutbox/_outbox.py:99-104 (enqueue INSERT, no depth check); src/sqloutbox/_outbox.py:181-186 (enqueue_batch INSERT, no depth check); src/sqloutbox/sync.py:651-658 (write failure returns, rows retained); src/sqloutbox/sync.py:665-678 (delete only on ok); README.md:186 (batch_size doc — caps read, not store)


**Recommendation.** Add an optional `max_pending_rows` (and/or `max_db_bytes`) cap to OutboxConfig/Outbox. When exceeded, either (a) make enqueue() return None + WARN (shed load at the producer), or (b) expose pending_count() prominently and document a monitoring requirement. At minimum, document in README that queue depth is unbounded and the operator MUST monitor pending_count()/disk and keep the drain healthy.


---


### MAJOR-26 · `resource-limits` · (confidence: high)

**Scenario.** A row repeatedly fails at the writer (e.g. a poison statement the remote always rejects: bad SQL, constraint violation, or a permanently-down target) while there is at least one pending row each cycle.


**Current behavior.** On write_batch raising, _flush_to_target logs WARN and returns immediately (sync.py:651-658); the rows stay pending. On a per-row {"ok": False}, the row is logged WARN and simply not confirmed (sync.py:667-672) so it is re-fetched next cycle. There is NO per-row failure counter, NO exponential backoff, NO dead-letter queue, and NO max-retry/quarantine. The only pacing is the fixed `await asyncio.sleep(flush_interval)` at the TOP of every cycle (sync.py:529). So a permanently-failing row is retried every flush_interval forever. Because the failing table still has pending>0 and (after a few cycles) elapsed>=table_max_wait, it stays 'ready' and is re-sent every cycle.


**Risk.** Not a tight CPU busy-spin (flush_interval enforces a floor of 1s/cycle by default), but it IS an unbounded fixed-rate retry storm against the remote: a poison row is re-sent every second forever, generating constant failing network calls + WARN log spam, and — critically — head-of-line behavior means the poison row's table never makes progress while it keeps consuming a full batch slot each cycle. If flush_interval is set very low (e.g. 0.01), it becomes a near-busy-loop hammering the remote. A poison row is never quarantined, so that table's queue grows forever (ties back to Finding 1).


**Why it matters standalone.** autopulse's writer and statements are controlled and well-formed, so poison rows are rare. An arbitrary OSS user generating SQL from user input, or pointing at a remote with a stricter schema, will eventually enqueue a statement the remote permanently rejects (constraint, type, syntax). sqloutbox will then retry it forever at fixed rate, spam logs, block that table's progress, and grow the queue unbounded — with no dead-letter escape hatch and no backoff. This is a realistic production trap the docs don't warn about.


**Evidence.** src/sqloutbox/sync.py:529 (only pacing = flush_interval sleep, no per-failure backoff); src/sqloutbox/sync.py:651-658 (raise → return, retry next cycle); src/sqloutbox/sync.py:667-672 (ok:false → not confirmed, retried); no retry-count/backoff/dead-letter anywhere (grep); contrast _worker.py:58-59 force-deletes only DECODE-corrupt rows, not writer-rejected rows


**Recommendation.** Add exponential backoff per table on consecutive write failures (so a down/poison target backs off instead of retrying every flush_interval), and add a per-row failure counter with a configurable max-retries → dead-letter (move to an outbox_dead table or mark with an error state) so one poison row cannot block a table indefinitely or grow the queue forever. Document the current 'retry forever, no backoff, no DLQ' behavior prominently.


---


### MAJOR-27 · `schema-versioning` · (confidence: high)

**Scenario.** A DB was created by an older sqloutbox build that allowed forked chains (two rows sharing the same prev_seq) — i.e. before prev_seq UNIQUE was enforced, or after any historical bug that produced a fork. A newer version opens that DB.


**Current behavior.** open_write_conn unconditionally runs `_MIGRATE_PREV_SEQ_UNIQUE` = `CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_prev_unique ON outbox_queue (prev_seq)` at _schema.py:101 with NO try/except (unlike _MIGRATE_ADD_SOURCE right above it, which IS guarded at lines 95-98). Building a UNIQUE index over data containing duplicate prev_seq values fails.


**Risk.** `sqlite3.IntegrityError: UNIQUE constraint failed: outbox_queue.prev_seq` propagates out of open_write_conn → out of Outbox.__init__ (_outbox.py:64) → crashes the producer at construction AND crashes the `sqloutbox verify` CLI (which constructs Outbox to scan). The one tool meant to diagnose chain corruption cannot even open the corrupted DB. A poison-pill DB takes down the whole drain service on startup with no recovery path in-library.


**Why it matters standalone.** autopulse only ever ran versions with prev_seq UNIQUE, so it has no legacy forked data. A third party upgrading from any earlier release, or whose DB was corrupted by an unclean crash/concurrent-writer bug, gets a hard crash on every start and cannot use the bundled verify tool to inspect or repair. The advertised 'tamper detection' is unreachable because opening the file is itself blocked.


**Evidence.** _schema.py:99-101 — comment claims 'no-op when the index already exists' but is silent about EXISTING DUPLICATE DATA; the call has no try/except while the sibling ADD COLUMN migration at lines 95-98 does. cli.py:482-494 and _verify.py:65/88 construct Outbox(...) → __init__ → open_write_conn. Reproduced: table with two rows prev_seq=5 → CREATE UNIQUE INDEX raised `IntegrityError: UNIQUE constraint failed: outbox_queue.prev_seq`.


**Recommendation.** Wrap _MIGRATE_PREV_SEQ_UNIQUE in try/except like _MIGRATE_ADD_SOURCE: on IntegrityError, log a clear error naming the duplicate prev_seq rows and instructing the documented recovery, but still allow read-only verify to proceed. Better: separate a read-only open path (no migrations, no WAL switch) for verify so corruption can always be inspected.


---


### MAJOR-28 · `security` · (confidence: high)

**Scenario.** A producer enqueues a perfectly legitimate but non-trivial INSERT/UPDATE on a table whose target has `inject_outbox_seq=True`, and the statement is anything other than a single-row `INSERT INTO ... (cols) VALUES (...)`: e.g. `INSERT ... SELECT`, multi-row `VALUES (...),(...)`, or an UPDATE/INSERT containing `?`, `) VALUES`, or ` WHERE ` inside a string literal.


**Current behavior.** `inject_outbox_seq` does naive case-insensitive substring surgery on the SQL text. Verified by executing the function: `INSERT INTO t (a,b) SELECT x,y FROM z` becomes `INSERT OR IGNORE INTO t (a, b, ?) SELECT x, y FROM z` (a literal `?` spliced into the COLUMN list); multi-row `INSERT INTO t (a) VALUES (?),(?)` becomes `... (a, outbox_seq) VALUES (?),(?, ?)` (column added but the placeholder appended only to the LAST tuple → column-count mismatch); for UPDATE it relies on `set_part.count('?')` and `upper.find(' WHERE ')`, both of which miscount when `?` or the text ` WHERE ` appears inside a quoted string literal in the SET clause.


**Risk.** Statement corruption. Best case the remote rejects it with a syntax/column-count error and the row is retried forever (silent stall / unbounded retry, never delivered — verify_chain won't help because the LOCAL chain is intact). Worst case (the UPDATE placeholder-miscount path) the `outbox_seq` value lands in the wrong bind slot and a wrong column is written / a different row is matched — silent data corruption at the destination.


**Why it matters standalone.** The README/CLAUDE.md document idempotency as a headline feature and only show the single-row `INSERT INTO ... VALUES (?, ?)` shape, never stating that this is the ONLY supported shape. An OSS adopter who writes a multi-row insert or `INSERT ... SELECT` (both normal SQL the producer would reasonably enqueue) gets silent corruption or permanent retry with no diagnostic. There is no validation or warning at enqueue time.


**Evidence.** src/sqloutbox/sync.py:142-175 (string manipulation: find(') VALUES'), rfind(')'), set_part.count('?'), find(' WHERE ')). Confirmed empirically: INSERT...SELECT and multi-row VALUES produce broken SQL; UPDATE with `?` inside a literal would misplace the outbox_seq arg.


**Recommendation.** Document the exact supported statement grammar for inject_outbox_seq (single-row INSERT INTO ... VALUES (...), simple UPDATE ... SET ... WHERE with no `?`/keywords inside literals). Better: detect unsupported shapes (INSERT...SELECT, multiple `) VALUES`/`),(`, etc.) and fail loudly at the writer boundary rather than silently emitting broken SQL. Long term, push outbox_seq injection into the writer/transport where the statement structure is known, instead of regex-rewriting opaque SQL text.


---


### MAJOR-29 · `writer-protocol` · (confidence: high)

**Scenario.** A third party implements OutboxWriter where write_batch returns a result dict missing the 'ok' key — e.g. a partial/degraded response, a SELECT-only result they shaped as {"rows": [...]}, or simply a writer bug that returns {} or {"status": "ok"} instead of {"ok": True}.


**Current behavior.** _flush_to_target reads result['ok'] with bracket subscripting (not .get), so any result dict lacking the literal key 'ok' raises KeyError. The KeyError propagates out of _flush_to_target into _worker_loop (which has no try/except around the _flush_to_target call at sync.py:619), crashing the entire drain task. Because run()/_worker_loop has no outer recovery, the whole sync service stops draining EVERY target and table — not just the offending one. Notably the seed path at sync.py:485 uses the defensive `result.get('ok')`, proving the codebase knows the key may be absent, but the hot delivery path does not.


**Risk.** Single malformed writer result permanently halts all delivery to all targets (silent stall — rows accumulate in the local SQLite outbox unbounded, disk fills). A defensive-coding asymmetry: seed is guarded, delivery is not.


**Why it matters standalone.** autopulse's own TursoWriter always returns a well-formed {"ok": ...} dict, so this never fires there. An arbitrary OSS consumer writing their first writer against the loosely-specified Protocol docstring (which shows {"ok": True, ...} but never says the key is MANDATORY on every dict) will hit this. The failure mode (total silent stall vs single-row skip) is severe and surprising.


**Evidence.** src/sqloutbox/sync.py:663-672 (`if result["ok"]:`); contrast src/sqloutbox/sync.py:484-485 (`result = results[i] if i < len(results) else {}` then `if not result.get("ok")`); _worker_loop call site src/sqloutbox/sync.py:617-622 has no exception guard around _flush_to_target


**Recommendation.** Use `result.get("ok")` in _flush_to_target like the seed path already does, treating a missing key as failure (retry). Additionally wrap the per-target _flush_to_target call in _worker_loop in a try/except so one target's malformed response cannot halt all targets. Document in the OutboxWriter docstring that every returned dict MUST contain a boolean 'ok' key.


---


### MAJOR-30 · `writer-protocol` · (confidence: high)

**Scenario.** A writer returns a results list whose length does not equal len(stmts): either FEWER results (e.g. it short-circuits on the first DB error and returns only the results it got, or batches internally and returns one summary dict), or MORE results (e.g. a transaction wrapper that prepends a BEGIN/COMMIT result, or a writer that expands a multi-statement string).


**Current behavior.** _flush_to_target iterates `for i, result in enumerate(results)` and indexes `stmt_info[i]`. If results is SHORTER than stmts: the trailing statements are never inspected, never marked synced, never deleted — they remain pending and are re-fetched and re-sent every cycle forever (infinite redelivery of the tail of every batch, with the remote relying on INSERT OR IGNORE idempotency to avoid duplicates — and for non-injected tables there is no idempotency, so genuine duplicates). If results is LONGER than stmts: `stmt_info[i]` raises IndexError once i exceeds the stmt count, crashing the drain task as in finding 1.


**Risk.** Shorter list → silent permanent redelivery / unbounded retry of the batch tail (duplication on non-idempotent tables, wasted writes, queue never drains past that point if the missing rows are also the chain head). Longer list → IndexError halts all delivery. Neither length mismatch is detected or reported.


**Why it matters standalone.** The len-must-equal-and-be-order-aligned requirement is the single most load-bearing part of the writer contract and it is stated only in passing prose ('in order'), never as an enforced precondition. A third-party writer that batches into a single remote transaction and returns one aggregate dict — a completely reasonable design — silently corrupts delivery. autopulse's writer happens to return exactly one dict per stmt so the contract is never tested at the boundary.


**Evidence.** src/sqloutbox/sync.py:663-664 (`for i, result in enumerate(results): table, outbox_seq = stmt_info[i]`); compare the seed path which explicitly bounds-checks at src/sqloutbox/sync.py:484 (`results[i] if i < len(results) else {}`). The OutboxWriter Protocol (sync.py:83-101) and README (README.md:383-392) say 'One result dict per statement (in order)' but nothing validates or enforces len(results)==len(stmts).


**Recommendation.** Validate `len(results) == len(stmts)` at the top of _flush_to_target; on mismatch, log an error and retry the whole batch (return without marking anything), rather than partially confirming by positional zip. Document the alignment+length contract explicitly and prominently in the OutboxWriter docstring.


---


## MINOR


### MINOR-1 · `completeness` · (confidence: high)

**Scenario.** Operator runs `sqloutbox verify --db-dir /path/to/data` (a first-class documented mode, cli.py:488-494) against a directory whose .db files were produced by the low-level public Outbox API with a namespace that differs from the filename, or that pack multiple namespaces into one shared file — both explicitly supported ('Multiple Outbox instances MAY share the same file — they are partitioned by namespace', _outbox.py:31-36).


**Current behavior.** CLI --db-dir derives the namespace from the file stem: `name = db_file.stem; outboxes[name] = Outbox(db_path=db_file, namespace=name)` (cli.py:492-494). verify_outbox then filters every query by `WHERE namespace = ?` using that stem (_verify.py:91-198). If the real namespace != stem, or the file holds several namespaces, verify only ever inspects rows whose namespace equals the filename. Confirmed: a file events.db holding namespace 'orders' reports total_rows=0, pending=0 — clean and empty.


**Risk.** False-clean verification: pending backlogs, chain gaps, and corruption in any namespace not matching the filename are invisible to the CLI. An operator using `verify` as the documented health check on a live deployment (the use case _verify.py:13 advertises) is misled into believing the outbox is empty/healthy.


**Why it matters standalone.** The library documents both 'namespace is independent of file path' and 'verify --db-dir scans *.db', but the CLI silently assumes namespace==stem and one namespace per file. A third party who used the low-level Outbox API as documented gets a verify tool that reports nothing about their actual data.


**Evidence.** /Users/sandeep.yadav/tmp/sqloutbox/src/sqloutbox/cli.py:492-494 (namespace=stem); _verify.py:91-198 (all queries filter WHERE namespace = ?); _outbox.py:31-36 (multi-namespace-per-file is documented). Reproduced 0 rows for mismatched namespace.


**Recommendation.** In --db-dir mode, enumerate the DISTINCT namespaces present in each file (SELECT DISTINCT namespace FROM outbox_queue UNION outbox_sync_log) and verify each, rather than assuming the stem is the namespace; or warn when the file contains namespaces other than the stem.


---


### MINOR-2 · `completeness` · (confidence: high)

**Scenario.** A running daemon's wall clock steps backward — NTP correction after drift, a VM resumed from a snapshot, a manual `date` set, or a leap-smear adjustment — so a later-enqueued row's created_at is earlier than an earlier row's. The chain (prev_seq) remains perfectly intact. Then `sqloutbox verify`, SIGUSR1, or request_verify() runs.


**Current behavior.** verify_outbox computes timestamps_monotonic by string-comparing created_at ordered by seq and sets ok = chain_ok AND seq_continuous AND timestamps_monotonic (_verify.py:166-200). now_iso() is wall-clock UTC (_schema.py:117-119), not monotonic. A single backward clock step makes timestamps_monotonic=False, so the table's ok=False, verify_all().ok=False, and the CLI exits 1 — even though delivery ordering (driven by seq, not timestamp) is completely correct. Confirmed: injecting an earlier created_at yields ok=False with an otherwise intact chain.


**Risk.** False-negative health signal: `sqloutbox verify` fails CI/monitoring gates and the integrity scan reports FAILED purely because of benign clock movement, with no actual data or ordering problem. Operators may take destructive 'recovery' action on a healthy queue.


**Why it matters standalone.** Delivery correctness depends only on seq/prev_seq; created_at is informational. Folding a wall-clock-monotonicity check into the pass/fail ok flag means any third party whose hosts experience normal NTP corrections gets spurious verify failures. The contract that 'ok' means 'safe to deliver' is broken.


**Evidence.** /Users/sandeep.yadav/tmp/sqloutbox/src/sqloutbox/_verify.py:166-183 (string-compare created_at), :200 (ok includes timestamps_monotonic); _schema.py:117-119 (wall-clock UTC now_iso); cli.py:567 (sys.exit(0 if result.ok else 1)). Reproduced.


**Recommendation.** Drop timestamps_monotonic from the ok computation (report it as an informational warning only), or document that created_at is wall-clock and non-monotonic and must not gate health. Ordering integrity is already covered by chain_ok + seq_continuous.


---


### MINOR-3 · `config-api` · (confidence: high)

**Scenario.** Any config error path: malformed TOML field, missing ${VAR}, bad writer_class, missing tables, or (post-fix) an out-of-range tuning value. A consumer wants to catch sqloutbox config errors specifically (e.g. except SqloutboxConfigError) to distinguish them from their own app errors.


**Current behavior.** The library raises only built-in/stdlib exception types: ValueError (config.py-adjacent loader: _runner.py:340,358,434,448,471,481,489,497), EnvironmentError/OSError (_runner.py:245), FileNotFoundError (423), ImportError/AttributeError (281,287), RuntimeError for missing tomllib (304-308) and Doppler failures (206-219). There is no sqloutbox exception hierarchy at all — grep shows no custom Exception subclass exported from __init__ (only data/service classes). enqueue() also swallows ALL sqlite3 errors and returns None (_outbox.py:109-118), leaking nothing but also signalling nothing typed.


**Risk.** Consumers cannot programmatically distinguish 'sqloutbox rejected my config' from 'my own code raised ValueError' — they must either catch broad Exception (masking real bugs) or string-match messages (brittle). EnvironmentError is also deprecated as an alias for OSError, an odd choice for missing-var. There is no documented exception contract.


**Why it matters standalone.** A mature OSS library gives consumers a typed exception root (e.g. SqloutboxError) so they can write targeted handlers and so the contract is greppable/documented. autopulse runs the service via CLI and lets it crash, so it never needs to catch typed config errors — a standalone integrator embedding load_config_toml() does.


**Evidence.** src/sqloutbox/_runner.py raises bare ValueError (multiple), EnvironmentError (245), RuntimeError (304,206); __init__.py:71-93 __all__ exports no exception types; no `class .*Error` defined in the package (grep).


**Recommendation.** Introduce a small exception hierarchy (SqloutboxError -> ConfigError, EnvError, WriterImportError) in a public module, export from __init__, and raise these from _runner/config instead of bare ValueError/EnvironmentError/RuntimeError. Document the exception contract in the API Reference.


---


### MINOR-4 · `crash-durability` · (confidence: high)

**Scenario.** Process is killed in the gap BETWEEN mark_synced() committing and delete_synced() committing. These are two separate asyncio.to_thread calls, each opening its own thread_conn (a distinct sqlite3 connection) and committing independently — so there are two distinct durable commit points with a crash window between them.


**Current behavior.** After mark_synced commits, the row is synced=1 in outbox_queue but has NOT yet been inserted into outbox_sync_log nor deleted. If the process dies here, on restart fetch_unsynced() filters WHERE synced=0 (_outbox.py:212), so the synced=1 row is never re-fetched and never delivered again — which is correct (it WAS delivered). BUT the row now sits permanently in outbox_queue: it is never re-examined by the drain loop (pending_count also filters synced=0, sync.py:566 / _outbox.py:352), so delete_synced is never retried for it, and no sync_log entry is ever written. The row leaks into the queue forever and never reaches the audit trail.


**Risk.** Permanent orphan rows in outbox_queue (synced=1, never deleted, never logged). Three consequences: (1) outbox_queue grows unbounded across crashes — no mechanism ever reclaims synced=1 rows; (2) the chain invariant degrades — verify_full's sequence-continuity check expects every gap to be covered by sync_log (_verify.py:148-163), but a synced=1-but-undeleted row is in neither the unsynced set nor sync_log, and prev_seq for the NEXT delivered+deleted row may now point at a row that is present-but-synced (verify_chain only checks _seq_accounted which is satisfied while it's in-queue, _outbox.py:414-421); (3) prune_sync_log only prunes sync_log, never queue, so these never expire.


**Why it matters standalone.** Any OSS consumer running the service long enough across hard crashes accumulates undeletable synced=1 rows that bloat the local SQLite file and confuse the verify tooling (verify_full will report inflated total_rows vs pending, and orphan/continuity logic was not designed for in-queue synced rows). There is no documented or automated cleanup. A maintainer auditing with `sqloutbox verify` gets misleading counts.


**Evidence.** sync.py:677-678 — two separate `await asyncio.to_thread(...)` calls, each entering its own `with thread_conn(...)` block (mark_synced _outbox.py:267, delete_synced _outbox.py:290) → two independent commits. fetch_unsynced WHERE synced=0 _outbox.py:212. pending_count WHERE synced=0 _outbox.py:352. delete_synced is only ever called immediately after mark_synced in the same flush — there is no recovery path that scans for synced=1 rows. prune_sync_log only touches outbox_sync_log _outbox.py:336-341.


**Recommendation.** Either (a) merge mark_synced + delete_synced into a SINGLE transaction (one thread_conn block doing UPDATE synced=1, INSERT sync_log, DELETE) so the intermediate state is never durable — this also removes the orphan window entirely; or (b) add a startup/periodic recovery sweep that finds synced=1 rows still in outbox_queue and completes their delete_synced. Option (a) is strictly better and simpler. Also document that mark_synced as a separate public step exists only for this two-phase pattern.


---


### MINOR-5 · `crash-durability` · (confidence: high)

**Scenario.** Restart recovery after any crash: does the service come back cleanly or can it wedge? Specifically the seed-from-remote + auto-schema startup path and the chain-verification path interacting with rows left synced=1-undeleted (from the mark/delete gap) or with a remote that already has rows.


**Current behavior.** Restart is generally clean: open_write_conn re-applies idempotent schema/migrations (_schema.py:89-101), _ensure_schema ADD/DROP outbox_seq is idempotent, _seed_from_remote re-advances the local AUTOINCREMENT above remote MAX(outbox_seq) and tolerates query failure (sync.py:435-499). The drain simply re-fetches synced=0 rows. It does NOT wedge on a genuine chain gap — verify_chain returning False only BLOCKS that one table's delivery (sync.py:599-605 / _worker.py:202-208) and logs an error; other tables proceed. However, a real lost-row gap (e.g. the README's documented 'force-skip a lost row') will block that namespace's delivery indefinitely until an operator manually inserts a sync_log row (README.md:474-477) — there is no automatic timeout or skip, so one lost row halts a whole namespace forever.


**Risk.** One unrecoverable chain gap permanently wedges delivery for that entire namespace (all later rows pile up, never delivered) with only an ERROR log — no alert, no automatic dead-lettering, no bounded retry. Combined with finding #2 (synced=1 orphan rows from the mark/delete crash gap), verify tooling can also misreport. The service does not crash-loop, but a single bad row silently stops a stream.


**Why it matters standalone.** An OSS consumer who hits a lost row (disk corruption, the power-loss tail-loss from finding #4, or manual DB surgery) gets a silently stalled namespace with unbounded local growth and no operational signal beyond a log line. They must know the manual recovery SQL exists. There is no built-in dead-letter or skip-after-N-cycles. This should be documented as an operational requirement (monitor pending_count / watch for chain-gap ERROR logs).


**Evidence.** verify_chain gap → `continue` (skip table) sync.py:599-605 and _worker.py:202-208; no retry budget / timeout / dead-letter. Manual recovery is the only path (README.md:461-477). _seed_from_remote tolerant of failure sync.py:472-480. Schema migrations idempotent _schema.py:95-101.


**Recommendation.** Document that a chain gap halts the affected namespace until manually resolved, and that operators MUST monitor for the chain-gap ERROR log and pending_count growth. Consider an optional bounded behavior (e.g. after N cycles of a persistent gap, emit a louder alert or auto-skip into sync_log with a clear data-loss log) and surface gaps via a health/metrics hook rather than only logs.


---


### MINOR-6 · `lifecycle` · (confidence: high)

**Scenario.** Long-running daemon (the normal case): the drain calls fetch_unsynced / verify_chain / mark_synced / delete_synced / prune_sync_log / pending_count millions of times over days/weeks via asyncio.to_thread, each going through `with thread_conn(self.db_path) as conn:`. Also abrupt process exit (SIGKILL) with no teardown.


**Current behavior.** thread_conn (_schema.py:106-112) returns a fresh `sqlite3.connect(...)` with NO close. The `with ... as conn:` block uses sqlite3's connection context manager, which commits/rolls back the transaction but does NOT close the connection (verified empirically: connection stays open after the with-block). The connection and its OS file descriptor are reclaimed only when the object's refcount drops to zero and CPython finalizes it. The persistent _write_conn (_outbox.py:64) is never closed anywhere — no close()/__del__/shutdown/aclose teardown exists in Outbox, OutboxSyncService, or the runner.


**Risk.** On CPython this mostly self-heals via prompt refcount finalization, so it is not a runaway leak there. But: (1) it relies on a CPython implementation detail — on PyPy or any non-refcounting runtime, connections accumulate until GC, transiently spiking open FDs and WAL reader locks, which can hit the process FD ulimit under high cycle rates with many tables/targets; (2) there is NO clean teardown on shutdown — _write_conn is never closed and no PRAGMA wal_checkpoint is issued, so the app never releases SQLite handles or checkpoints the WAL on exit.


**Why it matters standalone.** A consumer running sqloutbox on PyPy (a plausible target given the stdlib-only/portable selling point, requires-python>=3.10, no C deps) or any runtime without prompt refcounting will see FD/WAL-reader growth a CPython author never observed. Even on CPython, the absence of any close()/aclose() teardown means a consumer embedding OutboxSyncService in a larger app cannot cleanly release SQLite handles when reconfiguring/restarting the service in-process.


**Evidence.** _schema.py:106-112 thread_conn returns sqlite3.connect with no close; empirical test confirmed `with conn:` leaves the connection OPEN; _outbox.py:64 self._write_conn assigned, zero .close() calls in _outbox.py/_schema.py/_verify.py; _runner.py:591-598 shutdown path closes nothing


**Recommendation.** Make thread_conn a proper contextmanager (or wrap callers in try/finally) that calls conn.close() on exit. Add an explicit shutdown/aclose() to Outbox and OutboxSyncService that closes the persistent _write_conn (and optionally runs PRAGMA wal_checkpoint(TRUNCATE)), and call it from run_service_main's finally block after the task is cancelled.


---


### MINOR-7 · `lifecycle` · (confidence: high)

**Scenario.** The runner is invoked on a platform/loop where add_signal_handler is unsupported — Windows ProactorEventLoop, or from a non-main thread (e.g. embedded in a host asyncio app that already owns the loop).


**Current behavior.** run_service_main unconditionally calls loop.add_signal_handler(SIGTERM) and loop.add_signal_handler(SIGINT) (_runner.py:579-580) with no try/except and no fallback. add_signal_handler raises NotImplementedError on Windows ProactorEventLoop and ValueError when called off the main thread. cmd_runservice only catches KeyboardInterrupt (cli.py:452).


**Risk.** On Windows the runner cannot start at all — add_signal_handler raises NotImplementedError, uncaught, crashing before the drain begins. From a non-main thread it raises ValueError. There is no graceful-stop fallback on these platforms; the only stop is SIGKILL (or Ctrl+C surfacing as KeyboardInterrupt in cli.py, bypassing the orderly stop.set() path).


**Why it matters standalone.** pyproject declares requires-python>=3.10 with no OS restriction and the package presents as portable/stdlib-only. A Windows consumer running `python -m sqloutbox runservice` gets an immediate NotImplementedError crash. A consumer embedding the runner in a worker thread gets ValueError. The supported-platform / embedding contract is unstated.


**Evidence.** _runner.py:579-580 (loop.add_signal_handler for SIGTERM/SIGINT unconditionally, no guard); cli.py:450-453 (only KeyboardInterrupt handled); pyproject requires-python>=3.10 with no OS classifier excluding Windows


**Recommendation.** Wrap add_signal_handler in try/except (NotImplementedError, ValueError) and fall back to signal.signal() on the main thread / Windows, or document runservice as POSIX-main-thread only with an explicit runtime check and clear error. Provide a programmatic stop (pass an asyncio.Event) for in-process embedding.


---


### MINOR-8 · `observability` · (confidence: high)

**Scenario.** The remote DB is down or the writer raises on every cycle (network partition, expired auth token, target DB offline). The drain keeps fetching the same pending batch and re-calling write_batch().


**Current behavior.** In _flush_to_target, a raised exception from writer.write_batch() is caught and logged once per cycle at WARNING ('write failed ... will retry'), then the batch is left in the queue (sync.py:649-658). The drain loop runs every flush_interval (default 1.0s). Per-row failures (result['ok']==False) are likewise logged at WARNING once per row per cycle (sync.py:669-672). There is no error dedup, no escalation to ERROR after N consecutive failures, no backoff, and no 'persistent failure' summary state.


**Risk.** Log spam (DoS-on-your-own-logs): a single dead target emits a WARNING per cycle = ~86,400 WARNING lines/day per target at the default 1s interval, and one per failed row. This drowns real signal, can fill disk / blow log-ingestion quotas, and makes the line indistinguishable from a transient blip. An operator cannot tell from the log stream alone whether this is one 5-second outage or a 6-hour sustained outage without parsing timestamps across thousands of identical lines.


**Why it matters standalone.** A third-party operator running this as a systemd service with journald/Loki/CloudWatch will see their logging bill or disk fill from a single misconfigured target, and on-call paging built on 'WARNING rate' will be useless because healthy and unhealthy look like a continuous WARNING stream. autopulse may tolerate this because it has the Discord log handler and a human watching; an arbitrary consumer wiring this to standard log-based alerting gets either alert fatigue or a silent disk-fill incident.


**Evidence.** src/sqloutbox/sync.py:649-658 (exception path, WARNING every cycle, no backoff); src/sqloutbox/sync.py:663-672 (per-row failure WARNING every cycle); src/sqloutbox/sync.py:528-529 (loop sleeps flush_interval=1.0s default per config.py)


**Recommendation.** Track per-(target,table) consecutive-failure state. Log the first failure at WARNING, escalate to ERROR once (e.g. after K cycles or T seconds of sustained failure), then suppress repeats with periodic 'still failing for Ns, M rows stuck' summaries (exponential or fixed-interval log throttling). Apply a backoff to the retry interval for a failing target so a dead DB does not spin at 1Hz. Document the expected log cadence for a sustained outage.


---


### MINOR-9 · `observability` · (confidence: high)

**Scenario.** Delivery to a target is fully stuck (writer always raises, OR every row returns ok=False, OR the table never reaches the flush threshold, OR a chain gap blocks the table). Rows accumulate in the local outbox indefinitely.


**Current behavior.** The 'cycle delivered' summary INFO/DEBUG line is only emitted when total_confirmed > 0 (sync.py:688-702). On a cycle where a target flushed but nothing was confirmed, only the per-failure WARNINGs fire; on a cycle where a table never became 'ready' (pending < threshold AND elapsed < max_wait), the table is silently skipped (DEBUG-gated VERBOSE only, sync.py:573-581). There is NO log line that reports growing queue depth, age of the oldest unsynced row, or total backlog over time. pending_count()/total_pending() exist as Python methods (sync.py:714-728) but are never called by the service, never logged, and not exposed via CLI or signal.


**Risk.** Silent unbounded backlog growth. A queue that is steadily filling (producer faster than a degraded target, or a permanently-failing-but-non-raising writer) produces no INFO-level heartbeat and no depth metric. The local SQLite files grow without bound until the disk fills. An operator watching INFO logs sees silence (looks healthy) right up to disk exhaustion or producer enqueue failures.


**Why it matters standalone.** A standalone operator has no passive signal of backlog health. There is no heartbeat INFO log ('drained N, backlog M, oldest age Ts') on each cycle, so the only way to discover a stuck queue is to manually run 'sqloutbox verify' or stat the .db file sizes. autopulse can lean on its own external dashboards/pulseview; a generic OSS consumer expects the drain daemon itself to surface backlog depth and oldest-row age, and it does not.


**Evidence.** src/sqloutbox/sync.py:688-702 (summary line guarded by 'if total_confirmed'); src/sqloutbox/sync.py:573-581 (skip path logs only at VERBOSE level 5); src/sqloutbox/sync.py:714-728 (pending_count/total_pending defined but never invoked anywhere in the service loop); no created_at-based 'oldest row age' query exists anywhere (_outbox.py / _verify.py)


**Recommendation.** Emit a periodic heartbeat INFO log (e.g. every N cycles or every T seconds) with per-target backlog depth and age of oldest unsynced row (add a MIN(created_at) query to Outbox). Expose pending_count()/total_pending() and oldest-age via a CLI 'status' subcommand and/or a SIGUSR2 dump. Consider an optional metrics callback hook so consumers can wire Prometheus/StatsD without sqloutbox importing any client.


---


### MINOR-10 · `observability` · (confidence: high)

**Scenario.** An operator wants to inspect live queue state — how many rows are pending per namespace, which target is behind, the oldest unsynced timestamp — on a running production daemon, without restarting it or attaching a debugger.


**Current behavior.** The CLI exposes only three subcommands: init, runservice, verify (cli.py:579-618). 'verify' is an OFFLINE integrity scan that opens its own connections against the .db files (cli.py:459-567); it reports pending/synced/total counts and chain status but is a point-in-time structural check, not a live operational status, and it makes NO connection to the running daemon. There is no 'status', 'inspect', 'depth', or 'drain-once' subcommand. On a running daemon the only live introspection path is SIGUSR1 → integrity verification (Unix only, _runner.py:583-589), whose result goes only to the daemon's own INFO log, not to the invoking shell.


**Risk.** Operability gap: an operator cannot get an at-a-glance live status. They can run 'verify --db-dir' against the live files (safe because WAL + read-only), but that conflates 'integrity OK' with 'delivery healthy' — a queue can be perfectly chain-intact while 100k rows back up because the target is down. There is no command that answers 'is delivery keeping up right now and which target is behind'.


**Why it matters standalone.** A standalone operator's first instinct ('sqloutbox status') has no answer. They must either parse daemon logs, run an offline integrity scan and mentally map 'pending count' to 'delivery health', or open the SQLite files manually. autopulse has Discord '!status'/'!dsync' style commands wired into its own bot for this; the library itself ships no equivalent live-status surface, which a third party depending on it as a black-box service will expect.


**Evidence.** src/sqloutbox/cli.py:579-618 (subparsers: init, runservice, verify only); src/sqloutbox/cli.py:459-567 (verify is offline, opens own Outbox instances); src/sqloutbox/_runner.py:583-589 (SIGUSR1 only triggers verify, output to daemon log not caller); no 'status'/'inspect' command anywhere


**Recommendation.** Add a 'sqloutbox status --config|--db-dir' subcommand that reports, per target/table: pending count, oldest-unsynced age, sync_log size, and last-synced timestamp — read-only against the .db files (WAL-safe alongside a live daemon). Optionally have the running daemon expose this on SIGUSR2 or a unix socket. Document clearly that 'verify' is an integrity check, NOT a delivery-health check.


---


### MINOR-11 · `packaging-oss` · (confidence: high)

**Scenario.** A consumer pins `sqloutbox` and wants to know what changed between releases, or whether upgrading 0.3.x → 0.4.x is safe — the normal due-diligence for taking on an OSS dependency.


**Current behavior.** There is no CHANGELOG, no git tags, and no SemVer policy statement anywhere. `git tag` returns empty; releases are only discernible from commit subjects (e.g. 'feat: ... (v0.4.0)', 'feat: sqloutbox v0.2.0'). `[project.urls]` has Homepage/Repository/Issues but NO `Changelog` URL. The version is `0.4.1`; the spec in docs/specs proposes a drain BEHAVIOR change (head-of-line hold) that would alter delivery semantics — a consumer has no documented signal whether that is breaking or how it will be versioned.


**Risk.** No additive-vs-breaking change story. Consumers cannot assess upgrade risk; tooling (Dependabot/Renovate changelog surfacing) finds nothing; the pending semantics change has no migration note. For a 'durable' data-path library, opaque release history is a trust blocker.


**Why it matters standalone.** Third parties depend on changelogs + tags to pin and upgrade safely; without them every upgrade is a blind diff of source. A pre-1.0 library that is about to change delivery ordering MUST tell consumers via a versioned CHANGELOG and a stated SemVer (or ZeroVer) policy.


**Evidence.** `git tag` empty; `git log` shows version only in commit subjects (51771a4, 8117b05 '(v0.4.0)', 6d78a8d 'v0.2.0'); pyproject.toml:38-41 (urls lack a Changelog entry); no CHANGELOG.md in repo root (ls of repo root shows none); docs/specs/2026-06-11-durable-ordered-retry-and-health-signal.md is an unreleased behavior change with no version/migration mapping.


**Recommendation.** Add CHANGELOG.md (Keep a Changelog format), create annotated git tags matching each version, add `Changelog = "https://github.com/.../blob/main/CHANGELOG.md"` to `[project.urls]`, and state a versioning policy in README (e.g. 'pre-1.0: minor bumps may change behavior; the pending ordered-retry change ships as 0.5.0 and alters drain semantics').


---


### MINOR-12 · `packaging-oss` · (confidence: high)

**Scenario.** A consumer (or contributor) wants to reproduce the project's quality gates — lint, type-check, run the test suite from the published artifact — as part of vetting or vendoring the dependency.


**Current behavior.** No CI config exists (`.github/workflows/` absent). No ruff or mypy configuration despite CLAUDE.md asserting mypy-strict/ruff style; pyproject has only `[tool.pytest.ini_options]`. The sdist ships ONLY `src/`, README, LICENSE, .gitignore, pyproject — tests/ is NOT included (verified by `tar tzf`), so a downstream packager building from sdist cannot run the test suite. There is no CONTRIBUTING.md, no CODE_OF_CONDUCT.md, no SECURITY.md / security disclosure policy, and no examples/ directory.


**Risk.** Reduced trust and contributability for a professionally-maintained OSS package: no automated proof that the data-path code passes on the supported 3.10-3.13 matrix; downstream distro/conda packagers cannot validate the sdist; security researchers have no disclosure channel; new contributors have no on-ramp.


**Why it matters standalone.** An independent maintainer's package is judged on CI badges, a vetting-friendly sdist, and a security policy. A finance-adjacent durability library with zero CI and no SECURITY.md is hard for risk-averse third parties to adopt; distro packagers specifically need tests in the sdist to run during build.


**Evidence.** `ls .github/workflows` -> absent; pyproject.toml has no `[tool.ruff]`/`[tool.mypy]` (grep found only hatch + pytest tables); sdist `tar tzf sqloutbox-0.4.1.tar.gz` contains no `tests/`; repo root `ls` shows no CHANGELOG/CONTRIBUTING/CODE_OF_CONDUCT/SECURITY files; no examples/ dir (only docs/specs).


**Recommendation.** Add a GitHub Actions matrix (3.10-3.13, run pytest with and without the `toml` extra to catch the 3.10 gap), add `[tool.ruff]` and `[tool.mypy]` (strict) config to pyproject, include `tests/` in the sdist (`[tool.hatch.build.targets.sdist] include = ["src/", "tests/", "README.md", "LICENSE"]`), and add SECURITY.md + CONTRIBUTING.md + a small examples/ writer implementation.


---


### MINOR-13 · `poison-data` · (confidence: high)

**Scenario.** A payload contains bytes that are not valid UTF-8. This can happen at WRITE time (enqueue is given non-UTF-8 bytes) or be present on disk via external corruption / a foreign writer. Note enqueue stores via payload.decode() so a non-UTF-8 write is dropped at enqueue (_outbox.py:103) — but a row already on disk with non-UTF-8 TEXT, or any path that bypassed enqueue, reaches the drain.


**Current behavior.** fetch_unsynced reads the stored TEXT and re-encodes: `payload=r[2].encode()` (_outbox.py:217). In the drain, `row.payload.decode()` (sync.py:609) and then json.loads. A row whose stored text cannot round-trip, or whose bytes aren't decodable, raises UnicodeDecodeError at sync.py:609 — same unguarded path as the JSON case. Same fatal blast radius.


**Risk.** Same as the JSON-poison finding: whole-service permanent silent stall. Distinct trigger (encoding vs. JSON syntax) but identical un-guarded code path, so worth calling out as a second way to reach the same kill.


**Why it matters standalone.** _models.py:24 advertises payload may be 'msgpack, protobuf' (inherently binary, frequently non-UTF-8), and _outbox.py:80-87 only buries the 'Non-UTF-8 bytes are not supported' caveat in the enqueue docstring while the column is TEXT. A third party following the documented 'protobuf' example will produce rows that either fail at enqueue (silent drop, finding 6) or detonate the drain. The library's stated capability and its actual behavior diverge.


**Evidence.** _outbox.py:217 (payload=r[2].encode()); _outbox.py:103 (enqueue stores payload.decode() — TEXT column, so binary safety depends on the producer); sync.py:609 (row.payload.decode() then json.loads, unguarded)


**Recommendation.** Either enforce a BLOB payload column and stop calling .decode()/.encode() (true binary safety), or prominently document that payload MUST be UTF-8-decodable JSON for the bundled drain and reject non-UTF-8 at the API boundary with a clear error rather than a silent enqueue drop. Whatever the choice, guard sync.py:609 so a bad row is quarantined, not fatal.


---


### MINOR-14 · `poison-data` · (confidence: high)

**Scenario.** A producer calls enqueue/enqueue_batch with an empty payload, an empty/whitespace tag, or via the public low-level Outbox API. Specifically: empty payload (b'' or '') or a tag that is the empty string.


**Current behavior.** The schema declares `tag TEXT NOT NULL` and `payload TEXT NOT NULL` (_schema.py:18-19), but NOT NULL does not reject the empty string — `tag=''` and `payload=''` are accepted by enqueue (_outbox.py:99-104) and stored. At drain time, an empty payload string fails json.loads('') with JSONDecodeError → the fatal unguarded path (finding 1). An empty tag becomes empty SQL sent to write_batch → a per-row failure (finding 3, retried forever). The middleware path (middleware.py:77) always JSON-encodes args so it's safe, but raw Outbox.enqueue (public, exported) has no such guarantee.


**Risk.** Empty payload triggers the whole-service-fatal JSON-decode path; empty tag triggers the forever-retried per-row failure. Both are reachable through the public Outbox.enqueue API with no validation.


**Why it matters standalone.** Outbox and SQLMiddleware are both public exports, so OSS consumers may use the low-level Outbox.enqueue directly (it's documented in _models/_outbox docstrings). Nothing validates tag/payload non-emptiness at the boundary, so a producer bug (e.g. forgetting to set the SQL string) silently lands a poison row that later stalls or kills the drain far from the call site — hard to trace back.


**Evidence.** _schema.py:18-19 (NOT NULL, but empty string is allowed); _outbox.py:99-104 (enqueue inserts tag/payload with no non-empty validation); middleware.py:77 (only the middleware path guarantees JSON); sync.py:609 (json.loads('') raises)


**Recommendation.** Validate at the enqueue boundary: reject empty tag and empty payload with a clear ValueError (fail fast at the producer, not silently at the drain). Document that tag must be non-empty and payload must be non-empty (and JSON for the bundled drain).


---


### MINOR-15 · `resource-limits` · (confidence: high)

**Scenario.** Backlog (after an outage clears, or under sustained high produce rate) far exceeds batch_size — e.g. 1,000,000 pending rows in one table.


**Current behavior.** fetch_unsynced() is called by the drain with no argument, so it uses `limit or self.batch_size` (_outbox.py:202-215) — bounded by batch_size (TargetConfig/OutboxConfig default 500; Outbox class default 50). So memory per fetch is bounded — GOOD, no full-backlog load. BUT the worker drains at most batch_size rows per *ready table* per cycle, and cycles are gated by `await asyncio.sleep(flush_interval)` (default 1.0s) at sync.py:529. Drain throughput is therefore ceilinged at ~batch_size/flush_interval = 500 rows/sec/table by default. Also note pending_count() runs an unbounded `COUNT(*) WHERE synced=0` on every table every cycle (sync.py:566 → _outbox.py:348-356) — on a multi-million-row backlog that COUNT scans the index every second.


**Risk.** Slow drain / permanent backlog if produce rate exceeds ~500 rows/s/table; the queue can never catch up and grows forever even though the remote is healthy. The per-cycle COUNT(*) also becomes a measurable CPU/IO cost at large depth. There is no way to raise per-cycle throughput except by tuning batch_size/flush_interval, and there is no 'drain until empty' fast path.


**Why it matters standalone.** The memory bound is fine, but an OSS user is not told that steady-state throughput is capped at batch_size/flush_interval per table, nor that recovery from a large backlog is rate-limited. The Outbox(batch_size=50) default vs OutboxConfig(batch_size=500) default mismatch will surprise anyone constructing Outbox directly (e.g. via shared_outbox without passing batch_size) — they silently get a 10x lower per-cycle cap than the service config implies.


**Evidence.** src/sqloutbox/_outbox.py:202-207 (fetch_unsynced cap = batch_size); src/sqloutbox/_outbox.py:20 (Outbox default batch_size=50) vs config.py:151/186 (config default 500) — inconsistent default; src/sqloutbox/sync.py:529 (one sleep gate per cycle); src/sqloutbox/sync.py:566 + _outbox.py:348-356 (unbounded COUNT(*) every table every cycle)


**Recommendation.** Document the throughput ceiling (batch_size/flush_interval) and the recovery characteristics. Align the Outbox default batch_size with the config default (or document the divergence). Consider a 'while pending >= batch_size: drain again without sleeping' inner loop so a backlog drains at writer speed instead of 1 batch/sec, and cache/skip the COUNT(*) when a table is known non-empty.


---


### MINOR-16 · `resource-limits` · (confidence: high)

**Scenario.** Cleanup/prune is gated on the drain loop's cycle counter; the loop is busy, sleeping, blocked, or erroring early.


**Current behavior.** _prune_all() is only invoked when `self._cycle_count % cleanup_every == 0` at the END of the loop body (sync.py:627-628). _cycle_count is only incremented on NON-verify cycles (sync.py:548) — a verify cycle `continue`s before incrementing (sync.py:546). Retention is purely count-based (every Nth cycle, default 500), NOT time-based: prune_sync_log deletes sync_log rows older than retain_log_days (_outbox.py:330-346). Therefore prune frequency depends entirely on how often the loop completes a full iteration. If flush_interval is large (e.g. 15s) and few cycles complete, sync_log pruning lags. If a write_batch hangs (writer.write_batch is awaited with no timeout, sync.py:650), the cycle never completes and NEITHER drain NOR prune advances — the whole loop is stuck on one target. There is no separate timer for cleanup.


**Risk.** (a) sync_log retention is effectively 'every cleanup_every*flush_interval seconds' not 'daily' — with default 500 cycles * 1s = ~8min it's fine, but a user setting flush_interval=60 turns it into ~8h between prunes, and retain_log_days governs deletion age, so the log can exceed the intended window. (b) A single slow/hung target with no write timeout stalls the entire shared loop (all targets, all tables) — drain AND prune both stop. (c) cleanup runs in-band with drain, so a large prune DELETE blocks the next drain cycle.


**Why it matters standalone.** autopulse uses defaults so prune runs every few minutes and its writer presumably has its own HTTP timeout. An OSS user who (1) tunes flush_interval up, or (2) supplies an OutboxWriter without an internal timeout, gets either lagging retention or a fully stalled single-threaded drain across ALL targets from one slow remote. The library never imposes a write timeout and runs all targets serially in one loop, so one bad target = total stall — this coupling is undocumented.


**Evidence.** src/sqloutbox/sync.py:627-628 (prune gated on cycle_count % cleanup_every); src/sqloutbox/sync.py:548 vs 546 (count not incremented on verify cycles); src/sqloutbox/sync.py:650 (await writer.write_batch with no asyncio.timeout — a hung writer blocks the loop); src/sqloutbox/_outbox.py:330-346 (prune is age-based delete, frequency is cycle-based)


**Recommendation.** Document that all targets share one serial drain loop and that the supplied OutboxWriter MUST enforce its own timeout (or wrap write_batch in `asyncio.wait_for`/`asyncio.timeout` with a configurable bound). Decouple prune from the drain cycle counter (use a wall-clock timer) so retention is honored regardless of loop activity, and consider per-target concurrency so one slow remote cannot starve the others.


---


### MINOR-17 · `schema-versioning` · (confidence: high)

**Scenario.** An OSS user points db_dir at a directory that already contains a SQLite file named <table>.db that holds an unrelated table also named `outbox_queue` (e.g. they reused a directory, or a name collision with their own schema), OR a sqloutbox DB from a FUTURE version that renamed/retyped a core column. Outbox(db_path=...) is constructed.


**Current behavior.** open_write_conn (_schema.py:77-103) runs `CREATE TABLE IF NOT EXISTS outbox_queue (...)`. Because the table name already exists, CREATE is a SILENT no-op — SQLite does NOT compare column definitions. The pre-existing foreign columns survive. The idempotent `ALTER TABLE ... ADD COLUMN source` (line 96) is wrapped in try/except and may add `source`, but the other required columns (seq/created_at/namespace/tag/payload/prev_seq/synced) are never created. The first enqueue() INSERT then references columns that do not exist.


**Risk.** enqueue() INSERT raises `OperationalError: table outbox_queue has no column named created_at`. Because enqueue() catches all exceptions and only logs a WARNING then returns None (_outbox.py:109-118), EVERY event is silently dropped forever — the producer looks healthy (no crash, ~150µs returns) but persists nothing. Total, silent data loss with no surfaced error.


**Why it matters standalone.** autopulse always uses dedicated <table>.db files it created itself, so it never hits a foreign schema. A third-party who sets db_dir to a shared/reused path, or who upgrades from a hypothetical future version that altered a column, gets silent permanent event loss with zero diagnostics. There is no schema fingerprint to detect the mismatch.


**Evidence.** _schema.py:89 `conn.execute(_CREATE_QUEUE)` is CREATE TABLE IF NOT EXISTS (lines 12-13); _outbox.py:99-104 INSERT names columns; _outbox.py:109-118 swallows the failure as WARNING+return None. Reproduced: foreign `outbox_queue(id,foo)` table → CREATE IF NOT EXISTS no-op → enqueue INSERT raised `OperationalError: table outbox_queue has no column named created_at`.


**Recommendation.** After open, validate the actual schema (e.g. `PRAGMA table_info(outbox_queue)` must contain the expected columns) and raise a clear error on mismatch instead of proceeding. Stamp `PRAGMA application_id` + `PRAGMA user_version` on creation and verify on open so foreign/incompatible files are rejected loudly rather than silently mis-used.


---


### MINOR-18 · `schema-versioning` · (confidence: high)

**Scenario.** Any consumer runs `sqloutbox verify` (CLI) or `outbox.verify_full()` / `request_verify()` on existing .db files — documented as 'All checks are read-only — they never modify the database' (_verify.py:18, _verify.py:67).


**Current behavior.** verify constructs Outbox(...) (cli.py:482, _verify.py path), whose __init__ calls open_write_conn (_outbox.py:64). open_write_conn opens a WRITE connection, executes `PRAGMA journal_mode=WAL` (mutates the file format / creates -wal and -shm sidecars), runs the ADD COLUMN source migration, runs CREATE INDEX migrations, and commits (_schema.py:86-102). This is a write that alters journal mode and schema.


**Risk.** The 'read-only' verify is not read-only: it switches the DB to WAL, may add a `source` column, and creates indexes on a file the user only wanted to inspect. On a file from a forked-chain old DB it can crash (see prev_seq finding). On a read-only filesystem / file owned by another user it raises `OperationalError: attempt to write a readonly database` and verify fails even though the data is fine. Side effects on inspection violate the documented contract.


**Why it matters standalone.** autopulse runs verify on its own writable, current-schema DBs so the side effects are invisible. A third party auditing a production snapshot, a backup copy, or a file on read-only media expects a non-mutating scan (as documented) and instead gets WAL conversion, schema migration, or an outright write-error failure — surprising for a tool sold as an integrity inspector.


**Evidence.** _verify.py:18 and :67 explicitly promise read-only. But verify_outbox(outbox) requires a live Outbox whose __init__ already ran open_write_conn (_outbox.py:63-64); open_write_conn does PRAGMA journal_mode=WAL + synchronous=NORMAL + migrations + commit (_schema.py:87-102). cmd_verify constructs Outbox at cli.py:482 and 494.


**Recommendation.** Give verify a true read-only open (sqlite3 `file:...?mode=ro` URI, no PRAGMA WAL, no migrations) decoupled from the producer's open_write_conn, or at minimum update the docstrings to state that verify opens the DB for write and applies pending migrations + WAL conversion.


---


### MINOR-19 · `schema-versioning` · (confidence: high)

**Scenario.** On the REMOTE side, auto_schema=True (default) adds `outbox_seq INTEGER NOT NULL DEFAULT 0` to a remote table that ALREADY has many pre-existing rows that were written directly (not via the outbox), then creates the partial unique index `WHERE outbox_seq != 0`.


**Current behavior.** _add_outbox_seq (sync.py:319-340) ALTERs each remote table. All pre-existing rows get outbox_seq=0 (verified). The partial unique index excludes outbox_seq=0, so unlimited legacy rows coexist. The injected delivery path uses INSERT OR IGNORE with a real outbox_seq, deduped by that index (sync.py:145-157, verified idempotent). This part is correct. However: error classification in _add_outbox_seq relies on substring matching of the writer's error string — `if 'duplic' in err.lower() or 'already' in err.lower()` (sync.py:361) to decide the column already exists.


**Risk.** Because there is no `IF NOT EXISTS` for ADD COLUMN in SQLite and the code depends on the remote writer returning an error string containing 'duplic'/'already', a remote engine (Postgres, MySQL, libsql variant) whose duplicate-column error uses different wording (e.g. Postgres: 'column "outbox_seq" of relation ... already exists' contains 'already' — OK; but some engines say 'duplicate column name' OK; others may differ) will be MIS-classified as a real failure and logged as WARNING on every restart, OR a genuinely different ALTER failure could be mis-classified as benign. The contract that the remote is SQLite-like is implicit.


**Why it matters standalone.** autopulse's writers target libsql/Turso whose error strings happen to match. A third party with a different OutboxWriter backend (the whole point of the injected-writer abstraction) may see spurious WARNINGs each startup or, worse, a real schema error silently treated as 'already exists'. The benign-error detection is a fragile string heuristic across backends the library explicitly invites users to plug in.


**Evidence.** sync.py:359-370 — substring match on err string for 'duplic'/'already'; sync.py:332-335 emits bare `ALTER TABLE ... ADD COLUMN` (no IF NOT EXISTS). _drop_outbox_seq similarly substring-matches 'no such'/'does not exist' (sync.py:417). Verified existing rows get outbox_seq=0 and partial index permits them.


**Recommendation.** Document that auto_schema's ADD/DROP idempotency assumes SQLite/libsql error wording, and that other backends should set auto_schema=False and use schema_sql()/drop_schema_sql() under their own migration tool. Optionally let the OutboxWriter signal 'already exists' via a structured result field rather than English substring matching.


---


### MINOR-20 · `security` · (confidence: high)

**Scenario.** The service runs with `DOPPLER_TOKEN` set (a documented, first-class credential path). The Doppler API response — or a man-in-the-middle / compromised Doppler account / typo-squatted response — returns secret KEYs that the library does not expect, e.g. `PATH`, `LD_PRELOAD`, `PYTHONPATH`, `BASH_ENV`.


**Current behavior.** `_load_doppler` returns the raw `{KEY: VALUE}` dict and `_prepare_env` writes EVERY key into `os.environ` via `if k not in os.environ: os.environ[k] = v` with no key allow-listing or name validation (`_runner.py:221-229`). `os.environ.setdefault` semantics protect already-set vars, but any env var NOT already set (commonly `LD_PRELOAD`, `PYTHONPATH`, `BASH_ENV` in a minimal systemd unit) can be injected by whatever the Doppler endpoint returns. TLS is correctly enforced (hardcoded https URL, default CA verification), so the realistic vector is a compromised/misconfigured Doppler config rather than passive MITM.


**Risk.** Environment-variable injection escalating to code execution. If the secret store returns `LD_PRELOAD` / `PYTHONPATH` / `BASH_ENV`, the next subprocess or import can load attacker-controlled code. Even benign-looking injected vars (`DB_URL`-style) can redirect the drain to a different remote database, exfiltrating queued data.


**Why it matters standalone.** This trusts the entire Doppler secret namespace to be benign env-var names. autopulse owns its Doppler project so it is safe; an OSS adopter pointing at a shared or less-trusted secret config inherits an env-injection surface. The README presents Doppler as a clean, recommended credential source without noting that returned KEYS become process env vars verbatim.


**Evidence.** src/sqloutbox/_runner.py:132-162 (_load_doppler returns all keys), :221-229 (loop writes every key into os.environ unfiltered). URL is HTTPS-hardcoded (:146-149) and token masked in logs (:165-169), so transport is fine; the gap is unbounded key injection from the response body.


**Recommendation.** Only import Doppler keys that are actually referenced by `${VAR}` in the loaded TOML (the loader already knows them via the `_ENV_RE` matches), or at minimum skip/log dangerous well-known names (LD_PRELOAD, PYTHONPATH, BASH_ENV, PATH). Document that all returned secret keys are injected into the process environment.


---


### MINOR-21 · `security` · (confidence: high)

**Scenario.** A table/namespace name comes from a not-fully-trusted TOML config and contains path separators or traversal sequences, e.g. `tables = ["../../etc/cron.d/evil"]` or `tables = ["/abs/path"]`, or a `db_dir`/`app.NAME.db_dir` pointing outside the intended tree.


**Current behavior.** Namespace/table is used directly to build the SQLite filename: `db_dir / f"{table}.db"` (sync.py:233, middleware.py:66, cli.py:480/494). A table name like `../../foo` resolves the file outside `db_dir`; an absolute-path table name would (via pathlib `/`) reset to that absolute path. `db_dir` from TOML is joined to the config file's parent if relative (_runner.py:452-454) but an absolute `db_dir` is honored as-is, and `db_dir.mkdir(parents=True, exist_ok=True)` will create arbitrary directories. There is no validation that table names are bare identifiers or that the resolved path stays within db_dir. Note the same `table` string is ALSO used as a SQL identifier in auto-schema DDL (`ALTER TABLE {table} ...`, sync.py:332-338), so a malicious table name is a DDL-injection vector too.


**Risk.** Path traversal / arbitrary file (and directory) creation under the service's privileges, and DDL injection via the unquoted `{table}` in auto-schema statements. Limited blast radius because the writes are SQLite `.db` files (not arbitrary content) and the table name must come through config — but combined with auto_schema, a crafted table name can emit attacker-controlled DDL to the remote DB.


**Why it matters standalone.** autopulse hardcodes its table names, so traversal never occurs. An OSS adopter that derives table/namespace names from any external source, or accepts third-party config, has an unvalidated filename-and-DDL-identifier surface. Nothing documents that table names must be safe bare identifiers.


**Evidence.** src/sqloutbox/sync.py:230-238 (db_path=db_dir / f"{table}.db"), :332-340 and config.py:236-245 (f-string `{table}` into ALTER/CREATE INDEX DDL — no quoting/validation); _runner.py:452-454 (relative db_dir joined, absolute honored); middleware.py:64-66 (db_dir.mkdir + path build).


**Recommendation.** Validate that table/namespace names match a strict identifier regex (e.g. `^[A-Za-z_][A-Za-z0-9_]*$`) at config-load and Outbox construction time, and reject path separators/`..`. After building each db_path, assert it is contained within the resolved db_dir. Document the table-name constraint. This also closes the DDL-identifier vector.


---


### MINOR-22 · `writer-protocol` · (confidence: high)

**Scenario.** Per-batch vs per-row atomicity: a writer delivers a batch of N statements to the remote DB inside a single transaction (e.g. Turso pipeline with one COMMIT), but the destination commits rows and the worker process is killed (OOM, SIGKILL, deploy) AFTER the remote commit but BEFORE write_batch returns / before mark_synced+delete_synced run.


**Current behavior.** Confirmation (mark_synced + delete_synced) happens in _flush_to_target AFTER write_batch returns (sync.py:677-678). There is no persistence of 'in-flight' state. On restart, the rows are still synced=0 in the local outbox, so they are re-fetched and re-sent. Delivery is therefore strictly at-least-once. Correctness depends ENTIRELY on the remote write being idempotent — which sqloutbox only provides for inject_outbox_seq tables via INSERT OR IGNORE on outbox_seq. For inject_outbox_seq=False targets, or for UPDATE/DELETE statements, or for INSERTs the inject_outbox_seq transform fails to rewrite, a crash in this window causes the statement to be re-applied (duplicate INSERT on tables without a natural unique constraint; non-idempotent UPDATE re-applied; etc.).


**Risk.** Silent duplication / double-apply on crash for any non-idempotent statement path. The at-least-once guarantee and the idempotency precondition it places on the writer/destination are not documented as a hard requirement an OSS consumer must satisfy.


**Why it matters standalone.** autopulse routes only append-only analytics INSERTs and tolerates dupes / uses outbox_seq, so this is invisible there. A general OSS user delivering UPDATEs, or INSERTs to a table without outbox_seq, or using a writer with internal batch-transaction semantics, gets at-least-once with NO framework-provided dedup and no warning. The contract 'your remote write MUST be idempotent because delivery is at-least-once' is the core safety assumption and is not stated as such.


**Evidence.** Confirmation strictly follows the network write with no intervening durability: src/sqloutbox/sync.py:650 (write_batch) → :677-678 (mark_synced/delete_synced). inject_outbox_seq only rewrites INSERT/UPDATE and falls through unchanged for other statements (src/sqloutbox/sync.py:145-175, esp. the 'Unknown statement type' branch :174-175). inject is conditional on should_inject_seq (sync.py:610), and README explicitly supports inject_outbox_seq=False targets (README.md:245-250).


**Recommendation.** Document at-least-once delivery and the idempotency requirement as a first-class contract in README and the OutboxWriter docstring. Explicitly warn that inject_outbox_seq=False tables and non-INSERT statements receive NO framework idempotency and the writer/destination must guarantee it themselves (e.g. unique constraints, upserts).


---


### MINOR-23 · `writer-protocol` · (confidence: high)

**Scenario.** The drain task is cancelled (asyncio.CancelledError) mid write_batch — e.g. graceful shutdown cancels the run() task, the event loop is closing, or a supervising framework cancels the coroutine — while the await on writer.write_batch (or a downstream await mark_synced/delete_synced) is suspended.


**Current behavior.** The write_batch await is wrapped in `try/except Exception` (sync.py:649-658). asyncio.CancelledError is NOT a subclass of Exception in Python 3.10+, so it propagates correctly (good — it is not swallowed). BUT: if cancellation arrives AFTER write_batch returns successfully and the remote committed, but BEFORE or DURING mark_synced/delete_synced (the two separate awaited asyncio.to_thread calls at sync.py:677-678), the rows are delivered remotely yet never marked synced locally → redelivered on next start (at-least-once again). More subtly, mark_synced and delete_synced are TWO separate awaits per table: cancellation between them leaves rows synced=1 but not deleted — harmless on restart (delete_synced re-runs and will delete synced rows), but if cancellation lands mid the per-table loop (sync.py:675-679), some tables in the batch are confirmed and others are not, with no transactional grouping.


**Risk.** No data loss (the design fails safe toward redelivery), but the at-least-once duplication window is widened by cancellation, and there is no documentation that write_batch may be cancelled at any await point and that the destination commit may have already happened. A writer that holds external resources (open transaction, connection) must handle CancelledError to avoid leaks.


**Why it matters standalone.** An OSS consumer running under a graceful-shutdown supervisor (uvicorn-style lifespan, Kubernetes SIGTERM) needs to know write_batch can be cancelled mid-flight, that the destination may have committed, and that their writer must clean up its own in-flight transaction/connection on CancelledError. None of this is documented.


**Evidence.** Bare-Exception guard that correctly excludes CancelledError: src/sqloutbox/sync.py:649-658. Two-phase confirmation across separate awaits with no atomic grouping: src/sqloutbox/sync.py:677-678; per-table loop at :675. No shielding (asyncio.shield) around the confirmation phase.


**Recommendation.** Document the cancellation contract for write_batch (may be cancelled at any await; destination commit may have already occurred; redelivery will follow). Recommend writers treat write_batch as cancellation-safe. Optionally asyncio.shield the mark_synced/delete_synced confirmation phase so a confirmed remote write is recorded locally even under shutdown.


---


### MINOR-24 · `writer-protocol` · (confidence: high)

**Scenario.** Distinguishing the three writer-failure shapes — raising an exception vs returning {"ok": False} for some rows vs returning a malformed/short list — because they have DIFFERENT retry granularity and that difference is not documented.


**Current behavior.** Raise → the whole batch (all tables, all rows for that target in this cycle) is logged at WARNING and retried next cycle; nothing is confirmed (sync.py:649-658). Return ok=False per row → ONLY that row is left pending; every other row in the same batch with ok=True is marked synced and DELETED, even rows enqueued AFTER the failed one (no head-of-line hold) (sync.py:663-679). Return short/long list or missing 'ok' → undefined/crash per findings 1-2. So an implementer's choice between 'raise on first error' and 'return ok=False' silently changes whether delivery is whole-batch-atomic-retry or per-row-best-effort.


**Risk.** Out-of-order partial delivery within a batch: row seq=105 (ok=True) is delivered and deleted while row seq=103 (ok=False) stays pending and retries later, so the remote sees 105 before 103. For consumers that assume the chain ordering guarantee extends to remote apply order, this violates their expectation. The README advertises 'delivers them in strict order, with singly-linked chain integrity' (README.md:6-8) which an OSS user reasonably reads as ordered remote application, but per-row ok=False confirmation breaks intra-batch ordering on partial failure.


**Why it matters standalone.** autopulse delivers independent analytics rows where intra-batch order on partial failure is irrelevant, so the gap is invisible. A general OSS user who needs the advertised strict ordering to hold end-to-end (the whole selling point of a prev_seq chain) will be surprised that a single mid-batch ok=False lets later rows overtake the failed one at the destination, with the failed row redelivered out of order. The implementer-facing choice (raise = ordered atomic retry; ok=False = unordered best-effort) is a sharp, undocumented fork.


**Evidence.** Whole-batch retry on raise: src/sqloutbox/sync.py:649-658. Per-row independent confirm/delete with no head-of-line hold: src/sqloutbox/sync.py:661-679 (failed rows only increment failed_count and log; ok rows are appended to confirmed_by_table and deleted). README ordering claim: README.md:6-8 and :253-260 ('delivers them in strict order'). _worker.py docstring documents []→retry-all and raise→retry-all (_worker.py:60-61) but the OutboxSyncService path's per-row ok=False semantics are NOT documented anywhere.


**Recommendation.** Document the three failure shapes and their exact semantics in the OutboxWriter docstring: raise = whole-batch retry preserving order; per-row ok=False = that row retries while later ok rows are delivered and deleted (ordering NOT preserved across a partial failure). Clarify in the README that the 'strict order' guarantee is the LOCAL chain/fetch order and that remote apply order on partial failure depends on the writer's failure-reporting style. If strict remote ordering is intended, implement head-of-line hold (stop confirming at the first ok=False in seq order).


---


## NIT


### NIT-1 · `config-api` · (confidence: high)

**Scenario.** An OSS consumer reads the scaffolded Python config (sqloutbox init) and the README to learn the default flush_interval, and compares it to the TOML example.


**Current behavior.** Defaults are inconsistent across the documentation surface. OutboxConfig default flush_interval=1.0 (config.py:187, README:330) but the runner TOML docstring example shows flush_interval=15.0 (_runner.py:31) and the autopulse-derived comments imply 15s. The init scaffold sets retain_log_days=7 (cli.py:144) while OutboxConfig default and README say 30 (config.py:192, README:335). Outbox's own docstring says retain_log_days 'Default: 7 days' (_outbox.py:39) but the code default is 30 (_outbox.py:19). These are doc/contract drifts, not logic bugs.


**Risk.** A consumer copying example values gets surprising behavior: a 1.0s default flush_interval means the drain loop scans every second (fine), but someone expecting the documented '15s recommended' cadence from the runner example, or the scaffold's 7-day retention vs README's 30-day, ends up with inconsistent retention/cadence they didn't intend. The Outbox docstring directly contradicts its own code.


**Why it matters standalone.** Documentation/default consistency is part of API ergonomics for newcomers who learn the library by reading examples. The Outbox docstring stating 7 while the constant is 30 is a flat contradiction an OSS user will trip on. autopulse hardcodes its own values so never reads the defaults.


**Evidence.** src/sqloutbox/config.py:187 (flush_interval=1.0), 192 (retain_log_days=30); src/sqloutbox/_runner.py:31 (example flush_interval=15.0); src/sqloutbox/cli.py:144 (scaffold retain_log_days=7); src/sqloutbox/_outbox.py:19 (DEFAULT_RETAIN_LOG_DAYS=30) vs docstring 38-40 ('Default: 7 days').


**Recommendation.** Reconcile defaults to one source of truth: fix the Outbox docstring (7 -> 30), align the scaffold (cli.py retain_log_days=7) and runner example (flush_interval=15.0) with the documented OutboxConfig defaults, or explicitly annotate the examples as 'tuned for production, differs from default'.


---


## INFO (contracts to document)


### INFO-1 · `completeness` · (confidence: high)

**Scenario.** A consumer wants to reason about the README's headline guarantee — 'drains them to N remote databases in strict order' (__init__.py:9-10) and 'No cross-namespace ordering — each namespace is independent' (README Limitations) — for an event stream where global ordering across event types matters, and routes multiple related tables/namespaces through one target.


**Current behavior.** Ordering is strict only WITHIN a namespace (one .db file / one table). The drain processes tables independently with per-table flush triggers (threshold/max_wait, sync.py:565-615) and batches them into one write_batch, but each table is its own chain. Two namespaces in the same shared file get independent prev_seq chains (confirmed: ns_a and ns_b interleave and each verifies independently). Within a single write_batch, per-row independent confirmation (sync.py:663-679, the confirmed finding) means even intra-namespace 'strict order' is not preserved on partial failure.


**Risk.** A user relying on 'strict order' for cross-namespace or cross-table sequencing gets none — event A (table X) and event B (table Y) can land at the remote in either order or different cycles. Combined with per-row partial confirmation, even same-namespace successors can be delivered while a predecessor failed.


**Why it matters standalone.** autopulse's analytics tables are mutually independent so cross-namespace ordering is irrelevant to it. A general OSS adopter reading 'strict order' on the package front page may assume a global total order that the design explicitly does not provide, and the two contradictory statements ('strict order' vs 'no cross-namespace ordering') are far apart in the docs.


**Evidence.** /Users/sandeep.yadav/tmp/sqloutbox/src/sqloutbox/__init__.py:9-10 ('strict order'); README Limitations ('No cross-namespace ordering'); sync.py:565-615 (per-table independent flush); per-namespace chain confirmed by interleave test


**Recommendation.** State the ordering contract precisely and in one place: strict FIFO is per-namespace only, there is no ordering across namespaces/tables/targets, and (until head-of-line hold is implemented) even within a namespace a failed row does not block its successors in the same batch.


---


### INFO-2 · `concurrency` · (confidence: high)

**Scenario.** An OSS user reads the README Architecture section ('Run them in separate processes or in the same process', README.md:236) plus the one-line Limitation ('Single process only — one write connection per SQLite file', README.md:481) and tries to reason about what concurrency is safe, then threads their producer (calls enqueue from multiple threads on one Outbox instance).


**Current behavior.** The actual code-enforced contract is narrower than documented. open_write_conn passes check_same_thread=False (_schema.py:86) with a comment asserting enqueue 'is never called concurrently' (_schema.py:82-83) — but nothing ENFORCES single-threaded enqueue. There is a single persistent _write_conn shared by all enqueue calls (_outbox.py:64). Two threads calling enqueue interleave `BEGIN IMMEDIATE` on the SAME connection (_outbox.py:92/:150), which sqlite3 rejects ('cannot start a transaction within a transaction') or corrupts prev_seq linkage. Separately, the shared_outbox registry only dedupes within one process (_registry.py module-global), and WAL readers ARE genuinely safe concurrently with the single writer — but neither fact is documented.


**Risk.** Users cannot safely reason about their deployment. check_same_thread=False actively disables the thread guardrail and thus invites multi-threaded enqueue (a common pattern), while the design silently assumes single-threaded enqueue. A threaded producer gets interleaved transactions → exceptions or lost chain linkage, not a clean documented error.


**Why it matters standalone.** autopulse calls enqueue from a single hot-path context and knows the internal assumptions, so it is fine. An arbitrary third party has no way to know that (a) enqueue must be single-threaded despite check_same_thread=False, (b) only one drain process is allowed, (c) concurrent reads are safe. The gap between the disabled thread-check and the unenforced single-thread assumption is a trap.


**Evidence.** _schema.py:82-86 (check_same_thread=False with an unenforced 'never called concurrently' assumption); _outbox.py:64 (single persistent _write_conn shared across all enqueue calls); _outbox.py:92,150 (BEGIN IMMEDIATE on that shared conn); _registry.py:34-35 (per-process registry); README.md:236 and :481 (the only concurrency docs, both incomplete/ambiguous).


**Recommendation.** Document the precise concurrency contract: one writer connection per file (enforced by single drain + single-threaded enqueue), multiple concurrent readers OK under WAL, registry is per-process. Either keep check_same_thread=True on the write conn to enforce single-threaded enqueue, or add a threading.Lock around enqueue's BEGIN IMMEDIATE block so multi-threaded enqueue is actually safe rather than only superficially allowed.


---


### INFO-3 · `config-api` · (confidence: high)

**Scenario.** An app uses SQLMiddleware with OutboxConfig(db_dir=..., retain_log_days=7, cleanup_every=100, batch_size=300) expecting those retention/cleanup settings to take effect for the producer-created Outbox files.


**Current behavior.** SQLMiddleware._outbox() forwards ONLY batch_size to shared_outbox(); it never forwards retain_log_days or cleanup_every (middleware.py:65-69 — grep confirms batch_size is the sole kwarg). shared_outbox then constructs Outbox with its own hardcoded defaults: DEFAULT_RETAIN_LOG_DAYS=30 and DEFAULT_CLEANUP_EVERY=500 (_outbox.py:19-21,50-57). So a producer that set retain_log_days=7 silently gets 30, and cleanup_every is ignored.


**Risk.** Configured retention/cleanup is silently dropped on the producer side. The audit-trail (outbox_sync_log) is kept 4x+ longer than requested, and any consumer that relies on the Outbox-object cleanup_every gets the wrong cadence. Disk usage and audit-retention compliance diverge silently from the declared config.


**Why it matters standalone.** Producer-side retention is part of the documented OutboxConfig contract (README lines 334-335). An OSS user reasonably expects setting retain_log_days on the config object that the middleware holds to actually apply; it silently does not. autopulse drains via the sync service (which DOES read retain per-target), so it never noticed the producer path drops these fields.


**Evidence.** src/sqloutbox/middleware.py:57-69 (only batch_size passed); src/sqloutbox/_outbox.py:19-21 (DEFAULT_RETAIN_LOG_DAYS=30, DEFAULT_CLEANUP_EVERY=500), 50-62; _registry.py:57-69 (**kwargs forwarded to Outbox only on first create).


**Recommendation.** In SQLMiddleware._outbox(), forward retain_log_days=self._config.retain_log_days and cleanup_every=self._config.cleanup_every alongside batch_size. Or document explicitly that producer-side Outbox uses library defaults and only the sync service honors retention.


---


### INFO-4 · `config-api` · (confidence: high)

**Scenario.** Two callers fetch the same outbox with different tuning: ob1 = shared_outbox(p, 'orders', batch_size=500); later ob2 = shared_outbox(p, 'orders', batch_size=50, retain_log_days=7). This happens when middleware (batch_size from config) and the CLI verify path / sync path both touch the same file, or two SQLMiddleware subclasses with different OutboxConfigs write the same table.


**Current behavior.** Cache key is ONLY (str(db_path.resolve()), namespace) — kwargs are NOT part of the key (_registry.py:61). On a cache HIT the kwargs of the second caller are silently DISCARDED; ob2 IS ob1 with the FIRST caller's tuning. The docstring even states 'Ignored on cache hits' (_registry.py:59-60). Whichever caller wins the race at process startup wins the configuration for all subsequent callers.


**Risk.** Nondeterministic, startup-order-dependent configuration. Two components that legitimately disagree on batch_size/retention for the same file get whichever one constructed the Outbox first — silently. There is no warning, no error, no detection that incompatible kwargs were requested.


**Why it matters standalone.** autopulse has one config object so both its middlewares pass identical batch_size — no observable divergence. A third party with two independently-configured producers on a shared table file gets a footgun: the documented per-config tuning silently does not apply. At minimum it should warn when a cache hit receives non-empty, differing kwargs.


**Evidence.** src/sqloutbox/_registry.py:61 (key omits kwargs), 63-69 (cache hit returns existing instance, ignores new kwargs); docstring 57-60 explicitly documents 'Ignored on cache hits'. cli.py:482,494 and verify path also build raw Outbox() bypassing the registry entirely — a second connection to the same file with default tuning.


**Recommendation.** On cache hit, if kwargs are non-empty and differ from the cached instance's attributes, log a WARNING (or raise) instead of silently ignoring. Document the 'first caller wins' semantics prominently as a constraint, not a footnote.


---


### INFO-5 · `config-api` · (confidence: high)

**Scenario.** An OSS consumer wants to write a custom hot-path producer and looks at the documented SQLMiddleware API to know which methods are 'public' / stable to call and subclass.


**Current behavior.** The entire supported method surface of SQLMiddleware is underscore-prefixed: _push(), _push_many(), _source, _outbox() — and these are the methods the README documents as the API (README:360-365) and that the docstring tells subclasses to call (middleware.py:18-20, 'before calling _push() or _push_many()'). By Python convention, leading-underscore = private/unstable, yet here they ARE the public contract. There is no public (non-underscore) method on the class at all.


**Risk.** Ambiguous stability contract. Consumers cannot tell from naming what is stable vs internal. Tooling (linters, IDEs, deprecation checkers) treats _push as private and may warn on external use. A maintainer could rename _push believing it's internal, breaking every downstream subclass.


**Why it matters standalone.** For a 'professionally-maintained OSS package that arbitrary third parties depend on,' the public/private boundary is the core API contract. Documenting underscore methods as the supported surface inverts the universal Python convention and undermines any semver promise. autopulse is the maintainer's own consumer so the convention mismatch is invisible internally.


**Evidence.** src/sqloutbox/middleware.py:71 (_push), 81 (_push_many), 48-55 (_source property), 57-69 (_outbox); README.md:360-365 documents all four underscore members as the SQLMiddleware API.


**Recommendation.** Either (a) provide public aliases (push/push_many/outbox) as the documented surface and keep underscore versions as internal implementation, or (b) explicitly state in README + class docstring that these underscore methods ARE the stable subclass API and are exempt from the private-by-convention rule.


---


### INFO-6 · `config-api` · (confidence: high)

**Scenario.** A consumer on Python 3.10 (the declared minimum, requires-python >=3.10) installs `pip install sqloutbox` WITHOUT the optional [toml] extra, then runs `sqloutbox runservice --config outbox.toml`.


**Current behavior.** _load_tomllib() tries stdlib tomllib (absent <3.11), then tomli (absent without the extra), then raises RuntimeError with a clear, actionable message pointing to `pip install tomli` (_runner.py:293-308). This fires AT STARTUP before any work, which is correct. However: the core library advertises 'zero external dependencies' and requires-python>=3.10, yet the headline TOML-config workflow is non-functional on 3.10 unless the user happens to discover and install the extra. The base install on 3.10 silently lacks the documented primary entry point.


**Risk.** A 3.10 user following the README quick-start (`pip install sqloutbox` then `sqloutbox runservice`) hits a startup RuntimeError on their first run. Not data loss, but a broken out-of-box experience for the advertised primary (TOML) path on the declared-minimum interpreter.


**Why it matters standalone.** autopulse runs on 3.13 (tomllib built in), so the gap is invisible to the maintainer. A third party on 3.10 — explicitly supported per requires-python — gets a broken default workflow. The error is at least clear and fail-fast (good), but the packaging contract is inconsistent: either bundle tomli for <3.11 by default, or raise the floor to 3.11.


**Evidence.** pyproject.toml:10 (requires-python = '>=3.10'), 49-50 ([toml] extra = tomli, only for <3.11); src/sqloutbox/_runner.py:293-308 (_load_tomllib raises RuntimeError if neither present). README/__init__.py headline both lead with TOML config as recommended.


**Recommendation.** Make the tomli dependency conditional-but-default for <3.11 in the main dependencies (`dependencies = ["tomli>=2.0; python_version < '3.11'"]`) rather than an opt-in extra, OR raise requires-python to >=3.11 and drop the extra. Either way the advertised 'stdlib only / TOML recommended' combination should hold on every supported interpreter.


---


### INFO-7 · `crash-durability` · (confidence: high)

**Scenario.** Torn enqueue on crash: process dies mid-enqueue (between BEGIN IMMEDIATE / INSERT and commit), or power loss after commit returns but before the WAL frame is fsynced (the inherent synchronous=NORMAL window).


**Current behavior.** enqueue (_outbox.py:92-107) and enqueue_batch (_outbox.py:150-187) each wrap the MAX(seq) read + INSERT in a single BEGIN IMMEDIATE ... commit transaction. A crash before commit rolls back atomically (SQLite transaction atomicity) — no half-written payload, no half-updated chain. This part is correct. The residual hole is the documented synchronous=NORMAL tradeoff: on WAL+NORMAL, a committed transaction can be lost on POWER LOSS / OS crash (not on a mere process kill) if the WAL frames were not yet fsynced. SQLite guarantees the DB stays consistent (no torn chain), but the most-recent acked enqueue(s) can vanish.


**Risk.** Under power loss / kernel panic, enqueue() can return a seq to the producer (the event is 'accepted') yet the row is lost after recovery. Because the chain uses prev_seq and AUTOINCREMENT, losing the tail row is consistent (no gap mid-chain), so verify_chain won't flag it — the loss is silent. This is the standard WAL+NORMAL durability tradeoff but it contradicts the mental model a producer has when enqueue() returns a non-None seq ('my event is durably queued').


**Why it matters standalone.** The README sentence 'safe on OS crashes, ~3x faster than FULL' (README.md:459) and the _schema.py:81 comment 'survives OS crashes' can mislead an OSS consumer into believing enqueue() returning a seq means the event is power-loss-durable. It is not — NORMAL trades last-commit durability for speed. A consumer building on a stronger durability assumption (financial events) needs to know to set synchronous=FULL.


**Evidence.** enqueue transaction _outbox.py:92-107 (BEGIN IMMEDIATE → INSERT → commit, rollback on exception _outbox.py:110-113). _schema.py:88 synchronous=NORMAL. README.md:459 'synchronous=NORMAL — safe on OS crashes' overstates: NORMAL is safe against application/OS crash for CONSISTENCY but can lose the last commit(s) on POWER loss.


**Recommendation.** Clarify the durability contract: WAL+NORMAL guarantees consistency on crash and durability against process/OS crash, but the most recent committed transaction(s) MAY be lost on power loss / hard reset. Document how to opt into synchronous=FULL for stronger durability, and note enqueue() returning a seq means 'committed under NORMAL', not 'fsynced'.


---


### INFO-8 · `delivery-semantics` · (confidence: high)

**Scenario.** Documenting the TRUE delivery contract: a normal crash between writer.write_batch() succeeding remotely and the local mark_synced/delete_synced completing (e.g. SIGKILL, power loss, or the asyncio task cancelled mid-cycle by SIGTERM after the await on write_batch returns but before the to_thread mark/delete).


**Current behavior.** On restart, fetch_unsynced returns the rows that were delivered remotely but never locally deleted; verify_chain passes; they are re-sent. Idempotency relies ENTIRELY on inject_outbox_seq=True + the partial UNIQUE index turning the re-INSERT into INSERT OR IGNORE (sync.py:107-157, README.md:262-284). For any target with inject_outbox_seq=False (a first-class, documented option, README.md:300-317), there is NO idempotency — the re-delivered INSERT/UPDATE is applied a second time at the destination. So the contract is: at-least-once always; exactly-once ONLY for inject_outbox_seq=True INSERTs into a table with the outbox_seq unique index. UPDATEs get outbox_seq stamped but no dedup unless the WHERE makes them naturally idempotent.


**Risk.** Duplicate rows / double-applied UPDATEs at the destination after any crash, for inject_outbox_seq=False targets and for non-idempotent UPDATEs, despite the headline 'durable transactional outbox' framing. The README documents the idempotency mechanism but never states plainly 'with inject_outbox_seq=False you get at-least-once and MUST make your own statements idempotent'.


**Why it matters standalone.** autopulse knows its own tables (it picks inject_outbox_seq=false for wallet_transactions/loan_placements per the README example, presumably because those are naturally keyed) and accepts the semantics. A third party will read 'durable outbox' and assume exactly-once; with inject_outbox_seq=False or with UPDATEs they silently get duplicates on the very crash scenario an outbox exists to survive. The contract is correct and intentional but under-documented for someone who doesn't share autopulse's assumptions.


**Evidence.** src/sqloutbox/sync.py:650 then 677-678 (write_batch awaited, then mark_synced/delete_synced in separate to_thread steps — a crash window exists between them); src/sqloutbox/sync.py:107-175 (inject_outbox_seq only rewrites to OR IGNORE for INSERTs; UPDATE just appends outbox_seq=?, no dedup); README.md:262-284 (idempotency tied to inject_outbox_seq + unique index); README.md:300-317 (inject_outbox_seq=False is a supported mode with no idempotency note); README.md:479-486 Limitations omits the at-least-once/duplication caveat.


**Recommendation.** Add a 'Delivery guarantees' section to the README stating explicitly: delivery is at-least-once; exactly-once is achieved ONLY for INSERTs when inject_outbox_seq=True AND the remote table has the outbox_seq partial unique index; with inject_outbox_seq=False or for UPDATEs, consumers must make their statements idempotent themselves. Add this to the Limitations list too.


---


### INFO-9 · `lifecycle` · (confidence: high)

**Scenario.** Startup failure modes and SIGTERM arriving DURING the lengthy network startup phase (_ensure_schema auto-ALTER + _seed_from_remote round-trips) before the drain loop begins.


**Current behavior.** Config errors fail fast and loud: load_config_toml raises FileNotFoundError/ValueError/EnvironmentError/ImportError (_runner.py:418-539); cmd_runservice lets these propagate as a traceback and exits non-zero (cli.py:450-453 only catches KeyboardInterrupt). db_dir.mkdir(parents=True) in OutboxSyncService.__init__ (sync.py:228) raises on an unwritable dir — also a hard non-zero exit. That part is correct. HOWEVER, _ensure_schema()/_seed_from_remote() run INSIDE svc.run() (sync.py:278-279) i.e. inside the cancellable task, and each table is its own write_batch (sync.py:349/473). A SIGTERM during this phase cancels mid-loop, potentially after ALTER-ing some tables but not others. The only 'started' INFO log (_runner.py:558) is emitted BEFORE run() and thus before schema/seed actually succeed over the network.


**Risk.** Config errors are handled correctly. The gap: there is no startup-completion barrier and no readiness signal. A SIGTERM during the auto_schema ALTER loop or seed loop can interrupt between tables, leaving some migrated and others not (idempotently repaired on next start, but invisible). Consumers have no reliable signal that schema setup and seeding completed.


**Why it matters standalone.** An OSS consumer's readiness probe has nothing reliable to key on — the process logs 'config' before schema/seed complete over the network. If their orchestrator sends SIGTERM during a slow seed (cold remote DB), they get a partial-migration restart loop that is invisible from logs. The 'service is ready' contract is undocumented and unobservable.


**Evidence.** cli.py:450-453 (only KeyboardInterrupt caught; other exceptions → traceback + non-zero exit, correct); sync.py:228 mkdir in __init__ fails fast on unwritable dir; sync.py:278-279 schema+seed run inside the cancellable task; sync.py:349/473 per-table write_batch loops; _runner.py:558 'started' log precedes actual schema/seed success


**Recommendation.** Emit an explicit 'drain loop started, schema+seed complete' INFO log (or expose a readiness Event/flag) only AFTER _ensure_schema and _seed_from_remote return successfully. Run the auto_schema ALTERs in a single write_batch where the writer allows, or document that partial cross-table schema application is possible and idempotently repaired on restart.


---


### INFO-10 · `lifecycle` · (confidence: high)

**Scenario.** Whole-process crash/restart behavior of the daemon.


**Current behavior.** The runner has NO internal restart or backoff loop — run_service_main runs the drain once and returns on stop; restart is delegated entirely to the process supervisor. The only documented supervisor config (README:441-442 and cli.py:236-237 systemd template) is `Restart=on-failure` with `RestartSec=30`, no StartLimitIntervalSec/StartLimitBurst and no application-level rate limiting or backoff.


**Risk.** A deterministic startup crash (bad ${VAR}, unimportable writer_class, unreachable remote during seed) makes systemd restart every 30s forever with no exponential backoff, hammering the remote with reconnect+seed+schema attempts. More dangerously, per the silent-drain-death finding, most runtime errors do NOT crash the process — they only kill the drain task — so Restart=on-failure never fires for the worst failure mode.


**Why it matters standalone.** A third-party copying the README systemd unit verbatim (the intended path) gets fixed-30s restart with no burst limiting and no backoff. Combined with the silent-drain-death finding, their supervisor both tight-restarts on deterministic startup failures and fails to restart at all on the common runtime-exception-kills-the-task case. The supervision contract is under-specified.


**Evidence.** _runner.py:545-598 (single run, no retry/backoff loop); README:441-442 and cli.py:236-237 (systemd template: Restart=on-failure, RestartSec=30, no StartLimit*); _worker.py / sync.py have no crash-counter or backoff


**Recommendation.** Document that the supervisor owns backoff and recommend adding StartLimitIntervalSec/StartLimitBurst (or exponential RestartSec) to the systemd template. Pair with the fix that makes runtime worker-loop exceptions actually crash the process so Restart=on-failure becomes meaningful.


---


### INFO-11 · `observability` · (confidence: high)

**Scenario.** A consumer wants to monitor sqloutbox with Prometheus/StatsD/OpenTelemetry — drain rate, backlog depth, write latency, failure count per target — the standard way any production service is observed.


**Current behavior.** Observability is exclusively via the stdlib 'logging' module (logger per module). There are no metrics hooks, no callback/observer interface, no counters, and no structured-logging emission (log records use %-style positional args embedded in free-text human messages, e.g. sync.py:695-702). The only machine-readable artifacts are the VerifyResult/TableVerifyResult dataclasses, available only via the offline 'verify' path or the blocking request_verify() coroutine. Per-cycle stats (delivered, failed, write_ms, cycle_ms) exist but only as interpolated text in a log line (sync.py:695-702), not as values a consumer can intercept.


**Risk.** No first-class monitoring integration. To get a backlog-depth gauge or a delivery-rate counter into Prometheus, a consumer must scrape and regex-parse free-text log lines (brittle, breaks on any message wording change) or write their own sidecar that opens the SQLite files. There is no supported, stable contract for metrics.


**Why it matters standalone.** A standalone operator running this in a metrics-driven shop (the norm for any production service) has no clean integration point and must resort to log-parsing or DB-file-scraping. Because sqloutbox is deliberately zero-dependency, it cannot bundle a metrics client — but it also offers no hook for the consumer to plug their own in. autopulse routes everything through its Discord/pulseview stack; a generic consumer has nothing equivalent.


**Evidence.** src/sqloutbox/sync.py:56 (single module logger, no metrics interface); src/sqloutbox/sync.py:695-702 (per-cycle stats only as free-text log args); src/sqloutbox/__init__.py __all__ (no metrics/hook export); zero occurrences of metric/prometheus/statsd/callback-hook in source


**Recommendation.** Add an optional metrics-callback hook to OutboxSyncService (e.g. on_cycle(stats: dict) and on_failure(target, table, seq, error)) that defaults to a no-op, so consumers can bridge to any metrics backend without sqloutbox taking a dependency. Keep emitting the same data as structured 'extra={...}' on log records so JSON-logging configs capture it without regex. Document the stats dict shape as a stable contract.


_Note: partially mitigated elsewhere per verifier._


---


### INFO-12 · `observability` · (confidence: high)

**Scenario.** Operator runs 'sqloutbox verify' against a directory while the drain daemon is actively running (the documented way to check a live deployment, since there is no live status command).


**Current behavior.** cmd_verify opens fresh Outbox/thread connections against the .db files (cli.py:469-494; _verify.py uses read connections). The schema uses WAL, so concurrent read alongside the daemon's write connection is safe and will not corrupt data. The verify report shows pending/synced/total counts and chain integrity at a point in time. This is the only built-in path to inspect a live deployment's per-table state from a shell. Critically, a table's 'ok' is computed as chain_ok AND seq_continuous AND timestamps_monotonic (_verify.py:200) — pending backlog depth never affects 'ok'.


**Risk.** Minimal data-safety risk (WAL handles it), but a semantic trap: 'verify' is presented (cli.py help: 'run integrity verification') as the health tool, yet a verify that reports all-OK says nothing about whether delivery is keeping up. An operator may run 'verify', see 'X/X passed', and conclude the system is healthy while a target is silently backed up by 100k rows (verify reports the high pending count but labels the table OK as long as the chain is intact).


**Why it matters standalone.** A standalone operator needs to understand that 'verify OK' == 'data structurally intact', NOT 'delivery healthy'. Nothing in the verify output or docs draws this distinction, so a backed-up-but-intact queue reads as green. This is a documentation/contract gap an OSS consumer must be told about explicitly.


**Evidence.** src/sqloutbox/cli.py:459-567 (verify offline, reports pending_count but 'ok' is chain/seq/timestamp only); src/sqloutbox/_verify.py:200 (`ok = chain_ok and seq_continuous and timestamps_monotonic` — backlog depth never affects ok); src/sqloutbox/_schema.py (WAL PRAGMA makes concurrent read safe)


**Recommendation.** In the verify report and docs, clearly state that verify checks structural integrity only and does not assess delivery liveness/backlog. Consider adding a non-failing 'backlog warning' line when pending_count exceeds a threshold or oldest-row age exceeds a bound, and point operators to a dedicated 'status' command for liveness.


---


### INFO-13 · `packaging-oss` · (confidence: high)

**Scenario.** A typed downstream codebase imports sqloutbox and runs mypy/pyright expecting inline types to be honored (PEP 561).


**Current behavior.** Works correctly. `src/sqloutbox/py.typed` exists (0-byte marker) and IS shipped in both the wheel and sdist (verified: `sqloutbox/py.typed` in the wheel namelist, `src/sqloutbox/py.typed` in the tarball). The `Typing :: Typed` classifier is present and accurate. The `tomllib` 3.11+ import is correctly guarded via try/except in `_load_tomllib()` rather than at module top level, so import of the package itself does not break on 3.10 — only the TOML feature does (covered separately).


**Risk.** None — this is an info/confirmation item. The PEP 561 marker and typed classifier are correctly set up and packaged.


**Why it matters standalone.** Confirms downstream consumers DO receive types and the LICENSE is correctly embedded in dist-info — no action needed, documented so the maintainer knows these are already correct and shouldn't be 'fixed' into regressions.


**Evidence.** Wheel namelist includes `sqloutbox/py.typed`; sdist includes `sqloutbox-0.4.1/src/sqloutbox/py.typed`; pyproject.toml:31 (`"Typing :: Typed"`); src/sqloutbox/_runner.py:295-302 (tomllib import is lazy/guarded, not top-level); wheel METADATA confirms all classifiers and `License-File: LICENSE`.


**Recommendation.** No change required. When adding the sdist `tests/` inclusion (separate finding), do not disturb the `packages = ["src/sqloutbox"]` wheel target that currently captures py.typed correctly.


---


### INFO-14 · `resource-limits` · (confidence: high)

**Scenario.** The disk hosting the outbox .db / -wal files becomes full (ENOSPC) while a producer calls enqueue() on the hot path.


**Current behavior.** enqueue() wraps the INSERT+commit in a broad `except Exception`, rolls back, logs a WARNING 'enqueue failed — event dropped', and returns None (_outbox.py:109-118). enqueue_batch() does the same and returns [] (_outbox.py:189-198). A disk-full sqlite3.OperationalError is caught here, so the event is silently dropped (only a WARNING log) and the producer's hot path continues as if nothing failed unless it checks the return value.


**Risk.** Silent data loss. The whole point of a transactional outbox is durability; under disk pressure the durability guarantee inverts — events vanish with only a log line. Callers that ignore the return value (the docstring says 'Never raises — drops with WARNING') have no signal. Worse, this is exactly when a backed-up queue (Finding 1) makes ENOSPC most likely, so the two failures compound.


**Why it matters standalone.** An OSS user choosing a 'durable outbox' library reasonably expects enqueue to surface a hard failure (raise) when it cannot persist, so they can apply backpressure or alert. The current contract — swallow everything, drop the event — is a defensible design for a fire-and-forget producer but is a dangerous default for a durability primitive and is under-documented (the README sells durability; the drop-on-error behavior is only in a method docstring).


**Evidence.** src/sqloutbox/_outbox.py:109-118 (enqueue: catch-all, rollback, WARN, return None); src/sqloutbox/_outbox.py:189-198 (enqueue_batch: catch-all, return []); _outbox.py:76 docstring 'Never raises — drops with WARNING on error'


**Recommendation.** Document the drop-on-failure contract loudly in the README durability section, and consider an opt-in 'strict' mode where enqueue raises on persistence failure so callers can apply backpressure. At minimum, distinguish transient/retryable errors (disk full, locked) — which arguably should raise — from genuinely corrupt input.


---


### INFO-15 · `resource-limits` · (confidence: high)

**Scenario.** Long-running drain service with many namespaces/tables (e.g. 50 tables across several targets) running continuously for days; every cycle opens transient SQLite connections for each operation.


**Current behavior.** thread_conn() returns a bare `sqlite3.connect(str(db_path))` (_schema.py:106-112) and is used as `with thread_conn(...) as conn:` in fetch_unsynced/verify_chain/mark_synced/delete_synced/prune_sync_log/pending_count (_outbox.py:208,242,267,290,336,350) and in _verify.py. CRITICAL: a sqlite3 Connection used as a context manager only commits/rolls back on exit — it does NOT close (verified: `with conn:` leaves the connection open and usable afterward). No code path calls conn.close(). These connections are only reclaimed when the object is garbage-collected (refcount drop at end of `with` scope, plus their internal fd). Per cycle the loop opens: 1 pending_count per table (sync.py:566) for ALL tables, plus fetch/verify/mark/delete (4 more) for each READY table. With 50 tables that is 50+ short-lived connections per second, none explicitly closed.


**Risk.** Relies entirely on CPython refcounting to promptly close fds. As long as no reference leaks, fds are freed at scope exit — so this is not a guaranteed leak, but it is fragile: any future code that stores a returned conn, an exception that keeps a frame/traceback alive, or a switch to a non-refcounting runtime (PyPy) turns transient connections into accumulating open file handles and can hit the process RLIMIT_NOFILE. The persistent _write_conn (_outbox.py:64) is also never closed — Outbox has no close()/__del__, so registry/shared_outbox instances hold their write connection for process lifetime by design but with no documented teardown.


**Why it matters standalone.** autopulse uses a small fixed set of tables on CPython, so refcounting keeps fd count flat and the issue is invisible. An OSS consumer with many namespaces, running on PyPy, or wrapping operations in their own retry/traceback-holding code, can accumulate fds. There is also no documented/public way to cleanly shut down an Outbox (close the write connection) — a problem for short-lived tools, tests, and embedding scenarios.


**Evidence.** src/sqloutbox/_schema.py:106-112 (thread_conn — connect, no close); _outbox.py:208,242,267,290,336,350 (with-block, never .close()); verified sqlite3 `with conn:` does not close; src/sqloutbox/_outbox.py:64 (persistent _write_conn, never closed; Outbox has no close/__del__ method)


**Recommendation.** Explicitly `conn.close()` in a try/finally (or use `contextlib.closing(thread_conn(...))`) rather than relying on the connection's __exit__, which does not close. Add an `Outbox.close()` that closes `_write_conn`, and a `clear_registry()`-adjacent teardown that closes pooled connections. Document that thread_conn connections are not pooled and that high table counts mean high connection churn.


---


### INFO-16 · `resource-limits` · (confidence: high)

**Scenario.** Sustained moderate write volume with the default WAL settings over a long uptime; WAL checkpointing relies on SQLite's automatic 1000-page threshold but reads and writes use different connections.


**Current behavior.** open_write_conn sets journal_mode=WAL and synchronous=NORMAL (_schema.py:87-88) but never sets wal_autocheckpoint (defaults to 1000 pages, verified) and never issues a manual PRAGMA wal_checkpoint anywhere (grep confirms no checkpoint call in src/). The persistent write connection handles all enqueues; reads/deletes happen on separate transient thread_conn connections. WAL auto-checkpoint runs piggybacked on a connection's commit when the WAL crosses 1000 pages. Because the write connection is the one doing frequent commits, auto-checkpoint generally fires — but if a reader holds a long read transaction (e.g. a slow verify_full on a huge table, _verify.py) the checkpoint cannot reclaim WAL frames past the reader's snapshot, and the WAL grows until the reader finishes.


**Risk.** Under normal operation WAL stays bounded (auto-checkpoint at 1000 pages works). The failure mode is WAL bloat when a long-lived reader (verify_full, or any external reader opening the file) pins an old snapshot, or in low-write-then-burst patterns where the checkpoint cannot run. WAL growth consumes extra disk on top of the queue itself, compounding Finding 1. There is no manual checkpoint after large deletes to shrink the file either, so a drained queue keeps a large main DB file (deleted rows leave free pages; no VACUUM).


**Why it matters standalone.** Most OSS users on default WAL are fine, but those who run frequent verify scans, keep external readers on the file, or expect the .db file to shrink after a backlog drains will see disk not reclaimed. The absence of any checkpoint/VACUUM strategy and the reliance on SQLite defaults should be a documented operational note, not a silent assumption.


**Evidence.** src/sqloutbox/_schema.py:87-88 (WAL + NORMAL, no wal_autocheckpoint, no busy_timeout override); grep: no wal_checkpoint/VACUUM anywhere in src/; src/sqloutbox/_verify.py opens its own thread_conn for full-table scans (potential long reader)


**Recommendation.** Document the WAL/checkpoint behavior and that the main DB file does not shrink after drain (no VACUUM). Optionally set an explicit `PRAGMA wal_autocheckpoint`, set a `busy_timeout` on thread_conn (currently default 5s — a slow checkpoint or external writer can cause SQLITE_BUSY), and consider an occasional `PRAGMA wal_checkpoint(TRUNCATE)` and/or `VACUUM`/`incremental_vacuum` after large deletes for long-running deployments.


---


### INFO-17 · `schema-versioning` · (confidence: high)

**Scenario.** A third party upgrades the sqloutbox package across versions (or runs two versions during a rolling deploy) and wants to know whether the on-disk format changed and which direction of skew is safe.


**Current behavior.** There is NO schema/version metadata in the DB. No `PRAGMA user_version`, no `PRAGMA application_id`, no meta table — confirmed by grepping all of src/ (only hit is the unrelated logging-config `"version": 1` template in cli.py:349). Compatibility is entirely implicit: forward migrations are only additive ALTER ADD COLUMN + CREATE INDEX IF NOT EXISTS in open_write_conn. An older library opening a newer file works ONLY by luck of additive-only changes (verified: old INSERT/SELECT against a schema with the extra `source` column succeeds because source has DEFAULT '' and old code never names it).


**Risk.** The forward/backward compatibility guarantee is real today (additive-only) but undocumented and unenforced. The moment any future change is non-additive (rename, retype, NOT NULL without default, drop), older readers/writers will either crash or silently corrupt the chain, and nothing detects the skew. Operators have no way to query 'what schema version is this file' to gate a rolling deploy.


**Why it matters standalone.** autopulse controls both producer and consumer versions and deploys them together, so skew is a non-issue for it. An OSS consumer running producer and drain as separate services/hosts, or doing rolling restarts, needs an explicit, documented compatibility contract ('only additive migrations; any version reads any file') and ideally a `PRAGMA user_version` to assert it. Without it they cannot reason about upgrade safety.


**Evidence.** grep across src/ for user_version|schema_version|application_id|meta returns no DB-level versioning; only _schema.py:36-46 additive migrations exist. Verified old-lib INSERT (omitting source) and old-lib SELECT (omitting source) both succeed against the new schema because `source TEXT NOT NULL DEFAULT ''` (_schema.py:17,37).


**Recommendation.** Document the additive-only forward/backward-compat guarantee in README, and stamp `PRAGMA user_version = N` at create time so future non-additive changes can be detected and gated (refuse to open a file newer than the library understands, with a clear message).


---


### INFO-18 · `security` · (confidence: high)

**Scenario.** A third party adopts sqloutbox and lets ANY value that is not 100% under their control influence either the `tag` (SQL string) or the structure of the statement passed to `_push()`/`enqueue()`. The `tag` column is the literal SQL statement that the drain executes verbatim at the remote DB via `writer.write_batch()`.


**Current behavior.** Bind VALUES are safely parameterized (payload = JSON args, sent as args to the writer), but the SQL text in `tag` is executed as-is with no validation, allow-listing, or statement-type restriction. `middleware.py:79` passes `sql` straight through as the tag; `_outbox.py:99-104` stores it; `sync.py:608-612` reads `row.tag` as `sql` and hands it to `inject_outbox_seq`/`write_batch`. There is no parsing that confirms the statement is the INSERT/UPDATE the author intended. Whatever string is enqueued runs against the remote database with the writer's (typically high-privilege) credentials.


**Risk.** Second-order SQL injection / arbitrary remote SQL execution if a producer ever builds the `tag` by string-concatenating attacker-influenced data (table names, column names, dynamic WHERE built from user input — none of which can be parameterized). A single compromised or careless producer can enqueue `DROP TABLE`, `ATTACH DATABASE`, `UPDATE ... SET role='admin'`, etc., and the drain will faithfully execute it with the target DB credentials. The outbox also durably persists the malicious statement on disk and retries it idempotently.


**Why it matters standalone.** autopulse only ever enqueues hand-written constant SQL with bound params, so it never hits this. An arbitrary OSS adopter has no way to know from the README that `tag` is a fully-trusted code path — the docs present `_push(table, sql, args)` like a normal parameterized query API. Nothing states 'the SQL string must be a compile-time constant / fully trusted; never interpolate untrusted identifiers into it.' This is the single most important sentence missing from the threat model.


**Evidence.** src/sqloutbox/middleware.py:71-79 (_push stores raw sql as tag); src/sqloutbox/_outbox.py:99-104 (INSERT of tag); src/sqloutbox/sync.py:607-612 (sql=row.tag → write_batch); _schema.py:18 (tag TEXT). No validation anywhere; grep for sanitiz/trust/injection in src finds only the unrelated `outbox_seq` injection feature.


**Recommendation.** Add an explicit TRUST MODEL / SECURITY section to README and CLAUDE.md stating: the `tag` is raw SQL executed with the writer's credentials and MUST be a trusted, statically-known statement; only the `args` (payload) may carry untrusted data and they are the ONLY injection-safe channel. Optionally add an opt-in guard in the drain that rejects/logs tags whose leading keyword is not in an allow-list (INSERT/UPDATE/DELETE/SELECT) to fail closed on accidental DDL.


---


### INFO-19 · `security` · (confidence: high)

**Scenario.** The producer enqueues rows whose `payload` (the bind args) contains PII or secrets — e.g. customer email/PAN, auth tokens, financial amounts. The drain may lag (remote down, large backlog) so rows sit on local disk for an extended period; or an attacker gains read access to the host filesystem / a backup / a stolen laptop.


**Current behavior.** Payload is stored as plaintext TEXT in the SQLite file and the file is created with the process's default umask — no `chmod 0600`, no directory hardening, no encryption-at-rest. `open_write_conn`/`thread_conn` just call `sqlite3.connect(str(db_path))` with no permission management (`_schema.py:77-112`). WAL/SHM sidecar files inherit the same default perms. Grep confirms zero occurrences of chmod/0o600/umask/encrypt anywhere in src or docs.


**Risk.** Plaintext PII/secret disclosure at rest. Anyone who can read the `.db`, `.db-wal`, or `.db-shm` files (other local users if the data dir is world/group-readable, backups, container volume snapshots) sees every queued payload. Sensitive data also lingers in the WAL and in deleted-but-not-vacuumed pages even after `delete_synced`.


**Why it matters standalone.** autopulse runs as a dedicated single-tenant user on a private VM, so default perms are acceptable for it. A generic OSS adopter on a shared host, in a multi-tenant container, or shipping the dir in backups inherits a silent plaintext-PII-on-disk exposure with no guidance. There is no SECURITY.md and the README never states the at-rest threat model or recommends restrictive perms / disk encryption.


**Evidence.** src/sqloutbox/_schema.py:84-112 (sqlite3.connect with no os.chmod/umask); _schema.py:19 (payload TEXT NOT NULL); _outbox.py:103 stores payload.decode() verbatim. No encryption layer. README 'Limitations' (README.md:479-486) lists constraints but says nothing about data sensitivity or file permissions.


**Recommendation.** Document explicitly that payloads are stored UNENCRYPTED in local SQLite and that the consumer is responsible for at-rest protection. Recommend (or optionally enforce via os.chmod after create) `0600` on the .db files and `0700` on `db_dir`, advise full-disk/volume encryption for sensitive payloads, and warn that WAL + non-vacuumed pages retain data after delete. Consider adding an opt-in `os.umask`/`chmod` step in `open_write_conn`.


---


### INFO-20 · `security` · (confidence: high)

**Scenario.** A third party loads an `outbox.toml` (or calls `load_config_toml`) whose content is not fully trusted — e.g. config committed by a less-privileged team member, generated from a templating system, supplied by a tenant, or fetched from a shared/automation source. The TOML names the writer class to import.


**Current behavior.** `load_config_toml` reads `writer_class = "module.path:ClassName"` from the TOML and passes it to `_import_class`, which calls `importlib.import_module(module_path)` and then `cls(**conn_args)` with the `[connection]` kwargs. Module import executes that module's top-level code. So whoever controls the TOML chooses which Python module gets imported (and thus executed) and instantiated, with arbitrary keyword arguments, inside the service process.


**Risk.** Arbitrary code execution by config. Any importable module on the service's PYTHONPATH can be named as `writer_class`; importing it runs its module-level code, and `cls(**conn_args)` runs its constructor with attacker-chosen kwargs. If the adopter's environment has any module with side effects on import (or the attacker can drop a .py onto the path / sys.path[0]), this is RCE-by-config. There is no allow-list of permitted writer classes.


**Why it matters standalone.** autopulse fully controls its own outbox.toml and writer module, so this is a non-issue for it. An OSS adopter who treats the config file as 'just settings' (the README frames TOML as the recommended, friendly config path and never warns the file is executable trust) may expose RCE if the config is ever attacker-influenceable. The docs need to state the config file is a trusted/privileged artifact equivalent to source code.


**Evidence.** src/sqloutbox/_runner.py:267-290 (_import_class: import_module + getattr), :518-521 (writer_cls(**conn_args)); cli.py runservice and verify both call load_config_toml. The Python-mode scaffold (`_load_config` in the run_service template, cli.py:281-287) likewise `exec_module`s an arbitrary outbox_config.py.


**Recommendation.** Document that `outbox.toml` (and the Python scaffold) are TRUSTED inputs equivalent to executable code — never load a config from an untrusted source. Optionally support restricting `writer_class` to a registered/allow-listed set, or require the caller to pass writer factories programmatically when the config origin is not fully trusted.


---


### INFO-21 · `security` · (confidence: high)

**Scenario.** Any TOML `${VAR}` reference is resolved against the full process environment, and the resolved value is passed as a writer constructor kwarg. A config author (who may differ from the operator who controls the env) writes `${AWS_SECRET_ACCESS_KEY}` or `${SOME_OTHER_TENANTS_TOKEN}` into a connection field.


**Current behavior.** `_interpolate_env` resolves `${VAR}` from `os.environ` for ANY variable name with no scoping (`_runner.py:235-251`), and the result becomes a `[connection]` kwarg to the dynamically-imported writer. So whoever writes the TOML can read any environment variable visible to the process and route it into a writer they also named.


**Risk.** Secret exfiltration via config. A semi-trusted config author can pull any process env var (other services' tokens, cloud credentials) into a writer of their choosing, which could forward it to an attacker-controlled endpoint. This is the same trust-boundary issue as the writer_class RCE, narrowed to credential harvesting.


**Why it matters standalone.** Reinforces that the TOML is a trusted artifact: an adopter who lets a lower-privileged party author the config, while the service holds high-value env secrets, leaks those secrets. autopulse authors its own config so it is unaffected. Worth a one-line note in the trust-model docs alongside the writer_class warning.


**Evidence.** src/sqloutbox/_runner.py:235-264 (_interpolate_env / _interpolate_dict over the whole os.environ), :519-521 (interpolated conn_args → writer constructor).


**Recommendation.** Document that `${VAR}` can read ANY process environment variable and that the config author is therefore as privileged as the process environment. No code change required if the trust model is documented; optionally allow operators to namespace-restrict which env vars are interpolable.


---


### INFO-22 · `writer-protocol` · (confidence: high)

**Scenario.** A writer mutates or reorders the input `stmts` list in place (e.g. sorts statements for batching efficiency, dedups, or pops items as it sends them), or returns its results in a different order than the input.


**Current behavior.** _flush_to_target confirms rows by zipping the returned results positionally with stmt_info, which was built from the ORIGINAL stmt order (sync.py:607-613, :663-664). sqloutbox passes the same list object it built to write_batch and never re-validates order or identity on return. If the writer reorders results, sqloutbox marks the WRONG outbox_seq as synced for each ok/failed verdict — confirming-and-deleting a row that actually failed, and leaving a row that actually succeeded pending (which then redelivers). If the writer mutates the shared list, behavior is undefined relative to stmt_info indexing.


**Risk.** Silent DATA LOSS: a statement the destination rejected gets marked synced and permanently deleted from the outbox because a different (successful) row's verdict landed on its index. The chain-integrity machinery does not catch this — verify_chain only runs before send (sync.py:596-605) and checks local prev_seq linkage, not delivery correctness.


**Why it matters standalone.** A naive but well-intentioned third-party writer optimization (sort by table to batch INSERTs, or use asyncio.gather and collect results in completion order rather than submission order) silently maps verdicts to the wrong rows and deletes undelivered data. autopulse's writer preserves order so this is never exercised. This is exactly the kind of unstated invariant an OSS consumer cannot reverse-engineer without reading sqloutbox internals.


**Evidence.** stmt_info built in original order at src/sqloutbox/sync.py:607-613; positional confirmation at src/sqloutbox/sync.py:663-666; the same list object passed to writer at src/sqloutbox/sync.py:650 with no defensive copy. The Protocol docstring (sync.py:83-101) never states results must be in input order AND never states stmts must not be mutated.


**Recommendation.** Document explicitly that results MUST be returned strictly aligned to input order and that the stmts list must NOT be mutated. Consider hardening: pass a tuple (immutable) to write_batch, or correlate by a returned identifier instead of positional index for safety-critical correctness.


_Note: partially mitigated elsewhere per verifier._


---
