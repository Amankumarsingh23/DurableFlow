# Session 9: coordinating checkpoint transactions

## The race in Session 8

Before the Session 9 change, WorkflowEngineState.next read the workflow with findById,
read activity rows, selected a PENDING step and called markRunning on a managed entity.
Hibernate later flushed the update by row identity. There was no status predicate or
optimistic version check in that write, and no pessimistic lock on the earlier read.

Two independent WorkflowEngine instances could interleave like this:

| Invocation A | Invocation B |
| --- | --- |
| Read payment=PENDING, attempts=0 | Read payment=PENDING, attempts=0 |
| Decide payment is eligible | Decide payment is eligible |
| Set RUNNING, attempts=1; commit | Set RUNNING, attempts=1; commit |
| Execute handler | Execute handler again |

Both transactions can commit. The final counter can still be 1 even though two callers
believed they claimed the activity. This is both a duplicate claim and a lost update.
The ConcurrentHashMap guard in WorkflowEngine protects only calls through the same
engine object. It cannot coordinate separate objects or application processes.
@Transactional made each checkpoint atomic; it did not serialize these decisions.

This race analysis was recorded against the Session 8 implementation before changing
the checkpoint code. The regression tests retain a deliberately unsafe, test-only
read/modify/write example to demonstrate the interleaving without executing side effects.

## Chosen protocol

Session 9 uses a pessimistic write lock on the workflow_run row at the start of every
engine checkpoint transaction. Claim, completion, failure and terminal/no-progress
decisions all follow the same order: lock run, read activity state, modify state, commit.

The repository method findByIdForUpdate uses Spring Data @Lock(PESSIMISTIC_WRITE).
The conceptual SQL sequence is:

```sql
BEGIN;
-- Checkpoint transactions explicitly use READ COMMITTED.
SELECT * FROM workflow_run WHERE id = :run_id FOR UPDATE;
SELECT * FROM activity_execution WHERE workflow_run_id = :run_id;
-- After checking run status and dependency states under the run lock:
UPDATE activity_execution
SET status = 'RUNNING', input_json = :input,
    attempt_count = attempt_count + 1, started_at = :now, updated_at = :now
WHERE id = :activity_id;
COMMIT;
-- Only now call ActivityHandler.execute, without a DB transaction.
```

This is the protocol's SQL shape, not a verbatim Hibernate SQL log. Hibernate controls
the generated column list and dialect-specific locking clause; it may render FOR NO
KEY UPDATE rather than FOR UPDATE. The critical property is mutually exclusive run-row
write locks throughout the checkpoint, not the precise spelling of the ORM-generated
statement. The ORM increment is safe here because it is computed only after acquiring
that shared coordination lock. This implementation does not use an affected-row-count CAS.

Spring Data documents [lock annotations on repository queries](https://docs.spring.io/spring-data/jpa/reference/jpa/locking.html).
PostgreSQL documents the conflicting row lock modes and their transaction lifetime in
[explicit locking](https://www.postgresql.org/docs/current/explicit-locking.html).

### The contender re-reads after waiting

The second invocation waits for the run lock before reading any activity state.
After the winner commits, the second invocation acquires that lock and reads current
activity rows in a subsequent statement. Explicit READ COMMITTED isolation makes this
statement see the committed checkpoint, rather than a transaction-wide old snapshot.
See PostgreSQL's [isolation documentation](https://www.postgresql.org/docs/current/transaction-iso.html).

If the activity is now RUNNING, the contender invokes no handler, resets no status,
and changes no timestamp/counter. It returns the existing 409 RECOVERY_REQUIRED response.
The message acknowledges both possibilities: another invocation may still be active,
or a previous process may have disappeared. Without leases/heartbeats the engine cannot
distinguish them. This is a busy/uncertain state, not evidence of corrupt workflow data.
The run and activity remain RUNNING and inspectable, preserving Session 8 semantics.

If the contender arrives after completion, it derives the next eligible PENDING step
or finishes the graph. If the run is already terminal it exits with the existing 409.
There is no polling loop that waits for arbitrary handler work and no automatic retry.
A caller already executing may also encounter a terminal conflict if another caller
finalized the same graph first; clients can inspect the committed result using GET.

### Why lock the run rather than only one activity?

The current engine executes a run sequentially and checks its whole graph. A common
parent-row lock coordinates eligibility, activity transitions and run terminal decisions
with one protocol. Only one checkpoint for a run may make a decision at a time, while
unrelated runs have different row locks. A lock on an individual activity alone would
not coordinate competing terminal decisions or claims of different roots in the same run.

Completion and failure acquire the same run lock before reloading the activity. If
another transaction already changed its RUNNING status, the stale completion/failure
exits with a controlled 409 rather than overwriting the recorded outcome. Finalization
checks all expected activity rows and changes the run while still holding the run lock.
It cannot overwrite a previously committed terminal run. All engine checkpoint writers
must follow this protocol; direct repository saves or SQL that bypass it are not protected.

There is no need to hold a database transaction during an external call:

1. Lock run, choose/mark PENDING -> RUNNING, commit and release the lock.
2. Execute the handler with no database transaction or row lock held by the engine.
3. Lock run again, re-read, record completion or atomic activity/workflow failure, commit.

Acquire locks consistently: run first, then activity. Every current checkpoint touches
one run. Future code touching several runs must define a consistent cross-run order;
row locks are not a promise that arbitrary future code can never deadlock.

### Why no @Version or V3 migration?

WorkflowRun was a candidate for optimistic locking because concurrent checkpoints could
otherwise overwrite terminal outcomes. The run lock now serializes every existing
engine writer, with the current state checked under that lock. Adding @Version would
duplicate conflict detection for those writers and introduce schema/error-handling
changes without a present correctness benefit. There is no @Version column and no
Session 9 migration; V1 and V2 are unchanged. Revisit this decision when new writer
paths or parallel execution requirements appear.

### Row locks, leases and idempotency are different

The row lock ends at transaction commit/rollback. It coordinates a checkpoint; it is
not long-lived task ownership. Persisted RUNNING prevents another engine invocation
from executing the step after that lock is released. If the process dies, RUNNING is
still uncertain: there is no lease expiry, heartbeat or reclamation in Session 9.

Preventing simultaneous duplicate claims does not prevent duplicate external effects
across future retries. A side effect can still succeed before its database acknowledgement.
Idempotency and safe retry/recovery require later work. There is no queue table,
SKIP LOCKED consumer, external worker or new parallel activity execution in this change.

## Deterministic tests and verification boundary

WorkflowConcurrencyIntegrationTest uses PostgreSQL/Testcontainers, separate executor
threads and bounded CyclicBarrier/CountDownLatch coordination, with no Thread.sleep:

- A test-only reproduction forces both legacy transactions to read PENDING before
  either writes, showing two successful unsafe claim decisions and a lost increment.
- Two real checkpoint transactions start together; exactly one receives executable
  work and the other observes RUNNING without altering it. One execution row remains.
- Two independent WorkflowEngine objects (separate in-memory guards) compete. The
  winning handler is held at a latch until the loser exits. Only one handler call
  occurs; the loser does not fail the active run. The run then reaches COMPLETED.
- Inside that handler, a separate short transaction acquires the run write lock,
  demonstrating that the claim transaction has released it before handler work.
- Completion and failure compete for the same attempt; only one changes the outcome.
- Two graph finalizers compete; a terminal run and its completion timestamp remain intact.

The existing engine and context-restart recovery suites remain part of mvn verify.
The tests use independent engine objects and database transactions, not separate worker
processes, and make no distributed-worker or real-JVM-crash claim.

On the authoring machine Java, Maven and Docker are unavailable. These tests are added
but have not run here. Compilation, PostgreSQL migration/mapping validation, concurrency
tests, full regression tests and application startup remain runtime-unverified.
