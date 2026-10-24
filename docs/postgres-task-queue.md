# Session 10: PostgreSQL task queue

This document records the Session 10 checkpoint. Session 11 supersedes its abandoned-claim
limitation and owner-only validation with [renewable leases and generation checks](task-leases.md).
The current migration head is V4; the original V3 startup expectation below is historical.

The normal path is now orchestrator -> durable queue -> independent workers.
There is one Maven artifact, launched either as an HTTP server (default) or a non-web
worker (worker profile). No broker, event bus or Maven module split was introduced.

## Responsibilities and schema

- WorkflowEngine.advanceRun delegates to WorkflowTaskOrchestrator. It locks the run,
  reconstructs the immutable graph and logical execution state, and creates READY
  tasks for eligible PENDING steps. It never invokes a handler.
- activity_execution remains the logical step state. activity_task is work delivery:
  READY -> CLAIMED -> COMPLETED/FAILED, correlated with PENDING -> RUNNING -> outcome.
- TaskWorker claims one authorized task, resolves its activity type using ActivityRegistry,
  executes the handler without a DB transaction, then records the outcome.

V3__create_activity_task.sql adds UUID identity, the activity execution foreign key,
status, availability/creation timestamps, claim timestamp and worker ID. CHECK constraints
restrict statuses and require claim ownership metadata for non-READY rows. There is one
task ever per activity execution in this session, enforced by a unique constraint.
V1/V2 are unchanged. Retry attempt modeling is deliberately deferred.

Repeated/concurrent advanceRun calls use the existing run-row lock and check existing
tasks before insertion. The unique constraint is a second boundary against duplicates.
Creating tasks does not start activities: they stay PENDING until a worker claims them.
Only when all expected activities are durably COMPLETED may advancement complete the run.
Terminal advancement is a harmless inspection, never a restart/retry of a terminal run.

Independent roots in the existing DAG can produce independent tasks. Dependencies still
require every upstream activity to be COMPLETED; a join is not queued early. There is
no new in-process parallel scheduler or graph model. Each worker executes one task at a
time, while different workers can execute independently authorized tasks.

## Explicit claim SQL and query-driven index

ActivityTaskRepository.lockNextReady contains this native PostgreSQL query:

```sql
SELECT t.* FROM activity_task t
JOIN activity_execution a ON a.id = t.activity_execution_id
JOIN workflow_run r ON r.id = a.workflow_run_id
WHERE t.status = 'READY' AND t.available_at <= :now
  AND a.status = 'PENDING' AND r.status = 'RUNNING'
ORDER BY t.available_at, t.created_at, t.id
LIMIT 1 FOR UPDATE OF t SKIP LOCKED;
```

A plain SELECT could deliver the same task to two workers. FOR UPDATE alone would wait
behind a locked head task. SKIP LOCKED lets another worker select a different unlocked
task. OF t locks the task row rather than every joined row. Queue reads can omit locked
rows intentionally; this is not an appropriate replacement for ordinary consistent
business/reporting reads. PostgreSQL documents these semantics under
[SELECT locking clauses](https://www.postgresql.org/docs/17/sql-select.html).

The index is (available_at, created_at, id) WHERE status='READY'. The READY predicate is
literal in the query; available_at bounds the scan, while the full ordering including
UUID breaks timestamp ties deterministically. The partial index excludes terminal work.
See PostgreSQL's [partial index documentation](https://www.postgresql.org/docs/17/indexes-partial.html).
This is an access-pattern design, not an EXPLAIN/benchmark claim; query plans remain unverified.
Ordering is best effort among unlocked eligible tasks, with no strict FIFO/fairness guarantee.

TaskTransactions.claim selects and locks a task, acquires the existing run write lock,
re-reads the activity, validates current run/activity status, and atomically records
CLAIMED/worker_id/claimed_at plus activity RUNNING/input/started_at/attempt_count. A
scalar run-ID lookup avoids populating the JPA cache with stale activity state before
the run lock. The transaction commits before the detached claim is passed to a handler.
An empty queue or a concurrently invalidated candidate returns no work, not a spin loop.

## Transaction and lock ordering

Queue claim and completion/failure writers lock **task -> run**, then read/change the
activity. The orchestrator holds the run lock but never takes write locks on existing
task rows: its task lookups are ordinary reads, and only genuinely new task rows are
inserted. This avoids a run/task lock cycle between dispatch and completion. Legacy
checkpoint code also never locks an existing task. Future writers must follow this protocol.

1. Short transaction: select READY with SKIP LOCKED, claim task and mark activity RUNNING.
2. No transaction: resolve/call ActivityHandler using workflow input.
3. Short transaction: verify task owner, atomically write task/activity COMPLETED, or
   task/activity FAILED and workflow FAILED. Database failures propagate; handlers are not retried.
4. After a successful outcome commits, call the lightweight orchestrator in the worker
   process to materialize successors/finalize. This shares code, not HTTP requests.

The last call is intentionally not an event/outbox mechanism. If a worker dies after
outcome commit but before advancement, an operator can POST /advance again. Persisted
COMPLETED work is skipped. There is no automatic scan of every workflow on startup.

On handler failure, no further tasks for that run may be claimed (the claim query checks
run RUNNING). A sibling already CLAIMED can still record its real outcome without
reopening a FAILED run. Unclaimed READY siblings remain inspectable and unexecutable;
cancellation/cleanup is not added in this session. Task ownership checks reject outcomes
from a different worker ID or a task no longer CLAIMED.

## Roles, polling and process independence

The default server exposes registration, materialization, inspection and POST
/api/workflow-runs/{id}/advance. It has no worker polling loop. The worker profile loads
application-worker.yml, sets spring.main.web-application-type=none, excludes workflow
controllers/local execution, and enables the task poller. Workers depend only on shared
PostgreSQL and registered handler implementations, not on a running HTTP server.

WorkerIdentity uses a configured durableflow.worker.id (WORKER_ID in the worker profile),
or generates a UUID-based ID once at startup. Use distinct configured IDs per process.
Each polling tick claims at most one task. A fixed delay after the tick finishes avoids
busy-spinning when idle and keeps execution sequential within each process.

| Configuration | Default | Purpose |
| --- | --- | --- |
| durableflow.worker.id / WORKER_ID | generated per startup | Worker instance identity |
| durableflow.worker.poll-delay-ms / WORKER_POLL_DELAY_MS | 500 | Delay after each polling tick, milliseconds; must be positive |
| durableflow.worker.polling-enabled / WORKER_POLLING_ENABLED | true in worker role | Disable polling for controlled tests/manual invocation |

Stopping the worker process stops polling. No long database transaction or row lock
is held during activity code. The durable CLAIMED status persists after the row lock
is released; it is not a lease, and it does not expire.

## Uncertainty and compatibility boundary

A worker that dies after claim leaves CLAIMED/RUNNING stuck. Another worker selects
only READY tasks and will not re-execute it. Advancement recognizes tracked CLAIMED
work as in flight but cannot tell a live worker from a dead one. A legacy RUNNING
activity without a matching CLAIMED task still produces RECOVERY_REQUIRED and is never
reenqueued/reset. No new unknown/orphan status was added. Session 11 will address leases,
heartbeats and reclamation; idempotency and safe retries are not implemented here.

The old POST /execute and dev bounded execute endpoint are retained as explicit
synchronous development compatibility through WorkflowLocalExecutor. They are not the
queue's normal path. They reject runs with any queued tasks under the same run lock.
Conversely, advancement refuses a currently RUNNING untracked legacy activity. This
prevents mixing local execution and queue dispatch for the same pending step. A legacy
run paused between durably completed steps may be adopted by /advance: only remaining
PENDING work is queued. Earlier Session 8/9 tests keep exercising this compatibility
path; the independent-executor concurrency test names its extracted local runner explicitly.

## Verification boundary

PostgresTaskQueueIntegrationTest covers concurrent materialization, one-task claim races,
two distinct root claims, a locked-head bypass before the head lock is released,
exactly one worker handler invocation, transaction-free handler execution, atomic
task/activity outcomes (including an injected database failure/rollback), successor/join
eligibility, ownership validation, failures, availability gating and legacy recovery.
It starts an independent worker application context against the same database, checks
that it has no web context/workflow APIs, and consumes a previously persisted task.

ActivityTaskLifecycleTest checks controlled lifecycle behavior. The startup test now
expects Flyway V3. Existing engine, restart and concurrency suites remain in mvn verify.
These tests use threads and independent application contexts, not separate JVM subprocesses.
The README supplies real server/two-worker JVM commands, but that demo has not run here.

Java, Maven and Docker remain unavailable on the authoring machine. Compilation,
migrations/mapping validation, tests, startup, SQL query plans and the two-worker process
demo are unverified. Static checks do not prove distributed execution or crash safety.
