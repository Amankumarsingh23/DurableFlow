# Session 11: renewable ownership and crash reclamation

Session 11 checkpoint notes: [Session 13](retries.md) adds scheduled READY/RUNNING retries,
attempt budgets and V5. Its policy supersedes the no-ordinary-retry statements below.

Session 11 supersedes the abandoned-claim limitation in the Session 10 queue notes.
PostgreSQL remains the durable source of truth. Handlers execute outside transactions.
Reclamation permits **at-least-once execution**: an old handler and its replacement can
overlap. Deterministic demo handlers are suitable for this experiment. Arbitrary payment,
email or other external effects are not yet safe; Session 12 introduces idempotency.

## The five concepts

| Concept | Meaning |
| --- | --- |
| Row lock | Short transaction-scoped coordination while claiming or writing an outcome; released before handler execution |
| Lease | Durable, time-bounded ownership in the task row during execution |
| Heartbeat | Conditional renewal of the same live ownership attempt |
| Reclamation | Transfer of expired CLAIMED ownership to a new attempt |
| Zombie worker | Previous owner continuing after expiry or transfer; its result must be rejected |

Timeline: A claims generation 1, heartbeats, then dies. Its last deadline passes.
B locks and reclaims the same task as generation 2. A wakes late and attempts completion:
its generation/owner check fails, so no logical result changes. B's completion succeeds
while its lease is valid. If A wakes after expiry but before B claims, A is still rejected.
Heartbeat renewal cannot resurrect an expired lease.

## Schema and attempt visibility

V4 adds nullable `lease_expires_at TIMESTAMPTZ`, non-null `lease_generation BIGINT`
(default zero), lease-state checks, and a partial CLAIMED index on
`(lease_expires_at, created_at, id)`. The V3 READY partial index on
`(available_at, created_at, id)` remains. V1–V3 are unchanged.

READY has no lease and generation zero. Each claim/reclaim increments generation.
`claimed_at` is the **current attempt's start**, never a heartbeat timestamp.
Logical ActivityExecution `started_at` remains the first attempt's start; `attempt_count`
increments on reclamation while status stays RUNNING. Input, IDs and logical rows are
preserved. Terminal tasks retain ownership metadata for inspection.

Stop all Session 10 processes before applying V4; old binaries do not enforce leases.
V4 initializes existing non-READY generations to one and makes existing CLAIMED leases
immediately expired. Existing terminal outcomes remain terminal, with no invented lease
deadline. Restart only Session 11 binaries. This upgrade is not a mixed-version rollout.

## Claim/reclaim algorithm and database time

The repository uses this candidate selection (full query in ActivityTaskRepository):

```sql
SELECT t.* FROM activity_task t
JOIN activity_execution a ON a.id = t.activity_execution_id
JOIN workflow_run r ON r.id = a.workflow_run_id
WHERE r.status = 'RUNNING' AND (
    (t.status = 'READY' AND t.available_at <= statement_timestamp() AND a.status = 'PENDING')
    OR (t.status = 'CLAIMED' AND t.lease_expires_at <= statement_timestamp() AND a.status = 'RUNNING')
)
ORDER BY CASE WHEN t.status = 'READY' THEN t.available_at ELSE t.lease_expires_at END,
         t.created_at, t.id
LIMIT 1 FOR UPDATE OF t SKIP LOCKED;
```

In a READ COMMITTED transaction, lock task, then its parent run. Revalidate run/activity
eligibility after acquiring the run lock. Read `clock_timestamp()` after lock waits;
set owner, claimed_at, deadline (DB time plus configured duration), and increment generation.
For a first claim transition PENDING to RUNNING; for reclamation keep RUNNING and increment
its attempt count. Commit before calling the handler. Two reclaimers cannot own the same
generation; locked candidates are skipped. No duplicate logical activity or task is made.

The two partial indexes match the two status/time predicates. The OR plus CASE ordering
may still require a sort; query plans and production throughput have not been measured.
The run lock can still serialize brief mutations for siblings of one run.

Worker clocks do not decide ownership. PostgreSQL `NOW()` is transaction-start time,
which can become stale during a lock wait. Candidate filtering uses statement time;
actual claim and ownership writes use `clock_timestamp()` for current DB time, including
after waits. See [PostgreSQL date/time functions](https://www.postgresql.org/docs/17/functions-datetime.html).

## Renewal, results, and the small fencing token

Heartbeat is a separate short transaction (five-second transaction timeout):

```sql
UPDATE activity_task
SET lease_expires_at = clock_timestamp() + :leaseMillis * INTERVAL '1 millisecond'
WHERE id = :taskId AND status = 'CLAIMED' AND worker_id = :workerId
  AND lease_generation = :generation AND lease_expires_at > clock_timestamp();
```

The implementation casts the duration parameter to double precision for PostgreSQL
interval multiplication. Affected rows must equal one. Generation is a small useful
addition: if a process ID is reused, an old attempt with the same worker ID still fails
(the ABA case). This token fences DurableFlow's database writes only. External services
do not honor it automatically, and it provides no general external-effect idempotency.

Outcome transactions lock task then run, verify owner/generation/activity identity, and
flush logical activity/run changes. Their **final conditional task UPDATE** requires
CLAIMED, worker ID, generation and `lease_expires_at > clock_timestamp()`. Zero affected
rows throws OwnershipLostException (HTTP 409/domain log) and rolls back the entire
transaction, including already flushed logical changes. Checks before a lock wait alone
would be insufficient. The loaded task entity is not mutated during an outcome, avoiding
a stale JPA entity write overwriting a renewed deadline.

Existing run-failure semantics remain: no new tasks are claimed for a FAILED run. An
already claimed sibling with a valid lease can record its outcome without reopening it.
Expired siblings in a FAILED run are left inspectable; reclamation does not reopen runs.
Ordinary handler exceptions mark task/activity/run FAILED; they receive no automatic retry.

## Heartbeat and shutdown lifecycle

One daemon scheduler per application renews active attempts with fixed delay. Normal worker
polling is sequential, so one worker process ordinarily runs one handler at a time.
Each attempt has a cancellable session; completion/failure cancels its heartbeat before
the outcome transaction. Renewal and session close are coordinated so a concurrent failed
renewal cannot be missed. Closed sessions are removed and scheduled tasks are cancelled.

Any rejected renewal **or renewal exception** conservatively abandons the local attempt,
cancels further heartbeats and discards the handler's eventual result. This may abandon
an otherwise live lease during a temporary database outage; another worker can reclaim
after its deadline. Handler code cannot always be interrupted or external effects undone.
Database ownership checks remain authoritative even if the scheduler is delayed.

On context close, worker and poller stop accepting work, active sessions become lost,
and the heartbeat executor is shut down. There is no forced READY reset or premature
lease release. Active results are discarded; leases expire normally. Scheduler shutdown
waits up to five seconds after session cancellation. An in-flight JDBC call/session close
can still delay shutdown depending on driver/pool behavior; arbitrary handler cancellation
is not promised. Abrupt process death relies on DB lease expiry, never graceful cleanup.

## Configuration and running

| Property | Worker-profile environment | Default |
| --- | --- | --- |
| durableflow.worker.lease-duration-ms | WORKER_LEASE_DURATION_MS | 30000 |
| durableflow.worker.heartbeat-interval-ms | WORKER_HEARTBEAT_INTERVAL_MS | 10000 |
| durableflow.worker.poll-delay-ms | WORKER_POLL_DELAY_MS | 500 |

Heartbeat must be positive and at most one third of the lease duration. Poll delay must
be positive. Invalid values fail startup. Use distinct worker IDs per process.
The README server/two-worker commands still apply; for example:

```powershell
java -jar target/durableflow-0.0.1-SNAPSHOT.jar --spring.profiles.active=worker --durableflow.worker.id=worker-A --durableflow.worker.lease-duration-ms=30000 --durableflow.worker.heartbeat-interval-ms=10000
```

Inspect deadlines and generations using local Compose defaults:

```powershell
docker compose exec postgres psql -U durableflow -d durableflow -c "SELECT t.id, t.status, t.worker_id, t.claimed_at, t.lease_expires_at, t.lease_generation, a.status AS activity_status, a.attempt_count FROM activity_task t JOIN activity_execution a ON a.id=t.activity_execution_id ORDER BY t.created_at, t.id;"
```

For a real kill/reclaim experiment, use an isolated demo database: start A alone, enqueue
work, confirm CLAIMED with a live deadline, kill A before its outcome, then start B. After
the deadline B should claim the same task with generation +1 and complete it. Observe the
same logical activity ID and incremented attempt_count. Existing demo handlers finish
quickly, so this timing is not guaranteed; killing after completion proves nothing about
reclamation. The deterministic tests below force expiry/coordinate locks instead. No
real worker-kill demonstration was performed on this machine.

## Tests and verification status

```powershell
mvn -DskipTests compile
mvn "-Dtest=WorkerHeartbeatsTest,ActivityTaskLifecycleTest" test
mvn "-Dtest=TaskLeaseIntegrationTest,PostgresTaskQueueIntegrationTest,WorkflowConcurrencyIntegrationTest" test
mvn verify
```

TaskLeaseIntegrationTest uses PostgreSQL, DB-time deadline adjustments, barriers/latches
and a temporary trigger to test initial ownership, renewal, invalid renewals, reclaim
races, unchanged logical execution identity, generation reuse, locked-head skipping,
and rejection/rollback of stale outcomes, including expiry during logical flush.
WorkerHeartbeatsTest manually invokes scheduled callbacks to check cancellation,
renewal rejection/error, shutdown, and discarding a successful stale handler result.
No wall-clock sleep is needed. Prior queue/restart/concurrency tests remain in the suite.

Implemented and statically checked: source references, migration preservation, XML/JSON
syntax and whitespace. **Not runtime verified:** compilation, Flyway migration execution,
Hibernate mapping, tests, startup, server/multi-worker or kill/reclaim demos. Java, Maven
and Docker are unavailable. These commands and expected behaviors are a verification
plan, not a claim of successful execution.
