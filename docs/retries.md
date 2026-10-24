# Session 13: durable bounded retries

Session 13 checkpoint notes: [Session 14](activity-timeouts.md) adds queued attempt
deadlines and graceful drain, superseding the no-handler-timeout boundary below.

The queued worker path now retries explicitly transient handler failures. Legacy local
development execution remains fail-fast. PostgreSQL holds the schedule; no thread sleeps
or in-memory retry timer is used. Session 10–12 claiming, leases, heartbeats, fencing and
stable logical idempotency keys remain in place.

## Policy and classification

RetryPolicy contains maxAttempts (including the first attempt), initialDelayMillis,
multiplier, maxDelayMillis and FULL/NONE jitter. The application uses FULL jitter by
default. Values require maxAttempts >= 1, nonnegative delays, maxDelay >= initialDelay,
a finite multiplier >= 1 and a jitter strategy. Zero delay and multiplier 1 are valid.

| Property under durableflow.worker.retry | Worker-profile environment | Default |
| --- | --- | --- |
| max-attempts | WORKER_RETRY_MAX_ATTEMPTS | 3 |
| initial-delay-ms | WORKER_RETRY_INITIAL_DELAY_MS | 1000 |
| multiplier | WORKER_RETRY_MULTIPLIER | 2 |
| max-delay-ms | WORKER_RETRY_MAX_DELAY_MS | 30000 |

All workers should use the same global policy. It is configuration, not a per-run policy
snapshot; changing configuration affects existing work. No workflow JSON changes were
needed. The calculator accepts an injected random supplier; tests replace it deterministically.

FailureClassifier returns RETRYABLE only for RetryableActivityException. Activity adapters
must deliberately use that type for known transient conditions. Unknown exceptions,
programming errors, IllegalArgumentException and provider idempotency-key misuse are
NON_RETRYABLE. Message strings and nested exception-message heuristics are never used.
Infrastructure/database write errors propagate; they are not reclassified as handler
failures. Ownership loss never becomes a retry request against the replacement owner.

For a failed attempt numbered n:

```text
cap(n) = min(maxDelay, initialDelay * multiplier^(n - 1))
full-jitter delay = floor(random in [0,1] * cap(n))
```

Production randomness is in [0,1); tests can also supply 1 to inspect the upper bound.
The result is always in [0, cap]. Log-space saturation prevents overflow for enormous
attempt counts/multipliers, and zero delay avoids 0 * infinity. No-jitter policy returns
the cap. Delays have millisecond precision.

Immediate synchronized retries can amplify an outage. Backoff reduces request rate,
jitter spreads workers' requests, and maxAttempts bounds cost. Full jitter can sample
zero; it does not promise a minimum wait or successful recovery.

## Durable transition and schema

V5 relaxes only the V4 READY lease constraint to allow a nonnegative retained generation.
V1–V4 are unchanged. No new columns or indexes: available_at, the two partial indexes and
ActivityExecution.error_message already provide schedule, selection and latest failure
information. Stop older binaries before upgrading; they do not understand READY/RUNNING
retry state. Flyway owns the schema; Hibernate continues to validate it.

After a handler exception, the heartbeat session stops. A REQUIRES_NEW READ COMMITTED
transaction locks task -> run, validates task/worker/generation/activity identity, and
checks the actual activity attempt count. For retryable failure with budget left and a
RUNNING workflow it keeps the logical activity RUNNING, records the latest exception,
and performs a final conditional task UPDATE:

```sql
UPDATE activity_task
SET status = 'READY', worker_id = NULL, claimed_at = NULL, lease_expires_at = NULL,
    available_at = clock_timestamp() + :delayMillis * INTERVAL '1 millisecond'
WHERE id = :taskId AND status = 'CLAIMED' AND worker_id = :workerId
  AND lease_generation = :generation AND lease_expires_at > clock_timestamp();
```

The implementation casts the duration for interval multiplication. Ownership is checked
at the final write after flushing logical changes. Zero affected rows throws
OwnershipLostException and rolls the entire transaction back, including error metadata.
The managed task is not mutated before this native update, preventing stale entity writes.

READY retry tasks have no owner/deadline but retain generation > 0. Candidate selection
requires available_at <= DB statement time and RUNNING logical activity. Initial READY
work still requires PENDING activity and generation zero. Claim revalidates after locks
using current DB time, grants a fresh lease/generation, and starts the next attempt.
Expired CLAIMED reclamation still uses SKIP LOCKED. Delayed tasks do not block eligible
work. Advancement recognizes READY/RUNNING retries as tracked in-flight work, so repeated
advance is safe and downstream dependencies remain blocked until upstream COMPLETED.

Non-retryable failure or exhaustion atomically marks task/activity/workflow FAILED via
the existing final ownership predicate. A sibling already in a FAILED run cannot schedule
new retries or reopen the workflow. Terminal tasks are not claim candidates.

## Attempt identity and exhausted crash recovery

Attempt 1 is the initial committed execution dispatch. Claiming a scheduled retry or
expired lease starts the next attempt and increments attempt_count; scheduling, polling,
heartbeats and lease renewal do not. started_at remains the logical first-start time.
The durable start boundary is the claim commit immediately before handler invocation:
a crash between that commit and Java method entry still consumes a reserved attempt.
This conservative boundary prevents crash loops from evading the budget; it cannot count
physical method entries atomically with PostgreSQL. This refines the existing Session 11
claim/start semantics rather than introducing a second start transaction.

Lease generation identifies ownership changes, not handler executions. In particular,
when a final allowed attempt crashes, the next claimant takes the locked expired task
only to finalize exhaustion. It increments ownership generation, fails task/activity/run
and returns no executable claim; attempt_count stays at maxAttempts. This recovery write
uses the locked row and a fresh ownership transfer, not a stale worker's outcome.

The same activity ID/key survives every retry and reclaim. The latest error remains
available even after later success; this is latest failure information, not event history.
attempt_count is a budget/visibility counter, not an exactly-once execution claim.

## Execution and guarantee boundary

Avoid a handler that catches an error, sleeps and calls itself again. It occupies a
worker and loses the schedule on death. Instead the handler throws, the worker classifies,
PostgreSQL stores future available_at, and the worker becomes free to poll other work.
Any worker can claim the due task after restart; no HTTP callback or startup-wide scan
is needed. Once a retry schedule commits, a worker dying before observing its return does
not erase it. If it dies before that commit, lease reclamation handles the abandoned claim.

Retrying effects is safe only when the provider/activity honors Session 12 idempotency.
The in-memory fake provider still does not share durable state across JVMs. Fencing
protects DurableFlow state; idempotency protects business effects. Not every failure is
retryable, and exhaustion can end in failure. No hard activity/workflow timeout is added:
a hanging handler can continue heartbeating indefinitely. Timeouts are a later session.

## Verification plan and current status

```powershell
mvn -DskipTests compile
mvn "-Dtest=RetryPolicyTest" test
mvn "-Dtest=RetryIntegrationTest,TaskLeaseIntegrationTest,PaymentIdempotencyIntegrationTest,PostgresTaskQueueIntegrationTest" test
mvn verify
```

RetryPolicyTest covers growth, cap, deterministic jitter bounds, overflow, validation and
typed classification. RetryIntegrationTest uses isolated PostgreSQL, deterministic random
samples and direct DB-time availability/expiry adjustments instead of sleeps. It covers
two transient failures then success with one payment effect, delay gating, attempts,
dependency blocking, repeated advancement, permanent errors, actual idempotency misuse,
exact exhaustion, heartbeat neutrality, stale retry rejection and expiry during logical
flush with rollback. A new worker object consumes persisted due work; this is not a
separate-JVM restart demonstration. Existing lease/idempotency/concurrency tests remain.

For a manual demo on a capable machine, build the jar, start Compose/server/workers as in
README, and inspect activity_task available_at, status, generation plus activity attempt_count
and error_message. Ordinary demo handlers succeed; the deterministic transient behavior
is intentionally test-only, so use RetryIntegrationTest to reproduce retries rather than
expecting ordinary order demos to fail artificially.

Implemented and statically checked only. Java, Maven and Docker are unavailable on the
authoring machine. Compilation, Flyway execution/mapping, all tests (including timing and
concurrency), startup and worker demos remain runtime unverified. Static checks are not
evidence that the PostgreSQL transitions or retry timings have executed successfully.
