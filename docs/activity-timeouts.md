# Session 14: attempt deadlines and graceful worker shutdown

| Mechanism | Bounds or protects |
| --- | --- |
| Lease | Durable ownership lifetime, extended by heartbeats |
| Activity timeout | How long the queued worker supervises one handler attempt |
| Network timeout | One external client operation; real adapters must configure it too |
| Retry policy | Whether/when another attempt can execute, subject to maxAttempts |
| Fencing | DurableFlow state against old/expired generations and late results |
| Idempotency | Business effects when timeout-triggered attempts repeat a provider call |

A 30-second lease renewed every 10 seconds can legitimately support a 60-second handler
deadline. These settings are independent. No workflow-wide timeout, user cancellation,
compensation, timer node or signal is added. Legacy synchronous development endpoints
remain compatibility scaffolding: the new supervision/drain contract is the queued worker
path, not a replacement production guarantee for the legacy executor.

## Configuration

| Property | Worker-profile environment | Default |
| --- | --- | --- |
| durableflow.worker.execution-timeout-ms | WORKER_EXECUTION_TIMEOUT_MS | 60000 |
| durableflow.worker.shutdown-grace-ms | WORKER_SHUTDOWN_GRACE_MS | 10000 |

Execution timeout must be positive; grace must be nonnegative (zero abandons immediately).
No workflow-definition JSON or schema change is needed. V1–V5 remain unchanged.

## Bounded execution and cancellation

HandlerExecutions owns one daemon handler thread and a bounded queue. The worker reserves
its single execution slot **before** claiming PostgreSQL work, then commits the normal
claim/start transaction. The handler runs on that thread, outside any database transaction.
The supervisor waits with Future.get(timeout), distinguishing normal result, original
handler exception, local ActivityTimeoutException, and shutdown/interrupted supervision.
The wait includes dispatch overhead; it does not grant additional time for a queued task.

On timeout, Future.cancel(true) requests cooperative interruption and the supervisor
stops that claim's heartbeat session. ActivityTimeoutException extends the explicitly
retryable domain exception and enters the existing fenced handleFailure transaction.
Budget remains available: READY + DB-time available_at, logical activity/workflow RUNNING.
Budget exhausted: task/activity/workflow FAILED. There is no separate timeout retry system.
One committed dispatch consumes one attempt under Session 13 semantics; timing out and
scheduling do not increment it again. The next claim increments attempt/generation.

The handler thread never writes an outcome on completion; only its supervising runOne
does. A cancelled Future cannot publish a later result. Independently, any attempted old
outcome or timeout reschedule must still pass the DB owner/generation/live-deadline fence.
The entire logical update rolls back if that fence fails.

**Bounded from the engine's perspective does not mean forcibly stopped.** Java interruption
cannot guarantee arbitrary handler code, native I/O or an external payment has stopped.
There is no Thread.stop. A handler ignoring interruption retains the physical execution
slot until it actually exits; this process declines new claims rather than spawning
unbounded replacement threads or consuming attempts it cannot dispatch. Other worker
processes can still claim/retry/recover shared PostgreSQL work. The daemon handler cannot
keep the JVM alive after normal application infrastructure exits.

Result persistence and heartbeat cleanup are separate short database operations; their
latency can extend wall-clock runOne/shutdown time beyond the handler wait deadline.
Existing five-second heartbeat transaction timeout is not an activity timeout, and driver,
pool and network settings still matter. No promise is made to kill a stuck JDBC/native
operation asynchronously or to turn maxAttempts into a global wall-clock deadline.

## Payment after local timeout

```text
A claims generation 1 and calls payment with logical key K
provider creates/remembers payment R
A's local supervision times out; external work may still be running
fenced retry transaction schedules the same task for later; A heartbeat stops
B claims generation 2 and sends the same K and payload
provider returns R without a second business effect
B persists R; late A result is discarded/rejected
```

The key remains df:v1:<ActivityExecution.id>. Fencing protects local state; provider
idempotency protects the effect. The Session 12 fake remains single-JVM/in-memory; it is
not durable across separate worker JVMs or provider restarts. Configure actual payment
client/network deadlines as well as this local supervisor deadline. Arbitrary external
systems are not made exactly-once by a timeout or key alone.

## Shutdown lifecycle

```text
stop accepting claims -> drain active supervisors -> stop infrastructure -> exit
```

ContextClosedEvent invokes TaskWorker.stop before bean destruction. It atomically closes
the claim gate, then waits for active runOne calls (handler plus outcome/advancement) up
to the configured grace. The poller closes on the same context event; any racing tick or
direct runOne call sees the closed gate. Active heartbeats continue while intentionally
draining. A valid result can complete and advance normally during grace, and an ordinary
attempt timeout during grace still follows retry policy.

If grace expires, supervisors are marked abandoned, Futures are cancelled/interrupted,
execution infrastructure closes and heartbeat infrastructure cancels its sessions/tasks.
Shutdown itself does not invoke handleFailure, force FAILED, reset READY or release the
lease prematurely. An unfinished claim remains CLAIMED/RUNNING until normal lease expiry
allows another worker to reclaim it. Supervisor interruption is abandonment, not a
business exception. An outcome transaction already in flight may finish under its valid
database fence; it cannot be safely undone by Java interruption.

All executor shutdown calls are idempotent. Handler shutdown never waits indefinitely
for non-cooperative user code; heartbeat teardown follows its existing cancellation and
bounded executor wait. A concurrent in-flight claim/DB call or heartbeat/session lock can
delay shutdown according to driver behavior. Clean exit of arbitrary external code is not
guaranteed, but correctness never relies on graceful shutdown: abrupt process death uses
lease expiry/reclamation and stale-owner fencing.

## Tests and verification

```powershell
mvn -DskipTests compile
mvn "-Dtest=HandlerExecutionsTest,WorkerHeartbeatsTest" test
mvn "-Dtest=TimeoutShutdownIntegrationTest,RetryIntegrationTest,TaskLeaseIntegrationTest,PaymentIdempotencyIntegrationTest,PostgresTaskQueueIntegrationTest" test
mvn verify
```

New coverage includes fast result/original exception, an actual short Future deadline
after a handler-entry latch, deterministic deadline injection, cooperative interruption,
non-cooperative capacity retention, timeout classification, and no old heartbeat renewal.
PostgreSQL tests cover retry/attempt budgets, same-key payment result reuse, late-result
fencing, stale timeout handling, completion during drain, a closed new-work gate, closed
infrastructure, and expired-lease recovery after grace exhaustion. Coordination uses
latches and short bounded condition waits, not long sleeps. Existing tests are adapted
to inject the shared managed execution service.

The tests model independent workers in one test JVM, sharing the fake provider as an
external-service fixture. They do not prove a real multi-JVM kill or graceful-exit demo.
For manual verification on a capable machine, build/start Compose and worker processes
using README commands and the settings above, then stop a worker with Ctrl+C. Ordinary
demo handlers are fast: they do not deliberately hang, so the reproducible timeout/drain
demonstrations are the test fixtures rather than a timing-sensitive shell kill.

Implemented and statically checked only. Java, Maven and Docker are unavailable here.
Compilation, tests, migration/startup, timeout and shutdown demos are runtime unverified.
