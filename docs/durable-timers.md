# Session 16: durable workflow timers

A timer persists an absolute PostgreSQL wake_at, releases worker resources, and resumes
the graph when the server dispatcher observes it due. Sleeping a worker thread or storing
a long-lived in-memory scheduled Future would lose the wait on restart and occupy
execution capacity. Neither is used for workflow timers.

## Definitions and compatibility

The existing steps array now supports two node types:

```json
{
  "name": "PaymentWaitShipment",
  "version": 1,
  "steps": [
    {"id": "payment", "type": "ACTIVITY", "activity": "chargePayment"},
    {"id": "wait", "type": "TIMER", "delayMillis": 10000, "dependsOn": ["payment"]},
    {"id": "shipment", "type": "ACTIVITY", "activity": "createShipment", "dependsOn": ["wait"]}
  ]
}
```

Missing/null type means ACTIVITY, preserving old HTTP definitions, stored JSON documents
and three-argument StepDefinition construction. Dependencies omitted mean a root.
ACTIVITY requires its handler name and rejects delayMillis. TIMER requires a positive
integer delayMillis and no handler name. The current supported delay maximum is 100
Julian years (3,155,760,000,000ms), avoiding unrepresentable/overflowing database deadlines.
No timer ActivityHandler is registered or required.

Normalization, duplicate IDs, missing/self dependencies and Kahn cycle detection operate
over the complete mixed graph. New stored definitions include explicit type. Existing
definition versions remain immutable. Legacy synchronous /execute rejects timer workflows
with 409 without failing the run; use /advance and queued workers for mixed graphs.

## Timer state and scheduling

V7 adds workflow_timer: UUID id, run FK, logical timer_id, WAITING/FIRED status, absolute
wake_at, DB created_at and nullable fired_at. Constraints enforce unique(run, timer_id),
valid status and matching fired_at state. The partial WAITING index on (wake_at, id)
matches due selection/order. The uniqueness index serves current-state reads by run.
V1–V6 are unchanged.
Upgrade server and workers together before registering TIMER definitions; older binaries
do not understand the mixed definition model. This is not a mixed-version rollout.

Run creation materializes ActivityExecution only for ACTIVITY nodes. On advancement,
NodeSatisfaction centralizes dependency and whole-run completion rules:

- ACTIVITY is satisfied when its logical execution is COMPLETED.
- TIMER is satisfied when its persisted timer is FIRED.

An eligible unscheduled TIMER is inserted WAITING under the run-row lock, with
wake_at = PostgreSQL clock_timestamp() + definition delay. TIMER_SCHEDULED is appended in
that same transaction, containing logical timerId, delayMillis and wakeAt. Repeated and
concurrent advancement reuse the existing row and exact deadline; uniqueness adds a
database guard. Waiting timers have no ActivityTask, lease, heartbeat or handler thread.

Definition delay is relative to activation when dependencies are observed satisfied.
Once scheduled, the absolute deadline is the source of truth: restarting at 13:00 must
not change an existing 14:00 deadline to restart-time plus another hour. A WAITING or
not-yet-scheduled required timer prevents completion, even if all activities are done.
Downstream activities/timers remain blocked until their dependencies are satisfied.

## Firing and lock order

Only server/orchestrator role installs the timer poller. It executes one short fire
transaction per fixed-delay tick, using this query:

```sql
SELECT t.* FROM workflow_timer t JOIN workflow_run r ON r.id=t.workflow_run_id
WHERE t.status='WAITING' AND t.wake_at <= statement_timestamp() AND r.status='RUNNING'
ORDER BY t.wake_at, t.id LIMIT 1 FOR UPDATE OF t SKIP LOCKED;
```

The transaction locks timer then run, rechecks run status, and updates only still-WAITING
timers whose wake_at <= current clock_timestamp(). It sets FIRED/fired_at using DB time
and appends TIMER_FIRED with timerId, wakeAt and firedAt. No external handler runs here,
so no lease is needed. The repository requires an existing transaction for lock/write
operations, preventing accidental auto-commit claim coordination.

The dispatcher then invokes advanceInTransaction (MANDATORY), which materializes eligible
tasks/timers or completes the run **in this same transaction**. Regular /advance retains
its REQUIRES_NEW entry point. There is no committed-FIRED/before-advance crash gap: failed
advancement or history insertion rolls the entire firing back to WAITING. Another poller
can retry it. No broker, asynchronous callback, outbox or startup-wide workflow scan is
required.

Run-first orchestration reads existing timer state without write-locking those timer
rows; it only inserts missing timers. This avoids a run->existing-timer versus timer->run
lock cycle. Per-run mutations/history allocation still serialize briefly on the run row.
Separate due runs can progress independently; locked timer rows are skipped. Multiple
due timers in one run may briefly serialize on that shared run, which is intentional.

Timers in FAILED runs remain inspectable WAITING but are not dispatched; this session
does not introduce cleanup/cancellation. Firing can be later than wake_at because of
poll interval, contention, backlog or downtime, but it is never intentionally earlier.
DB time is authoritative; this is not a real-time punctuality guarantee.

## Roles, configuration and shutdown

| Property | Environment | Default |
| --- | --- | --- |
| durableflow.timers.polling-enabled | TIMER_POLLING_ENABLED | true in server role |
| durableflow.timers.poll-delay-ms | TIMER_POLL_DELAY_MS | 500 |

Delay must be positive. Worker role never installs the timer poller, even if the common
polling-enabled property is true. One global server polling loop checks due rows; there
is no in-memory scheduled task per timer. The fixed delay prevents idle busy-spinning.

On context close, the poller gates off new dispatches. A racing/in-progress short
transaction can finish normally; the close gate itself does not interrupt it. Spring
owns scheduler teardown. If shutdown or process death interrupts the transaction, the
uncommitted firing rolls back and another instance can fire the persisted WAITING row.
Correctness does not depend on graceful shutdown. Server downtime makes timers late,
not lost, and no worker needs to remain alive during the wait.

## Visibility and history

GET /api/workflow-runs/{runId} retains activities for ACTIVITY nodes and adds a timers
array containing scheduled timer records: timerId, status, wakeAt, firedAt (plus UUID,
run UUID and createdAt). Unscheduled timer nodes have no row yet and are absent from that
array; their definition still determines run completion. This is immutable DTO data, not
a JPA entity. Full event history remains in the separate /events endpoint.

The history timeline explains payment ACTIVITY_COMPLETED -> TIMER_SCHEDULED(wakeAt=T) ->
TIMER_FIRED -> shipment ACTIVITY_SCHEDULED. Timer event activityExecutionId is null,
because timers are not logical activities; metadata.timerId identifies the timer node.
Polls and not-yet-due checks create no history noise. All timer state/history transitions
share the existing transaction and per-run sequence allocation.

## Documented demo

Build/start Compose, server and workers with the existing README commands, then:

```powershell
curl.exe -i -X POST http://localhost:8080/api/workflow-definitions -H "Content-Type: application/json" --data-binary "@examples/payment-wait-shipment-v1.json"
$body = @{ workflowName = "PaymentWaitShipment"; version = 1; input = @{ orderId = "TIMER-1" } } | ConvertTo-Json
$run = Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/workflow-runs -ContentType "application/json" -Body $body
$runId = $run.runId
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/workflow-runs/$runId/advance"
Invoke-RestMethod "http://localhost:8080/api/workflow-runs/$runId" | ConvertTo-Json -Depth 10
Invoke-RestMethod "http://localhost:8080/api/workflow-runs/$runId/events" | ConvertTo-Json -Depth 10
```

After payment, inspect WAITING and save its exact wakeAt. Stop the server while waiting;
workers need not stay running. Restart the server against the same database after the
deadline. The overdue timer should fire and queue shipment; start a worker to finish it.
Restarting earlier should preserve the deadline and leave it WAITING until due. The
example uses ten seconds for convenience; it is not a measured multi-hour durability
experiment. Real deployments must still honor the existing provider idempotency contract.

## Tests and verification status

```powershell
mvn -DskipTests compile
mvn "-Dtest=WorkflowDefinitionValidatorTest,TimerPollingConfigurationTest" test
mvn "-Dtest=DurableTimerIntegrationTest,WorkflowHistoryIntegrationTest,PostgresTaskQueueIntegrationTest" test
mvn verify
```

Nine PostgreSQL tests cover mixed completion/visibility/history, exactly-once scheduling,
two-poller firing races, locked-head skipping, fresh application contexts preserving
deadlines, overdue recovery, scheduling/firing event rollback, atomic downstream advancement,
timer chains and legacy rejection. Four new unit tests cover mixed definition validation,
JSON compatibility, poller shutdown/continuation and configuration. The existing worker
context test also checks that it has no timer poller. Prior isolated test cleanup includes
workflow_timer and startup now expects V7.

Tests disable background timer polling to coordinate dispatch explicitly. They use barriers,
latches and deliberate DB-time deadline adjustments instead of sleeping for an hour. The
fresh-context tests share preserved PostgreSQL data in the same test JVM; they are not a
separate-JVM kill demo, and backdating in fixtures simulates elapsed time rather than
altering the database clock or a production scheduling rule.

Implemented and statically checked only. Java, Maven and Docker are unavailable here.
Compilation, V7 execution/mapping, tests, startup and the timer demo remain runtime
unverified. The design supports durable waiting across restarts; no live durability or
multi-hour demo was performed. External signals and human approvals remain unsupported.
