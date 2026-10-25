# Session 20: durable workflow schedules

A TIMER pauses an existing workflow; a SCHEDULE creates new WorkflowRuns. Server/orchestrator
instances poll schedules; workers execute ordinary queue tasks. V1 supports START_AT and
FIXED_INTERVAL, without cron, dynamic fan-out or an external broker.

## API and V10 schema

POST /api/workflow-schedules accepts name, workflowName, exact version, input, type, startAt
(absolute timestamp) and intervalMillis for FIXED_INTERVAL. START_AT forbids an interval;
FIXED_INTERVAL requires a positive interval up to 100 Julian years. The definition must exist.
GET /api/workflow-schedules/{id} reads current state; /{id}/occurrences lists run references.
Names are unique. Editing/enabling/deleting schedules through HTTP is deferred.

| Table | Persisted fields/invariants |
| --- | --- |
| workflow_schedule | UUID/name UNIQUE, exact definition FK, input JSONB object, type, next_fire_at, interval_millis, enabled, DB timestamps |
| workflow_schedule_occurrence | UUID, schedule FK, scheduled_for, unique run FK, DB created_at, UNIQUE(schedule_id, scheduled_for) |

Exact immutable definition identity stays pinned when newer versions are registered. Inputs
are copied to each run, with no expression/data-mapping language. An occurrence's stable
identity is scheduleId + scheduledFor: the persisted nominal deadline, not actual poll time.
V10 also contains child composition schema; V1-V9 are unchanged.

## Atomic materialization and concurrency

ScheduleTransactions selects the earliest enabled due schedule using PostgreSQL
next_fire_at <= statement_timestamp() and FOR UPDATE SKIP LOCKED. No application clock or
long-lived scheduler lease is involved. Independent schedules can progress across servers.

One REQUIRES_NEW READ COMMITTED transaction locks a schedule, captures next_fire_at, checks
the occurrence identity, and creates a normally initialized run if absent. It inserts the
occurrence and advances the run's initial nodes through the existing orchestrator. Normal
WORKFLOW_STARTED history, initial tasks/gates/children and occurrence share the transaction.
No handler executes here. It then disables START_AT or advances FIXED_INTERVAL from its old
nominal deadline by the interval, and commits everything together.

A failure before commit rolls back run, executions, occurrence, history, initial work and
schedule changes. Another poll retries the same identity. A committed transaction includes
the changed deadline/disabled flag. Even a legacy/imported schedule rewound to an existing
occurrence reuses that run and advances the deadline without creating a duplicate. Uniqueness
is the final database defense. Normal runs remain subject to existing provider idempotency.

Interview scenario: committing the 02:00 run but not next_fire_at is impossible in this
transaction protocol. A crash before commit leaves neither; after commit leaves both. If a
legacy split state contains the 02:00 occurrence and old deadline, the identity lookup skips
run creation and advances to the next nominal deadline. This is one logical scheduled run,
not exactly-once external side effects.

## Misfires and bounded catch-up

A past START_AT fires once on recovery and disables. Fixed intervals retain every missed
nominal occurrence: next_fire_at advances by the interval, never directly to now or now+interval.
A five-hour hourly backlog remains durable until all missed occurrences are materialized.

Each server processes at most SCHEDULE_MAX_CATCH_UP occurrences (default 10, valid 1-1000)
per poll cycle across schedules, in separate short transactions. Remaining backlog stays in
next_fire_at for later polls (default 500ms). The cap is per instance, not cluster-global.
Earliest-deadline order may favor old backlog; V1 does not promise fairness or exact real-time
starts. Persistent configuration/materialization errors leave the schedule due and log the
failure; missed occurrences are not silently discarded or coalesced.

Restart never resets deadlines. The batch cap limits each polling cycle, not total future
volume. The worker queue supplies execution backpressure. Intervals are durations between
absolute instants; cron timezone/DST semantics are deferred.

## Demo

Start database/server/worker and register ShippingChild from the child demo. Override the
examples' sample timestamps with near-current instants for a controlled demonstration:

```powershell
$base = 'http://localhost:8080/api'
$once = Get-Content examples/schedule-start-at.json -Raw | ConvertFrom-Json
$once.name = 'ShippingOnce-' + [guid]::NewGuid().ToString()
$once.startAt = [DateTimeOffset]::UtcNow.AddSeconds(5).ToString('o')
$schedule = Invoke-RestMethod -Method Post -Uri "$base/workflow-schedules" -ContentType 'application/json' -Body ($once | ConvertTo-Json -Depth 20)
Invoke-RestMethod -Uri "$base/workflow-schedules/$($schedule.id)"
Invoke-RestMethod -Uri "$base/workflow-schedules/$($schedule.id)/occurrences"
```

Poll after the deadline: one occurrence references one run and enabled is false. Follow its
run/history URLs. For a bounded recurring backlog, use a recent start rather than the example's
sample date:

```powershell
$recurring = Get-Content examples/schedule-fixed-interval.json -Raw | ConvertFrom-Json
$recurring.name = 'ShippingHourly-' + [guid]::NewGuid().ToString()
$recurring.startAt = [DateTimeOffset]::UtcNow.AddHours(-3).ToString('o')
$recurring.intervalMillis = 3600000
$hourly = Invoke-RestMethod -Method Post -Uri "$base/workflow-schedules" -ContentType 'application/json' -Body ($recurring | ConvertTo-Json -Depth 20)
Invoke-RestMethod -Uri "$base/workflow-schedules/$($hourly.id)/occurrences"
```

Stop/restart only the HTTP server, retaining the PostgreSQL volume, for downtime catch-up.
SCHEDULE_POLLING_ENABLED=false disables automatic materialization in controlled experiments;
re-enable to recover. Schedule HTTP and both composition pollers are absent in worker profile.

## Tests, files and verification

WorkflowScheduleIntegrationTest adds nine PostgreSQL scenarios: future/due DB-time decisions,
one-time disabling, concurrent multi-context pollers, rollback before deadline update,
occurrence replay, interval/catch-up bounds, restart, exact-version pinning and validation.
ChildWorkflowIntegrationTest separately covers child concurrency/recovery. Integration cleanup
includes the three new tables and the Flyway expectation becomes V10.

Main additions are workflow/child/*, workflow/schedule/*, config/CompositionPollingConfiguration,
V10, both integration suites, child definition tests, JSON demos and these two documents.
Definition/document/validator, run initializer/response/entity, NodeSatisfaction, orchestration,
Saga settlement, history vocabulary and worker-role assertions integrate the additions.

Java, Maven and Docker are unavailable on PATH. Compilation, V10 execution, full suites,
concurrency/restart tests, startup and both live demos are not runtime verified. Static checks
cover Java syntax/imports/packages, Maven XML, JSON, preserved earlier migrations and whitespace.
Run mvn verify and live demos with JDK 21/Maven/Docker before claiming runtime success.
No cron, dynamic mapping, child cancellation, successful-child undo contract or AI layer is added.

8. A timer waits inside one run; a schedule creates new runs.
9. Stable occurrence identity prevents duplicate starts on repeated polling/recovery.
10. Due one-time schedules fire once; missed intervals catch up in bounded batches without loss.
