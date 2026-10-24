# DurableFlow

A durable workflow orchestration engine built incrementally for learning and SDE interview preparation.

DurableFlow separates orchestration from activity execution through a durable PostgreSQL task queue. Independent non-HTTP workers use FOR UPDATE SKIP LOCKED, renewable leases and heartbeats; expired claims can be reclaimed, and stale worker results are rejected. Stable activity keys support provider idempotency, and explicitly transient failures receive durable bounded retries with backoff and jitter. Earlier synchronous development/recovery endpoints remain available for unqueued runs. See [retry design and verification](docs/retries.md). Runtime verification, the two-worker demo and a manual JVM-crash experiment remain outstanding.

## Prerequisites

- JDK 21 (JAVA_HOME and java on PATH).
- Maven 3.9.x (mvn on PATH).
- Docker Engine 24+ or Docker Desktop with Linux containers, and Docker Compose v2.
- A running Docker daemon for both local PostgreSQL and Testcontainers tests.

## Run locally

From the repository root:

```powershell
docker compose up -d --wait
mvn spring-boot:run
```

In another terminal:

```powershell
Invoke-RestMethod http://localhost:8080/actuator/health
```

Expected response: {"status":"UP"}. Flyway applies V1 before Hibernate validates the schema and the HTTP application becomes ready. Health includes the database connection.

Stop the application with Ctrl+C. Stop infrastructure with:

```powershell
docker compose down
```

The named postgres_data volume preserves database data across container restarts. Removing the volume deletes local data.

## Tests

```powershell
mvn verify
```

The integration tests start an isolated PostgreSQL 17.6 Testcontainers database, apply Flyway migrations, start the HTTP application with Hibernate validation, check health, and round-trip all three entities. Docker is required; tests deliberately do not silently skip when Docker is missing.

## Configuration

| Variable | Local default | Purpose |
| --- | --- | --- |
| DB_NAME | durableflow | Database name |
| DB_USERNAME | durableflow | Database user |
| DB_PASSWORD | durableflow_local | Local development password |
| DB_PORT | 5432 | Host PostgreSQL port |
| DB_URL | jdbc:postgresql://localhost:5432/durableflow | Optional full JDBC URL override |

For overrides, set environment variables in the terminal before running both Compose and Maven. For example:

```powershell
$env:DB_PORT = "5433"
$env:DB_PASSWORD = "my-local-password"
docker compose up -d --wait
mvn spring-boot:run
```

Compose also reads .env, but Spring does not automatically read that file. Export matching variables when using Maven. PostgreSQL initialization variables apply only to a fresh volume; changing credentials does not update an existing database.

## Files and responsibilities

- pom.xml: Java 21, Spring Boot 4.1.2, framework-managed dependency versions.
- docker-compose.yml: PostgreSQL only, localhost port binding, health check and named volume.
- application.yml: environment configuration, Flyway and ddl-auto=validate.
- V1__create_workflow_tables.sql: authoritative schema with keys and constraints.
- workflow/definition, workflow/run, workflow/activity: JPA entities, enums and CRUD repositories.
- PersistenceIntegrationTest: migration, startup, health and PostgreSQL persistence checks.

## Persistence decisions

workflow_definition stores a versioned JSON recipe, unique by (name, version), with positive versions.
workflow_run references one definition and stores input and lifecycle timestamps.
activity_execution references one run, unique by (workflow_run_id, activity_id), and stores nullable input/output, lifecycle timestamps and a nonnegative attempt counter.

IDs use generated UUIDs; timestamps use Instant/TIMESTAMPTZ. Callers explicitly supply timestamps; lifecycle behavior will be introduced in later sessions. JSONB uses Hibernate @JdbcTypeCode(SqlTypes.JSON) with Map<String, Object> for portable JSON values. Status enums use string mappings and matching database CHECK constraints.

Foreign keys are mapped as UUID fields rather than ORM associations, keeping persistence simple and avoiding implicit graph loading. The database enforces references. There are no cascading deletes.

Flyway's Boot starter enables migration auto-configuration; flyway-database-postgresql supplies PostgreSQL support. Hibernate only validates and never creates or updates tables. Once applied, migrations should remain unchanged; evolve the schema with a new numbered migration.

## Session 5 verification status

On the authoring machine, Java, Maven and Docker are unavailable on PATH. Compilation, integration/concurrency tests, migrations and application startup remain unverified through Session 10. Run the commands above after installing the prerequisites. XML/JSON parsing, Java package/path checks and Git whitespace checks are the available static checks. Neither the two-worker JVM demo nor a manual JVM crash/restart experiment has run.

## Session 6 API examples

Start PostgreSQL and the application using the commands above. Run these commands from the repository root in PowerShell. Use curl.exe to avoid the Windows PowerShell curl alias (on Linux/macOS, use curl instead).

Register the immutable OrderFulfillment v1 recipe (201 Created):

```powershell
curl.exe -i -X POST http://localhost:8080/api/workflow-definitions -H "Content-Type: application/json" --data-binary "@examples/order-fulfillment-v1.json"
```

Read the recipe:

```powershell
curl.exe http://localhost:8080/api/workflow-definitions/OrderFulfillment/versions/1
```

Start a run (201 Created):

```powershell
curl.exe -i -X POST http://localhost:8080/api/workflow-runs -H "Content-Type: application/json" --data-binary "@examples/start-order.json"
```

Copy runId from the response, then inspect persisted state:

```powershell
$runId = "replace-with-returned-runId"
curl.exe "http://localhost:8080/api/workflow-runs/$runId"
```

The run is RUNNING with a startedAt timestamp in the database. All three logical activities are PENDING with attempt_count zero and no execution timestamps. Session 6 does not execute activities or determine eligibility. PENDING means unfinished, and does not mean dependencies are satisfied.

POST and GET responses use DTOs and never expose JPA entities. Run responses include runId, workflowName, version, status, input and activity states (activityId and status). GET lists activities by logical ID for deterministic output; this ordering is not an execution plan.

Definition registration trims names, step IDs, activity names and dependency references, defaults omitted/null dependsOn to an empty list, and removes repeated dependency references. Typed Bean Validation checks required fields and database length limits. The service rejects duplicate step IDs, unknown/self dependencies and cycles using Kahn's algorithm in O(V + E) time. No recursion is needed. Name and logical IDs are case sensitive.

Definitions are immutable through the API: there is no update or delete operation. Register a new version to change a recipe. Duplicate (name, version) is rejected by both the service and the database unique constraint, including concurrent registrations.

WorkflowRunService.start is the transaction boundary: it resolves the stored recipe, creates the RUNNING run, and inserts exactly one PENDING activity per step. All inserts commit together or roll back together. Activity input/output remain null until a future execution session. The run input must be a JSON object (use {} when empty).

Errors consistently use {"status":400,"message":"..."}. Invalid requests/graphs return 400, missing definitions/runs return 404, duplicate definitions or persistence conflicts return 409, and unexpected failures return a generic 500. Server details are not returned to clients.

### Session 6 tests

```powershell
mvn -Dtest=WorkflowDefinitionValidatorTest test
mvn verify
```

The validator unit tests cover sequential/branching graphs, duplicate IDs, missing/self dependencies, multi-node/disconnected cycles, normalization and invalid fields. They need Java/Maven but not Docker.

WorkflowApiIntegrationTest uses real PostgreSQL and HTTP to check definition registration/retrieval/duplicates, input errors, run retrieval and initial row states. A test-only PostgreSQL trigger forces an activity insert failure and verifies that neither the run nor any activity remains after rollback. The trigger is removed in finally and is never part of production migrations.

No Session 6 schema changes were needed; V1 remains unchanged. No workers, queues, execution engine or external infrastructure were added.

## Session 7: execute the first workflow

Start PostgreSQL and the app as above, then register and materialize a run. These PowerShell commands use the existing example JSON files:

```powershell
curl.exe -i -X POST http://localhost:8080/api/workflow-definitions -H "Content-Type: application/json" --data-binary "@examples/order-fulfillment-v1.json"
curl.exe -i -X POST http://localhost:8080/api/workflow-runs -H "Content-Type: application/json" --data-binary "@examples/start-order.json"
$runId = "replace-with-returned-runId"
curl.exe "http://localhost:8080/api/workflow-runs/$runId"
curl.exe -i -X POST "http://localhost:8080/api/workflow-runs/$runId/execute"
curl.exe "http://localhost:8080/api/workflow-runs/$runId"
```

If OrderFulfillment v1 already exists, registration returns 409; reuse that definition. Creating a run still only materializes it: RUNNING run, PENDING activities, and no handler calls. The explicit execute request runs the workflow synchronously and returns its final DTO. Successful execution returns 200 with a COMPLETED run and activity outputs:

| Logical step | Handler type | Deterministic output |
| --- | --- | --- |
| payment | chargePayment | {"paymentId":"pay-demo-<stable UUID for this logical activity>"} |
| inventory | reserveInventory | {"reservationId":"res-demo-456"} |
| shipment | createShipment | {"shipmentId":"ship-demo-789"} |

These are demo handlers with no real payment, inventory or shipping integration. Every handler receives the workflow input; outputs are stored on the activity, without automatic output-to-input propagation.

### Engine and state transitions

ActivityRegistry indexes injected ActivityHandler implementations by stable type. Duplicate types fail bean creation. WorkflowEngine reloads the stored definition and activity states before every decision, validates the stored graph, and requires an exact match between logical steps and execution rows. It picks the first PENDING step in definition order whose dependencies are all COMPLETED. Multiple eligible roots execute sequentially; array ordering never overrides dependencies.

The engine uses controlled entity methods for PENDING -> RUNNING -> COMPLETED/FAILED. Starting an activity records its input, started_at, updated_at and increments attempt_count from zero to one. Completion records output, completed_at and updated_at. A run becomes COMPLETED only after every expected activity row is COMPLETED. Production uses an injected UTC Clock; integration tests replace it with a fixed clock.

A missing handler or thrown handler exception records activity FAILED with error_message, marks the run FAILED in the same transaction, and stops without executing dependents. The execute request returns 200 with the FAILED run representation: this is an execution outcome, not an HTTP transport failure. GET includes activity output and errorMessage. Diagnostics contain a bounded exception type/message, without stack traces.

Missing runs return 404. Terminal/non-RUNNING runs and simultaneous execute calls through the same engine object return 409. Invalid stored graphs, missing/extra activity rows and no-progress states mark the run FAILED and return a clear 409. Existing RUNNING activities return RECOVERY_REQUIRED without changing persisted state. The in-memory guard remains local; Session 9 adds run-row locking for checkpoint decisions across cooperating engine instances. This is still a development engine without distributed worker ownership, leases or automatic reclamation.

### Short checkpoint transactions

WorkflowEngine refuses an enclosing transaction. It calls a separate Spring bean, WorkflowEngineState, whose methods use new, short PostgreSQL transactions:

1. Reload state, choose an eligible step, persist RUNNING and commit.
2. Execute the handler **without a database transaction**.
3. Persist COMPLETED/output, or atomically persist activity FAILED and workflow FAILED; commit.
4. Reload state and choose the next eligible step, or complete the workflow.

The engine never holds a database transaction open around arbitrary handler code. Every successful iteration durably completes another activity or terminates; invalid/no-progress state produces an error rather than a polling loop. Persistence failures propagate and do not trigger an automatic repeat of the handler.

This intentionally leaves a crash window: a side effect could succeed before COMPLETED is committed. PostgreSQL would still report RUNNING. Session 8 implements explicit reconstruction from COMPLETED/PENDING checkpoints, while uncertain RUNNING activities remain unsupported for automatic recovery. Session 10 adds queued worker execution as the normal path, with no retries/idempotency yet. The synchronous execute endpoint remains temporary compatibility scaffolding for unqueued runs.

### Migration and verification

V2__add_activity_failure_details.sql adds nullable TEXT error_message to activity_execution. V1 is unchanged. Flyway remains the schema owner and Hibernate still uses ddl-auto=validate. The current startup test expects version 3 after Session 10's queue migration.

```powershell
mvn "-Dtest=WorkflowDefinitionValidatorTest,ActivityLifecycleTest,ActivityRegistryTest" test
mvn verify
```

The unit tests cover registry lookup/duplicate types, deterministic demos and valid/invalid lifecycle transitions. WorkflowEngineIntegrationTest adds PostgreSQL tests for dependency ordering, two eligible roots and a join, persisted outputs/timestamps, missing/throwing handlers, terminal-run rejection, impossible/interrupted states, corrupt definitions and the HTTP execute/GET flow. Test handlers inspect committed database state and verify no database transaction is active while they execute. Runtime tests, migrations and startup still require the Java/Maven/Docker prerequisites and remain unverified on the authoring machine.

## Crash Recovery Demo

**Status: implemented and statically checked; the automated restart tests and this manual demo are unverified on the authoring machine.** Java 21, Maven and Docker are required. Recovery is explicit: startup never scans or automatically executes RUNNING workflows.

### Pause at a safe checkpoint

In terminal 1, start the database and enable the temporary development checkpoint endpoint:

```powershell
docker compose up -d --wait
mvn spring-boot:run "-Dspring-boot.run.profiles=dev"
```

In terminal 2, register the same three-step OrderFulfillment recipe and start a fresh run. If registration returns 409 because v1 already exists, reuse it:

```powershell
curl.exe -i -X POST http://localhost:8080/api/workflow-definitions -H "Content-Type: application/json" --data-binary "@examples/order-fulfillment-v1.json"
$run = Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/workflow-runs -ContentType "application/json" -InFile examples/start-order.json
$runId = $run.runId
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/dev/workflow-runs/$runId/execute?maxActivities=2" | ConvertTo-Json -Depth 10
$beforeRestart = Invoke-RestMethod "http://localhost:8080/api/workflow-runs/$runId"
$beforeRestart.activities | Format-Table activityId,status
```

Expected persisted state: run RUNNING, payment COMPLETED, inventory COMPLETED, shipment PENDING. The limit counts successful activities during this invocation; it stops before recording RUNNING for the next step. No sleep, paused thread or uncommitted transaction is needed. A budget that completes the whole graph still finalizes the workflow. Zero/negative budgets return 400 without changing the run.

The temporary endpoint is POST /api/dev/workflow-runs/{runId}/execute?maxActivities=N, available only with the dev profile. The ordinary POST /api/workflow-runs/{runId}/execute remains unchanged and runs until completion/failure. Run creation still performs no activity execution.

### Stop, restart, inspect, resume

Stop only the Spring app in terminal 1 with Ctrl+C. Keep PostgreSQL running and preserve its named volume. Restart the app without the dev profile:

```powershell
mvn spring-boot:run
```

Keep terminal 2 open so its runId variable survives. Inspect the same run before resuming; startup must not have advanced it:

```powershell
$afterRestart = Invoke-RestMethod "http://localhost:8080/api/workflow-runs/$runId"
$afterRestart.activities | Format-Table activityId,status
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/workflow-runs/$runId/execute" | ConvertTo-Json -Depth 10
Invoke-RestMethod "http://localhost:8080/api/workflow-runs/$runId" | ConvertTo-Json -Depth 10
```

Expected: only createShipment appears in the restarted app's execution log for this run, all three activities become COMPLETED, and the run becomes COMPLETED. Payment/inventory outputs are preserved. Repeating execute on the terminal run returns 409.

For database evidence, run this query before stopping and after resuming (adapt the local default user/database if overridden):

```powershell
docker compose exec postgres psql -U durableflow -d durableflow -c "SELECT activity_id, status, attempt_count, started_at, completed_at FROM activity_execution WHERE workflow_run_id = '$runId' ORDER BY activity_id;"
```

Payment/inventory keep their original timestamps and attempt_count=1. Shipment changes from PENDING/attempt_count=0 to COMPLETED/attempt_count=1. Deterministic output values alone would not prove absence of repeated execution; the automated test additionally checks handler invocation counters.

This orderly stop demonstrates context/process restart from a committed boundary, rather than an unexpected JVM crash. For a separate manual forced-termination experiment, first reach the same two-completed/one-pending checkpoint. In another terminal run jps -l, identify the exact io.durableflow.DurableFlowApplication JVM, then use its PID:

```powershell
$appProcessId = 12345 # Replace with the verified application JVM PID from jps -l.
Stop-Process -Id $appProcessId -Force
```

Restart the app and repeat the inspect/resume commands. Target only that application JVM, keeping PostgreSQL alive. This forced-termination experiment is documented but has not been run here. There is no claim of a completed JVM-crash test.

### The separate uncertain RUNNING case

PENDING -> RUNNING committed -> external side effect -> JVM dies before COMPLETED is committed leaves an uncertain execution. The side effect may have succeeded. Session 8 never resets this row to PENDING, invokes its handler again, or assumes it failed. The next execute request returns HTTP 409 with status/message plus code=RECOVERY_REQUIRED, runId and the uncertain logical activityIds. The run stays RUNNING; all existing activity states, outputs, attempts and timestamps remain available through GET/database inspection. No new ORPHANED status or migration is required.

A FAILED activity is still terminal and is never retried; ordinary handler failures still fail both activity and workflow. Orphaned RUNNING is deliberately different from a confirmed handler failure. Later sessions will address at-least-once execution, idempotency and safe recovery/reclamation semantics.

### Recovery architecture and test boundary

Recovery is reconstruction, not resurrection: a new process reads the immutable definition and durable logical state, then recomputes eligible PENDING steps. It does not restore old threads, stack frames, instruction pointers or JVM objects. COMPLETED steps never become eligible; normal execution and recovery use the same engine/checkpoint methods. The per-invocation execution budget and per-process concurrency guard are not recovery history.

Implemented guarantee boundary: resume between activities after successful completion is durably committed. Unsupported automatic recovery: a crash during an externally side-effecting RUNNING activity. The workflow's logical lifetime can outlast the orchestrator process, so run-level RUNNING does not require a live execution thread.

```powershell
mvn "-Dtest=WorkflowRecoveryIntegrationTest,WorkflowEngineIntegrationTest" test
mvn verify
```

WorkflowRecoveryIntegrationTest starts a complete Spring web application against one Testcontainers PostgreSQL database, executes two activities, closes the entire application context (HTTP server, connection pool, JPA state, engine, registry and counters), and starts a new application context on the same database. Only the run ID and immutable assertion snapshots cross the boundary. Fresh handler counters must show only shipment executing, while completed row identities, outputs, timestamps and attempt counts remain unchanged. A second test restarts with payment COMPLETED, inventory RUNNING and shipment PENDING, checks the recovery-specific exception/HTTP response, and verifies no handler runs and no persisted checkpoint changes. Additional engine tests cover budget boundaries and terminal failure behavior. The Session 7 interrupted-state assertion is updated to the intentional Session 8 behavior: preserve uncertainty instead of marking the workflow FAILED.

These are real application-context restart tests when run, but they do not kill or start a separate JVM. Implemented: yes. Static checks: performed. Runtime/migration/startup verification: blocked by missing tools. Manual forced JVM-crash test: not performed. Existing V1/V2 migrations, Hibernate validation and the PostgreSQL schema remain unchanged in Session 8.

## Session 9: concurrent checkpoint safety

Two independent engines could previously both read the same PENDING row and commit
RUNNING before each called its handler. @Transactional and the local in-memory guard
did not coordinate those separate instances.

Every engine checkpoint now acquires a pessimistic write lock on its workflow_run row
before inspecting activity state. A contender waits for the short transaction to commit,
then re-reads the committed checkpoint. Existing RUNNING work returns the conservative
409 RECOVERY_REQUIRED response without executing a handler or changing persisted state.
The response now explicitly acknowledges that the other invocation may still be active.
Completion, failure and graph finalization use the same lock protocol. Database locks
are released before handler execution. No APIs, migrations or statuses were added.

@Version was evaluated and deferred: the shared run lock already coordinates every
current engine writer and protects terminal transitions. No new version column is needed.
The [concurrency engineering notes](docs/concurrency.md) document the original race,
SQL shape, re-read behavior, lock ordering, limitations, and row-lock/lease/idempotency
distinctions.

```powershell
mvn "-Dtest=WorkflowConcurrencyIntegrationTest" test
mvn "-Dtest=WorkflowEngineIntegrationTest,WorkflowRecoveryIntegrationTest" test
mvn verify
```

New tests use barriers/latches and independent engine objects to check one claim winner,
one handler invocation, unchanged contender-visible RUNNING state, valid completion/failure,
and terminal-run protection. A test-only unsafe legacy example demonstrates the old race.
The test handler also acquires the run lock in a separate transaction to check that no
claim lock remains held during its work. These tests and full runtime verification remain
unexecuted here because Java, Maven and Docker are missing. Crash-during-side-effect
recovery, retries, idempotency, leases and the task queue remain outside Session 9.

## Session 10: server plus two worker processes

**Implemented, but the runtime demo and tests are unverified on this machine.** The normal
architecture is now orchestrator -> PostgreSQL activity_task -> independent workers.
See [PostgreSQL task queue engineering notes](docs/postgres-task-queue.md) for the exact
claim SQL, index rationale, transaction/lock ordering and failure boundaries.

Build once (Java 21/Maven/Docker required), then launch the same artifact in three terminals:

```powershell
docker compose up -d --wait
mvn verify
```

Terminal 1: server/orchestrator. It exposes the HTTP APIs and does not poll tasks:

```powershell
java -jar target/durableflow-0.0.1-SNAPSHOT.jar
```

Before starting workers, register OrderFulfillment, materialize a run, and authorize its
first task. A registration 409 means the same version already exists and may be reused:

```powershell
curl.exe -i -X POST http://localhost:8080/api/workflow-definitions -H "Content-Type: application/json" --data-binary "@examples/order-fulfillment-v1.json"
$run = Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/workflow-runs -ContentType "application/json" -InFile examples/start-order.json
$runId = $run.runId
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/workflow-runs/$runId/advance" | ConvertTo-Json -Depth 10
Invoke-RestMethod "http://localhost:8080/api/workflow-runs/$runId" | ConvertTo-Json -Depth 10
```

POST /workflow-runs still only creates logical state. POST /advance creates the READY
payment task while the activity remains PENDING. Repeating advance does not duplicate it.

Optional: authorize two independent roots before launching workers, using the existing
DAG semantics (shipment requires both roots to complete):

```powershell
curl.exe -i -X POST http://localhost:8080/api/workflow-definitions -H "Content-Type: application/json" --data-binary "@examples/independent-checks-v1.json"
$body = @{ workflowName = "IndependentChecks"; version = 1; input = @{ orderId = "ROOTS-1" } } | ConvertTo-Json
$roots = Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/workflow-runs -ContentType "application/json" -Body $body
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/workflow-runs/$($roots.runId)/advance" | ConvertTo-Json -Depth 10
```

Terminal 2: worker A, with no HTTP port or workflow API:

```powershell
java -jar target/durableflow-0.0.1-SNAPSHOT.jar --spring.profiles.active=worker --durableflow.worker.id=worker-A
```

Terminal 3: worker B, independently polling the same database:

```powershell
java -jar target/durableflow-0.0.1-SNAPSHOT.jar --spring.profiles.active=worker --durableflow.worker.id=worker-B
```

Use matching DB_URL/DB_NAME/DB_USERNAME/DB_PASSWORD/DB_PORT settings in every terminal
when overriding local defaults. Worker processes generate their own identity if none is
configured. Polling runs sequentially per worker with a default 500ms fixed delay after
each tick; configure durableflow.worker.poll-delay-ms or WORKER_POLL_DELAY_MS to change it.
Server role is the default; only the worker profile enables polling and disables the web server.

Workers commit CLAIMED plus activity RUNNING before calling a demo handler, then atomically
persist both outcomes. Successful completion calls lightweight advancement to enqueue the
next eligible task or finish the workflow, without an HTTP callback or event bus.

Inspect from another terminal (PostgreSQL commands below use the default local credentials):

```powershell
Invoke-RestMethod "http://localhost:8080/api/workflow-runs/$runId" | ConvertTo-Json -Depth 10
docker compose exec postgres psql -U durableflow -d durableflow -c "SELECT t.id, a.workflow_run_id, a.activity_id, t.status AS task_status, a.status AS activity_status, t.worker_id, t.claimed_at FROM activity_task t JOIN activity_execution a ON a.id=t.activity_execution_id ORDER BY t.created_at, t.id;"
```

Each task has at most one claim owner. Different workers can claim different unlocked
tasks, including independently eligible roots; joins still wait for all dependencies.
Very fast demos do not guarantee fair distribution between workers or both worker IDs
appearing in a small sample. The locked-head/concurrent-claim tests provide deterministic
coverage of this capability. This demo is documented, not evidence that it ran here.

At the Session 10 checkpoint, a worker dying after CLAIMED left its task stuck. Session 11
adds leases, heartbeats and reclamation as described below. If a worker dies after a successful outcome
but before advancement, repeat POST /advance: completed activities are never rerun.
Stop each application with Ctrl+C and stop
PostgreSQL with docker compose down, preserving the named volume.

The old /execute and dev-budget endpoints now delegate to WorkflowLocalExecutor solely
for Session 7-9 compatibility. They reject any run with queued tasks. A run paused at a
COMPLETED/PENDING boundary may switch to /advance, which queues only remaining work.
Untracked RUNNING activity state still returns RECOVERY_REQUIRED rather than being retried.

V3__create_activity_task.sql adds the task table, foreign key, one-task-per-activity
uniqueness, status/owner checks and a partial READY index on (available_at, created_at, id).
V1/V2 remain unchanged; Hibernate still validates the Flyway-owned schema.

```powershell
mvn "-Dtest=ActivityTaskLifecycleTest" test
mvn "-Dtest=PostgresTaskQueueIntegrationTest" test
mvn verify
```

New tests cover queue concurrency, locked-head skipping, worker outcomes/rollback,
dependency advancement, ownership, an independent non-web worker context and adoption
of persisted partial progress. Existing Session 8/9 suites remain in the full test run.
Compilation, migrations, tests, server/worker startup and the two-JVM-worker demo remain
unverified because Java, Maven and Docker are unavailable here.

## Session 11: leases, heartbeats and reclamation

V4 adds lease_expires_at, a monotonic lease_generation and an expired-CLAIMED partial
index. V1–V3 remain unchanged. Stop older application processes before upgrading:
historic claims become immediately reclaimable; old binaries cannot enforce leases.

Workers now claim READY or expired CLAIMED tasks under the same SKIP LOCKED transaction.
Claim/reclaim assigns a fresh DB-time deadline, increments generation, and preserves the
logical activity ID. Reclamation leaves the activity RUNNING and increments attempt_count.
Heartbeats renew only the current unexpired owner/generation. Final outcome writes check
ownership and DB time atomically; rejection rolls back logical changes too.

Worker-profile defaults are a 30000ms lease, 10000ms heartbeat and 500ms polling delay.
Configure WORKER_LEASE_DURATION_MS, WORKER_HEARTBEAT_INTERVAL_MS and WORKER_POLL_DELAY_MS,
or the corresponding durableflow.worker properties. Heartbeats must be positive and at
most one third of the lease. A failed/unconfirmed renewal abandons the local attempt.
Shutdown stops new claims, cancels heartbeats and leaves leases to expire without resetting
tasks. Arbitrary external handler work cannot always be cancelled.

Reclamation permits at-least-once execution. The token protects DurableFlow database
outcomes, including reused worker IDs; arbitrary external effects still need Session 12
idempotency. Ordinary handler failures remain terminal, with no automatic retry/backoff.

[Lease documentation](docs/task-leases.md) includes the A/B zombie-worker timeline,
claim SQL, lock/lease distinction, shutdown limitations, configuration, inspection and
kill/reclaim guidance. New tests cover DB-time ownership, concurrent reclamation,
late-result rollback, heartbeat cancellation and shutdown without sleep-heavy coordination.

```powershell
mvn "-Dtest=WorkerHeartbeatsTest,ActivityTaskLifecycleTest" test
mvn "-Dtest=TaskLeaseIntegrationTest,PostgresTaskQueueIntegrationTest,WorkflowConcurrencyIntegrationTest" test
mvn verify
```

Session 11 is implemented and statically checked. Compilation, migrations, all tests,
startup and process demos remain unverified because Java, Maven and Docker are unavailable.

## Session 12: stable keys and provider idempotency

Handlers now receive ActivityExecutionContext (workflow run ID, logical activity ID and
derived `df:v1:<activityExecutionId>` key). Both queued and legacy local execution supply
this context. The key stays unchanged across workers and lease generations; no schema
change or redundant key column is needed. Existing attempt counters remain observable.

chargePayment uses a small in-memory FakePaymentProvider. The provider atomically records
one canonical payload and payment result per key. Same key/payload returns the original
payment ID; changed payload is rejected. Fencing still protects DurableFlow's state while
provider idempotency protects the simulated external effect.

The fake is shared within one application/test process only. It loses its memory on
restart, and separate worker JVMs have separate fake instances. It demonstrates the
provider contract, not durable multi-process payment deduplication. Deterministic IDs
alone do not prove deduplication. Real integrations need a provider that honors the key
or an application-specific durable strategy. No universal exactly-once claim is made.

[Idempotency notes](docs/idempotency.md) explain the crash-after-payment window, provider
limitations and test procedure. Reclamation remains the only repeat-execution mechanism
added so far; ordinary handler failures do not receive automatic retries.

```powershell
mvn "-Dtest=PaymentIdempotencyTest,ActivityRegistryTest" test
mvn "-Dtest=PaymentIdempotencyIntegrationTest,TaskLeaseIntegrationTest,PostgresTaskQueueIntegrationTest" test
mvn verify
```

Session 12 implementation and static checks are complete. Tests, migrations/startup and
process demos remain unverified because Java, Maven and Docker are unavailable here.

## Session 13: durable retries, backoff and budgets

Queued workers now classify explicit RetryableActivityException failures for bounded
retries; unknown/programming errors and idempotency-key misuse fail immediately.
Defaults: maxAttempts 3 total, initial delay 1000ms, multiplier 2, max delay 30000ms,
full jitter. See [retry design and configuration](docs/retries.md).

A fenced transaction returns the task to READY, clears ownership/deadline, and stores
DB-time available_at plus latest failure information. The activity/workflow stay RUNNING;
downstream work stays blocked. No sleeping worker or in-memory retry schedule is involved.
Due claims increment attempts/generation while preserving the stable idempotency key.
Exhaustion fails task/activity/workflow, including recovery of a crashed final attempt.
No hard handler timeout is added; a hung handler may continue heartbeating.

V5 updates the lease constraint for scheduled READY retries; V1–V4 remain unchanged.
Stop older binaries before upgrading. Legacy local execution remains fail-fast.
Earlier session sections describe their historical checkpoints; Session 13 supersedes
the blanket no-ordinary-retry statements for the queued worker path.

```powershell
mvn "-Dtest=RetryPolicyTest" test
mvn "-Dtest=RetryIntegrationTest,TaskLeaseIntegrationTest,PaymentIdempotencyIntegrationTest,PostgresTaskQueueIntegrationTest" test
mvn verify
```

Session 13 is implemented and statically checked. Compilation, migration execution,
tests, startup and worker demos remain unverified because Java, Maven and Docker are
unavailable. The fake provider's single-JVM limitation still applies.

## Session 14: activity timeouts and graceful worker shutdown

Queued handlers now run under dedicated bounded supervision. The default per-attempt
deadline is 60000ms, independent of the lease duration. Timeout raises an explicit
retryable ActivityTimeoutException and uses the existing fenced retry/budget pipeline.
Interruption is cooperative: late results are discarded, and external work may continue.
A non-cooperative handler retains its one local execution slot; other workers can recover
the durable task without this process creating unbounded replacement threads.

Shutdown closes the claim gate, drains active work for up to 10000ms while maintaining
heartbeats, then closes execution/heartbeat infrastructure. Grace exhaustion abandons
unfinished claims for lease expiry rather than failing them solely because of shutdown.
Configure WORKER_EXECUTION_TIMEOUT_MS and WORKER_SHUTDOWN_GRACE_MS or corresponding
durableflow.worker properties. V1–V5 are unchanged; no schema changes are needed.

[Activity timeout and shutdown notes](docs/activity-timeouts.md) explain cancellation
limits, network timeouts, configuration and test/demo procedures. Earlier session notes
describe historical checkpoints; Session 14 supersedes their no-attempt-timeout and
immediate-shutdown-abandonment statements for queued workers.

```powershell
mvn "-Dtest=HandlerExecutionsTest,WorkerHeartbeatsTest" test
mvn "-Dtest=TimeoutShutdownIntegrationTest,RetryIntegrationTest,TaskLeaseIntegrationTest,PaymentIdempotencyIntegrationTest,PostgresTaskQueueIntegrationTest" test
mvn verify
```

Session 14 is implemented and statically checked. Compilation, migrations/startup, tests
and timeout/shutdown demos remain runtime unverified because Java, Maven and Docker are
unavailable. No workflow cancellation or compensation is added.
