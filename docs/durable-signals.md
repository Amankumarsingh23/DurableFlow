# Session 17: durable signals and external approval waits

A SIGNAL node waits for one accepted external event without occupying a worker, execution
thread, lease or heartbeat. The database holds the wait and any early delivery. This is
single-use signal receipt, not a rules engine or conditional approval/rejection branch.

## Definition and identity

```json
{
  "name": "HumanApproval",
  "version": 1,
  "steps": [
    {"id": "submitRequest", "type": "ACTIVITY", "activity": "submitRequest"},
    {"id": "managerApproval", "type": "SIGNAL", "dependsOn": ["submitRequest"]},
    {"id": "finalizeRequest", "type": "ACTIVITY", "activity": "finalizeRequest", "dependsOn": ["managerApproval"]}
  ]
}
```

SIGNAL has no activity handler, delayMillis or separate matching name. The exact node ID
is its semantic target, avoiding ambiguous duplicated names. IDs remain unique across
all ACTIVITY/TIMER/SIGNAL nodes. Kahn validation, dependency existence/self checks and
normalization apply to the entire mixed graph. Existing ACTIVITY/TIMER definitions and
Java constructors remain compatible; missing type still means ACTIVITY.

These three identities differ:

- runId selects the workflow instance and its immutable definition version.
- signalNodeId selects one exact logical SIGNAL node in that definition.
- signalId is the sender's stable delivery/idempotency identity, scoped to this run.

Payload is JSON event data, not an instruction to execute arbitrary code. Signal IDs and
target IDs are literal, case-sensitive strings; send the exact registered target spelling
and preserve signalId across retries. This version intentionally accepts one logical
delivery per target: another ID cannot replace the already accepted signal.

## Schema and inbound idempotency

V8 adds workflow_signal_wait with run FK, node ID, WAITING/RECEIVED, DB created/received
timestamps and nullable payload_json. Unique(run, node) ensures one logical wait.
Checks require WAITING to have no receipt/payload, and RECEIVED to have both timestamp
and JSON-object payload.

workflow_signal_delivery is the durable signal inbox: UUID, run FK, caller signal_id,
target node ID, JSONB object payload, DB received_at, nullable consumed_at and consumed
wait FK. Unique(run, signalId) enforces delivery identity; unique(run, node) enforces the
single-use target contract. Consumption timestamp and wait ID must be set together.
The unique indexes cover current reads and exact lookups; there is no signal poller or
subscription index. V1–V7 are unchanged. Upgrade server and workers together before using
SIGNAL definitions; older binaries do not understand them.

First delivery stores its target/payload. Same ID + same target + same logical payload
returns the existing receipt without another application or event. PostgreSQL JSONB
equality compares payloads structurally: object-key order and equivalent JSON numbers
(1 and 1.00) do not change identity; arrays retain their order. Different target/payload
with the same signalId returns 409. A different ID for an already accepted node also
returns 409. No cryptographic fingerprint or process-local cache is required.

The existing Jackson 2 dependency is now compile-scoped for explicit JDBC payload JSON
serialization; the HTTP JSON stack is unchanged. The mapper is private to the signal
repository, with no polymorphic deserialization or replacement HTTP mapper.

## Receive/materialize/consume transaction

SignalReceiver uses one REQUIRES_NEW READ COMMITTED transaction. It locks the run first,
checks existence and existing delivery identity, rejects new deliveries to terminal
runs, and validates the target against the run's immutable definition. It inserts the
delivery, records SIGNAL_BUFFERED only if its wait is not yet materialized, and calls
the existing advanceInTransaction entry point. The API returns after this short commit,
without waiting for downstream handlers.

On advancement, an eligible SIGNAL node creates one WAITING row and appends
SIGNAL_WAIT_STARTED. SignalWaitCoordinator then checks for its exact accepted unconsumed
delivery. If present, one transaction marks the wait RECEIVED, copies the payload onto
the wait, sets the inbox consumed_at/consumed_wait_id, and appends SIGNAL_RECEIVED.
The receipt timestamp on the wait records consumption; the inbox received_at records
acceptance, which may have happened long before the wait existed.

All signal writes are serialized by the same parent run-row lock. API arrival and
orchestrator advancement cannot both miss each other: whichever commits first leaves a
durable wait or delivery visible to the next lock holder. No process-local listener,
in-memory subscription or external broker participates. Repository mutation/coordinator
methods require an existing transaction.

Advancement now runs a bounded fixed-point pass: consuming an early signal may satisfy
a dependency of a node listed earlier in the definition. It repeats only when a new
SIGNAL becomes RECEIVED; each single-use node can do that once. Newly queued tasks are
tracked within the pass to avoid duplicates. This also handles buffered signal chains
without depending on definition order or another HTTP request.

NodeSatisfaction remains the centralized rule:

| Node | Satisfied when |
| --- | --- |
| ACTIVITY | ActivityExecution COMPLETED |
| TIMER | WorkflowTimer FIRED |
| SIGNAL | SignalWait RECEIVED |

Waiting or not-yet-materialized required signals prevent completion and downstream
eligibility. SIGNAL creates no ActivityExecution or ActivityTask. Existing timer firing
can materialize/consume signals through the same advancement path.

## Atomicity, retries and terminal runs

Receipt, possible wait creation/consumption, history and downstream task/timer creation
share one transaction. A history or downstream insertion failure rolls them all back;
the sender gets an error and must retry the same ID. An accepted early signal remains
buffered if a later, separate wait-materialization transaction rolls back.

Duplicates create no events and do not re-run advancement. Existing accepted receipts
can be replayed even after COMPLETED/FAILED so a sender can recover a lost acknowledgement.
New deliveries to those runs return 409 and cannot resume them. The receipt's UUID,
acceptance timestamp and identity stay stable; its status/consumedAt report current
BUFFERED/CONSUMED state, so an early receipt may be consumed by the time it is replayed.

The existing activity-outcome/before-advance recovery boundary remains: if an activity
commits completion and the process dies before its advance callback, POST /advance
reconstructs remaining work and consumes buffered signals. This session adds no global
startup scan. While a materialized wait exists, delivery itself advances atomically,
leaving no accepted-receipt/before-resume callback gap.

## HTTP and visibility

```http
POST /api/workflow-runs/{runId}/signals
Content-Type: application/json

{"signalId":"approval-delivery-42","signalNodeId":"managerApproval","payload":{"decision":"approved","reference":"REQ-42"}}
```

Success/replay returns HTTP 200 with receipt id, workflowRunId, signalId, signalNodeId,
receivedAt, nullable consumedAt and status BUFFERED/CONSUMED. Missing run returns 404;
invalid fields, non-object/null payload or a new invalid/non-SIGNAL target return 400;
misuse, a new terminal delivery or replacement identity returns 409. Reuse of an existing
ID with another target is always idempotency misuse, even if that target is invalid.

GET run adds signalWaits with wait/node IDs, WAITING/RECEIVED and createdAt/receivedAt.
Unmaterialized waits are absent. Routine snapshots/receipt DTOs do not echo payload;
the database stores it on the inbox and received wait for correlation/future adapters.
It is not automatically merged into workflow input or forwarded to activity handlers.
Full history stays separate. Signal event activityExecutionId is null; metadata carries
signalNodeId and, for delivery-related events, signalId. Payload is never copied into
history, and duplicate HTTP retries are not new history transitions.

## Human approval demo and security boundary

The new submitRequest/finalizeRequest handlers are deterministic demo stand-ins with no
external effect. Start the server and workers using README commands, then:

```powershell
curl.exe -i -X POST http://localhost:8080/api/workflow-definitions -H "Content-Type: application/json" --data-binary "@examples/human-approval-v1.json"
$body = @{ workflowName = "HumanApproval"; version = 1; input = @{ requestId = "REQ-42" } } | ConvertTo-Json
$run = Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/workflow-runs -ContentType "application/json" -Body $body
$runId = $run.runId
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/workflow-runs/$runId/advance"
Invoke-RestMethod "http://localhost:8080/api/workflow-runs/$runId" | ConvertTo-Json -Depth 10
$signal = @{ signalId = "approval-delivery-42"; signalNodeId = "managerApproval"; payload = @{ decision = "approved"; reference = "REQ-42" } } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/workflow-runs/$runId/signals" -ContentType "application/json" -Body $signal
Invoke-RestMethod "http://localhost:8080/api/workflow-runs/$runId/events" | ConvertTo-Json -Depth 10
```

After submitRequest, managerApproval is WAITING and uses zero worker execution resources.
The signal changes it to RECEIVED and queues finalizeRequest. Repeating the same POST is
harmless. Sending before submitRequest completes buffers it, and later advancement
consumes it. Waiting can last across server/worker restarts without rebuilding subscriptions.

SIGNAL currently means **external event received**. The decision field is not interpreted:
even a rejected/false decision unlocks the same fixed downstream graph. There is no full
approve/reject routing, caller identity verification, RBAC, cancellation or compensation.
Production signal endpoints require authentication and authorization for both run and
target node. This local demo endpoint does not implement those protections. Do not submit
secrets/tokens/credentials in IDs or payloads; use safe references. The service does not
intentionally copy payloads to history or log request bodies, but is not a general secret
classification/redaction system.

If DurableFlow is fully offline before accepting HTTP, nothing has been durably received:
the sender must retry using the same signalId. The inbox begins at DurableFlow's successful
transaction commit; no external message broker or universal outbox/inbox system is added.

## Tests and verification status

```powershell
mvn -DskipTests compile
mvn "-Dtest=WorkflowDefinitionValidatorTest" test
mvn "-Dtest=DurableSignalIntegrationTest,DurableTimerIntegrationTest,WorkflowHistoryIntegrationTest,PostgresTaskQueueIntegrationTest" test
mvn verify
```

Fourteen PostgreSQL tests cover waiting/progress, early buffering, reverse-ordered chains,
JSONB idempotency/misuse, concurrent duplicate delivery, concurrent materialization and
upstream completion, terminal behavior, fresh-context waits and buffered deliveries,
rollback of receipt/history/downstream work, recovery after failed later materialization,
HTTP DTO/validation/payload visibility, and timer-triggered consumption. Two new unit tests cover three-type graph
registration/round-trip and invalid SIGNAL fields, duplicate IDs and mixed cycles.
Existing isolated cleanup includes both signal tables; the startup test expects V8.
The worker-context test verifies the inbound HTTP controller is absent.

Tests use barriers and fresh application contexts against preserved PostgreSQL, not
sleep-based races or process-local subscriptions. Fresh-context coverage is same-JVM,
not a real process-kill or multi-day approval demo. Implemented and statically checked
only: Java, Maven and Docker are unavailable. Compilation, V8 migration, tests, startup
and the human-approval demo remain runtime unverified.
