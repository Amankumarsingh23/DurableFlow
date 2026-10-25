# Session 18: orchestrated Saga compensation

Rollback undoes changes inside one database transaction. Compensation is a **new
business operation** that counteracts an already-committed external effect. PostgreSQL
cannot roll back a payment provider's successful charge. A refund leaves both a charge
and a refund in the audit trail; fees, emails and temporary account impacts may remain.

A Saga coordinates local transactions and explicit compensations. It provides eventual
business consistency, not distributed ACID or a guarantee that every effect can be undone.
Some actions, such as sending an email, have no true inverse. Only activities with explicit
compensation metadata participate, and only after their forward success is durably recorded.
An ambiguous external success without a durable completion still needs idempotent recovery
or reconciliation; Saga does not resolve that uncertainty by guessing.

```text
ChargePayment -> ReserveInventory -> CreateShipment fails permanently
ReleaseInventory -> RefundPayment
```

## Definition and supported graph

[Example definition](../examples/saga-order-v1.json) declares `compensation: refundPayment`
on payment and `compensation: releaseInventory` on inventory. Omitted metadata remains
valid. Names must match `[A-Za-z][A-Za-z0-9_.-]{0,199}` after trimming. TIMER and SIGNAL
cannot declare compensation. Handler existence is resolved by the worker, as for forward
handlers; an unregistered compensation handler fails permanently.

Session 18 handles **sequential reverse-order compensation**. A definition containing any
compensation must form one connected, acyclic dependency chain: one root, at most one parent
and one child per node. Timer/signal gates may occur in that chain. Parallel-DAG compensation
remains deferred. Existing DAG definitions without compensation retain their prior behavior.
The old synchronous `/execute` path rejects Saga definitions before starting any work;
use `/advance` and queue workers.

Order comes from dependencies, never JSON array order, timestamps or activity names.
The coordinator walks the validated chain, collects only COMPLETED forward executions
with compensation metadata, and reverses that list. After A and B complete and C fails,
only B's compensation is created/queued. A's record/task is created only after B completes.
Repeated advancement sees the existing logical record and leaves delayed retries alone.

## Persistence and lifecycle

V9 is the only new migration; V1-V8 remain unchanged.

| Storage | Change |
| --- | --- |
| workflow_run | CHECK accepts COMPENSATING, COMPENSATED, COMPENSATION_FAILED |
| compensation_execution | UUID, run FK, unique original activity FK, handler, status, attempts, input/output JSONB, error, timestamps |
| compensation_execution active index | At most one PENDING/RUNNING compensation per run |
| activity_task | FORWARD/COMPENSATION kind, nullable forward FK, unique nullable compensation FK; CHECK requires exactly the matching FK |

Compensation records are separate from forward graph nodes. Original activity rows retain
their results and failure state. A compensation's durable input snapshots contain
`originalInput` and `originalOutput`; its handler and original execution identity do not
change across attempts. GET run adds a `compensations` current-state list containing ids,
handler, status, attempts, output and failure details, without copying the input snapshots
or entire event history into that list.

```text
RUNNING -- terminal forward failure, completed compensatable effects --> COMPENSATING
RUNNING -- terminal forward failure, no eligible effects -------------> FAILED
COMPENSATING -- all required undo operations complete ----------------> COMPENSATED
COMPENSATING -- permanent/exhausted undo failure ---------------------> COMPENSATION_FAILED
```

A transient forward error does not start compensation. Explicit failure, exhausted retry
policy and recovery of a crashed final forward attempt all enter the same terminal-failure
coordinator. FAILED remains the behavior for ordinary workflows without eligible undo work.
COMPENSATED means compensation finished after a forward failure; it does **not** mean the
forward workflow succeeded or history vanished. Compensation failure stops the reverse
chain and requires operational intervention; this session adds no manual retry/cancellation API.

## Queue, transactions and fencing

There is one PostgreSQL queue and one handler supervision pipeline. A task's kind selects
the logical execution table and handler. Native SKIP LOCKED selection requires RUNNING
for forward work and COMPENSATING for compensation. The run is locked and rechecked after
selection. Stale READY forward rows remain visible for diagnosis but cannot begin once
compensation starts. Orchestration never schedules forward work in COMPENSATING or terminal
states. Timer dispatch and new signals also require RUNNING.

Claim/reclaim, generation fencing, DB-time lease renewal, heartbeat ownership checks,
retry classification, backoff/full jitter, maxAttempts, local execution timeout and graceful
shutdown are shared. DurableExecution shares the logical lifecycle API; there is no second
synchronous executor. A handler timeout remains an uncertainty about external work, not a
promise that the provider cancelled it. Noncooperative handlers retain the existing bounded
executor behavior.

Writers lock task -> run -> read/change logical execution. The Saga coordinator assumes
the parent run lock, inserts fresh records/tasks and never locks an existing task under that
lock. A terminal forward failure atomically persists the failed activity/task, transitions
the run, appends history and creates the first compensation/task. Run locks plus unique
original-execution/task constraints prevent duplicates; a partial unique index also bounds
outstanding compensation to one per run.

Compensation completion uses the existing final native ownership CAS (worker, generation,
CLAIMED status and unexpired database deadline). The logical completion/output, task outcome,
history, next compensation/task or final COMPENSATED state share that transaction. Failure
of the CAS or any history insertion rolls everything back, including already flushed changes.
A successful external effect is outside this transaction and still needs provider idempotency.

## Context and provider idempotency

`ActivityExecutionContext.activityExecutionId` always refers to the original forward
execution. Compensation context also exposes `workflowRunId`, `compensation=true`,
`originalInput`, `originalOutput` and the stable key:

```text
forward:      df:v1:<originalActivityExecutionId>
compensation: df:v1:compensate:<originalActivityExecutionId>
```

Lease generations and attempt counts never enter either key. Different operations require
different keys so a refund is not deduplicated as a charge. Retrying/reclaiming the same
logical compensation reuses its compensation key and snapshots.

RefundPayment uses the original payment id and original input. The fake provider atomically
compares canonical complete payloads, records one simulated refund effect and returns the
remembered result for repeats; changing payload with the same key is rejected. Charges and
refunds use separate ledgers. ReleaseInventory similarly deduplicates a release using the
original reservation id/input. The reservation demo remains a fixed-result simulation.

These fake ledgers are **per JVM, in memory**. They demonstrate the crash window when a worker
loses its lease/outcome while the provider object survives. They do not prove provider
persistence across JVM restarts or deduplication across independent workers with separate
fake ledgers. Production providers need their own durable, shared idempotency contract.

Recovery example: release completes, refund succeeds externally, worker dies before recording
completion. The compensation/task stay RUNNING/CLAIMED until lease expiry. Another worker
reclaims with a higher generation and the same compensation key; the provider returns the
original refund result rather than refunding again. Its fenced completion atomically stores
the refund output and marks the workflow COMPENSATED. A late old worker cannot commit.

## Failure and history

Explicit RetryableActivityException (including ActivityTimeoutException) retries while the
attempt budget remains. The task becomes READY with available_at/backoff and retained
generation; logical compensation remains RUNNING and the run COMPENSATING. Permanent errors
or exhaustion mark compensation/task FAILED and the run COMPENSATION_FAILED, without moving
to earlier compensations. A crashed final attempt is recovered into terminal failure without
invoking an extra handler.

Saga events are WORKFLOW_COMPENSATING, COMPENSATION_SCHEDULED, COMPENSATION_COMPLETED,
COMPENSATION_FAILED, WORKFLOW_COMPENSATED and WORKFLOW_COMPENSATION_FAILED. Claim/reclaim,
retry and timeout retain the existing vocabulary and add taskKind/compensationExecutionId
for compensation work. Original activity FK/id provide correlation. Metadata contains
identifiers, handler, attempt, generation or safe failure codes, never complete inputs,
outputs or raw exception messages. Existing current-state error fields retain failure details.
All event sequence increments and state writes roll back together. History is retained,
not erased or used as a replay engine.

## Run the demo

Requires JDK 21, Maven and a running Docker daemon. Start the server/database as in the README.
Run one worker in a second terminal:

```powershell
mvn spring-boot:run '-Dspring-boot.run.profiles=worker'
```

Register, start and advance from the repository root in another terminal:

```powershell
$base = 'http://localhost:8080/api'
Invoke-RestMethod -Method Post -Uri "$base/workflow-definitions" -ContentType 'application/json' -Body (Get-Content examples/saga-order-v1.json -Raw)
$run = Invoke-RestMethod -Method Post -Uri "$base/workflow-runs" -ContentType 'application/json' -Body (Get-Content examples/start-saga-order.json -Raw)
Invoke-RestMethod -Method Post -Uri "$base/workflow-runs/$($run.runId)/advance"
Invoke-RestMethod -Uri "$base/workflow-runs/$($run.runId)"
Invoke-RestMethod -Uri "$base/workflow-runs/$($run.runId)/events"
```

Poll the last two reads until terminal. `simulateShipmentFailure=true` is an explicit demo
input that makes CreateShipment throw a permanent failure. Expected eventual state is
COMPENSATED, with ReleaseInventory before RefundPayment. Omitting/false leaves the prior
shipment demo behavior. Registration is immutable; use the same definition or a new version
on repeat runs. Use one worker for this fake-provider demo because ledgers are process-local.

## Tests and verification record

Added SagaCompensationIntegrationTest (16 PostgreSQL scenarios) covers dependency-derived
reverse ordering, original context, no premature undo, distinct/stable keys, refund-effect
before outcome recovery, backoff/timeout, stale fencing, retry/crash budget exhaustion,
terminal stop policy, stale READY forward exclusion, concurrent advancement/completion,
real worker demo handlers, actual handler-executor timeout supervision, local-path rejection,
and transaction rollback when history fails during transition or next-item scheduling.
SagaIdempotencyTest covers charge/refund single effects, changed-payload rejection, release
deduplication and ineligible forward states. Definition tests cover metadata round trips,
invalid names, TIMER/SIGNAL rejection and sequential graph restrictions. Existing integration
cleanup includes the new table and the migration expectation is V9.

Verification in this environment: Java/Maven/Docker are not on PATH. Compilation, V9 execution,
all runtime tests, startup and live compensation demo are **not verified**. Java source syntax
was parsed for all 109 files with tree-sitter (zero syntax errors); this is not compilation.
JSON examples, Maven XML, source imports/packages, unchanged V1-V8 and whitespace are checked
statically. Run `mvn verify`, `docker compose up -d --wait`, server startup and the above demo
on a machine with the prerequisites. No parallel-DAG compensation, branching, cancellation,
outbox, child workflows or other later-session features are included.


## Learning checkpoint

1. PostgreSQL rollback controls its own transaction, not an already-committed transaction at Stripe.
2. Compensation is a new counteracting business operation; rollback reverses uncommitted local changes.
3. An orchestrated Saga has a coordinator durably choosing forward and compensating local operations.
4. Reverse dependency order undoes dependent effects before their prerequisites.
5. Only durably COMPLETED activities with explicit compensation metadata are eligible here.
6. Undo calls have the same side effects and crash windows as forward calls, so they need the same reliability machinery.
7. Charge and refund are different logical effects; sharing a key could deduplicate the wrong operation.
8. Permanent/exhausted undo failure records COMPENSATION_FAILED and stops earlier undo work for intervention.
9. Further forward work could create new effects while the system is counteracting prior effects; COMPENSATING closes that path.
10. COMPENSATED records a failed forward workflow whose required undo operations completed, retaining both histories.

The refund-crash interview scenario is walked through in the idempotency section above.
