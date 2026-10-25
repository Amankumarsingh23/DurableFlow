# Session 20: durable child workflows

A child workflow has a separate WorkflowRun, DAG, tasks, retries, timers, signals,
history and Saga boundary. A large activity still has one logical execution boundary.
The parent waits on PostgreSQL relationship/state rows, without holding a worker or thread.

## Definition and input

```json
{"id":"shipping","type":"CHILD_WORKFLOW","childWorkflow":"ShippingChild","childVersion":1,"dependsOn":["submit"]}
```

Exact immutable name/version is pinned; recovery never resolves latest. Activity, delay
and compensation fields are forbidden. Old definitions and StepDefinition constructors
remain compatible. Omitted/null childInput inherits the parent's input snapshot; an explicit
JSON object, including {}, replaces it entirely. There is no merge, expression language,
output mapping or separate payload-size quota beyond existing request handling.

## Schema and transaction

V10 adds workflow_run.child_depth (root=0) and child_workflow_execution: UUID, parent run FK,
child node id, unique child run FK, pinned definition FK, created_at, observed terminal_status,
completed_at and checked_at for reconciliation. UNIQUE(parent_workflow_run_id, child_node_id)
plus the parent run lock ensures one logical child. Child-run uniqueness assigns one owner.
V1-V9 remain unchanged.

Eligible materialization holds the parent lock, resolves the pinned definition, uses the
normal run initializer at depth+1, inserts the relationship and records CHILD_WORKFLOW_STARTED
on the parent. The child initializer creates normal ACTIVITY rows and its own WORKFLOW_STARTED.
These writes share one transaction; history failure leaves no orphan child or false event.
Server reconciliation advances the child's initial nodes separately, avoiding nested locks
on existing parent/child runs. A new child has no parent-side activity task.

Multiple child nodes can fan out. NodeSatisfaction considers a CHILD_WORKFLOW satisfied only
when its referenced run is COMPLETED, so an AND-join waits for every required child/gate.
Parent GET includes child ids/current statuses; follow childRunId for detailed state/history.

| Child state | Parent interpretation |
| --- | --- |
| RUNNING/COMPENSATING | Unsatisfied, continue waiting |
| COMPLETED | CHILD_WORKFLOW_COMPLETED, node satisfied |
| FAILED/COMPENSATED/COMPENSATION_FAILED | CHILD_WORKFLOW_FAILED with explicit status; parent failure/Saga |
| CANCELLED | Failure if encountered; no cancellation API added |

Parent events contain high-level ids/version/status, not copies of internal activity history,
payloads or raw errors. Observed terminal status and parent progression/history commit together.
Repeated polling/advancement cannot duplicate child lifecycle events. A terminal parent may
record a later child outcome without reopening or creating new parent work.

## Durable notification and recovery

The server-only ChildPoller is the notification mechanism, default every 500ms with up to ten
reconciliation attempts per poll. A short transaction selects an unobserved relationship
with SKIP LOCKED, ordered by checked_at/id, then updates checked_at. After releasing that lock,
the reconciler advances the child and parent in separate transactions. Existing parent/child
run locks are never held together. Checked timestamps provide recurring fair scans, not leases.
A crash after touching a row does not permanently suppress it.

ChildOutcomeObserver reads durable status under the parent lock, including during Saga decisions.
If the child commits completion and the process dies before parent advancement, a later poll
or manual parent /advance observes it and continues exactly once. The same mechanism recovers
new children's initially pending work. No callback, in-memory barrier or outbox is required.
Poll delay is not an exact notification latency guarantee.

## Hierarchical Saga boundary

Children compensate their own internal activities. A child COMPENSATING is still active;
COMPENSATED means its forward goal failed, not parent success. Terminal child failure makes
the parent enter ordinary failure/Saga, selecting only the parent's own completed configured
activities. The parent never compensates a child's internal execution ids.

Parent Saga settlement waits for active children as well as started parent activities.
Children continue their own workflows, including signals/timers, before parent undo begins.
A child awaiting an external signal indefinitely can hold settlement open; child cancellation
and global deadlines are deferred. Children also continue if their parent fails without Saga.

A successful child is not automatically undone when a later parent node fails. Reversing its
business result needs an explicit future contract. COMPENSATED covers configured operations
of that run, not every external effect in its hierarchy.

## Recursion protection

Registration traverses pinned child definitions and rejects direct self-reference and reachable
cycles such as A:1 -> B:1 -> A:1. Forward references are allowed; materialization returns a
recoverable 409 until the exact target exists. Registering an edge that closes a cycle fails.
A transaction-scoped PostgreSQL advisory lock serializes definition-graph registrations across
servers, preventing racing A/B registrations from both missing the other. This lock applies
only to registration, not workflow execution. Traversal is bounded to 128 levels.

CHILD_MAX_DEPTH defaults to 8 (valid 1-128). Persisted runtime child_depth survives restart.
Exceeding it records CHILD_WORKFLOW_FAILED/CHILD_DEPTH_EXCEEDED and normal failure/Saga without
another child. It also bounds corrupted legacy recursive definitions that bypass validation.

## Demo

Start database/server/worker as in README. Register child before parent:

```powershell
$base = 'http://localhost:8080/api'
Invoke-RestMethod -Method Post -Uri "$base/workflow-definitions" -ContentType 'application/json' -Body (Get-Content examples/shipping-child-v1.json -Raw)
Invoke-RestMethod -Method Post -Uri "$base/workflow-definitions" -ContentType 'application/json' -Body (Get-Content examples/parent-order-v1.json -Raw)
$parent = Invoke-RestMethod -Method Post -Uri "$base/workflow-runs" -ContentType 'application/json' -Body (Get-Content examples/start-parent-order.json -Raw)
Invoke-RestMethod -Method Post -Uri "$base/workflow-runs/$($parent.runId)/advance"
Invoke-RestMethod -Uri "$base/workflow-runs/$($parent.runId)"
Invoke-RestMethod -Uri "$base/workflow-runs/$($parent.runId)/events"
```

Poll GET until COMPLETED, then follow children[].childRunId and its /events. Parent waiting
reserves no worker slot. CHILD_POLLING_ENABLED=false disables automatic reconciliation for
controlled experiments; manual parent/child advancement still reads database truth.

## Verification and checkpoint

ChildWorkflowIntegrationTest adds 16 PostgreSQL scenarios for pinning/input, unique concurrent
creation, notification/restart recovery, failure/compensation outcomes, history separation,
hierarchy ownership/settlement, child fan-out/joins, buffered-signal fixed-point passes,
unresolved-target recovery, recursion races, depth and rollback.
Definition tests cover child metadata/round trips. Worker-profile checks exclude both new
pollers and schedule HTTP. Java/Maven/Docker are unavailable: V10 execution, tests, startup
and live demo are not runtime verified. Run mvn verify with JDK 21 and Docker.

1. Children have durable internal orchestration; a large activity has one execution boundary.
2. Pinning prevents restart/deployment from silently changing child behavior.
3. Parent waiting is a persisted relationship/status, not a blocked thread.
4. Parent run locking plus UNIQUE(parent_workflow_run_id, child_node_id) prevents duplicates.
5. Polling/manual advance recovers a lost notification from the child's committed status.
6. Hierarchical ownership avoids double compensation and preserves the child's abstraction.
7. Cycle checks, serialized graph registration and persisted runtime depth bound recursion.
