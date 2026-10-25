# Session 19: static DAG execution and durable AND-joins

DurableFlow materializes all eligible nodes in a short PostgreSQL transaction. Independent
handlers execute concurrently across worker processes; joins derive exclusively from
persisted node states. Compensation now follows reverse topological order and waits for
already-started forward activities to settle before beginning undo work.

## Eligibility and fan-out

The existing full advancement loop already handled multiple eligible nodes; this session
makes that contract explicit, skips satisfied nodes, rejects an incompatible pending-task
lease generation and removes Session 18's restriction on Saga graph shape. All registered
static DAGs may now carry compensation metadata. Missing dependencies, cycles, duplicate ids
and invalid node metadata remain invalid.

For every node, all parents must be satisfied. The centralized NodeSatisfaction rules are:

| Type | Satisfied state | Eligible materialization |
| --- | --- | --- |
| ACTIVITY | COMPLETED execution | One READY forward task |
| TIMER | FIRED timer | One WAITING timer |
| SIGNAL | RECEIVED wait | One WAITING wait; consume an existing buffered delivery if present |

The loop materializes every eligible unscheduled node, without stopping at the first one.
The bounded fixed-point pass still handles early buffered signals that unlock nodes listed
earlier in JSON. Newly created tasks, timers and waits are tracked during the transaction;
existing compatible representations are reused, not reset. Definition array order supplies
no dependencies.

Fan-out means durable READY work, not a Java thread per branch. The shared queue absorbs
READY tasks and worker count controls execution concurrency and backpressure. Each current
worker has one bounded handler slot; run multiple workers for parallel branches. There is
no orchestrator-owned branch executor, JVM workflow mutex or in-memory join counter.

## AND-joins, concurrency and restart

A node with `dependsOn: [B, C, D]` is an AND-join: B AND C AND D must be satisfied. A special
JOIN node is unnecessary. Mixed ACTIVITY/TIMER/SIGNAL parents use the same rule. RUNNING,
retry-delayed, waiting timer and waiting signal parents block the join and whole-run
completion. Only all required nodes satisfied permits COMPLETED.

Advancement locks the parent run row before reading runtime states. Worker completion also
locks that run before writing the logical outcome. READ COMMITTED plus this protocol gives
each advancement a current committed view after any lock wait. Concurrent callbacks serialize
only short decisions for that run; handlers execute outside transactions and remain parallel.
Unrelated runs have independent locks. Queue workers still use FOR UPDATE OF task SKIP LOCKED.

Unique task-by-execution, timer-by-run/node and signal-wait-by-run/node constraints defend
logical identities. Parent locking makes the check/create/history protocol repeat-safe:
one materialization and one scheduling event per logical node. No speculative join task is
created while a parent remains incomplete. Several callbacks observing the final completion
reuse the same task; they do not each create one.

A completion may commit before its ordinary advance callback runs. After a process restart,
POST `/advance` loads the same definitions and durable execution/timer/signal rows and
reconstructs eligibility. A lost callback needs another advancement (manual or another
completion/signal/timer trigger); this session does not add a global startup scan. There is
no memory barrier to restore. Branch outputs remain retrievable by activity/node id through
GET run. Handler inputs retain the existing workflow input contract; no data-mapping DSL or
automatic branch-output aggregation is introduced.

## Failure and the already-running sibling policy

A terminal forward error closes normal advancement atomically. New PENDING forward activities
cannot start after COMPENSATING, including stale READY tasks. No new forward tasks, timers or
signal waits are scheduled. Waiting gates do not hold compensation open, because they perform
no already-started activity side effect.

An already-running sibling may physically finish after another branch fails. Hard cancellation
cannot safely undo its external effect. Session 19 therefore chooses **settle started work
before compensation**:

1. Enter COMPENSATING if there are completed compensatable effects, or a Saga definition has
   started forward work that may still complete. Record WORKFLOW_COMPENSATING once.
2. While any forward ActivityExecution is RUNNING, schedule **no compensation**, including
   ancestor compensation. Valid leased completions/failures remain fenced and are recorded.
3. A RUNNING logical activity's existing generation-positive task may retry or reclaim an
   expired lease in COMPENSATING. This is continuation/reconciliation of an already-started
   operation, using the same stable key, timeout, backoff and maxAttempts. Generation-zero
   READY tasks with PENDING executions remain excluded. The claim transaction rechecks these
   rules after acquiring the run lock.
4. Once all started activities are terminal, compute the compensation set from their durable
   outcomes. Late COMPLETED compensatable siblings are included. Failed or never-started
   activities are excluded. The last late outcome and first compensation scheduling share one
   fenced transaction; no completion-callback gap is required for this transition.
5. Only after all required undo operations finish may the run become COMPENSATED. If no
   completed compensatable effects remain after settling, finish FAILED, not fake COMPENSATED.

Example: A fans out B/C/D. B fails while C is RUNNING and D is still PENDING/READY. The run
enters COMPENSATING; D never starts. C may finish or recover its lease/retry with the original
forward key. If C completes, compensate C before A. If C exhausts/fails, compensate only
completed configured effects such as A. Compensation retries/failures retain Session 18's
stop-on-terminal-failure policy.

Settlement is bounded by existing attempt counts, execution timeouts and leases under active
workers, not by a new global cancellation deadline. A dead final attempt is terminalized on
reclaim without another handler invocation. No workers means no progress; orphaned/corrupt
RUNNING records without recoverable tasks require existing operational recovery. A physically
running obsolete/noncooperative attempt can outlive its lease or supervisor; provider
idempotency and reconciliation remain necessary. An external effect without a recovered
durable success is still uncertain, not automatically classified as compensatable.

For definitions with no configured compensation, existing FAILED behavior remains, including
fenced outcome recording for already-claimed siblings. COMPENSATED covers configured undo
operations only; non-compensatable effects such as emails may remain.

## DAG-aware compensation algorithm

WorkflowGraph applies Kahn's algorithm to the **entire validated graph**, including gates and
activities without compensation. A priority queue of node ids breaks ties deterministically
among ready independent nodes. Reverse the resulting topological list, then retain only
COMPLETED forward executions declaring compensation. Sorting the whole graph before filtering
preserves transitive paths through non-compensatable nodes and TIMER/SIGNAL gates.

For A -> B/C, B -> D, C -> E, D/E -> F (failed), completed compensatable A/B/C/D/E unwind
E, D, C, B, A with the chosen tie-breaker. Another stable order among independent nodes could
also be valid; dependencies, not timestamps, constrain correctness. Every completed
compensatable descendant precedes its ancestor, even through intermediate unconfigured nodes.
Priority queue sorting costs O((V + E) log V); runtime state remains in PostgreSQL.

Compensation execution remains **sequential**. Exactly one outstanding compensation per run
is enforced by the V9 active-row index; unique original-execution/task references and the run
lock prevent duplicate logical work. Repeated advancement reuses the existing record and
leaves delayed retries intact. No forward logical execution may newly complete once settlement
has finished, because no RUNNING forward executions remain and PENDING claims are closed.
Thus the derived set is stable across restarts without another plan-freezing table or flag.

## History and schema

No new migration or event type is needed. V1-V9 are unchanged. Existing run states, execution
rows, queue kinds and uniqueness constraints encode this protocol. WORKFLOW_COMPENSATING may
now precede a settlement interval and late ACTIVITY_COMPLETED/FAILED events before the first
COMPENSATION_SCHEDULED event. If settlement finds nothing to undo, WORKFLOW_FAILED records
NO_COMPLETED_COMPENSATABLE_EFFECTS.

Parallel completion events appear in the commit order serialized by PostgreSQL's run-row
protocol. Sequence does not establish a universal external-effect wall-clock order.
Compensation history records the actual sequential orchestration. Scheduling/history remains
atomic; no per-heartbeat or per-poll event is added.

## Static DAG demo

[ParallelOrder definition](../examples/parallel-order-v1.json):

```text
                  +--> checkFraud --------+
chargePayment ----+--> reserveInventory --+--> finalizeRequest --> createShipment
                  +--> scoreRisk --------+
```

Payment has refundPayment; inventory has releaseInventory. Fraud/risk checks are pure demo
handlers. This is a static graph fixed at registration, not dynamic/map fan-out.

Start the database/server as in README, then two separate worker terminals:

```powershell
mvn spring-boot:run '-Dspring-boot.run.profiles=worker' '-Dspring-boot.run.arguments=--durableflow.worker.id=dag-worker-A'
mvn spring-boot:run '-Dspring-boot.run.profiles=worker' '-Dspring-boot.run.arguments=--durableflow.worker.id=dag-worker-B'
```

Each command runs in its own terminal. Register/start/advance in another terminal:

```powershell
$base = 'http://localhost:8080/api'
Invoke-RestMethod -Method Post -Uri "$base/workflow-definitions" -ContentType 'application/json' -Body (Get-Content examples/parallel-order-v1.json -Raw)
$run = Invoke-RestMethod -Method Post -Uri "$base/workflow-runs" -ContentType 'application/json' -Body (Get-Content examples/start-parallel-order.json -Raw)
Invoke-RestMethod -Method Post -Uri "$base/workflow-runs/$($run.runId)/advance"
Invoke-RestMethod -Uri "$base/workflow-runs/$($run.runId)"
Invoke-RestMethod -Uri "$base/workflow-runs/$($run.runId)/events"
```

After payment completion, one advancement schedules fraud/inventory/risk. Workers may claim
these independently. Finalize waits for all three. These pure handlers finish quickly; event
worker ids show claims, but a particular run is not guaranteed to distribute evenly. The
integration fixture holds two test handlers open to demonstrate actual overlap.

The supplied input succeeds. To demonstrate Saga after all branches, start another run with
`simulateShipmentFailure=true`: inventory release precedes payment refund. To demonstrate
branch failure, use `simulateRiskFailure=true`: finalize never schedules; already-started
siblings settle and completed configured effects unwind. Inventory is released only if it
actually durably completed before settlement ends. The fake payment/refund ledgers remain
per JVM; multi-worker/process-restart demos do not prove durable cross-process provider
deduplication. Use a real shared provider idempotency store in production.

For a concrete restart/join walkthrough, add a root approval signal to a copy of the demo
and make finalize also depend on it:

```powershell
$approval = Get-Content examples/parallel-order-v1.json -Raw | ConvertFrom-Json
$approval.name = 'ParallelOrderApproval'
$approval.steps += [pscustomobject]@{ id='approval'; type='SIGNAL'; dependsOn=@() }
($approval.steps | Where-Object id -eq 'finalize').dependsOn += 'approval'
Invoke-RestMethod -Method Post -Uri "$base/workflow-definitions" -ContentType 'application/json' -Body ($approval | ConvertTo-Json -Depth 20)
$start = Get-Content examples/start-parallel-order.json -Raw | ConvertFrom-Json
$start.workflowName = 'ParallelOrderApproval'
$waiting = Invoke-RestMethod -Method Post -Uri "$base/workflow-runs" -ContentType 'application/json' -Body ($start | ConvertTo-Json -Depth 20)
Invoke-RestMethod -Method Post -Uri "$base/workflow-runs/$($waiting.runId)/advance"
Invoke-RestMethod -Uri "$base/workflow-runs/$($waiting.runId)"
```

Wait until payment/branches are COMPLETED and approval remains WAITING. Stop/restart only
the HTTP server with Ctrl+C and `mvn spring-boot:run`; retain PostgreSQL's volume and the
worker terminals. Then deliver the gate in the same request terminal:

```powershell
$delivery = @{ signalId='restart-approval-1'; signalNodeId='approval'; payload=@{ approved=$true } } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "$base/workflow-runs/$($waiting.runId)/signals" -ContentType 'application/json' -Body $delivery
Invoke-RestMethod -Uri "$base/workflow-runs/$($waiting.runId)/events"
```

Finalize schedules exactly once after receipt, then shipment completes. Repeating the same
delivery or advancing cannot duplicate finalize. The automated fresh-context test separately
recovers an expired last-branch lease, commits completion, closes that context before
advancement, and materializes exactly one join from a second fresh context.

## Main changed files

| Files | Purpose |
| --- | --- |
| workflow/definition/WorkflowGraph.java | Deterministic topological ordering |
| workflow/definition/WorkflowDefinitionValidator.java | Accept compensation metadata on static DAGs |
| workflow/task/WorkflowTaskOrchestrator.java | Full eligibility contract and pending-task compatibility |
| workflow/task/ActivityTaskRepository.java | Recover only already-started forward work during settlement |
| worker/TaskTransactions.java | Fenced late outcomes and atomic settlement progression |
| workflow/compensation/SagaCoordinator.java | Wait for started work, then sequential reverse-DAG undo |
| workflow/run/WorkflowRunEntity.java | FAILED when settlement leaves no configured effects to undo |
| workflow/activity/demo/FraudCheckActivity.java, RiskScoreActivity.java | Pure demo branch handlers |
| ParallelDagIntegrationTest.java, WorkflowGraphTest.java, WorkflowDefinitionValidatorTest.java | New scenarios and updated validation boundary |
| examples/parallel-order-v1.json, start-parallel-order.json | Static fan-out/fan-in demo |
| README.md, docs/parallel-dag.md, docs/saga-compensation.md | Current contract and historical-session cross-reference |

Java paths above are relative to src/main/java/io/durableflow; test classes live under
src/test/java/io/durableflow.

## Tests and verification

ParallelDagIntegrationTest adds 17 PostgreSQL scenarios: complete fan-out, distinct claims and
real simultaneous worker handlers, partial/final AND-join, racing callbacks, mixed nodes and
unique scheduling events, early buffered signals, fresh-context lease/join recovery, unrelated
progress during delayed retry, terminal branch failure and stale READY exclusion, late sibling
completion/reclaim/fencing, bounded timeout/retry/crash settlement, no fake compensation,
reverse topological/transitive ordering, concurrent late outcome/advance and the complete
payment/branch/shipment demo. WorkflowGraphTest adds deterministic ordering, transitive gate
paths and invalid graph cases. Definition tests now accept Saga DAGs while retaining metadata,
unknown dependency and cycle validation; only the superseded sequential-only assertion changes.
Existing Session 10-18 suites remain in place.

Java, Maven and Docker are still unavailable on PATH. Compilation, migration execution,
all tests (including concurrency), multi-worker demo and restart demo are **not runtime
verified**. Static verification covers Java syntax, internal imports/packages, JSON examples,
Maven XML, unchanged migrations and Git whitespace. Run `mvn verify` with JDK 21 and Docker
before claiming these tests pass. No runtime map fan-out, branching, parallel compensation,
child workflows, scheduling or external broker is added.

## Learning checkpoint

1. Fan-out durably authorizes every currently eligible independent node.
2. READY tasks survive process failure and distribute to bounded workers; Java threads alone do not.
3. An AND-join requires every declared dependency to be durably satisfied.
4. dependsOn already expresses that rule; a JOIN type would add no semantics here.
5. PostgreSQL definitions and node states reconstruct readiness after restart.
6. The run lock serializes check/materialize/history, and unique keys protect logical task identity.
7. Only short state transactions serialize; expensive handlers execute outside them concurrently.
8. Static fan-out has registered nodes/edges; dynamic fan-out would create runtime-sized work sets.
9. Completion timestamps can reorder independent work and cannot represent prerequisite relationships.
10. A running sibling can create a late effect; settlement must include its recorded success before undo/finalization.

Interview scenario: B/C success remains durable while D retries under its own task/key. E stays
blocked until D's durable success. Racing callbacks acquire the same run lock one at a time;
the first that observes all three successes creates E plus its scheduling event, subsequent
calls reuse that task. A restart loses no readiness data; another advance reconstructs the
same join and database uniqueness still prevents duplicates. This is one logical scheduling
decision, not a claim that handler side effects execute exactly once.
