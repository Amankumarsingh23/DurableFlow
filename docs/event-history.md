# Session 15: append-only execution history

[Session 16](durable-timers.md) extends this vocabulary with TIMER_SCHEDULED and TIMER_FIRED;
timer identity is carried in metadata because timers have no ActivityExecution row.
[Session 17](durable-signals.md) adds SIGNAL_WAIT_STARTED, SIGNAL_RECEIVED and SIGNAL_BUFFERED,
with node/delivery identities in metadata and no copied signal payload.

Current state answers **what is true now?** Event history answers **how did we get here?**
DurableFlow now stores operational snapshots plus an append-only audit/debugging history.
Snapshots remain authoritative for scheduling, claiming, retrying and recovery. This is
**not full event sourcing, deterministic replay, or an outbox/message publication system**.
No handler is moved into a database transaction.

## Schema and ordering

V6 adds workflow_event with UUID primary key, workflow_run_id FK, optional
activity_execution_id FK, positive BIGINT sequence_number, event_type, JSONB object
event_data and TIMESTAMPTZ created_at. UNIQUE(workflow_run_id, sequence_number) both enforces
ordering uniqueness and serves the actual history-by-run ordered query. No extra activity
or timestamp index is needed for the current API. V1–V5 are unchanged.

workflow_run gains history_sequence BIGINT NOT NULL DEFAULT 0. The history writer flushes
the transition's managed state, then atomically increments this counter with PostgreSQL
UPDATE ... RETURNING. All normal transition callers already hold the run-row lock (or
are creating the new uncommitted run). The counter update independently takes that lock
and serializes allocation. No MAX(sequence)+1 or process-local counter is used.

The counter is intentionally managed only through JDBC, not mapped into the JPA run
entity. Ordinary run updates therefore cannot overwrite it with a stale entity snapshot.
The existing JpaTransactionManager shares its database transaction/connection with
JdbcTemplate. Counter allocation and event insertion participate in the same transaction
as the transition. BIGINT overflow fails the transaction rather than wrapping.

Committed event sequences are unique and strictly increasing per run; transactions for
one run serialize through its row lock. Multiple events from one transition appear in
their append order within the same commit. Rollback currently restores the counter as
well as its events, but gaplessness is not a public accounting guarantee. Sequences are
the canonical ordering; timestamps/UUIDs are not. Sequence order records DurableFlow's
committed observations, not a universal physical-time ordering of external effects.
created_at is read from PostgreSQL clock_timestamp(), not a worker's local clock.

Existing runs are not given invented historical events. Their observed history begins
with their first transition after V6; a preexisting terminal run can have an empty list.
History cannot reconstruct events the system never recorded.

## Append and transaction boundaries

WorkflowHistoryWriter.append is MANDATORY: calling without a transition transaction fails.
It uses EntityManager.persist/flush for an immutable event entity and exposes no update
or delete behavior. WorkflowEventRepository provides only ordered reads. The normal HTTP
API has no append/update/delete endpoint. This is application-level append-only behavior,
not a tamper-proof database against privileged direct SQL or maintenance operations.

| Transition transaction | Events |
| --- | --- |
| Run and logical activities created | WORKFLOW_STARTED |
| New eligible READY task created | ACTIVITY_SCHEDULED |
| READY task claimed, including a scheduled retry | TASK_CLAIMED |
| Expired CLAIMED ownership transferred | TASK_RECLAIMED |
| Retry durably scheduled | RETRY_SCHEDULED |
| Valid owned timeout outcome recorded | ACTIVITY_TIMED_OUT before retry/failure events |
| Task/logical activity completed | ACTIVITY_COMPLETED |
| Task/logical activity terminally failed | ACTIVITY_FAILED |
| Run first transitions to FAILED | WORKFLOW_FAILED |
| Advancement finishes run | WORKFLOW_COMPLETED |
| Legacy local execution starts a handler | ACTIVITY_STARTED with executionMode LOCAL |

Legacy local success/failure/completion also records the matching generic events.
Repeated advancement does not duplicate scheduling or terminal events. A failing sibling
already in a FAILED run records its own ACTIVITY_FAILED without another WORKFLOW_FAILED.
Final-attempt crash recovery records its bookkeeping ownership transfer with
executionStarted=false, then ACTIVITY_FAILED/WORKFLOW_FAILED; it does not claim that an
extra handler invocation occurred.
For ordinary claims, executionStarted=true denotes the committed dispatch/start boundary
from Session 13; it cannot prove physical Java method entry after that commit.

For owned outcomes/retries, the final existing ownership/generation/live-deadline update
must succeed before history append. A stale worker cannot leave a timeout, retry or result
event. A local timeout abandoned before it can commit its outcome may therefore be absent
from durable history; logs are still useful for uncommitted observations.

Every event is in the transition transaction, never a separate REQUIRES_NEW history
transaction. State failure rolls back events. Event insert failure rolls back state and
counter. For example, a timeout plus RETRY_SCHEDULED is one commit: rejection of the retry
event removes the earlier timeout event and restores CLAIMED ownership too. A database
failure after an external effect still leaves the existing idempotency/reclamation
problem; audit history does not make that external effect transactional.

## Metadata and noise

Events contain flat, event-specific metadata: task/worker identifiers, old/new worker on
reclaim, generation, attemptCount, executionStarted, timeoutMillis, retry delayMillis,
DB-derived availableAt and safe classification/failure codes. They do not copy workflow
inputs, handler outputs, arbitrary exception messages, secrets or provider credentials.
Configure worker identifiers as opaque instance names. Existing snapshot error fields
and logs are separate surfaces; this session does not retrofit their redaction policy.

Heartbeats, empty polls and incidental reads create **no events**. They are high-volume
operational telemetry for logs/metrics, not meaningful workflow history. The event table
is not used to drive execution or introduce new fan-out/fan-in behavior.

## Query API

```http
GET /api/workflow-runs/{runId}/events
```

Returns a JSON array ordered by sequence. Each DTO contains id, sequence, eventType,
timestamp, nullable activityExecutionId, nullable logical activityId and metadata:

```json
{
  "id": "<event UUID>",
  "sequence": 3,
  "eventType": "TASK_CLAIMED",
  "timestamp": "2026-10-25T10:00:00Z",
  "activityExecutionId": "<logical execution UUID>",
  "activityId": "payment",
  "metadata": {"workerId": "worker-A", "generation": 1, "attemptCount": 1, "executionStarted": true}
}
```

JPA entities are not exposed. Missing run returns the existing 404 ApiException convention;
an existing run with no recorded events returns []. The ordinary GET run DTO stays
unchanged and does not embed history. Worker role exposes no history HTTP controller.
This initial API returns the full list; pagination/retention is not introduced here.

## Documented timeout/retry/reclaim example

| Sequence | Event | Explanation |
| --- | --- | --- |
| 1 | WORKFLOW_STARTED | Run materialized |
| 2 | ACTIVITY_SCHEDULED | Payment task ready |
| 3 | TASK_CLAIMED | A, generation 1, attempt 1 |
| 4 | ACTIVITY_TIMED_OUT | Local attempt deadline observed |
| 5 | RETRY_SCHEDULED | Same logical activity, future DB availableAt |
| 6 | TASK_CLAIMED | B, generation 2, attempt 2 |
| 7 | TASK_RECLAIMED | B's lease expired; C owns generation 3, attempt 3 |
| 8 | ACTIVITY_COMPLETED | Payment outcome accepted from C |
| 9 | ACTIVITY_SCHEDULED | Shipment dependency now satisfied |
| 10 | TASK_CLAIMED | Shipment execution starts |
| 11 | ACTIVITY_COMPLETED | Shipment completed |
| 12 | WORKFLOW_COMPLETED | Final workflow checkpoint |

The final snapshot only says COMPLETED with payment attempt_count 3. This history explains
the timeout, delayed retry and expired owner that preceded success. It does not prove
exactly when the payment provider performed its external charge.

```powershell
Invoke-RestMethod "http://localhost:8080/api/workflow-runs/$runId/events" | ConvertTo-Json -Depth 10
mvn "-Dtest=WorkflowHistoryIntegrationTest" test
mvn verify
```

## Verification status

Twelve new PostgreSQL integration tests cover the example timeline, failure vocabulary,
concurrent roots and unique ordered sequences, immutable old prefixes, fresh-service
reads, HTTP DTO ordering/404/separate snapshots, legacy local transitions, sibling failure,
outer transaction rollback, deferred state failure at commit, and injected history-insert
failures during creation, claim, completion and timeout/retry. Coverage also verifies that
append without a transition transaction is rejected and stale results add no events.
Isolated-database cleanup includes workflow_event; the startup test expects migration V6. Coordination uses
barriers, database-time expiry adjustments and transaction triggers, not ordering sleeps.

Implemented and statically checked only. Java, Maven and Docker are unavailable here.
Compilation, migration execution/mapping, tests, startup and the history API demo remain
runtime unverified. The timeline above is a documented example, not a captured execution.
Fresh-service tests read the persisted table; no separate-JVM restart demo was performed.
