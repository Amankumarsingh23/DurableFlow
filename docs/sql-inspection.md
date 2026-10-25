# Critical SQL and existing indexes

Session 27 adds no speculative index. Run representative EXPLAIN (ANALYZE, BUFFERS) on an isolated, populated staging/test database **after correctness tests**; ANALYZE executes the query and FOR UPDATE acquires locks. Use BEGIN/ROLLBACK, never an uncontrolled production load. Record rows, planning/execution time, buffers, sort/spill, rows filtered, index usage and competing lock load. Cold/warm cache and empty-queue results differ from a busy queue. Re-extract queries from current source when comparing later commits.

## Claim and reclaim

The actual unified query from `ActivityTaskRepository.lockNextClaimable` is:

```sql
BEGIN;
EXPLAIN (ANALYZE, BUFFERS)
SELECT t.* FROM activity_task t
LEFT JOIN activity_execution a ON a.id = t.activity_execution_id
LEFT JOIN compensation_execution c ON c.id = t.compensation_execution_id
JOIN workflow_run r ON r.id = COALESCE(a.workflow_run_id, c.workflow_run_id)
WHERE ((t.task_kind = 'FORWARD' AND (r.status = 'RUNNING'
        OR (r.status = 'COMPENSATING' AND a.status = 'RUNNING' AND t.lease_generation > 0)))
    OR (t.task_kind = 'COMPENSATION' AND r.status = 'COMPENSATING')) AND (
    (t.status = 'READY' AND t.available_at <= statement_timestamp() AND
        ((t.lease_generation = 0 AND COALESCE(c.status, a.status) = 'PENDING')
         OR (t.lease_generation > 0 AND COALESCE(c.status, a.status) = 'RUNNING')))
    OR (t.status = 'CLAIMED' AND t.lease_expires_at <= statement_timestamp()
        AND COALESCE(c.status, a.status) = 'RUNNING')
)
ORDER BY CASE WHEN t.status = 'READY' THEN t.available_at ELSE t.lease_expires_at END,
         t.created_at, t.id
LIMIT 1 FOR UPDATE OF t SKIP LOCKED;
ROLLBACK;
```

Exercise READY-heavy, expired-CLAIMED-heavy, mostly-not-due, empty, mixed compensation and concurrent-lock distributions. `ix_activity_task_ready_order(available_at,created_at,id) WHERE status='READY'` and `ix_activity_task_expired_claim(lease_expires_at,created_at,id) WHERE status='CLAIMED'` support filtering/order paths, but the combined OR, joins and CASE ordering may require sorting or different plans. Do not claim either partial index eliminates every scan. Task PK and unique forward/compensation identities support owned mutations. Inspect heartbeat and completion predicates with realistic ownership/generation distributions, without executing actual outcome mutations as casual diagnostics.

## Timers and schedules

```sql
BEGIN;
EXPLAIN (ANALYZE, BUFFERS)
SELECT t.* FROM workflow_timer t JOIN workflow_run r ON r.id=t.workflow_run_id
WHERE t.status='WAITING' AND t.wake_at<=statement_timestamp() AND r.status='RUNNING'
ORDER BY t.wake_at,t.id LIMIT 1 FOR UPDATE OF t SKIP LOCKED;
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM workflow_schedule WHERE enabled AND next_fire_at<=statement_timestamp()
ORDER BY next_fire_at,id LIMIT 1 FOR UPDATE SKIP LOCKED;
ROLLBACK;
```

Existing `ix_workflow_timer_waiting(wake_at,id) WHERE status='WAITING'` and `ix_schedule_due(next_fire_at,id) WHERE enabled` match their due-row paths. Unique `(workflow_run_id,timer_id)` and `(schedule_id,scheduled_for)` identities prevent duplicate timer/occurrence materialization. `ix_child_reconciliation(checked_at,id)` supports child polling. Observe overdue backlogs and bounded schedule catch-up, not just an empty table.

## Inspection/history

In psql, set a known fixture ID with `\set run_id 'UUID-FROM-YOUR-TEST'` before these read-only queries:

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM workflow_event WHERE workflow_run_id=:'run_id'::uuid ORDER BY sequence_number;
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM activity_execution WHERE workflow_run_id=:'run_id'::uuid ORDER BY activity_id;
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM workflow_run WHERE id=:'run_id'::uuid;
SELECT state,wait_event_type,wait_event,count(*) FROM pg_stat_activity
WHERE datname=current_database() GROUP BY state,wait_event_type,wait_event;
SELECT pid,clock_timestamp()-xact_start AS transaction_age,wait_event_type,wait_event
FROM pg_stat_activity WHERE xact_start IS NOT NULL ORDER BY xact_start;
```

Existing unique `(workflow_run_id,sequence_number)` and `(workflow_run_id,activity_id)` indexes support ordered lookup; run inspection uses the PK. Signal delivery/wait unique identities support exact run/node/delivery lookup; compensation has `ix_compensation_run`. Agent iteration uniqueness, tool run/proposal uniqueness and route-attempt identity constraints support their bounded inspection paths. Large history responses still require a future pagination/retention design; an index does not bound API output size.

Use pg_stat_database/pg_locks, pool metrics and host IO/CPU to separate lock waits from connection waits or slow storage. pg_stat_statements is optional environment-owned instrumentation. Only propose a new index after preserving an actual plan/workload and measuring read improvement against write/vacuum/storage cost. Update statistics and consider data distribution before blaming the planner. No EXPLAIN/DB measurement has run in this workspace.
