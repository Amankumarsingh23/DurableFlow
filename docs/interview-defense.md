# Interview defense

Describe implemented mechanisms and authored assertions; do not present unrun tests or illustrative numbers as evidence. Runtime verification is the next milestone.

| Question | Concise answer |
| --- | --- |
| What problem does DurableFlow solve? | Long-running work must survive process failure and delayed inputs without losing state or blindly repeating effects. DurableFlow persists orchestration and executes work through fenced workers. |
| Why persist workflow state? | Process memory disappears. Persisted definitions, status, outputs and waits allow readiness to be reconstructed after restart. JVM stacks are not resumed. |
| Why PostgreSQL queue? | Task/state/history transitions can commit together using the dependency already required for durability. This avoids an initial broker dual-write gap and extra operations. Capacity remains unmeasured. |
| Why SKIP LOCKED? | Concurrent consumers skip locked candidate rows rather than waiting behind one claim. It coordinates short claim transactions, not the whole handler execution. |
| Row lock vs lease? | Row locks protect a transaction; a persisted expiring lease represents ownership after the transaction commits. No DB lock is held during external work. |
| Lease vs heartbeat? | The lease expires unless renewed. Heartbeats extend a valid lease while work continues; they cannot resurrect already expired ownership. |
| Why fencing? | A paused worker can resume after a replacement takes over. Owner/generation/deadline predicates reject its stale local writes, even if the process still thinks it owns the task. |
| At-least-once vs effectively-once? | Physical attempts can repeat. Stable provider idempotency plus immutable semantics and local fencing can produce one business effect where the provider cooperates. This is not universal exactly-once execution. |
| Idempotency identity? | It belongs to the logical activity/tool request, not a worker or retry attempt. Reusing a key with changed semantics is rejected. Real provider retention/atomic dedup are required. |
| Retry/backoff/jitter? | Only explicitly transient classifications get bounded retries; available_at persists backoff and jitter. Permanent validation/permission failures should not blindly retry. |
| Timers? | Store a wake deadline and select due rows under locks; restarting late fires after the deadline, not before it. No sleeping thread represents the durable wait. |
| Signals? | Store exact-target delivery identity/payload. Early signals can buffer; same identity replays, changed payload conflicts, and receipt/consumption/downstream readiness commit atomically. |
| Saga vs rollback? | A Saga issues new compensating business actions after committed effects. Compensation can fail and must itself be idempotent; it does not undo the past like an uncommitted SQL transaction. |
| DAG fan-out/fan-in? | Several static eligible nodes get distinct tasks. A join waits for durable predecessor satisfaction and unique scheduling. Arbitrary runtime map fan-out is deferred. |
| Late sibling during compensation? | Already-running forward siblings must settle so successful effects are included before compensation order is chosen. Starting undo too early could miss a late success. |
| Child workflows? | Separate pinned child runs have durable parent/node links and terminal reconciliation; they compose lifecycles without merging all steps into a single definition. |
| Schedules? | Persist exact occurrence identity and create occurrence/run atomically; bounded catch-up limits overdue work. This is not an in-memory cron callback. |
| Model calls as activities? | Queue reliability, durable inputs/results and budgets apply around generation. Generation can repeat if a response is lost; only a stored valid result is reused. |
| RAG checkpoints? | Scope/filter retrieval and freeze source-tagged context before model dispatch; retries must not silently query a different corpus and change the logical request. |
| Proposal vs execution? | A model emits untrusted structured intent. Host schema, allowlist, permission, policy and exact human approval authorize a separate immutable tool request and worker action. |
| MCP discovery vs authorization? | Advertising a tool/schema does not permit invoking it. The host's trusted registry/permissions/policy and reviewer controls decide authority. Transport here is fake. |
| Agent budgets? | Iterations/calls/token/cost reservations are persisted before work so restart cannot reset limits. Known usage and maximum authorized reservation are distinct. |
| Retry vs model fallback? | Retry repeats the same eligible route for a transient failure. Fallback moves to a frozen, prevalidated alternative after bounded conditions; it cannot silently relax quality or spend unreserved cost. |
| Observability vs evaluation? | Observability describes executions; evaluation compares measured behavior with independent expected contracts. Neither replaces transactional state or safety enforcement. |
| Why no Kafka yet? | No measured broker requirement exists. First measure PG queue throughput/tail latency and lock/IO costs; a broker would require outbox/inbox and reconciliation, not just a library. |
| Current limitations? | Compilation/runtime/benchmarks remain unverified here; no production auth/HA/collector/real AI adapters, local circuit breakers, static DAG, explicit advancement/agent driver, and unbounded inspection histories. |

## Session 27 checkpoint

1. Queue latency isolates time waiting for execution capacity from actual handler/provider time; tuning them requires different actions.
2. More workers eventually compete for the same primary, locks, connections, CPU/IO and external capacity; increased contention can worsen tails without raising throughput.
3. Indexes cost writes, storage and vacuum. Measure a specific query/data distribution and validate its plan before adding one.
4. Crash after effect tests stable semantic identity and provider-owned atomic dedup/replay; it proves the simulated provider behavior, not an unsupported external vendor guarantee.
5. Leases expire but cannot stop a paused process. Fencing rejects a zombie's stale commit after ownership has transferred.
6. Application crash recovery assumes durable DB state survived. Database loss requires tested backups, WAL/PITR and environment-owned failover/restore procedures.
7. Login establishes identity; authorization separately controls which runs/signals/risk levels a caller can access or approve.
8. Attackers can consume parser memory, graph work, DB writes and model/tool budgets with valid-looking requests. Resource bounds limit that abuse but do not replace rate/tenant quotas.
9. Sustained required load beyond a measured PG target, material poll/lock load, tail-latency SLO failure or independent queue scaling/semantics can justify a broker after simpler measured tuning.
10. Universal exactly-once, production-ready/hyperscale, event-sourced replay, complete prompt-injection safety, verified tests or any throughput/p99 numbers would be dishonest claims before evidence.

## Final broker/scaling answer

I chose PostgreSQL because workflow state, task readiness and history often need one atomic commit. SKIP LOCKED permits concurrent short claims; leases and fencing govern ownership after commit while external work runs without holding the transaction. It keeps the first deployment operationally simple and avoids a state-to-broker dual-write problem.

I cannot yet claim a scaling number. I added an opt-in isolated PG harness for workflow starts, claims/completions, queue latency, DAGs, timers and signals, with 1/4/16-thread comparisons, p50/p95/p99 and error/completion checks. It still needs real execution, repeated representative loads and multi-process tests; no-op SQL/service throughput is not real AI/payment capacity. The primary, pool, polling, row locks, writes/vacuum and provider limits can plateau.

If measurements miss an agreed sustained-throughput or tail-latency target, show material queue contention with state traffic, or require independent queue scaling, I would evaluate an appropriate broker and design transactional outbox/inbox, deduplication and reconciliation. A broker would move delivery, not remove workflow truth, fencing or provider-idempotency requirements.
