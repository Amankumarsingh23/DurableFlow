# ADR 0001: retain PostgreSQL as the initial durable task queue

Status: accepted for this implementation; capacity evidence pending.

Workflow transitions, task creation, signal/timer consumption and history often must commit together. Keeping the initial queue beside authoritative state avoids a second independently acknowledged message system and its dual-write failure window. Short transactions with FOR UPDATE SKIP LOCKED let concurrent consumers claim distinct eligible rows. Leases, heartbeat and generation fencing then govern work after the row lock is released. Operationally this uses the PostgreSQL dependency already required by the workflow state model.

Consequences: the primary, indexes, connection pool, polling/row-lock load, write amplification and vacuum can become bottlenecks. FIFO under concurrency is approximate; head-of-line/global ordering is not promised. PostgreSQL is not unlimited, and the repository currently has no verified throughput target or measured scaling curve. The [benchmark harness](../benchmarks.md) will measure starts, claims/completions, queue tails, timer lag, DAG advancement and signal delivery before changing architecture.

Reconsider when repeated representative measurements show sustained required task throughput exceeds the explicitly agreed PostgreSQL design target, poll/lock/IO overhead is material to application state traffic, queue p95/p99 latency cannot meet its target after justified query/pool tuning, or independent queue scaling/retention/consumer semantics are required. Record hardware/workload/SLO/error/invariant evidence and operational cost. No arbitrary tasks/sec threshold is invented here.

Kafka may suit durable streams/replay and partitioned consumers; SQS/RabbitMQ/NATS may suit different queue/delivery/operational needs. Select against measured requirements rather than their names. A broker would not replace PostgreSQL workflow truth, fencing or provider idempotency. Design a transactional outbox/inbox or equivalent reconciliation protocol for state-to-message delivery, duplicate handling, ordering, backpressure and acknowledgement failure windows. No broker/Redis implementation is added in Session 27.
