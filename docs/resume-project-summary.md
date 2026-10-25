# Resume material

## Title

**DurableFlow - Durable Workflow and AI Agent Runtime**

## Resume bullets

- Built a Java/Spring Boot/PostgreSQL workflow runtime with SKIP LOCKED task claims, renewable leases, generation fencing, bounded retries, durable timers/signals, Saga compensation and static DAG/child-workflow composition.
- Implemented checkpointed AI orchestration with scoped retrieval, frozen model routes, bounded fallback, policy-controlled immutable tools, request-bound human approval and persisted agent budgets.
- Added deterministic failure/regression tests, a versioned synthetic AI evaluation suite, best-effort bounded telemetry and opt-in PostgreSQL benchmark harnesses; runtime validation and measured performance remain pending.

Use these as implementation statements, not proof of deployment or measured capacity. Before interviews, run the checklist and replace the last clause only with actual evidence. Do not copy teaching examples' illustrative throughput figures.

## 30-second pitch

DurableFlow is a workflow and AI agent runtime I implemented with Java, Spring Boot and PostgreSQL. It persists state and queues work transactionally, uses concurrent SKIP LOCKED claims with leases and fencing, and separates external effects from orchestration. I then added timers, signals, compensation and DAG composition, plus retrieval/model/tool activities with human approval and persisted agent budgets. The next milestone is executing the full regression/benchmark plan; I have not claimed a throughput number or production readiness.

## Two-minute pitch

Long-running business workflows fail in the gaps between a side effect and a local checkpoint. My design persists definitions, node state, task ownership and outputs in PostgreSQL rather than relying on process memory. Claim and completion are short transactions around external work. A lease supports recovery when a worker dies, while a generation fence prevents a paused worker from committing after a replacement takes ownership. Stable provider idempotency is required to avoid duplicate effects when attempts repeat.

On top of that I implemented durable timers and early signals, static parallel joins, Saga compensation that accounts for in-flight siblings, child workflows and recurring occurrence identities. In the AI layer, model calls and retrieval are normal activities. A frozen route plan bounds provider retries/fallback; tools require trusted schema, permission and policy checks, and high-risk refunds wait for approval bound to an immutable request fingerprint. Agent iterations and reservations survive restarts.

I added deterministic safety/evaluation contracts, sanitized aggregate telemetry and a test-driven chaos matrix. I kept PostgreSQL as the initial queue because it lets task/state/history writes commit atomically and avoids another infrastructure dependency. The main tradeoffs are primary-database scaling, explicit advancement/agent driving, local breakers and fake integrations. Runtime verification is still pending in this environment, so my benchmark harness is a plan for obtaining evidence rather than an invented performance result.

## Stack and concepts

Stack: Java 21, Spring Boot, Spring MVC/Validation, JPA/Hibernate, JDBC, PostgreSQL 17.6, Flyway V1-V13, Micrometer/Actuator, Maven, JUnit/AssertJ/Mockito, Testcontainers and PowerShell. See pom.xml for the pinned framework version. No Kafka/Redis/vector DB or real LLM SDK is implied.

Distributed systems: transaction boundaries, durable state machines, lock/lease distinction, SKIP LOCKED, heartbeat/fencing, at-least-once processing, idempotent effects, retry backoff/jitter, timer/signal recovery, compensation, durable joins/composition and atomic occurrence identity.

AI agents: deterministic fake adapters, scoped/frozen RAG evidence, logical-call vs physical-attempt accounting, capability/quality/cost-constrained routes, tool proposal vs authority, exact human approval binding, durable reservations/budgets, objective golden evaluations and cardinality/privacy-aware diagnostics.

## Claims to avoid

Universal exactly-once execution; event sourcing/replay as the source of state; production-ready/hyperscale; completed runtime/chaos/benchmark verification; any throughput/latency/availability number; real MCP/LLM/vector integrations; built-in PostgreSQL HA; authenticated production approvals; complete prompt-injection immunity; multi-host scaling measured by a single-JVM SQL harness. Write down measured hardware/workload/commit/results before adding numerical resume claims.
