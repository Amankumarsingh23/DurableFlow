# DurableFlow project status - runtime verification started

Current evidence: [runtime verification board](runtime-verification-progress.md). Main/test compilation, packaging and 141 unit tests pass; Docker-backed checks remain pending.

Implementation status is separate from execution evidence. Major feature sessions end here; next work is runtime verification, defect correction, evidence collection and presentation polish. This repository is not claimed production-ready.

## IMPLEMENTED

- Versioned persisted workflows, logical activity checkpoints, ordered transactional history and a PostgreSQL task queue.
- SKIP LOCKED claims, renewable leases/heartbeats, generation fencing, bounded retries/backoff/jitter, handler timeouts and shutdown controls.
- Timers, early/bound signals, static parallel DAGs, Saga compensation, child workflows and recurring schedule occurrence identities.
- Fake model/retrieval/MCP-style tools, frozen route plans, constrained fallback/local circuit breakers, immutable requests, exact approval binding and durable agent reservations/budgets.
- Best-effort after-commit telemetry, safe metadata/cardinality controls, agent summary, deterministic synthetic evaluation contracts and report renderer.
- Session 27 raw HTTP/service JSON byte limits, definition node/dependency bounds, structural JSON limits, strict control-DTO unknown-field setting, placeholder secret guidance and validation tests.
- Opt-in isolated PostgreSQL benchmark scenarios, a deterministic chaos test runner/matrix, retrieval-grounded agent demo input, deployment/SQL/queue ADR, concise README, interview/resume material and runtime checklist.

No schema/index migration or new dependency in Session 27; existing V1-V13 remain unchanged. Existing tests are retained. New test files contain nine focused unit checks, two HTTP/PostgreSQL integration checks and one explicitly opt-in benchmark method. Counts describe authored tests, **not executed tests**.

## STATICALLY VERIFIED

Checks executed on 2026-10-26 in the Windows workspace:

- All 259 Java source/test files parse without tree-sitter syntax errors. This does not check types, imports, overloads, Spring wiring or behavior.
- All JSON artifacts and pom.xml parse; example definitions/start requests are consistent with their intended names and versions.
- PowerShell parser reports zero syntax errors for the new benchmark/chaos runners and changed bounded-agent demo.
- Chaos class/method selectors exist in test sources; local Markdown file links are checked separately from heading anchors.
- `git diff --check` passes. Targeted example/config secret review found placeholders and existing local demo DB fallbacks, not real provider credentials; this is not a comprehensive secret/security audit.

## RUNTIME VERIFIED

Main compilation (210 source files), test compilation (49 source files), executable JAR packaging and the complete non-integration unit selection are now verified under Microsoft OpenJDK 21.0.13.0.2 / Maven 3.9.16. Unit result: **141 tests, zero failures/errors/skips**. Three initial test compilation errors were fixed without weakening assertions. See the [sanitized unit evidence](verification/unit-results-2026-10-26.json).

Migrations, application startup, PostgreSQL integration/chaos/evaluation, demos and benchmarks remain unverified. No full-suite/production/capacity claim is implied.

## NOT YET VERIFIED

The following table records the initial Session 27 setup attempts, **before** portable Java/Maven setup. The current verification board above supersedes the old missing-Java/Maven blockers; Docker/WSL and script policy remain unresolved.

| Initial attempt / category | Initial result |
| --- | --- |
| java -version, mvn -version, docker version | Commands not found on PATH |
| mvn -B -DskipTests compile, mvn -B test | Maven unavailable; neither compilation nor tests ran |
| docker compose up -d --wait postgres | Docker unavailable; PostgreSQL/migrations not started |
| mvn -B spring-boot:run / java -jar target/durableflow-0.0.1-SNAPSHOT.jar | Maven/Java unavailable; Flyway/app startup blocked |
| BenchmarkIntegrationTest smoke via Maven | Maven unavailable; no new benchmark report |
| run-chaos.ps1, run-benchmarks.ps1, demo-bounded-agent.ps1 | PowerShell policy rejects script loading; policy was not changed |
| GET localhost:8080/actuator/health, 2-second timeout | Timed out; live API unverified |
| Security/resource bounds and strict DTO behavior | Tests authored; Java/Spring/HTTP behavior awaits execution |
| Independent worker processes, real OS kills, network faults and DB restore | Not performed; controlled fixtures are not substitutes for these experiments |

Follow [runtime-verification-checklist.md](runtime-verification-checklist.md), retain raw evidence with commit/settings, and update only categories actually verified. Old target reports must not be presented as a fresh invocation's result.

## DEFERRED

Production authentication/reviewer authorization and tenant quotas; complete ingress/time/rate controls; history pagination/retention; real LLM/vector/MCP transport; external collector/dashboards/alerting; arbitrary runtime map fan-out; general automatic forward-run reconciliation/agent scheduling; cluster-wide circuit health; PostgreSQL HA/backup/PITR automation and restore validation; HTTP/multi-process/soak scaling evidence; universal exactly-once execution (not a valid generic claim). Kafka/SQS/RabbitMQ/NATS adoption requires measured need and a state/message consistency design.

External effects still depend on provider idempotency/retention. Model generation can repeat. PostgreSQL primary capacity is finite, prompt-injection mitigation is layered, and explicit advancement/driver recovery remains necessary as documented. See [architecture](architecture.md), [deployment](deployment.md), [security](security-resource-limits.md), [benchmarks](benchmarks.md) and [interview defense](interview-defense.md).
