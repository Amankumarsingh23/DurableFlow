# DurableFlow project status - runtime verification started

Current evidence: [runtime verification board](runtime-verification-progress.md). Full local regression and packaging pass: **368 passed, zero failures/errors, one intentional benchmark skip**. Flyway V1-V13, API startup, two independent workers, live DAG/Saga/agent demos and 12 synthetic golden cases have passed.

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

On 2026-10-26, Microsoft OpenJDK 21.0.13.0.2 / Maven 3.9.16 / Docker Desktop 29.9.2 / PostgreSQL 17.6 executed the full default test selection through `mvn -B -ntp package`: **369 selected, 368 passed, zero failures/errors, one explicitly opt-in benchmark skipped** across 49 suites. Main/test compilation and executable packaging passed. See [per-suite counts, tested commit, hardware and corrections](verification/runtime-2026-10-26/regression.json).

Flyway applied and Hibernate validated V1-V13 against disposable integration databases and the shared Compose database. A live API returned UP; two independent worker JVMs both processed tasks. Live parallel DAG completion, compensated Saga, retrieval checkpoint, exact request-bound refund approval, FINAL_ANSWER termination and MAX_ITERATIONS termination passed. One fake refund effect was recorded. The intentional failed Saga shipment is retained in the task-outcome evidence.

Controlled lease/reclaim/fencing, crash-after-effect idempotency, early signals, overdue timers, routing/fallback/budgets, resource bounds, immutable approved payloads and telemetry-isolation assertions passed in the regression suite. Golden evaluation: **12/12 synthetic cases**, [report](verification/runtime-2026-10-26/eval-report.md). These are fake-provider and controlled-hook results, not real-provider or arbitrary process-crash guarantees. See the verification board for the separate opt-in benchmark evidence.

Initial missing-tool/Docker failures and subsequent timezone, test lifecycle, stale schema assertion, absent-output and numeric-format corrections are preserved in the evidence history. Portable tools are outside Git; PowerShell script policy was not changed. The earlier [unit-only summary](verification/unit-results-2026-10-26.json) remains historical evidence, superseded by the complete run.

## DEFERRED

Production authentication/reviewer authorization and tenant quotas; complete ingress/time/rate controls; history pagination/retention; real LLM/vector/MCP transport; external collector/dashboards/alerting; arbitrary runtime map fan-out; general automatic forward-run reconciliation/agent scheduling; cluster-wide circuit health; PostgreSQL HA/backup/PITR automation and restore validation; HTTP/multi-process/soak scaling evidence; universal exactly-once execution (not a valid generic claim). Kafka/SQS/RabbitMQ/NATS adoption requires measured need and a state/message consistency design.

External effects still depend on provider idempotency/retention. Model generation can repeat. PostgreSQL primary capacity is finite, prompt-injection mitigation is layered, and explicit advancement/driver recovery remains necessary as documented. See [architecture](architecture.md), [deployment](deployment.md), [security](security-resource-limits.md), [benchmarks](benchmarks.md) and [interview defense](interview-defense.md).
