# Session 26: observability and deterministic evaluation

Observability explains running behavior: latency, queue pressure, failures, retries, cost and routing transitions. Evaluation checks observed behavior against independently authored expectations on a versioned dataset. Healthy HTTP responses and low latency do not establish answer quality or permission safety.

## Architecture and failure boundary

`DurableFlowTelemetry` is the vendor-independent sink. `MicrometerTelemetry` uses the existing Actuator dependency; no SDK, collector or new Maven dependency was added. `SafeTelemetry` catches call-boundary exceptions, sanitizes metadata and bounds metric labels. `TelemetryDispatcher` owns one daemon consumer and a queue of 1,024 observations. Full queues drop observations; consumers catch runtime exceptions. The local `durableflow.telemetry.dropped` and `.failed` gauges expose those losses independently of the sink.

Inside a transaction, hooks register an after-commit callback that **only submits work**. Rollback produces no observation. Sink calls and `HistoryTelemetry` projection queries run on the consumer thread, outside the workflow transaction. Provider calls keep their existing out-of-transaction boundary. No remote exporter is configured. An eventual remote adapter needs its own connection/read deadlines: a blocked sink stalls telemetry and causes drops, without holding a workflow lock. Process death/shutdown can lose the queue. This is deliberately not an outbox or exactly-once telemetry pipeline.

History append remains mandatory and atomic with the state transition. The new projection cannot roll back history or workflow state. Lease generations, idempotency identities, approval binding, route snapshots, reservations and compensation transactions retain their existing authority. Metrics can lag, disappear, or be absent on another instance. Queue metrics are per-process views of the same shared database and **must not be summed across API/worker replicas**. Polls default to five seconds (`durableflow.telemetry.queue-poll-ms`); failed polls retain the last depth and increase sample age. Scrapes do not query the database.

## Logs, metrics, traces and history

| Signal | Purpose | Durability / cardinality |
| --- | --- | --- |
| Metrics | Aggregate counts, distributions and current depth | Best effort; fixed names and bounded labels |
| Logs | Correlated events, classifications and diagnostics | Best effort; safe IDs allowed; retention/access controlled by operator |
| Trace links | Parent/child causal relationships across work | Internal immutable context contract; no collector or exported spans yet |
| Workflow history | Ordered committed state transitions and business audit | PostgreSQL transactionally authoritative; never reconstructed from metrics |

`TelemetryCorrelation` supports workflow/activity/agent/logical-model/tool IDs, iteration and safe provider request IDs. Workflow logs include run/activity IDs; agent history adds agent IDs; route logs link through the logical model activity ID; tool logs/history link request IDs and fingerprints. IDs let an operator join durable history, route attempts and iterations. They never enter metric labels.

`TraceContext` defines explicit immutable parent/child links for workflow -> activity -> retrieval or logical model -> provider attempt or tool execution. It has no ThreadLocal and requires explicit propagation across threads. This is a future tracing adapter seam, **not a claim that distributed spans are currently exported**. Existing durable run/activity/child/iteration relationships remain available for reconstructing causality.

History projection logs reclaim, retry scheduling, tool approval/rejection, agent limit and compensation failures. Routing hooks log fallback, terminal routing and circuit state transitions. Tool attempts and provider outcomes emit classified events. No exception message, business payload or generated reasoning is added to the new telemetry path.

## Metric inventory

Names below have prefix `durableflow.`. Counter suffixes/formats depend on the eventual registry exporter. In the built-in Actuator metrics API, use the dotted names shown here. Timers expose count and total/max latency; summaries expose count/total/max, without enabling high-volume percentile histograms by default.

| Family | Measurements | Bounded dimensions |
| --- | --- | --- |
| Engine | `workflow.events`, `activity.events`, `workflow.duration`, `activity.duration`, `activity.attempt.duration` | status; activity type for attempts |
| Queue | `queue.depth`, `queue.sample.age` (seconds) | READY / CLAIMED |
| Model | `model.logical.calls`, `model.attempts`, `model.attempt.duration`, `model.input.tokens`, `model.output.tokens`, `model.cost.micros` | provider, model, outcome where applicable |
| Routing | `model.retries`, `model.fallbacks`, `model.exhausted`, `model.circuit.skips`, `circuit.transitions` | provider/reason/state where applicable |
| Retrieval | `retrieval.calls`, `retrieval.duration`, `retrieval.candidates`, `retrieval.selected`, `retrieval.context.chars`, `retrieval.budget.utilization` | DENSE / SPARSE / HYBRID and outcome |
| Tools | `tool.events`, `tool.attempts`, `tool.duration`, `tool.approval.wait`, `tool.retries`, `tool.dedup` | trusted tool, risk and status |
| Agents | `agent.events`, `agent.iterations`, `agent.model.calls`, `agent.model.attempts`, `agent.tool.calls`, `agent.tokens`, `agent.cost.micros`, `agent.approval.waits`, `agent.fallbacks` | status / termination reason where applicable |

Workflow events include started, completed, failed, compensating, compensated and compensation failure. Activity status separates starts/claims, completed/failed, retries, timeouts and reclaims. Logical activity duration uses persisted started/completed timestamps; attempt duration measures each actual handler call. A timed-out, uncooperative handler may finish later: its attempt timer is not the task timeout authority. Standalone synchronous execution emits history observations but does not pass through the worker attempt timer.

Model logical calls count durable route creation, with legacy fake model logical calls projected from first claims/history starts. Provider attempts count invocations separately from logical calls. Outcome-specific timer counts expose success/failure. Token and cost summaries include known returned usage, with fake model rates only; failures with unknown usage are not assumed free. They are **not billing accounting**. Legacy fake models use the established fake input=1/output=2 micro-unit convention. Model aliases outside the bounded catalog collapse to `other`; legacy aliases share `legacy`. Fallback counts committed route transitions; compute a transition-per-logical-call rate with consistent time windows. This can exceed one when multiple routes are traversed. Circuit skips are reservation-time skips of a frozen route, not every router candidate filtered out by a local circuit heuristic.

Retrieval candidate count is the count of matching, authorized candidates before top-K, not the full corpus or unauthorized tenant count. The four-argument response compatibility constructor can only report returned candidates; real adapters must populate the explicit count. Selected count measures ranked top-K chunks; context size/utilization measures whole chunks actually packed by ContextBuilder. A packed source may be omitted due to budget. Retrieval quality uses ranked selected chunk IDs and a declared K.

Tool events count request creation, approval/rejection and committed execution; attempts measure success/failure separately. Approval wait uses approval/event time minus request creation, including rejection wait. Read-only auto-approval also increments the APPROVED counter with READ_ONLY risk, but has no human-approval event/wait timer; exclude READ_ONLY when calculating human acceptance. Dedup counts replay observations at either the already-executed handler or fake provider ledger; it is not a count of unique prevented effects. Agent terminal summaries sample committed totals; events retain FAILED vs COMPLETED vs LIMIT_REACHED. Approval waits count requests that required review, including denied reviews. Metrics are diagnostics rather than correctness evidence.

`GET /api/agent-executions/{id}/summary` returns compact status, iteration and logical-call counts, actual durable route attempts, known route usage/cost, reservations, fallback transitions, approval counts and termination reason. A repeatable-read transaction gives a consistent snapshot. It excludes goals, prompts, tool arguments/results and final answer text. Pending/lost provider attempts may have unknown usage. Legacy direct test fixture execution is intentionally not a billing source.

## Cardinality and safe metadata

Metric names are an enum. `MetricLabels` permits only provider, model, tool, risk, strategy, status, activity and reason keys, each with a finite explicit catalog and a per-metric key allowlist. Unknown values collapse to `other`; unknown keys are discarded. Run IDs, activity IDs, tool-request IDs, user IDs, tenants, hashes, provider request IDs, queries and arbitrary definition names are never labels. A new model/tool/reason needs a deliberate catalog change to obtain a distinct series. Adding every future provider/model dynamically would defeat this bound.

IDs have one value per execution, so a run ID label would create a new time series for every run and multiply every other dimension. Safe IDs in logs/trace links permit targeted diagnosis without this metric explosion. New types of metric labels require review even if the values look innocuous.

`SafeMetadata` denies fields by default. It accepts UUID identities, bounded nonnegative counts/sizes, SHA-256 fingerprints, finite classifications and explicitly recognized local fake provider-request-ID formats. Field aliases normalize internal `code`, `attemptCount`, `toolName` and `riskLevel`. Strings are not made safe merely by truncation or a character regex. Unknown classifications become `other`; nested objects and business text are dropped. The Micrometer/logging adapter sanitizes again.

Full prompts, goals, RAG chunks, arguments, results, secrets and chain-of-thought are excluded. Fingerprints can reveal equality and are still sensitive operational metadata. This helper is an allowlist, not a PII detector. Existing business checkpoints still contain the data needed for durable execution: restrict database access and apply a retention policy separately. Eval fixtures remain synthetic. A future real provider-request-ID format must be explicitly approved before logging it.

## Deterministic evaluation

`EvalCase` declares synthetic input, scenario, independent expectations, case version, known relevant IDs and K. `EvalSuite` carries required version metadata. `EvalRunner` invokes a scenario executor; it never invents observed facts. `Evaluator` is the extension seam; `ObjectiveEvaluator` checks equality (including numeric normalization), nonnegative upper bounds, required/forbidden collection membership and exact canonical tool fingerprints. Schema validity is evidence measured using `AgentDecision.parse` in the real fixture, not a judge opinion. Missing/malformed evidence fails. Executor exceptions fail that case and allow the rest of the suite to continue; reports omit exception messages because those may contain content.

`src/test/resources/eval/golden-v1.json` defines twelve cases:

| Case | Actual exercised path and objective contract |
| --- | --- |
| A final agent | Terminal FINAL schema, no tool execution, iteration/cost bound |
| A read-only | One bounded getOrder agent iteration; automatic permission path, no human approval or refund |
| B approved refund | Full refund agent, exact fingerprint and approval binding, zero effects before approval, one refund |
| C rejected refund | Human denial terminates the agent, no refund |
| D exact identifier | SPARSE ACCT-1042 retrieval, relevant account source, no private-b source |
| E hybrid | HYBRID policy retrieval with known relevant refund chunks |
| F hostile document | Real hostile-note retrieval frozen into agent input; write still waits for review; rejection prevents effect; tenant-b content absent |
| G unknown tool | Explicit synthetic adversarial model checkpoint injection; schema may be valid but unknown tool is rejected before request/effect |
| H fallback | Two failed primary attempts then fake-b success through durable retry/route records |
| I quality floor | Only primary meets required quality; exhaustion cannot execute cheaper weak candidate |
| J runaway | Agent reaches the iteration limit, with reservations inside token/cost budgets |
| K crash | External effect commits before worker completion; replacement replays same identity, old owner is fenced, approved arguments cannot mutate |

`GoldenEvalIntegrationTest` resets an isolated Testcontainers PostgreSQL database for each case, invokes the real services/workers with local fake providers, collects persisted facts, and produces `target/eval-report.json` and `.md`. It writes the report before asserting zero failures. It does not synthesize observed outcomes from expected values. The hostile-document case tests the host policy boundary, not the robustness of a commercial model. The read-only case deliberately ends at a one-iteration budget; A-final separately tests successful finalization. Synthetic checkpoint injection is test-only and is labeled in the fixture.

The optional schedule/child golden case was not duplicated here: `WorkflowScheduleIntegrationTest` and `ChildWorkflowIntegrationTest` remain part of the complete regression command. Their invariants are listed below. No schema migrations were needed: V1-V13 remain the durable source of facts.

For cases with known relevance, Recall@K is unique relevant hits in the first K results divided by the known relevant set size. Reciprocal rank is 1/rank of the first relevant hit within K, otherwise zero; its mean is MRR@K over only relevance-labeled cases. Duplicate hits cannot inflate recall. These metrics do not measure generated answer faithfulness. A retriever can return the right evidence and a generator can still misuse it; a good answer on a lucky case cannot establish retrieval quality.

Report fields include total/pass/fail/pass-rate, failed-case counts per category, total/average measured cost, average iterations/model/provider/tool calls, fallback totals and retrieval metrics. Measured-case counts accompany aggregates; missing usage does not silently become zero. Per-case versions include dataset/suite, prompt/context/request, route policy/provider/model, retrieval index/strategy, tool policy and workflow fixture versions. A version mismatch is a failing check. Fixture family versions are declared explicitly; actual route policy/model and workflow versions are also read from the database. Random fixture workflow names are replaced by their versioned family in reports. Bump the declared prompt/fixture version when changing those implementations; this is not automatic semantic version detection.

No real eval report has been generated in this environment: Java/Maven/Docker are unavailable. A committed passing report would misrepresent verification. Renderer behavior and negative cases are covered by unit tests, which also await execution on a configured machine.

## Running and inspecting

Prerequisites: JDK 21, Maven and Docker/Testcontainers with access to pull postgres:17.6.

```powershell
mvn -B -DskipTests compile
mvn -B test
mvn -B -Dtest=GoldenEvalIntegrationTest test
mvn -B '-Dtest=TelemetryBoundaryTest,AiTelemetryTest,TelemetryFailureIntegrationTest' test
# Or use the convenience script after prerequisites are installed:
.\scripts\run-evaluation.ps1
# With the API and PostgreSQL already running:
Invoke-RestMethod http://localhost:8080/actuator/metrics
Invoke-RestMethod http://localhost:8080/actuator/metrics/durableflow.queue.depth
Invoke-RestMethod http://localhost:8080/api/agent-executions/<agent-id>/summary
```

Actuator exposes health and metrics. Names are registered when observed, apart from dispatcher self-health; absence of a series is not proof of zero events. Keep management endpoint access within the intended deployment network. There is no Prometheus exporter, dashboard or remote collector in this session. JSON and Markdown reports are generated in ignored `target/`, so execution history stays local unless deliberately exported.

## Correctness invariants and tests

| Target | Evidence |
| --- | --- |
| Duplicate external effects = 0 | Golden K, tool/payment idempotency integration tests |
| Unauthorized tool executions = 0 | Golden C/F/G/I, existing permission/policy tests |
| Accepted stale fenced commits = 0 | Golden K, TaskLease/timeout/recovery/routing tests |
| Duplicate scheduled occurrence runs = 0 | WorkflowScheduleIntegrationTest, DB occurrence identity constraint |
| Accepted approved-payload mutations = 0 | Golden K and ToolExecutionIntegrationTest DB immutability tests |
| Agent iterations beyond max = 0 | Golden J and BoundedAgentIntegrationTest |
| Telemetry exceptions roll back workflow state = 0 | TelemetryFailureIntegrationTest using a throwing primary sink |
| Exporter work in workflow transaction = 0 | TelemetryBoundaryTest checks background thread transaction context; failure integration checks actual workflow with sink |

Metrics showing zero are insufficient evidence for an invariant: a dead sink also reports nothing. Database constraints, fencing, state transitions and adversarial integration tests enforce these properties. Unit tests additionally cover random-ID cardinality, sanitizer leakage, rollback suppression, saturated queues, retrieval/model measurements, evaluator negative cases, version mismatch, missing evidence and report arithmetic. Existing Session 10-25 tests remain included in the full suite.

## Subjective judgments and online signals

Structured schema, tool identity, arguments, approval, source membership and budgets are objective. Use deterministic evaluators for them: cheaper, reproducible and directly tied to contracts. Correctness of an open-ended answer, faithfulness to evidence and relevance to a question may need human review. `GenerationJudge` is a future optional interface with no implementation, remote calls or automatic approval authority. Future judges need calibration and labeled human examples; disclose model/prompt version, nondeterminism, bias, self-preference, prompt sensitivity and cost. A judge's number is not ground truth.

Later online analysis can join approval acceptance/rejection, tool/workflow success, compensation/fallback rate, user feedback and cost per successful workflow with deployed versions. Keep denominators explicit: approved/(approved+rejected human decisions), success/(attempted executions), known cost/completed workflows. Separate cohorts and delayed/pending decisions. A low fallback rate with many failed workflows is not an improvement. No warehouse or alerting system is added here.

## Session checkpoint and interview defense

1. Observability describes executions; evaluation compares measured behavior with expectations.
2. Logs are diagnostic records, metrics aggregate series, traces causal links, history a committed ordered business/state audit.
3. Unique IDs multiply time-series cardinality; logs/traces permit searchable identity joins without aggregate series proliferation.
4. Record logical calls vs attempts, provider/model, outcome/latency, known usage/rates/cost, retries, route transitions, exhaustion and circuit skips; join the durable plan and reservations for individual runs.
5. Payloads contain private content, secrets and hostile instructions; retain only deliberately approved metadata in operational telemetry.
6. Retrieval quality checks evidence selection, while generation quality checks use of that evidence. Their failures require different fixes.
7. Objective contracts are reproducible and inspectable; an LLM judge adds uncertainty, cost and bias without improving an exact comparison.
8. A golden dataset is a small versioned collection of independently specified expected behaviors; replay exposes regressions in known safety and functional cases.
9. Versions establish what changed and make comparisons meaningful; otherwise dataset/prompt/index/policy drift can masquerade as a model regression.
10. Workflow correctness must survive unavailable diagnostics. Mandatory state/history persists, while lossy telemetry cannot hold locks or decide execution outcomes.

For approval acceptance falling 92% -> 61%, rising fallback and doubled cost per successful workflow: first align time windows, completed human-decision denominators, deployment versions and tenant/use-case mix; check telemetry drop/failure and queue age. Separate failed workflows from completed ones and known usage from unknown cost. Compare provider attempt failure/latency, circuit transitions, route plans, quality constraints and per-attempt cost before/after. More expensive fallbacks caused by provider health differ from a route policy selecting an expensive primary.

Use correlated activity/agent/route/tool IDs to inspect a small authorized sample of durable checkpoints and approval outcomes. Compare retrieval strategy/index, exact-identifier hits, source authorization, top-K ranks and packed-context omissions against the previous configuration. Separate evidence quality from structured decision quality. Check tool schemas, risk classification, exact fingerprints and policy/approval requirements for deployment drift rather than assuming every rejection is a bad model answer.

Replay the same golden dataset with four pinned combinations: old routing+old retrieval, new routing+old retrieval, old routing+new retrieval, new routing+new retrieval. Hold prompt, model scripts and tool policy fixed initially, then vary one factor at a time. Compare objective failures, Recall@K/MRR, attempts, fallback and cost/iterations, and inspect interactions. Add synthetic cases representing the observed regression without copying private production payloads. Open-ended quality still needs calibrated human review. Roll back the implicated configuration only after evidence identifies it; never relax permissions or approval binding to improve acceptance statistics.
