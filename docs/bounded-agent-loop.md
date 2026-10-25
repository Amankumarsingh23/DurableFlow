# Session 24: bounded durable agent loop

Session 25 extends model execution with [durable routing/fallback and route-aware accounting](model-routing-fallback.md). This document records the Session 24 baseline.

A model decision is a checkpoint, not an imperative to run another tool. AgentLoopService
advances one orchestration transition per transaction. It never invokes a model or tool provider.
The existing task workers execute those steps. There is no in-memory while(true), arbitrary
model-generated DAG, parallel agent branch, collaboration or self-modifying workflow.

## Execution and iteration state

V12__bounded_agent_loop.sql adds agent_execution and agent_iteration. V1-V11 are unchanged.
AgentExecution stores a host workflow id, immutable goal/scenario/limits/frozen retrieval context,
a frozen provider-attempt cap, current iteration, logical model/tool calls, observed model
attempts, known input/output tokens and estimated cost, conservative budget reservations,
termination reason/final output and lifecycle timestamps.

Agent statuses are RUNNING, WAITING_APPROVAL, COMPLETED, LIMIT_REACHED and FAILED. Terminal
statuses include a reason/completion time and are immutable. Iteration statuses are
MODEL_PENDING -> DECIDED -> TOOL_PENDING -> COMPLETED, with FINAL completing directly from
DECIDED; any terminal failure/limit closes the active iteration as FAILED. Iteration numbers
start at one, advance monotonically and are never reused. The schema enforces unique
(agent_execution_id,iteration_number), unique model/tool references, and a partial unique
index allowing only one MODEL_PENDING/DECIDED/TOOL_PENDING iteration per agent. Database
triggers protect immutable execution settings, nondecreasing counters and frozen iteration
identity/decisions/tool references. SQL checks also enforce configured logical/reservation caps.

Each advance locks agent_execution FOR UPDATE, then its host workflow run for history writes.
Concurrent calls serialize on durable state; no JVM mutex establishes identity. Repeated calls
observe the same pending step, attach one checkpoint, or no-op after termination. No new
iteration or provider call can be authorized after a terminal outcome.

Four fixed code-owned version-1 workflow templates are registered once under a transaction
advisory lock; an existing name/version with different steps is rejected:

- DurableFlowAgentHostV1: internal agentFinished SIGNAL.
- DurableFlowAgentModelV1: one agentModelCall activity.
- DurableFlowAgentReadV1: one executeTool activity.
- DurableFlowAgentWriteV1: approval SIGNAL -> executeTool activity.

Iterations reference the model/tool runs explicitly. These are linked agent work units, not
new CHILD_WORKFLOW nodes or mutations to an existing workflow definition. Fixed templates let
Session 23 retain its one-ToolRequest-per-tool-run invariant. The model never chooses handlers,
graph topology, provider configuration or risk. The host's internal terminal receipt completes
its wrapper workflow for every agent outcome; agent_execution.status/reason is the authoritative
business/limit result (host COMPLETED alone does not mean agent success). Public callers cannot
spoof this host SIGNAL. Actual tool review uses the existing public SIGNAL endpoint.

## One transition at a time

1. With no active iteration, check iteration/model/reservation budgets, deterministically build
   input, create a fixed model run/activity, persist the iteration and reserve its logical call
   and budget, enqueue work, commit and return.
2. While that activity is pending/running, return without creating more work. Retries/reclaims
   belong to TaskWorker. When output is COMPLETED, persist one decision plus observed attempts,
   usage and estimated cost in the agent transaction, then return.
3. For DECIDED/FINAL, store final output and FINAL_ANSWER and close the agent/host. For TOOL_CALL,
   check the tool budget and duplicate-write rule, validate the registered tool, create a fixed
   read or approval/tool run and exactly one Session 23 ToolRequest, count the logical tool call,
   materialize its queue or SIGNAL wait, commit and return.
4. While a request awaits approval, keep WAITING_APPROVAL and return. Approval permits execution
   but does not create another model call. When EXECUTED, copy its durable result to the iteration,
   mark that iteration complete and return. A later advance may create the next model iteration.

The control-plane caller must keep invoking advance as steps finish. There is no automatic agent
poller in V1; the demo uses a bounded client-side pump and the normal independent worker. This
satisfies completion permitting the next advance without a provider call inside the driver.
A process crash between any of these commits loses no durable logical state.

Workers lock only their own step runs and never the agent row. The driver reads existing step
checkpoints without locking those runs; it only locks newly created, not-yet-visible work runs.
This avoids an agent->existing-step vs step->agent lock inversion. History transitions hold the
host run lock; tool state/events and fenced completion keep Session 23's own run locking.

## Strict model decisions and usage

AgentDecision accepts exactly:

```json
{"type":"TOOL_CALL","toolName":"getOrder","arguments":{"orderId":"O-42"}}
```

or:

```json
{"type":"FINAL","answer":"Refund reference issued."}
```

Unknown types, prose, malformed arguments, extra risk/permission fields and reasoning fields
are rejected. FINAL answers are limited to 4000 characters; tool names/arguments are bounded.
The agent handler validates tool registry/schema before checkpointing arguments; permission/policy
validation still occurs independently before execution. No prose
parsing, arbitrary method resolution or private chain-of-thought storage is introduced.

agentModelCall wraps the existing ModelCallActivity/ModelGateway and uses the same activity
worker. It retains only version/provider/model, usage, latency, request reference and a validated
structured decision. Invalid decision content is discarded before activity checkpointing;
decisionSchemaValid=false preserves the safe usage summary and causes INVALID_MODEL_DECISION
on the next advance. Provider/gateway failures follow the existing retry classifier. The
activity's final persisted output is reused after restart; it is never regenerated just because
the agent has not yet copied its decision into agent_iteration.

Usage plus the iteration decision are accounted once in the driver transaction. Repeated or
concurrent advancement cannot count them twice. model_attempts records observed worker delivery
attempts when the model activity reaches a checkpoint/failure; a guard-rejected delivery may
have no provider invocation. Logical calls count semantic identities, not delivery attempts.
A transient retry can therefore yield one logical call and two observed attempts.

## Persistent budgets and termination

Default per-agent limits, configurable through the start request and frozen thereafter:

| Limit | Default | Bound |
| --- | --- | --- |
| maxIterations | 8 | 1-20 |
| maxLogicalModelCalls | 8 | 1-20 |
| maxLogicalToolCalls | 6 | 0-20 |
| maxTotalTokens | 500000 | 1-2000000 |
| maxEstimatedCostMicros | 1000000 | 1-10000000 |
| maxContextChars (including system text) | 12000 | 1000-24000 |
| maxOutputTokens per model request | 128 | 24-1024 |

Known usage counters sum persisted successful model responses. Estimated cost uses the explicit
V1 demo rates: one micro per input token, two micros per output token. These are synthetic
accounting units, not provider prices or billing reconciliation. Tools have no token/cost tariff
in this fake demo; their logical call count still applies.

Before each model activity is created, reserve input-character count plus maxOutputTokens for
EVERY permitted provider attempt. Cost reserves input characters + 2*maxOutputTokens per attempt.
For the explicitly supported fake provider, character count bounds its reported input-token
estimate. The attempt cap is frozen from the existing retry policy (maximum ten); agentModelCall
checks it before calling the gateway, even if a later worker deployment increases retries.
Real tokenizer/provider billing bounds would need a different estimator and are not supported
by the V1 agent scenarios.

reserved_tokens/reserved_cost_micros are cumulative conservative charges, not refundable pending
balances. Do not add actual usage to them a second time. This pessimistic policy may stop an
agent early even when known successful usage is small, but a lost/crashed/timeout attempt cannot
silently restore spending capacity. Known usage/cost and conservative budget consumption are
exposed separately. A provider can bill a response lost before persistence; its exact tokens/
price remain unknowable here. Reservations bound authorized attempts under the fake contract,
not real-world billing. Tools retain Session 23's provider-side idempotency contract.

Before new work, insufficient iteration/model/tool/token/cost/context capacity produces
LIMIT_REACHED with MAX_ITERATIONS, MAX_MODEL_CALLS, MAX_TOOL_CALLS, MAX_TOTAL_TOKENS,
MAX_ESTIMATED_COST or CONTEXT_LIMIT. No new work for that step is queued. Rejected tools,
invalid decisions, terminal activity failures and duplicate writes produce FAILED with a compact
reason. FINAL produces COMPLETED/FINAL_ANSWER. Terminal advances no-op and budgets cannot reset
across restart or configuration changes.

maxIterations=N permits at most N durable model iterations; logical tool calls have a separate
bound and each iteration permits at most one request. Thus a runaway model cannot create
unbounded autonomous work. This is a bound on work, not a wall-clock deadline: an absent worker,
unavailable control-plane caller or an unanswered human SIGNAL can leave pending/waiting work
indefinitely. Session 24 does not add approval expiry or claim guaranteed liveness without those
external participants. Existing worker attempts/timeouts remain bounded when execution proceeds.

## Tools, approval and repeated writes

ToolRequestFactory extracts Session 23's shared validation/permission/policy/frozen-persistence
path; static Session 23 proposals retain their direct-model/graph validation. Agent code passes
the actual model activity as source and the fixed tool execution activity as request-creation
identity. The unique iteration/tool reference plus deterministic request id yields one immutable
request per tool-decision iteration. Creation, logical tool count and run materialization share
one transaction. There is no fabricated completed model checkpoint or second tool executor.

Session 23's registry, tenant/permission model, policy snapshots, immutable arguments/fingerprint,
exact SIGNAL approval, stable df:v1:tool:<toolRequestId> key, provider ledger and fenced result
persistence are unchanged in meaning. The registry adds read-only getRefundPolicy. The approval
guard also recognizes a persisted request's exact node for the fixed root SIGNAL template.
Discovery still grants no execution authority. Trusted read/write metadata selects the fixed
template; incompatible changed policy fails closed rather than skipping review.

Approval waits hold no worker/thread and do not create new iterations. Rejection terminates the
agent with TOOL_REJECTED; no reformulation loop occurs. A queued rejection handler cannot invoke
the provider. Known terminal tool failures stop the agent rather than starting a fresh request.
A previously EXECUTED request always reuses its result. V1 additionally rejects the same canonical
write fingerprint after a completed write in this agent (DUPLICATE_WRITE), so a repeated refund
cannot create another identity/effect. This is a bounded demo guard, not universal cross-agent or
cross-provider deduplication. Read repetition is allowed and stopped by hard budgets.

## Context and trust boundaries

AgentContextBuilder projects durable state into deterministic JSON: current iteration, original
goal, optional frozen retrieval evidence, and at most four latest completed tool facts including
structured decisions, results and authorization outcome. Older whole facts are dropped until the
context including fixed system text fits; strings/evidence are never cut in half. If the base
context cannot fit, terminate before the next model call. Limits/system policy do not come from
this transcript. Full decisions/results remain in durable records even if omitted from context;
write-repeat checks inspect the complete persisted history.

Optional retrievalActivityId must reference a completed retrieveContext activity in the configured
demo tenant. Its selected context/source labels are copied at agent start and preserved if the
source corpus later changes. The existing local demo control plane is unauthenticated; this scope
check is not end-user IAM. Authenticate identities/reviewers before production exposure.

User goal, retrieved references, tool descriptions and tool results are untrusted data. JSON
escaping prevents them from adding root envelope fields; fixed system text explicitly says not
to follow embedded commands. They cannot set registry risk, permission grants, approval policy
or budget counters. Prompt formatting is not an injection-proof sandbox. No tool-result model
prose is interpreted as executable instructions, and no chain-of-thought is stored.

## Recovery and observability

- Before model output commit: existing retry/reclaim may make another provider attempt under the
  same logical call and frozen attempt/reservation limits.
- After model output but before decision accounting: copy the existing output/usage exactly once.
- After decision accounting but before ToolRequest creation: reuse DECIDED; check remaining tool
  budget before creating anything. Never regenerate that decision to evade a cap.
- After request creation while waiting: preserve the same request id/hash and SIGNAL wait.
- After provider effect before tool completion: Session 23's stable key returns the same result;
  fenced completion atomically commits tool_request/worker state and tool audit.
- After result completion before next model: attach the persisted result once, then project it
  into the next frozen model request.
- After terminal state: repeated advancement returns the same outcome without more work/events.

AGENT_STARTED, AGENT_ITERATION_COMPLETED, AGENT_LIMIT_REACHED and AGENT_COMPLETED contain compact
ids/numbers/status/reason only, with transitions in the same transaction. Existing workflow/
activity/tool history remains in the relevant work run. Arguments/results/prompts/decisions are
not copied into workflow_event. Inspect agent/iterations plus linked run/tool-request endpoints.

## Demo, tests and verification

With JDK 21/Maven/Docker available, start the API and a separate worker as in README:

```powershell
docker compose up -d --wait
mvn spring-boot:run
# Separate terminal:
mvn spring-boot:run '-Dspring-boot.run.profiles=worker'
# Separate terminal: stops before refund approval for inspection.
./scripts/demo-bounded-agent.ps1 -Scenario refund
# A new fake run with explicit approval of the displayed exact O-42/4500 request:
./scripts/demo-bounded-agent.ps1 -Scenario refund -ApproveRefund
# Always asks for another getOrder, stopped at maxIterations=2:
./scripts/demo-bounded-agent.ps1 -Scenario runaway
mvn -B verify
```

The refund fake produces getOrder -> getRefundPolicy -> refundOrder (human SIGNAL) -> FINAL
with the actual persisted fake refund reference. It does not infer real eligibility. Rejection
uses approved=false on the linked tool-run SIGNAL. The client pump is bounded to 120 advances;
provider execution stays in the independent worker. It can stop at approval, exposing ids for
manual continuation after review/restart. POST /api/agent-executions starts from the supplied
JSON; POST /{id}/advance performs one transition; GET /{id} and /{id}/iterations inspect state.
Continue a paused existing agent using these endpoints rather than starting a new identity.

AgentContractsTest adds ten unit scenarios for strict decisions, immutable/bounded payloads,
deterministic context, untrusted data separation, whole-fact truncation and reservations/limits.
AgentModelActivityTest adds two output-sanitization unit scenarios, including private/unsafe
argument rejection before checkpointing. BoundedAgentIntegrationTest adds 22 PostgreSQL scenarios covering concurrent first advance,
crashes around model/decision/request checkpoints, exactly one request, context/result flow,
approval/restart/final, every hard cap, durable reservations, logical calls versus retries,
malformed/private-field rejection, duplicate refund protection, host-signal rejection,
concurrent usage accounting, frozen RAG evidence and retry-cap configuration drift. Existing
Session 10-23 suites remain; only their cleanup table lists are extended for V12 foreign keys.

Compilation, mvn verify, Docker startup and Spring startup were attempted but java/mvn/docker
are unavailable on PATH. Both refund/runaway demo script invocations were blocked by the machine's
PowerShell execution policy. Runtime behavior, migrations, dependency injection, tests and live
demos remain unverified. Static checks passed for 207 Java syntax trees, packages/internal imports,
core independence, 24 JSON examples, Maven XML, PowerShell demo syntax, Markdown fences,
cleanup fixtures, whitespace and unchanged V1-V11. Static parsing/checks cannot establish
compilation or runtime correctness.

Changed areas: agent/*, V12, shared ToolRequestFactory/S23 approval adaptation, one read-only
fixture tool, deterministic fake model scenarios, two test suites/old cleanup lists, two JSON
examples, demo script, README and documentation. No new Maven dependency, transport, frontend,
agent scheduler, real pricing/billing integration, parallel agent branches or arbitrary DAG DSL.

## Learning checkpoint

1. An in-memory loop loses identity, budget and checkpoints on crash and cannot safely wait for review.
2. An iteration is one logical decision (and optional tool); retries are deliveries of its same activity.
3. Durable iteration identity/uniqueness prevents concurrent advance/restart from creating different work at the same number.
4. Recovery reuses the stored decision, checks budget and creates its one missing request, without another model call.
5. Durable budgets survive process replacement and serialize concurrent authorization of new work.
6. Iteration/model limits bound decisions; tool limits independently bound actions, including before FINAL.
7. Approval pauses the same iteration/request without occupying a thread or authorizing another model call.
8. Context is a bounded disposable projection; durable facts remain authoritative when older context is omitted.
9. Structured decisions are auditable action/result records; private reasoning is neither necessary nor persisted.
10. N unique monotonic iterations plus bounded deliveries/actions cap autonomous work; approval/caller liveness remains external.

Interview: after three tool calls, iteration four's stored TOOL_CALL stays DECIDED across the
crash. Recovery locks the agent, sees logical_tool_calls=3 and maxLogicalToolCalls=3, then
terminates MAX_TOOL_CALLS without creating request four. It does not rerun the model. Counters,
limits and the fourth decision are persisted; restarting cannot reset or reinterpret them.
