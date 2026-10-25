# Session 25: model routing, fallback and provider resilience

ModelGateway owns the boundary between a logical ModelRequest and a provider invocation.
ModelRouter selects from a trusted catalog; RouteTransactions persists the plan and authorizes
one provider attempt per worker delivery. The existing queue supplies backoff, timeout, reclaim,
fencing and shutdown. There are no commercial SDKs, remote model calls, hedging or nested retry
loops. Logical prompts/structured-output mode remain unchanged across retries and fallback.

## Catalog and deterministic routing

ModelCatalog contains bounded code-owned ModelDescriptor metadata: provider/model, quality tier,
input/output cost units, context characters, structured/tool capabilities, enabled flag and tags.
ModelCapabilities, ModelRouteRequest and RouteDecision are provider-neutral contracts. Model
output, user goal and tool/retrieval content cannot change their trusted authority.

| Provider | Model | Tier | Input/output micros | Capabilities | Context chars |
| --- | --- | --- | --- | --- | --- |
| fake-a (fakeA) | balanced-v1 | 2 | 1/2 | structured + tools | 24000 |
| fake-b (fakeB) | premium-v1 | 3 | 3/4 | structured + tools | 48000 |
| fake-b | economy-v1 | 1 | 1/1 | structured, no tools | 12000 |
| fake-a | text-v1 | 1 | 0/1 | text, no tools | 12000 |

All are local deterministic simulations. These cost units and quality tiers are illustrative,
not actual prices/quality measurements. fake-a additionally carries the restricted tag; all
carry local. Enablement/tags/cost/capability metadata come from trusted code.

Routing first filters enabled descriptors, required capabilities, input context length, allowed/
forbidden providers, required tags, per-attempt token/cost feasibility, and local breaker
availability. It then ranks qualified quality tiers before explicitly permitted lower tiers,
then estimated cost, higher tier on cost ties, stable provider/model ids. No random choice.
Input cost feasibility uses input character count as a conservative bound for the fake tokenizer;
output reservation uses maxOutputTokens. Context bounds use input system + user characters.

Default trusted RoutingPolicy: quality >=2, no downgrade, providers fake-a/fake-b, required tag
local, at most two routes, two attempts per route, three provider attempts total. The agent's
frozen attempt cap and core worker attempt cap can further restrict execution. Policy settings
are durableflow.routing.required-quality, allow-quality-downgrade, allowed-providers,
forbidden-providers, required-tags, max-routes (1-3), attempts-per-route (1-3), and
max-provider-attempts (1-10). Constraints are snapshots, not inferred from model-generated text.
Set required-tags=restricted or allowed-providers=fake-a to forbid B; neither health nor price
may relax this. This is a deterministic demo policy, not full compliance/RBAC.

allowQualityDowngrade=false excludes every descriptor below the requested tier. true permits
lower tiers only later in ranking; it never relaxes capability, context or privacy restrictions.
Thus economy-v1 still cannot serve an agent tool-use requirement even with downgrade enabled.
ModelRouteRequest can express per-request constraints; the deployed service obtains them from
trusted configuration. No untrusted workflow/model field can rewrite this policy.

## Frozen plans and V13

V13__durable_model_routes.sql adds model_route and model_route_attempt and an extra routing-cost
reservation counter on agent_iteration. V1-V12 are unchanged. Existing integration cleanup lists
include the new FK tables; previous behavioral assertions remain.

model_route is keyed by logical activity_execution_id, optionally references agent_execution,
and stores the request SHA-256, immutable plan JSON (primary, fallback descriptors, policy
version, reason, full constraints), active route index, total provider attempts, primary base
cost, cumulative authorized tokens/cost, supplemental agent reservations, bounded status/code
and transition reason/timestamps. Its trigger prohibits plan/request rewrites and counter/index
regression. A plan is not reconstructed from current catalog settings on retry.

model_route_attempt uniquely identifies each global and per-route attempt. It records route
index, provider/model, STARTED/SUCCESS/HEALTH_FAILURE/REQUEST_FAILURE, failure code, known usage,
estimated cost and provider request reference/timestamps. No prompt, response content or private
reasoning is copied here. STARTED after a crash means unknown completion, not zero provider cost.
Attempt facts and counters allow reconstruction of the bounded path without copying large data
into workflow_event. Generic worker events remain unchanged.

AgentLoopService freezes the plan in its existing model-materialization transaction, before the
worker can invoke a provider. A standalone modelCall with provider=routed freezes its plan on
first worker invocation before the RPC. Agent model aliases transparently use routing through
the Spring ModelGateway. Existing pinned fake recommendation/tool calls retain their legacy
behavior. New fake-a/fake-b providers cannot be directly selected through the pinned gateway to
bypass routing constraints. A routed compensation/local synchronous call is rejected; use the
normal forward queue. The one-argument ModelGateway constructor remains a direct-provider unit
fixture, not the deployed routed path.

The logical request hash covers provider/task alias, version, system/user text, temperature,
output cap and structured flag. A reused activity with changed semantic input is rejected.
Only the provider/model fields of the invocation copy change on fallback. Agent context carries
a fixed semanticModel fixture selector in JSON; it is frozen before routing and identical for
all routes. It cannot alter permissions, schema or trusted routing policy.

## Retry, fallback, failure and fencing

RETRY means the same provider/model and same route key on a later queue delivery. FALLBACK
means a persisted active-index transition to another prevalidated descriptor. Default A gets
two attempts; after two typed health failures, the transaction advances to B and returns a
retryable signal to the normal task queue. The next delivery invokes B. There is no provider
loop inside ModelGateway and no random/ad hoc rerouting.

| Failure | Behavior |
| --- | --- |
| UNAVAILABLE / RATE_LIMITED / TIMEOUT | Health count, same-route retry, then bounded fallback |
| Invalid request/model, auth, unsupported capability, malformed response | Terminal; no blind fallback, no health count |
| Unknown provider exception | Conservative terminal UNCLASSIFIED |
| All route/total attempt allowances spent | MODEL_ROUTE_EXHAUSTED |
| Fallback cost/token reservation impossible | Persist BUDGET_BLOCKED; no provider invocation |
| Worker/core retry budget exhausted first | Existing terminal worker behavior; never add another loop |

429 uses existing configured backoff/jitter; no fake Retry-After header or real transport is
implemented. A local worker timeout may leave a STARTED route attempt whose response is unknown.
Recovery consumes its reserved allowance; it does not pretend the attempt was free. Advancing
past a spent route/circuit can consume a queue delivery without an RPC. Core retry limits remain
in force, so some crash paths terminate before every candidate is invoked.

ActivityExecutionContext now optionally carries task id/worker/generation; old constructors and
stable idempotency keys remain compatible. ClaimedTask supplies this token to ModelCallActivity/
ModelGateway. Route reservation locks task -> agent (if present) -> route, verifies a live lease,
reserves, and verifies ownership again before returning. No provider runs inside this transaction.
Standalone plan creation is similarly fenced. Failure-driven transitions require current task
ownership and the latest route attempt; a stale outcome may record an attempt fact but cannot
advance a newer route. Activity output still commits through the original final fenced worker
write. Agent orchestration never acquires an existing model task lock, avoiding lock inversion.

## Agent budgets and accounting

Session 24 logical counters remain authoritative: A twice plus B once is ONE logical model
call and one iteration decision, with three provider attempts. Tool budgets and approval gates
are unaffected. A routed agent must already have a frozen agent-owned plan; the gateway cannot
silently initialize it as an unbudgeted standalone call.

Before the model activity is queued, reserve every permitted agent attempt at the selected
primary's conservative rate, along with its existing token slots. Before EVERY provider call,
check the immutable per-call token/cost envelope and total attempts. If fallback's per-attempt
cost exceeds the primary base, atomically reserve the positive difference against the agent's
remaining hard cost cap and the iteration's supplemental counter. Token slots use the same
frozen input/output envelope across routes and cannot exceed the agent's frozen cap. No RPC
occurs until this independent reservation transaction commits.

Reservations are cumulative and nonrefundable, including unknown/crashed attempts and unused
primary slots, as in Session 24. This conservative policy can block a fallback that would fit
under optimistic actual-success usage; it never borrows future budget. A failed reservation
commits BUDGET_BLOCKED/MAX_ESTIMATED_COST (or MAX_TOTAL_TOKENS) before the handler fails. The agent
driver recognizes that durable outcome and terminates LIMIT_REACHED with the correct reason.
Fallback cannot reset logical counts, token slots, reservations or limits after restart.

Accepted provider-response usage/cost is recorded per attempt before worker completion, including
responses generated before a crash. Agent checkpointing aggregates those known facts once,
instead of applying the old fixed rate only to the final provider's response. Legacy direct
unit fixtures retain their old accounting path. Output exposes logical id, primary/final routes,
fallback/index/count, reason/version, provider request reference, final usage, actual provider
attempt count, known aggregate usage/cost, and authorized token/cost bounds. Known cost is a
synthetic estimate, not a claim about billed money. A lost response or failed usage write leaves
unknown real usage, already conservatively reserved. Very late responses after agent termination
can add attempt facts beyond the driver's terminal accounting snapshot; billing reconciliation
and late-usage reconciliation are outside this session.

## Circuit breaker scope

ProviderCircuitBreaker uses CLOSED/OPEN/HALF_OPEN, default threshold two and cooldown 30000ms
(configurable breaker-threshold/breaker-cooldown-ms). Only typed provider-health failures count;
request/configuration/auth failures release probes without opening the circuit. Success resets
CLOSED. OPEN providers are skipped on new selection; after cooldown a bounded HALF_OPEN probe
is allowed, success closes and health failure reopens. Probe permits can expire if abandoned.

This breaker is in-memory, per instance, per provider: NOT cluster-global, not durable, and
not an authorization/budget authority. Restarts reset it. Separate API and worker processes have
separate health views. A plan selected in the API may contain a provider open in the worker;
the worker explicitly advances within the frozen fallback list, never reroutes beyond it. New
standalone selection in that worker skips the open provider. The circuit demo therefore uses
one fresh worker for both calls. No cross-worker-health guarantee is claimed.

## Crash boundaries

A. A fails, B succeeds, activity output commits: ordinary restart reuses that output and decision;
no router/provider is called again. Agent decision accounting can attach the same persisted fact.

B. B generates but worker output does not commit: route B/index, attempt facts, immutable prompt
and spent reservations survive. Recovery may invoke B again if it has allowance, or advance/
exhaust according to the frozen route state. The answer may differ for a real model; generation
is NOT exactly once. With default three attempts, A2+B1 can spend the entire allowance before
the crash, so recovery may terminate exhausted instead. The recovery test uses a fourth allowed
attempt to demonstrate another B invocation and fences the stale worker's completion.

C. Plan commits but provider has not been invoked: recovery reuses exactly that plan/version and
prompt, even if current policy settings have changed. New calls use the new policy. Retroactive
security revocation of already-frozen plans is not implemented; do not confuse snapshot replay
with a live policy-revocation service.

## Demos and verification

With JDK 21, Maven and Docker available, start database/API normally. For each scenario, start a
fresh SINGLE worker configured to fail A twice (B succeeds):

```powershell
docker compose up -d --wait
mvn spring-boot:run
# Separate terminal, restart this worker before each scenario:
mvn spring-boot:run '-Dspring-boot.run.profiles=worker' '-Dspring-boot.run.arguments=--durableflow.routing.fake-a-script=UNAVAILABLE,UNAVAILABLE'
# Separate terminal:
./scripts/demo-model-routing.ps1 -Scenario fallback
./scripts/demo-model-routing.ps1 -Scenario circuit
./scripts/demo-model-routing.ps1 -Scenario budget
mvn -B verify
```

fallback registers the routed-model example and shows A/A/B. circuit uses two calls in the same
worker process: first A/A/B opens A, next selects B as primary. budget starts a bounded FINAL
agent with a 3000-micro cap and demonstrates blocked fallback without calling B. The deterministic
budget test computes an exact primary reservation; the demo's goal/context/output cap are fixed.
Each client poll is bounded. Inspect GET /api/workflow-runs/{runId}/model-routes for snapshot,
activity status and ordered attempts; agent iterations link their model work run.

New tests: ModelRouterTest (12) covers hard constraints, deterministic cost/quality selection,
structured/context capabilities, allow/forbid/tags, both downgrade policies, route bounds and
open-provider exclusion. ProviderCircuitBreakerTest (3) covers threshold, fake-clock cooldown,
bounded probes, success/reopen, neutral failures and per-instance scope. ModelRoutingIntegrationTest
(13) covers same-route retry/fallback, persisted output reuse, exhaustion, typed failures,
open-provider selection, policy-change recovery, lost fallback generation/fencing, logical versus
physical attempts, hard agent budget, prompt/plan mutation rejection and stale reservation fencing.
Existing Session 10-24 suites remain, with FK cleanup lists extended.

Compilation, tests, Docker and Spring startup were attempted but java/mvn/docker are unavailable
on PATH. All three demo script attempts were blocked by this machine's PowerShell execution
policy. Runtime behavior, Flyway, wiring and tests remain unverified. Static checks passed for
225 Java syntax trees, packages/internal imports, core independence, 27 JSON examples, Maven
XML, PowerShell demo syntax, Markdown fences, FK fixtures, whitespace and unchanged V1-V12.
These checks do not establish compilation or runtime success.

Changed files: ai/routing/*, ModelGateway/ModelResponse/ModelCallActivity/provider failure kinds,
agent plan/accounting/context integration, optional worker ownership metadata, V13, three test
suites and old cleanup fixtures, three JSON examples, demo script, README and this document.
No new Maven dependency, frontend/dashboard, remote SDK, hedging, router training or multi-agent
system is added. Catalog/usage/quality/billing are fake; breaker scope and conservative budgeting
are explicit limits. There is no push as part of this session.

## Learning checkpoint

1. Retry repeats the same route; fallback explicitly switches the provider/model.
2. Fallback changes capability/quality/cost/privacy choices, so it is policy-controlled and durable.
3. Capability, size and provider restrictions are hard; cost/quality ranking is a preference within them.
4. Filtering before invocation prevents unsupported work, accidental data exposure and wasted budget.
5. Fallback must fit persistent remaining reservations before it can invoke another provider.
6. Disabling downgrade prevents reliability pressure from silently weakening required output quality.
7. Breakers stop repeated pressure on an unhealthy provider across logical calls, beyond per-call retries.
8. Health/transient failures count; malformed requests, unsupported configuration and auth do not.
9. Attempts deliver one frozen semantic operation; they do not create additional logical decisions.
10. Frozen plans/reasons make replay, spending and fallback choices explainable across crashes.

Interview: preserve the logical request, A/B plan/version, active B index, attempt facts and spent
budgets. B may generate again only within those durable allowances; a stochastic answer may
change if the previous output never committed. Once output commits, reuse it. DurableFlow offers
bounded, fenced checkpoint/recovery semantics, not exactly-once generation or exact billing for
lost responses. Never reset reservations or silently choose a different primary after restart.
