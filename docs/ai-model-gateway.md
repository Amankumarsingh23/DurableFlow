# Session 21: durable model activities

LLM/model calls are side-effecting external operations that can fail, time out, be retried or
return after their worker loses ownership. DurableFlow already has the queue, supervision,
leases, fencing and checkpoints for this work. A registered modelCall ACTIVITY reuses them;
there is no MODEL node, separate AI worker protocol or orchestrator-owned model executor.

## Package boundary and contract

| Package/files | Responsibility |
| --- | --- |
| ai/model/ModelRequest, ModelResponse, ModelUsage | Small provider-neutral value contracts |
| ai/model/ModelGateway | Explicit provider resolution, delegation and typed failure translation |
| ai/provider/ModelProvider, ModelProviderException | Provider adapter boundary and failure kinds |
| ai/provider/FakeModelProvider | Deterministic local generation and test failure/slow-call scripts |
| ai/activity/ModelCallActivity | Parse durable request, invoke gateway, return JSON output |
| ai/activity/ApplyApprovedActionActivity, ai/demo/FakeApprovedActionProvider | Separate named demo effect with idempotency |

Core workflow/orchestration/worker code has no AI-provider imports or SDK types. No real
provider SDK, network call, API key or fallback policy is added. ModelGateway indexes providers
by explicit id; duplicate ids fail configuration. Missing providers and unsupported fake model
ids fail terminally rather than resolving latest or silently falling back.

The workflow input contains a modelRequest object:

```json
{
  "requestVersion": 1,
  "provider": "fake",
  "model": "fake-recommendation-v1",
  "systemInstructions": "Recommend a safe next step for human review.",
  "userContent": "Supplied local demo context for order DF-21.",
  "temperature": 0.0,
  "maxOutputTokens": 256,
  "structuredOutput": true
}
```

Request version, provider, model, system instructions, user content and max output tokens are
required. Version 1 rejects unknown fields/versions, caps each prompt string at 64,000 characters,
requires nonblank user content, bounds tokens to 1-16,384 and temperature to finite [0,2].
Optional temperature has the fixed v1 default 0.0; structuredOutput defaults to false.
There are no environment-selected prompts, model aliases or mutable template lookups.
Future contract changes must preserve v1 interpretation or introduce a new version.

The request is already in durable WorkflowRun.input at start. On first claim, the ordinary
worker copies that input to ActivityExecution.input_json; retry/reclaim reads that execution
snapshot. Thus even a changed run-input source does not replace a started activity's request.
This pins request/configuration semantics, not the behavior or weights of a future external
model provider. Model responses may remain nondeterministic even with identical input.

ModelResponse contains exactly one text content or structuredJson object, provider, actual
model, ModelUsage(inputTokens, outputTokens), latencyMillis and optional providerRequestId.
ModelCallActivity persists a JSON-compatible outputVersion=1 object with those fields in
ActivityExecution.output_json through the normal fenced completion transaction. Usage is
available for later budgets/observability; this session adds no billing or cost enforcement.

## Failure and timeout behavior

Provider adapters use ModelProviderException.Kind, never error-message substring matching:

| Failure | Existing worker policy |
| --- | --- |
| RATE_LIMITED, UNAVAILABLE, TIMEOUT | RetryableActivityException; bounded durable backoff/jitter |
| INVALID_REQUEST, INVALID_MODEL, MALFORMED_RESPONSE | Terminal failure |
| Unknown programming failure | Conservative/terminal under existing classifier |
| Local handler deadline | Existing Session 14 ActivityTimeoutException/supervision path |

The gateway requires an object response when structured output was requested. A fake malformed
response fails rather than becoming completed success. Response contracts validate usage and
representation shape. Full JSON-schema/business-output validation is deferred.
Typed provider errors are translated to safe codes without copying provider payloads/prompts.

Heartbeats, reclamation, maxAttempts, fencing and shutdown are unchanged. Local timeout can
interrupt a handler but does not guarantee cancellation at an external provider. An obsolete
handler may still generate/bill a result; its stale completion cannot overwrite durable state.

## Checkpointing, nondeterminism and the crash window

Once a model activity is COMPLETED, ordinary advancement/recovery reuses its persisted output.
It does not call the model again to reconstruct a recommendation. A process restart or human
approval wait preserves that checkpoint, usage and provider request id.

If a provider returns a response and the worker crashes before committing completion, that
result has not become DurableFlow's durable truth. Lease reclamation may call the provider
again and obtain a different recommendation. A valid owner can commit that later response;
this does not violate the checkpoint contract. The guarantee is one fenced committed logical
outcome, not exactly-once model generation or exactly-once provider charging.

The stable activity idempotency key is passed to the provider interface as operationKey for
correlation. The fake does not deduplicate model generation. A future provider supporting
request idempotency or lookup could use that stable key, or durably capture a retrievable
provider receipt/result, to recover the first response. Request ids stored only with final
output cannot recover a response whose id was itself lost before persistence. A durable
result cache/receipt protocol is outside this session.

Only successful committed response usage is recorded in the activity output. Failed, timed-out
or lost generations may also incur real costs; these output counters are not aggregate
attempt billing and cannot establish a total provider bill.

## Fake provider and action limitations

FakeModelProvider supports fake-recommendation-v1 and fake-text-v1. Default responses derive
deterministically from the complete request; structured mode returns a recommendation and
input fingerprint. Counts and the 7ms latency are synthetic demo metadata, not real model
measurements. Each generation attempt has its own fake request id. Same-key repeated calls
still count as separate generations. In-memory invocation logs/scripts reset across JVMs.

Tests construct scripted SUCCESS/RATE_LIMITED/UNAVAILABLE/TIMEOUT/INVALID_REQUEST/
MALFORMED_STRUCTURED outcomes. A before-response hook supports deterministic blocking and
timeout tests without sleeps. The default application fake always succeeds for supported
valid requests. There is no user-controlled arbitrary tool registry or provider switching.

The separate demo action marks a configured target reviewed. Its fake provider atomically
deduplicates the stable activity key and rejects changed payload on the same key. This ledger
is per JVM, not a production shared effect store. Real risky effects still require provider
idempotency and explicit policy, rather than execution embedded inside generated model text.

## Agent workflow demo

```text
PrepareRequest (submitRequest)
  -> GenerateRecommendation (modelCall)
  -> HumanApproval (SIGNAL)
  -> MarkTargetReviewed (applyApprovedAction)
```

The first step is an ordinary deterministic preparation demo, not RAG/retrieval. Supplied
context and prompts are frozen in modelRequest; existing forward inputs remain workflow input.
There is no automatic branch-output mapping or inference of action arguments from generated
text. The action is explicitly configured in approvedAction, while the operator reviews the
persisted recommendation before sending the signal.

Approval is a receipt gate: any accepted payload for the exact approval node unlocks it.
The approved boolean is not interpreted for routing, and authentication/authorization or
rejection branches are not added. This is no autonomous planner or general tool execution loop.
While the signal waits, no handler or worker slot is occupied by the workflow.

With database, server and worker running as in README:

```powershell
$base = 'http://localhost:8080/api'
Invoke-RestMethod -Method Post -Uri "$base/workflow-definitions" -ContentType 'application/json' -Body (Get-Content examples/durable-agent-v1.json -Raw)
$run = Invoke-RestMethod -Method Post -Uri "$base/workflow-runs" -ContentType 'application/json' -Body (Get-Content examples/start-durable-agent.json -Raw)
Invoke-RestMethod -Method Post -Uri "$base/workflow-runs/$($run.runId)/advance"
$state = Invoke-RestMethod -Uri "$base/workflow-runs/$($run.runId)"
($state.activities | Where-Object activityId -eq 'recommendation').output
```

Poll GET until approval is WAITING. Inspect generated output. Stop/restart server/worker with
the database volume retained: the completed recommendation remains, and the workflow still
waits. Send the signal after review:

```powershell
$delivery = @{ signalId='review-1'; signalNodeId='approval'; payload=@{ approved=$true } } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "$base/workflow-runs/$($run.runId)/signals" -ContentType 'application/json' -Body $delivery
Invoke-RestMethod -Uri "$base/workflow-runs/$($run.runId)"
Invoke-RestMethod -Uri "$base/workflow-runs/$($run.runId)/events"
```

The action then runs and the workflow reaches COMPLETED. Replaying that signal does not
reschedule completed work. All detailed prompts, model results and usage remain in activity
input/output; generic workflow_event payloads contain no full prompts/responses. No new
provider-specific event vocabulary or migration is needed; V1-V10 remain unchanged.

## Verification and changed files

ModelGatewayTest adds eight unit scenarios for neutral mapping, deterministic fake responses,
typed failures, conservative unknown errors, malformed/config errors, JSON output, versioned
request validation and action idempotency. ModelActivityIntegrationTest adds nine PostgreSQL
scenarios for output/metadata persistence, durable retry with frozen input across restart,
terminal/malformed failures, completed-output reuse, DAG joins, approval/restart/action flow,
different post-crash generation with fencing, and real existing timeout supervision.
Existing Session 10-20 suites and engine code are unchanged.

Added files are the ai packages listed above, both test classes, durable-agent-v1.json,
start-durable-agent.json and this document; README links the session. No schema, dependency,
provider SDK, core orchestrator or worker protocol changes are required.

Java, Maven and Docker are unavailable on PATH. Compilation, tests and startup attempts
were blocked; the live demo could not run. Runtime behavior is not verified. Static checks cover Java syntax,
imports/packages, Maven XML, JSON examples, unchanged migrations and whitespace. Run
mvn verify with JDK 21/Maven/Docker and the demo before claiming runtime success.

## Learning checkpoint

1. A model call fits ACTIVITY's existing execution/reliability boundary; a MODEL node adds no needed semantics here.
2. A giant runAgent activity loses internal checkpoints and couples generation, waits and risky effects into one retry unit.
3. The gateway separates stable workflow contracts from provider selection, adapters and failure translation.
4. Typed transient rate-limit/unavailable/timeout errors retry; invalid requests/configuration and malformed results fail terminally.
5. Regenerating a completed output could change the durable decision already used downstream.
6. A response lost before checkpoint may be regenerated differently; external generation is not exactly-once.
7. Frozen versioned input prevents retry from silently changing prompt/model settings after deployment.
8. Risky effects need their own idempotency, policy and checkpoints instead of direct model-text execution.
9. SIGNAL records a durable human gate with no blocked worker while waiting.
10. Repeated generations can consume provider tokens/money as well as add latency; successful-output usage is not total billing.
