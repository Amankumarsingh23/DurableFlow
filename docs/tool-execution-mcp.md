# Session 23: durable tools, policy and MCP-style interoperability

The model proposes. DurableFlow validates, authorizes, applies policy, optionally waits for
human approval, then executes through the existing worker. One bounded proposal per workflow
run is supported. There is no autonomous model/tool loop or model-generated workflow definition.

## Contracts and allowlist

`tools/ToolDescriptor` owns name, bounded description, the input contract, sideEffecting,
ToolRiskLevel, required permissions, provider and source version. ToolRegistry explicitly
registers only getOrder and refundOrder. It never reflects arbitrary strings into Java methods
and never populates its allowlist from discovery. ToolProvider separates discovery/invocation
from these trusted descriptors. ToolResult carries toolName, safe structured data, provider
reference and execution metadata. ToolRequest holds immutable semantic identity and snapshots.

The schema contract is deliberately small: exact field set, no additional fields, orderId
matching O-[0-9]{1,12}, and positive numeric money with at most two fractional digits, capped
at 1,000,000. No numeric strings, SQL fragments, arbitrary URLs or executable text arguments.
The schema is a bounded Java validation contract, not a complete JSON Schema implementation.

Schema validation asks whether the proposal is well formed. Permission checking asks whether
the configured actor may use the capability on this tenant-owned resource. Policy evaluates
this particular invocation. These are three independent gates; valid JSON grants no authority.

Trusted deployment settings durableflow.tools.actor-id (default demo-agent) and
durableflow.tools.tenant-id (default tenant-a) select the demo identity. Code grants demo-agent
ORDER_READ/ORDER_REFUND, demo-reader only ORDER_READ, and others nothing. Only the tenant-a
fixture O-42 is accessible. Model/root workflow input cannot supply roles, grants or tenant
privileges. This is a local demo identity model, not authenticated end-user IAM or enterprise
RBAC. The existing HTTP control plane, including SIGNAL approval and the inspection endpoint,
is unauthenticated: keep it local/trusted. Production must authenticate reviewers and bind
actors/tenants to verified identities; knowing an id/fingerprint is not authorization.

DemoToolPolicy version demo-policy-v1 auto-approves getOrder. Every refund up to 10,000 requires
human approval; larger schema-valid refunds are denied. Permission and policy decisions,
versions, actor/tenant and approval authority are persisted once. Recovery does not re-decide
an existing request using a new policy or new configured grants. Registry descriptor drift at
execution fails closed instead of silently lowering risk or changing a provider/schema.

## Workflow and proposal binding

The examples are:

```text
SafeRefundAgent:
policies(retrieveContext) -> recommendation(modelCall) -> proposal(createToolRequest)
    -> approval(SIGNAL) -> execute(executeTool)

SafeOrderLookup:
recommendation(modelCall) -> proposal(createToolRequest) -> execute(executeTool)
```

FakeModelProvider adds two explicit fixtures, fake-tool-refund-v1 and fake-tool-read-v1.
They emit a structuredJson.toolProposal with exactly toolName and arguments. The refund is
O-42/4500; the read is O-42. These fixtures are deterministic proposals, not actual inference,
policy interpretation or autonomous planning. Existing fake model behavior is unchanged.
The refund example preserves Session 22 retrieval/grounding before generation.

createToolRequest reads the completed model activity checkpoint in the same run. toolBinding
selects the direct model dependency, execute node and optional approval node. It verifies the
actual stored graph and handlers: a review SIGNAL must depend on creation and execution must
depend on that SIGNAL; the read path directly depends on creation. High-risk proposals cannot
omit the approval node. Additional risk/permission fields in a proposal are rejected. Model
output, workflow inputs and retrieved references cannot supply descriptor/policy authority.
The local actor settings still cap all user-selected graph/input proposals.

Creation is a short independent run-locked database transaction with no provider invocation.
It freezes canonical validated arguments, a deterministic request id derived from the logical
creation activity id, SHA-256 fingerprint of toolName + recursively sorted canonical arguments,
registry descriptor and decision snapshots. Equivalent numeric forms (4500 vs 4500.00) share
identity. Changed tool/arguments for the same creation identity are rejected. If creation
commits but its worker completion is lost, retry reuses this exact request and decision.
Malformed/unknown proposals fail the activity before a request is materialized. Valid but
permission/policy-denied proposals are stored REJECTED and creation fails non-retryably.

## V11 persistence and immutable semantics

V11__durable_tool_requests.sql adds tool_request with:

- UUID id and workflow_run_id (one request per run), model source and creation activity FKs;
- execution/approval node ids, tool name, arguments JSON and 64-character fingerprint;
- registry descriptor JSON, risk level, permission/policy decision JSON;
- bounded status, result JSON, created/approved/executed timestamps.

States are AWAITING_APPROVAL -> APPROVED -> EXECUTED, or AWAITING_APPROVAL -> REJECTED.
Read-only auto-approval starts at APPROVED; denied creation starts at REJECTED. Delivery failures
remain in ActivityExecution/task retry/failure state rather than adding a second execution state
machine. Exhausted/terminal execution failure can leave the semantic request APPROVED with a
failed workflow/activity; this does not authorize an automatic retry outside the existing budget.

A PostgreSQL trigger makes tool identity, arguments, fingerprint, risk, decisions, graph binding
and creation timestamp immutable. It enforces transitions, immutable approval time and terminal
result immutability. Java checks the fingerprint on every loaded request and before invocation.
There is no payload-edit API. V1-V10 are unchanged. Existing integration cleanup lists include
both new tables because of the new foreign keys; previous assertions/behavior remain intact.

fake_tool_effect is a separate simulated provider ledger: key, fingerprint, tool, result and time.
It deliberately has no run FK. Its independently committed transaction simulates an external
provider's atomic effect/deduplication boundary; it is not a claim of distributed exactly-once
execution. A real integration must supply equivalent provider-side durable idempotency.

## Exact approval through existing SIGNALs

Use the existing POST /api/workflow-runs/{runId}/signals endpoint with:

```json
{
  "signalId": "refund-review-1",
  "signalNodeId": "approval",
  "payload": {
    "toolRequestId": "<persisted UUID>",
    "fingerprint": "<persisted SHA-256>",
    "approved": true
  }
}
```

SignalAcceptanceGuard is a generic bounded database extension invoked inside SignalReceiver's
existing transaction/run lock before insertion. ToolApprovalGuard recognizes protected SIGNAL
nodes from the stored graph even before creation. Such early approvals are rejected rather
than buffered. It requires the existing AWAITING_APPROVAL request, exact node/id/fingerprint,
and exactly those payload fields with a boolean decision. State transition, compact audit,
signal delivery, consumption and downstream materialization share a commit. Ordinary signals
retain existing buffering and receipt semantics. No second wait store or approval polling loop
is introduced. Replaying the same signal id/payload is harmless, including after completion;
changed payload or a different delivery for the same node conflicts. Rejection stores REJECTED;
the downstream activity terminates without calling the provider (there is no branching DSL).

Invalid approval attempts return a conflict and leave the wait/request unchanged. Binding
mismatches produce a compact server warning with run/request ids, never arguments. Direct
illegal database mutations raise TOOL_IMMUTABLE_REQUEST. Do not mislabel these attempts as
successful approvals in workflow history. Production incident logging/identity is deferred.

## Worker execution, results and crash ambiguity

executeTool ignores mutable workflow/model arguments and resolves the frozen request for its
bound execution node/run. Only APPROVED may invoke ToolRegistry. The stable provider key is
`df:v1:tool:<toolRequestId>` across all attempts, worker generations and restarts. The fake
provider atomically writes a refund result/effect once per key and compares the fingerprint on
replay. Same key/different semantics is a terminal error. getOrder is a safe read, but its result
also becomes a checkpoint; it is not refreshed automatically after completion.

The handler runs outside controller/orchestrator transactions through ActivityHandler and
TaskWorker: SKIP LOCKED queue, renewable leases, heartbeat, timeout, reclaim, fencing, retry
backoff and shutdown are unchanged. Both new handlers requireDurableWorker; the legacy local
/execute runner fails without invoking them. Use /advance and workers. There is no tool retry
loop. Typed simulated unavailable/response-loss errors use RetryableActivityException; invalid
arguments, unknown tools, denied approval and idempotency misuse remain non-retryable.

ActivityCompletionParticipant is a generic local database hook after the worker's final owned
write, inside the same completion transaction. ToolCompletionParticipant commits tool_request
EXECUTED/result plus TOOL_EXECUTED with the activity/task completion. A stale completion cannot
write this checkpoint; any failure rolls back all completion writes/events. No provider is
called by the hook. Existing non-tool executions are unaffected.

Crash before provider effect: normal retry. Crash after effect before completion: provider replay
returns the same refund under the stable key, then the valid owner checkpoints it. A stale
worker result is fenced. Crash after completion: durable result is reused without invocation,
even by explicit handler replay. An expired task or timeout cannot undo an external effect;
idempotent provider semantics handle that ambiguity. No automatic refund compensation is added.

Generic activity events describe delivery/execution. TOOL_REQUEST_CREATED, TOOL_APPROVED,
TOOL_REJECTED and TOOL_EXECUTED carry only id/fingerprint/status, atomically with their semantic
transitions under the run lock. Full arguments/results stay in tool_request, not workflow_event.
GET /api/workflow-runs/{runId}/tool-request exposes the frozen proposal, decisions and result for
local review. Source activity ids and node bindings preserve traceability.

## MCP and trust boundaries

FakeMcpToolProvider is an in-process MCP-style ToolProvider, not a real remote MCP transport.
Bounded discovery returns name/description/input contract for getOrder, refundOrder and an
unallowlisted deleteAllOrders with a hostile description. The registry never ingests this
metadata as trusted configuration. Discovering deleteAllOrders does not make it callable.
MCP-style discovery/interoperability answers what exists/how to call it, not who may call it.
No network client, credentials, OAuth, dynamic registration or external server is introduced.

Model output is an untrusted proposal; retrieved context remains untrusted user data from
Session 22. External tool descriptions are also untrusted, even if they claim to disable review.
Tool results can carry indirect prompt injection if later sent to a model. They remain data,
never executable instructions, grant lists, risk declarations or policy updates. This session
does not feed tool results into another model call. Later composition must preserve that data
boundary; string delimiters alone do not provide a security sandbox. Real adapters must use typed
APIs/parameterized queries and bounded validation rather than trusting model-provided strings.

## Demo and verification

With JDK 21, Maven and Docker available, start database/server and a separate worker as in README:

```powershell
docker compose up -d --wait
mvn spring-boot:run
# Separate terminal:
mvn spring-boot:run '-Dspring-boot.run.profiles=worker'
# Separate terminal, read-only auto-approved path:
./scripts/demo-safe-tools.ps1 -Scenario read
# Inspect the frozen refund; this invocation stops at AWAITING_APPROVAL:
./scripts/demo-safe-tools.ps1 -Scenario refund
# A new fake refund run, explicitly approve the displayed O-42/4500 request:
./scripts/demo-safe-tools.ps1 -Scenario refund -ApproveRefund
mvn -B verify
```

The script registers the exact examples if absent, starts/advances a run, checks the inspection
view, and binds approval to the displayed persisted id/fingerprint. Review arguments/decision
before approving. It uses the fake provider only. Reject instead using the same SIGNAL payload
with approved=false. History is at /api/workflow-runs/{runId}/events; request/result at the new
inspection endpoint. Keep the DB volume through restarts to exercise durable checkpoints.

ToolContractsTest adds 12 unit cases for allowlisting/discovery, schemas, trusted risk,
permission/tenant checks, policy, canonical fingerprints, immutable identity, approval gating,
configuration drift and hostile result data. ToolExecutionIntegrationTest adds 17 PostgreSQL
cases for grounded/refund/read flow, binding/early/duplicate/rejected approval, immutable DB
payload, retries, effect-before-response loss, effect-before-completion crash/reclaim/fencing,
restart reuse, permission/policy denial, untrusted proposals, creation recovery, provider-key
misuse, atomic completion rollback and rejection of legacy local execution. These tests use no sleeps or network providers.

Compilation, mvn verify, Docker startup and Spring startup were attempted; java, mvn and docker
are unavailable on PATH. Both demo script attempts were blocked by this machine's PowerShell execution policy; runtime behavior, Flyway,
Spring wiring and all new/old tests remain unverified. Static Java parsing and JSON/XML/import/
whitespace/migration checks do not establish compilation or runtime success. Final static checks
passed for 190 Java syntax trees, 22 JSON examples, internal imports/packages, core independence,
PowerShell demo syntax, Maven XML, Markdown fences, updated fixtures and unchanged V1-V10.

Files: tools/* contracts/services/handlers/adapter/controller; generic completion and signal
hooks and the local-runner guard; V11; two new suites and existing cleanup fixtures; four JSON
examples; demo script; README and this document. No new Maven dependency. Real MCP, authenticated
reviewer IAM, remote payments, tool-result model iteration and autonomous loops remain deferred.

## Learning checkpoint

1. A model proposal can be wrong or injected; it cannot grant itself executable authority.
2. Schema checks shape, permissions check actor/capability/resource, policy checks this invocation.
3. Only trusted registry risk can determine review requirements; model-supplied risk is untrusted.
4. Approval binds an immutable id/fingerprint, so the reviewed action cannot change afterwards.
5. Ordinary activities already provide durable delivery, retry, lease, timeout and fencing mechanics.
6. Stable provider idempotency closes the effect-before-checkpoint ambiguity for a write operation.
7. Discovery describes a capability; authorization separately decides whether use is permitted.
8. Newly discovered capabilities have no default trust, permissions or registry membership.
9. Descriptions/results can contain indirect injection; neither may change control-plane policy.
10. Completed results are durable facts; repeating them could duplicate effects or change history.

Interview: approving O-42/4500 cannot authorize O-42/45000 under the same request id. The canonical
fingerprint/immutable-arguments invariant rejects this via Java identity checks and the database
trigger. Approval is bound to the original hash; execution uses only stored arguments. Record a
rejected/conflicting attempt in safe operational security logs, preserving the original request
and approval. Never overwrite the approved payload or append a misleading successful approval.
A different action requires a distinct request identity and its own authorization/review.
