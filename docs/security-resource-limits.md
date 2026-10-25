# Security and resource boundaries

This repository is a learning/portfolio implementation with **no production authentication or end-user authorization middleware**. Demo actor/tenant settings and tool permission checks are not an authenticated API security system. Do not expose the API publicly unchanged. Signal and tool-approval endpoints **MUST be authorization-protected in production**.

## Session 27 limits

`ResourceLimits` checks serialized UTF-8 JSON using a counting output stream rather than allocating a second unbounded JSON string. Structural checks run before serialization. Oversized payloads return HTTP 413 with a constant code; invalid non-JSON values return 400. `BoundedRequestFilter` caps actual HTTP bytes before JSON binding, for POST/PUT/PATCH, including bodies without Content-Length. It buffers at most limit+1 bytes and rejects a declared oversized body before reading it. Ingress must separately enforce connection/read deadlines, request rates, header limits, concurrent requests and decompression limits; this filter alone does not prevent slow clients or CPU exhaustion.

| `durableflow.limits.*` property | Default | Enforcement |
| --- | --- | --- |
| `http-body-bytes` | 1,048,576 | Whole raw write-request body, before binding |
| `definition-bytes` | 262,144 | New definition registration, serialized DTO |
| `nodes` | 128 | Raw steps before graph validation |
| `dependencies-per-node` | 64 | Raw dependency list before deduplication |
| `workflow-input-bytes` | 262,144 | Workflow start, including child starts; inline child input at registration |
| `signal-bytes` | 32,768 | Public signal receipt before run lock/lookup |
| `tool-argument-bytes` | 8,192 | Tool request factory, before persistence/policy |
| `schedule-input-bytes` | 131,072 | Schedule creation before definition lookup/write |
| `json-depth` | 32 | Decoded business JSON values in the above paths |
| `json-elements` | 10,000 | Value/container visits, not bytes; map entries/list elements bounded |

Byte properties must be 1..8,388,608; node limit 1..1,024; dependencies 1..256; depth 1..64; element limit 1..100,000. These ceilings prevent configuration from silently removing the bound, not a recommended production allocation. Definition DTO serialization includes null/default fields, so it may differ from received wire size. Structural bounds apply to child inputs and business maps; the HTTP cap also bounds the outer DTO. JSON map keys must be strings; values must be JSON scalars/maps/collections. Cycles hit the depth bound before serialization.

Tune defaults against measured request distributions, memory and latency. Existing agent iteration/model/tool/token/cost/context budgets and the tool fingerprint's established 8,000-character canonical envelope remain authoritative and can be stricter. These are not tenant-wide rate limits, global queue quotas, retention limits or output/history pagination. New registration limits are not retroactively applied to an already persisted definition during orchestration/recovery; new work starts and new deliveries use the configured current input limits. Lowering limits requires checking pending schedules/children and deployments for compatibility first. Tool replay also checks current argument bounds: do not lower those below persisted authorized request sizes without a migration plan.

## Authorization matrix for a real deployment

| Boundary | Required production control |
| --- | --- |
| Definition registration | Authenticated publisher role; handler allowlist, version governance; deny untrusted code/configuration |
| Workflow/agent starts | Tenant ownership, allowed definition/scenario, resource/rate/cost quotas |
| Inspection/history/route/tool/agent summary | Read authorization on the run/tenant; payload access and redaction, not just a valid UUID |
| Signal submission | Permission on that exact run and signal type; service-to-service identity, replay/conflict handling |
| Human tool approval | Reviewer authority on the specific tenant/tool/risk; bind immutable request ID + fingerprint; record authenticated reviewer identity |
| Schedules | Scheduler/admin role, allowed pinned definition/input, creation and future update/delete authorization |
| Advance/legacy execute/dev endpoints | Operator/internal-only; disable dev profile externally; compatibility synchronous execution is not a public production runner |
| Actuator metrics/health | Management network/access controls; do not expose operational metadata indiscriminately |

Authentication establishes identity. Authorization decides whether that identity may read a run, send a signal or authorize a refund. Knowing a UUID or having permission to start an agent does not grant refund approval authority. Derive retrieval tenant/resource scope from authenticated server-side policy; accepting a client-supplied tenant filter is not tenant authorization. Current approval binding prevents payload substitution; it **does not authenticate the human**. No fake OAuth provider or shared API-key approximation was introduced. Schedule modification endpoints are not currently implemented; apply the same controls if added.

## Input and SQL review

Controllers validate typed records with Bean Validation; service-level guards also cover internal callers. Jackson control DTO unknown fields now fail via `spring.jackson.deserialization.fail-on-unknown-properties=true`; an HTTP integration test covers this contract, pending runtime execution. Business input maps intentionally remain open within the JSON bounds. Model/retrieval/approval/tool schemas keep their explicit field allowlists and normalized fingerprints. Unknown tool names or metadata cannot redefine trusted permissions/risk.

Repository/JDBC mutation paths use positional or named SQL parameters for external values. Dynamic SQL fragments inspected are internal choices: fixed table names in telemetry duration projection and optional constant lock clauses in routing. They do not accept user-supplied identifiers. The benchmark uses fixed queries and isolated fixtures. Do not concatenate future user filters, ORDER BY fields or table names; map allowed choices to fixed fragments. This is a targeted source review, not a completed penetration test.

Read-only inspection/history endpoints can still return large histories. Add authorized pagination, retention and admission quotas before real multi-tenant use. A JSON body cap is not a complete denial-of-service defense. End-to-end parser/depth behavior, error codes and compatibility must be verified by the new tests on a real runtime.

## Secrets, privacy and future tools

`.env.example` contains placeholders only; Compose reads `.env`, while Spring Boot needs environment variables exported into the API/worker processes or a secret-manager integration. The existing `durableflow_local` fallback is a local demo credential, not a production secret. Real deployment must replace it. `.env` is ignored; `.env.example` is safe to commit. No commercial provider key is needed by the fake adapters.

Use environment/secret-manager injection for database/provider credentials. Never put secrets in workflow input, signal payload, history, model context, examples or tool arguments: those are durable data and may be inspected. The telemetry sanitizer intentionally drops payloads/secrets, but it cannot remove secrets already stored in business checkpoints or old generic exception logs. Review handler error strings, database access, backups and retention as well. No full PII detector or existing-history scrubber is claimed.

There is no generic URL fetch/webhook tool. Any future HTTP tool needs explicit destination allowlists, DNS/IP validation including redirect/rebinding handling, private/link-local/metadata-address denial, egress isolation, TLS checks, timeout/response-byte bounds and authenticated tool policy. Schema validation alone does not prevent SSRF. MCP discovery advertises capabilities; it never grants authority.

Tests: `ResourceLimitsTest`, `BoundedRequestFilterTest`, `SecurityBoundaryIntegrationTest`. Auth/OIDC, tenant quotas, pagination, hardened ingress, real key rotation and security testing remain deployment work.
