# Session 22: durable retrieval and grounded context

Retrieval-augmented generation (RAG) supplies selected reference material to a model request.
Retrieval is a separate retrieveContext ACTIVITY, so selection, source provenance and context
become a durable checkpoint before generation. Both retrieval and model calls use the existing
queue, leases, retry/backoff, timeout, fencing and history paths. There is no RAG worker protocol.

## Package and request boundary

ai/retrieval contains Retriever, RetrievalGateway, RetrievalRequest/Response, RetrievalStrategy,
RetrievedChunk, DocumentMetadata, RetrievalFilters, FakeCorpus/FakeRetriever, ContextBuilder,
RetrieveContextActivity, GroundedPromptBuilder and RagModelInputPreparer. Core orchestration
contains no retrieval/vendor SDK types. No external embeddings API or vector database is added.

The explicit versioned request is stored in normal workflow/activity input:

```json
{"requestVersion":1,"retriever":"fake","corpusVersion":"policies-v1","rankingVersion":"local-v1","query":"refund POL-RFD-042","strategy":"HYBRID","topK":4,"maxContextChars":3000,"filters":{"tenantId":"tenant-a","documentType":"policy"}}
```

Wrap it in retrievalRequest in workflow input. Version, retriever/corpus/ranker identifiers,
query, strategy, topK, context budget and filters are required. Unknown fields/strategies,
missing tenant scope, topK outside 1-20, budget outside 1-32000 or query over 4000 characters
are invalid. Document type is optional (policy/note). Unsupported corpus/ranker versions fail,
with no automatic latest-version lookup or fallback.

## Corpus, chunks and ranking

FakeCorpus has seven explicit paragraph chunks: refund policy (two), fraud policy, shipping
policy, tenant-a account notes, an intentionally hostile tenant-a note and a private tenant-b
refund policy. Source URIs are local demo identifiers; nothing is fetched from them.

Stable chunk ids include document id/version/index. Each chunk preserves title/source,
document version, index, text, score, strategy, matched strategies and safe metadata, including
tenant/type. No raw embedding vectors enter the response or workflow output. Metadata is
essential to trace a recommendation to the exact source, distinguish document updates and
enforce scope; bare text cannot provide that identity.

Chunks are preselected paragraphs with no overlap, not a general ingestion platform. Small
chunks improve selection precision and fit budgets but can lose surrounding qualifications;
large chunks preserve context while adding irrelevant text and cost. Overlap can preserve
boundary information but repeats content. Chunk-id deduplication does not claim to remove
semantic overlap between different chunks. Future ingestion must preserve version/source ids.

| Strategy | Deterministic local behavior |
| --- | --- |
| DENSE | Cosine similarity over five hand-written concept-count dimensions: refunds, fraud, shipping, accounts and review |
| SPARSE | Case-insensitive term-frequency matching with hyphen/underscore identifiers kept intact |
| HYBRID | Reciprocal rank fusion of positive dense/sparse rankings: sum 1/(60+rank), starting at rank 1 |

Dense synonym groups let money back match refund/reimbursement. This is a toy representation,
not a learned embedding model. Sparse search finds identifiers such as ACCT-1042 that these
concept dimensions do not represent. Sparse scoring is simple term scoring, not full BM25.
Hybrid merges by chunk id, records matched strategies, then sorts by fused score descending
and chunk id ascending. Raw cosine and term-frequency scores are not added directly.
Combining complementary signals can improve coverage; it does not guarantee better results.

The conceptual pipeline is scope filter -> candidate rankings -> fusion/deduplication ->
optional rerank -> topK -> context. No separate reranker implementation is needed here;
a future reranker can reorder candidates before topK without changing the durable activity
contract. This session adds no external reranking model or evaluation framework.

## Context construction and source labels

ContextBuilder retains deterministic ranking order, deduplicates by chunk id and assigns
S1/S2/... to included chunks. Each block contains the complete source metadata header plus
complete text. It stops at the first block that would exceed maxContextChars; it never
silently cuts text or metadata identities. An oversized first block produces empty context.
Output includes text, source metadata/labels, character count, budget and omitted chunk count.

The budget counts Java string characters in the constructed reference block, including headers
and separators. It is approximate context capacity, not a real tokenizer count or the total
model request size. Sources may occupy fewer slots than topK after budgeting. The final model
request still has Session 21's independent size validation; excessive combined input fails
rather than silently truncating source identities. Empty retrieval/context is a valid checkpoint.

## Durable binding into modelCall

The example uses retrieveContext -> modelCall -> approval SIGNAL -> applyApprovedAction.
Workflow input also declares grounding: {groundingVersion:1, sourceNodeId:"policies"} and the
ordinary versioned modelRequest. The source must be a direct dependency, in the same run,
declared as retrieveContext and durably COMPLETED. This is one bounded binding, not a general
dataflow language or tool registry.

ActivityInputPreparer/ActivityInputPreparation are provider-neutral first-claim snapshot hooks.
TaskTransactions invokes the optional adapter only for a first PENDING forward claim. It runs
bounded local/DB reads under the existing run/task transaction, never a retriever or provider.
Compensation input remains its original envelope; retries/reclaims retain the existing snapshot.

RagModelInputPreparer reads the source execution's persisted request/output and verifies matching
tenant/type scope. GroundedPromptBuilder places the user question, retrieval query and complete
referenceData in userContent JSON, with explicit UNTRUSTED_DATA marking. Trusted system text
adds a fixed v1 rule that reference text is data, not instructions, and requests source labels
and an insufficient-evidence response where applicable. It never promotes retrieved text into
system instructions. JSON quoting separates data structurally; it is not a prompt-injection cure.

The assembled actual modelRequest and groundingPreparation provenance are saved atomically in
ActivityExecution.input_json with the first committed claim, before provider execution. A retry
does not rebuild that prompt from a changed corpus, source configuration or deployed template.
The preparation version's semantics must remain stable across deployment. Known invalid binding
is frozen as INVALID, then modelCall reports ordinary terminal failure without calling the model;
it does not leave a task repeatedly rolling back claims. Storage failures can still roll back
the whole claim safely. Grounded workflows use /advance and queue workers for this preparation;
the legacy synchronous execution path does not perform this binding.

Model output adds grounding provenance: source activity/node, corpus/ranker versions, filters
and actual context source labels/identities. It does not duplicate full chunk text there.
The fake model produces its existing deterministic recommendation/fingerprint; this demonstrates
that retrieved context reached the request, not factual reasoning or citation verification.
Provenance shows what was supplied, not proof that a generated claim is supported.

## Recovery and failure semantics

Completed retrieval output is reused after restart, including exact selected chunks, context,
labels and versions. Re-running an index query could otherwise replace A/B/C with newer D/E/F
and silently change the basis of an already-generated recommendation. Model completion is also
checkpointed, so a restart while awaiting approval regenerates neither activity.

If a worker crashes before persisting retrieval completion, there is no retrieval checkpoint.
A reclaimed attempt may see changed corpus/index/ranking data and choose different chunks.
Fencing allows one valid committed result, not exactly-once querying. Explicit requested versions
aid reproducibility; a provider/backend must actually honor their immutability. The mutable-index
test fixture deliberately changes documents under the same requested corpus id to show the
pre-checkpoint ambiguity. This adds no cross-attempt index snapshot or global retrieval cache.

Typed UNAVAILABLE and TIMEOUT backend errors translate to RetryableActivityException. Invalid
configuration/filter/strategy is terminal. Unknown programming errors remain conservative;
classification never matches error-message substrings. Local slow retrieval uses the existing
Session 14 timeout supervisor. No retrieval-specific retry loop is introduced.

## Trust and authorization scope

FakeRetriever filters tenant/type before scoring, merging or context construction. Gateway
validation also rejects an out-of-scope backend response before it can become workflow output
or context. Grounded preparation checks source scope again. Tests prove the private tenant-b
chunk never enters tenant-a context, even for a matching identifier/query, and reject a leaky
backend adapter.

These filters express scope; they do not authenticate the caller. The local API accepts tenant
scope as input. Production must derive/validate that scope from an authenticated identity rather
than trusting an arbitrary client-supplied tenant id. Full auth/RBAC is outside this session.
Filtering after generation would be too late: the model would already have received the text.

Authorized documents can still contain hostile instructions (indirect prompt injection). A test
fixture says Ignore earlier instructions; it remains exclusively in reference user data while
trusted system instructions reject following it. Prompt formatting reduces accidental confusion,
but is no security sandbox. Models may still hallucinate, follow malicious text, use stale policy
or cite unsupported statements. RAG improves grounding opportunities, not guaranteed truth.
Risky effects remain separate configured idempotent activities behind the signal gate.

## Demo and history

Start database/server/worker as in README. Register and start the example:

```powershell
$base = 'http://localhost:8080/api'
Invoke-RestMethod -Method Post -Uri "$base/workflow-definitions" -ContentType 'application/json' -Body (Get-Content examples/grounded-agent-v1.json -Raw)
$run = Invoke-RestMethod -Method Post -Uri "$base/workflow-runs" -ContentType 'application/json' -Body (Get-Content examples/start-grounded-agent.json -Raw)
Invoke-RestMethod -Method Post -Uri "$base/workflow-runs/$($run.runId)/advance"
$state = Invoke-RestMethod -Uri "$base/workflow-runs/$($run.runId)"
($state.activities | Where-Object activityId -eq 'policies').output
($state.activities | Where-Object activityId -eq 'recommendation').output
```

Poll until approval is WAITING. Review source identities and the fake recommendation, restart
server/worker with the volume retained if desired, then send the approval receipt:

```powershell
$receipt = @{signalId='rag-review-1'; signalNodeId='approval'; payload=@{approved=$true}} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "$base/workflow-runs/$($run.runId)/signals" -ContentType 'application/json' -Body $receipt
Invoke-RestMethod -Uri "$base/workflow-runs/$($run.runId)/events"
```

The configured markReviewed action follows signal receipt. As in Session 21, this is a receipt
gate: payload booleans are not routing/authorization decisions. No arbitrary branching or model-
generated action is added. Generic history contains no full chunk text, prompts or vectors;
detailed retrieval/model data stays in activity input/output. No new event vocabulary or schema
is needed; V1-V10 are unchanged.

## Tests, changed files and verification

RetrievalTest adds 11 unit scenarios for metadata, deterministic dense/sparse/hybrid ranking,
exact identifiers, fusion deduplication/topK, scope rejection, context budgets/labels, typed errors,
hostile-reference separation and invalid configuration. RagRetrievalIntegrationTest adds nine
PostgreSQL scenarios for checkpoints/fresh contexts, retry, invalid configuration, grounded
model/approval flow, frozen first-claim request recovery, invalid binding, changed pre-checkpoint
corpus with fencing, DAG joins and existing timeout supervision. Prior suites remain in place.

Main changes are ai/retrieval/*, the two generic input-preparation types, a bounded claim hook
in TaskTransactions, optional grounding handling in ModelCallActivity, both test suites, the two
grounded-agent JSON examples and documentation/README. No provider SDK or dependency changes.

Java, Maven and Docker are unavailable on PATH. Compilation/test/startup attempts were blocked;
the live demo could not run. Runtime behavior is not verified. Static checks cover Java syntax,
internal imports/packages, JSON/XML, unchanged migrations, dependency cycles and whitespace.
Run mvn verify and the demo with JDK 21/Maven/Docker before claiming runtime success.

## Learning checkpoint

1. RAG supplies reference context to generation; separate retrieval checkpoints preserve its evidence independently.
2. Smaller chunks improve precision/budget fit; larger chunks preserve surrounding qualifications but add noise/cost.
3. Metadata supplies source/version/citation identity and scope, beyond text alone.
4. Dense representation similarity can bridge vocabulary; sparse scoring preserves exact lexical matches/identifiers.
5. Hybrid fusion combines complementary rankings without assuming their raw score scales agree.
6. Completed A/B/C evidence remains the durable basis after an index changes to D/E/F; pre-checkpoint retry may select the new data.
7. Reranking would reorder a candidate set before final topK/context selection; it is deferred here.
8. A budget limits reference size, noise and eventual token/cost pressure; it is not an exact tokenizer guarantee.
9. Scope must be checked before model exposure; removing text from output afterwards cannot undo disclosure.
10. Retrieved text can carry hostile commands despite being authorized reference data, so it cannot become trusted instructions.
