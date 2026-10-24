# Session 12: idempotent payment attempts

Session 12 checkpoint notes: [Session 13](retries.md) adds bounded queued-handler retries;
the stable-key/provider contract and single-JVM fake-provider limitation below still apply.

| Term | Contract |
| --- | --- |
| At-least-once | A task can execute again after an unrecorded outcome and lease reclamation |
| Idempotency | Repeating one logical operation creates no additional business effect |
| Effectively-once | Several handler/provider invocations produce one intended effect because all attempts reuse a key that the provider honors |
| Exactly-once execution | Not promised: the handler and payment API can physically be invoked more than once |

## Logical identity and handler context

`ActivityExecutionContext(workflowRunId, activityExecutionId)` is immutable and derives
`idempotencyKey()` as `df:v1:<activityExecutionId>`. A different logical activity gets a
different UUID/key, even for the same activity type or workflow input. No key column or
new migration is necessary; V1–V4 remain unchanged. The namespace/version is part of the
contract and must remain stable for outstanding activities.

`ActivityHandler.execute(context, input)` is used by TaskWorker and the legacy local
executor. Ownership validation remains in worker infrastructure. Business handlers do
not need worker IDs, task IDs or fencing tokens to construct the key. Existing
`attempt_count` and `lease_generation` remain available in persisted inspection state;
they are deliberately excluded from the logical context and key.

Logical operation identity differs from attempt identity: reclamation increments both
current counters while retaining the activity ID/key. Heartbeats change neither counter.
This session does not add retries/backoff or timeouts for ordinary handler failures.

## Tiny provider simulation

chargePayment passes the context's key and complete input JSON to FakePaymentProvider.
An atomic per-key ConcurrentHashMap compute remembers `(canonical payload, result)`:

- First request creates one simulated payment and immutable result with a stable paymentId.
- Same key and same logical payload returns that original result without another effect.
- Same key and changed payload throws a clear IllegalArgumentException without replacing
  the original record. Under normal worker handling this is a terminal handler failure.

The full JSON input is this demo's payment payload. Objects compare without key-order
sensitivity, numbers normalize through BigDecimal, arrays preserve order, and nulls are
supported. A recursive snapshot prevents later caller mutation changing the stored
request. Changes to any input field count as misuse; a real payment adapter should
define its own explicit material payment fields. No cryptographic fingerprint is needed.
`effectCount()` counts remembered successful payment records, not invocations. UUID-based
payment IDs are deterministic fixtures, not evidence by themselves that an effect ran once.

**Scope:** the fake lives inside one application JVM. The tests deliberately share it
across worker attempts, representing an external provider that survives those attempts.
It is not durable across provider/application restarts, is not shared by separate worker
JVMs, has no retention policy, and makes no real charges. Do not use the ordinary
server/two-JVM demo as proof of shared provider deduplication. An arbitrary external
system must honor keys itself or use an application-specific durable deduplication and
reconciliation strategy. A local cache cannot make an arbitrary non-idempotent API exactly
once. DurableFlow's completion transaction is not atomic with an external payment.

## The crash window and fencing

1. A claims payment (generation 1), commits CLAIMED/RUNNING, then calls the provider with K.
2. Provider creates payment R and remembers K/payload/R.
3. A dies before committing DurableFlow completion. Its logical output is still absent.
4. Lease expires; B reclaims the same task/activity (generation 2, attempt_count 2).
5. B calls payment with the same K/payload. Provider returns R, with effect count still one.
6. B persists R and advances the workflow to its successor.

If A wakes late, it may physically call payment again, but the same K still deduplicates.
A's DurableFlow completion/failure is independently rejected by lease/owner/generation
fencing. **Fencing protects DurableFlow state; idempotency protects the external effect.**
Neither mechanism substitutes for the other. Existing short transactions, SKIP LOCKED,
heartbeats, expiry and stale-result rollback are preserved.

## Tests and verification

PaymentIdempotencyIntegrationTest creates a payment -> shipment workflow in a real
Testcontainers PostgreSQL database. A claims through TaskTransactions and invokes the
real registered payment handler outside a transaction, then deliberately omits completion.
The test expires its lease with DB time. B runs through TaskWorker, reclaiming and invoking
the real payment handler through a test observer. It checks the same key/result, incremented
generation/count, one effect, stable stored output, and successful successor execution.
A second case resumes zombie A during B's handler, verifies deduplicated payment, and
rejects both stale completion and failure before B commits. No process kill or sleep is
required; this simulates the failure window, not a separate-JVM restart.

PaymentIdempotencyTest covers key identity, canonical result reuse, changed payload,
snapshot isolation from caller mutation, distinct logical keys and concurrent duplicate
requests using a barrier. Updated registry/engine/lease tests keep the handler contract
consistent. Existing Session 11 tests remain in the full suite.

```powershell
mvn -DskipTests compile
mvn "-Dtest=PaymentIdempotencyTest,ActivityRegistryTest" test
mvn "-Dtest=PaymentIdempotencyIntegrationTest,TaskLeaseIntegrationTest,PostgresTaskQueueIntegrationTest" test
mvn verify
```

Implemented and statically checked only. Java, Maven and Docker are unavailable on the
authoring machine. Compilation, migration execution, tests, startup and demos have not
been runtime verified. The documented commands and test assertions are expected behavior,
not recorded successful runtime results.
