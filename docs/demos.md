# Two primary demo stories

Prerequisites and commands are in [runtime verification](runtime-verification-checklist.md). The parallel DAG, compensated Saga, retrieval checkpoint, approved refund and bounded runaway demos **passed locally on 2026-10-26**, with one API and two independent worker JVMs. See [evidence](verification/runtime-2026-10-26/live-demos.json). Use disposable local data. Run the default API and one or more separate `worker` profile processes against the same PostgreSQL database. Assign each worker a unique WORKER_ID. PowerShell scripts may be blocked by local execution policy; do not bypass it silently. Equivalent Maven test commands are provided for automated scenarios.

## A. Core durability / ChaosCommerce

The commerce story uses small composable fixtures rather than claiming one huge end-to-end workflow has been verified.

```powershell
$api='http://localhost:8080/api'
# Register once per fresh database; duplicate name/version correctly conflicts.
Invoke-RestMethod -Method Post -Uri "$api/workflow-definitions" -ContentType 'application/json' -Body (Get-Content examples/parallel-order-v1.json -Raw)
$run=Invoke-RestMethod -Method Post -Uri "$api/workflow-runs" -ContentType 'application/json' -Body (Get-Content examples/start-parallel-order.json -Raw)
Invoke-RestMethod -Method Post -Uri "$api/workflow-runs/$($run.runId)/advance"
Invoke-RestMethod -Uri "$api/workflow-runs/$($run.runId)"
Invoke-RestMethod -Uri "$api/workflow-runs/$($run.runId)/events"
# Separate Saga fixture simulates shipment failure and configured compensation.
Invoke-RestMethod -Method Post -Uri "$api/workflow-definitions" -ContentType 'application/json' -Body (Get-Content examples/saga-order-v1.json -Raw)
$saga=Invoke-RestMethod -Method Post -Uri "$api/workflow-runs" -ContentType 'application/json' -Body (Get-Content examples/start-saga-order.json -Raw)
Invoke-RestMethod -Method Post -Uri "$api/workflow-runs/$($saga.runId)/advance"
Invoke-RestMethod -Uri "$api/workflow-runs/$($saga.runId)/events"
# Deterministic claim death, provider-effect death and stale-owner evidence:
.\scripts\run-chaos.ps1 -Scenario leases
.\scripts\run-chaos.ps1 -Scenario effects
# Durable timer downtime + early signal + compensation suites:
mvn -B '-Dtest=DurableTimerIntegrationTest,DurableSignalIntegrationTest,SagaCompensationIntegrationTest' test
```

Inspect polling state/history until the commerce run completes and the Saga reaches its documented terminal compensation outcome. A single immediate GET can still show RUNNING. Explain parallel branch eligibility, unique join scheduling, payment identity, claim generation, lease expiry and compensation state. The automated scenarios isolate each failure precisely; they are not proof that manually terminating an arbitrary worker always catches a chosen crash window. See the [chaos matrix](chaos-matrix.md) and existing [timer](durable-timers.md)/[signal](durable-signals.md)/[Saga](saga-compensation.md) instructions for detailed checkpoints.

## B. Retrieval-grounded, routed AI refund agent

Fake providers need no API key. Create a real scoped retrieval checkpoint first, then pass its execution ID into the bounded refund agent:

```powershell
$api='http://localhost:8080/api'
Invoke-RestMethod -Method Post -Uri "$api/workflow-definitions" -ContentType 'application/json' -Body (Get-Content examples/retrieval-checkpoint-v1.json -Raw)
$retrieval=Invoke-RestMethod -Method Post -Uri "$api/workflow-runs" -ContentType 'application/json' -Body (Get-Content examples/start-retrieval-checkpoint.json -Raw)
Invoke-RestMethod -Method Post -Uri "$api/workflow-runs/$($retrieval.runId)/advance" | Out-Null
# Bound the client wait; provider work runs in the separate worker.
$view=$null
for($i=0;$i -lt 60;$i++) {
    $view=Invoke-RestMethod "$api/workflow-runs/$($retrieval.runId)"
    if($view.status -in @('COMPLETED','FAILED')){break}
    Start-Sleep -Milliseconds 250
}
if($view.status -ne 'COMPLETED'){throw 'Retrieval checkpoint did not complete; inspect worker/history.'}
$history=@(Invoke-RestMethod "$api/workflow-runs/$($retrieval.runId)/events")
$retrievalActivity=($history | Where-Object { $_.eventType -eq 'ACTIVITY_COMPLETED' -and $_.activityId -eq 'retrieve' } | Select-Object -First 1).activityExecutionId
if(-not $retrievalActivity){throw 'No completed retrieval execution identity found.'}
# Explicit demo authorization to approve only the expected synthetic O-42 / 4500 request.
.\scripts\demo-bounded-agent.ps1 -Scenario refund -RetrievalActivityId $retrievalActivity -ApproveRefund
# Separate bounded-runaway, routing outage and objective evaluation stories:
.\scripts\demo-bounded-agent.ps1 -Scenario runaway
.\scripts\run-chaos.ps1 -Scenario models
.\scripts\run-evaluation.ps1
Invoke-RestMethod http://localhost:8080/actuator/metrics
# Substitute the agent ID printed by the demo:
# Invoke-RestMethod "$api/agent-executions/<agent-id>/summary"
```

The agent freezes retrieval as untrusted data, uses routed model activities to propose read/refund tools, checks trusted schema/permissions/policy, and pauses before refund. The explicit demo switch submits an exact request-bound approval, not a general production reviewer identity. Provider idempotency prevents a repeated refund effect when a result is lost. The loop persists iterations and reservations; eval/telemetry diagnose behavior without authorizing actions. Omit `-ApproveRefund` to stop and inspect the pending request rather than approve it.

Run `run-chaos.ps1 -Scenario effects` to demonstrate crash-after-effect recovery and `-Scenario models` for retry/fallback/circuit assertions. Do not configure provider failure scripts for the base success demo unless intentionally demonstrating outage/budget behavior. A real model, real MCP transport, authenticated reviewer and complete production RAG pipeline are not included. Reports in target are evidence only after the commands actually execute successfully.
