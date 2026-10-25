param(
    [ValidateSet('refund','runaway')][string]$Scenario = 'refund',
    [switch]$ApproveRefund,
    [Guid]$RetrievalActivityId,
    [string]$BaseUrl = 'http://localhost:8080/api'
)
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Invoke-RestMethod -Uri "$($BaseUrl -replace '/api$', '')/actuator/health" -TimeoutSec 2 | Out-Null
$file = if ($Scenario -eq 'refund') { 'start-bounded-refund-agent.json' } else { 'start-runaway-agent.json' }
$body = Get-Content -LiteralPath (Join-Path $repoRoot "examples/$file") -Raw | ConvertFrom-Json
if ($PSBoundParameters.ContainsKey('RetrievalActivityId')) {
    $body | Add-Member -NotePropertyName retrievalActivityId -NotePropertyValue $RetrievalActivityId.ToString()
}
$agent = Invoke-RestMethod -Method Post -Uri "$BaseUrl/agent-executions" -ContentType 'application/json' -Body ($body | ConvertTo-Json -Depth 12)
$approved = $false
# Bounded client-side orchestration pump. Providers run only in the separate durable worker.
for ($transition = 0; $transition -lt 120; $transition++) {
    $agent = Invoke-RestMethod -Method Post -Uri "$BaseUrl/agent-executions/$($agent.id)/advance"
    if ($agent.status -in @('COMPLETED','LIMIT_REACHED','FAILED')) {
        if ($Scenario -eq 'runaway' -and $agent.terminationReason -ne 'MAX_ITERATIONS') { throw 'Unexpected runaway termination.' }
        if ($Scenario -eq 'refund' -and $agent.terminationReason -ne 'FINAL_ANSWER') { throw 'Refund agent did not finish with FINAL_ANSWER.' }
        $agent | ConvertTo-Json -Depth 12 | Write-Output
        Invoke-RestMethod -Uri "$BaseUrl/agent-executions/$($agent.id)/iterations" | ConvertTo-Json -Depth 14 | Write-Output
        return
    }
    if ($agent.status -eq 'WAITING_APPROVAL') {
        $iterations = @(Invoke-RestMethod -Uri "$BaseUrl/agent-executions/$($agent.id)/iterations")
        $latest = $iterations | Sort-Object number | Select-Object -Last 1
        $request = Invoke-RestMethod -Uri "$BaseUrl/workflow-runs/$($latest.toolRunId)/tool-request"
        $request | ConvertTo-Json -Depth 12 | Write-Output
        if (-not $ApproveRefund) { Write-Output "Paused for review. Agent=$($agent.id), tool run=$($latest.toolRunId). No refund authorized."; return }
        if ($approved -or $request.status -ne 'AWAITING_APPROVAL' -or $request.toolName -ne 'refundOrder' -or $request.arguments.orderId -ne 'O-42' -or $request.arguments.amount -ne 4500) { throw 'Unexpected approval request.' }
        $signal = @{signalId='bounded-agent-review-1';signalNodeId='approval';payload=@{toolRequestId=$request.toolRequestId;fingerprint=$request.fingerprint;approved=$true}} | ConvertTo-Json -Depth 5
        Invoke-RestMethod -Method Post -Uri "$BaseUrl/workflow-runs/$($latest.toolRunId)/signals" -ContentType 'application/json' -Body $signal | Out-Null
        $approved = $true
    }
    Start-Sleep -Milliseconds 250
}
throw 'Demo transition bound reached. Inspect the agent and check the separate worker process.'
