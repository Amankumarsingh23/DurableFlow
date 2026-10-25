param(
    [ValidateSet('read','refund')][string]$Scenario = 'read',
    [switch]$ApproveRefund,
    [string]$BaseUrl = 'http://localhost:8080/api'
)
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
# Fail promptly if the API is unavailable; worker must be started separately.
Invoke-RestMethod -Uri "$($BaseUrl -replace '/api$', '')/actuator/health" -TimeoutSec 2 | Out-Null
$definitionFile = if ($Scenario -eq 'read') { 'safe-order-lookup-v1.json' } else { 'safe-refund-agent-v1.json' }
$startFile = if ($Scenario -eq 'read') { 'start-safe-order-lookup.json' } else { 'start-safe-refund-agent.json' }
$definitionText = Get-Content -LiteralPath (Join-Path $repoRoot "examples/$definitionFile") -Raw
$definition = $definitionText | ConvertFrom-Json
try {
    Invoke-RestMethod -Method Post -Uri "$BaseUrl/workflow-definitions" -ContentType 'application/json' -Body $definitionText | Out-Null
} catch {
    if ($_.Exception.Response.StatusCode.value__ -ne 409) { throw }
    Invoke-RestMethod -Uri "$BaseUrl/workflow-definitions/$($definition.name)/versions/1" | Out-Null
}
$run = Invoke-RestMethod -Method Post -Uri "$BaseUrl/workflow-runs" -ContentType 'application/json' -Body (Get-Content -LiteralPath (Join-Path $repoRoot "examples/$startFile") -Raw)
Invoke-RestMethod -Method Post -Uri "$BaseUrl/workflow-runs/$($run.runId)/advance" | Out-Null
function Wait-ToolRequest([string]$expected) {
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        $state = Invoke-RestMethod -Uri "$BaseUrl/workflow-runs/$($run.runId)"
        if ($state.status -eq 'FAILED') { throw 'Workflow failed; inspect activities/events.' }
        try { $view = Invoke-RestMethod -Uri "$BaseUrl/workflow-runs/$($run.runId)/tool-request" }
        catch { if ($_.Exception.Response.StatusCode.value__ -ne 404) { throw }; $view = $null }
        if ($null -ne $view -and $view.status -eq $expected) { return $view }
        Start-Sleep -Milliseconds 500
    }
    throw "Timed out waiting for $expected. Check the separate worker process."
}
if ($Scenario -eq 'refund') {
    $request = Wait-ToolRequest 'AWAITING_APPROVAL'
    if ($request.toolName -ne 'refundOrder' -or $request.arguments.orderId -ne 'O-42' -or $request.arguments.amount -ne 4500) { throw 'Unexpected fake refund proposal.' }
    if ($request.result.PSObject.Properties.Count -gt 0) { throw 'Result appeared before approval.' }
    $request | ConvertTo-Json -Depth 12 | Write-Output
    if (-not $ApproveRefund) { Write-Output 'Stopped for review; no refund authorized. Reuse the displayed request in a SIGNAL receipt.'; return }
    $signal = @{signalId='demo-tool-review-1';signalNodeId='approval';payload=@{toolRequestId=$request.toolRequestId;fingerprint=$request.fingerprint;approved=$true}} | ConvertTo-Json -Depth 5
    Invoke-RestMethod -Method Post -Uri "$BaseUrl/workflow-runs/$($run.runId)/signals" -ContentType 'application/json' -Body $signal | Out-Null
}
$completed = Wait-ToolRequest 'EXECUTED'
$completed | ConvertTo-Json -Depth 12 | Write-Output
Invoke-RestMethod -Uri "$BaseUrl/workflow-runs/$($run.runId)/events" | ConvertTo-Json -Depth 8 | Write-Output
