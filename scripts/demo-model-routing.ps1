param([ValidateSet('fallback','circuit','budget')][string]$Scenario='fallback',[string]$BaseUrl='http://localhost:8080/api')
$ErrorActionPreference='Stop'
$repoRoot=Split-Path -Parent $PSScriptRoot
Invoke-RestMethod -Uri "$($BaseUrl -replace '/api$','')/actuator/health" -TimeoutSec 2 | Out-Null
# Run one worker with fake-a-script=UNAVAILABLE,UNAVAILABLE; start it fresh for each scenario.
function Start-RoutedRun {
    $body=Get-Content -LiteralPath (Join-Path $repoRoot 'examples/start-routed-model.json') -Raw
    $run=Invoke-RestMethod -Method Post -Uri "$BaseUrl/workflow-runs" -ContentType 'application/json' -Body $body
    Invoke-RestMethod -Method Post -Uri "$BaseUrl/workflow-runs/$($run.runId)/advance" | Out-Null
    for($n=0;$n -lt 100;$n++) {
        $view=Invoke-RestMethod -Uri "$BaseUrl/workflow-runs/$($run.runId)"
        if($view.status -eq 'FAILED'){throw 'Model run failed; inspect activities and model-routes.'}
        if($view.status -eq 'COMPLETED'){return $view}
        Start-Sleep -Milliseconds 250
    }
    throw 'Worker/demo deadline reached.'
}
if($Scenario -eq 'budget') {
    $agent=Invoke-RestMethod -Method Post -Uri "$BaseUrl/agent-executions" -ContentType 'application/json' -Body (Get-Content -LiteralPath (Join-Path $repoRoot 'examples/start-routing-budget-agent.json') -Raw)
    for($n=0;$n -lt 100;$n++) {
        $agent=Invoke-RestMethod -Method Post -Uri "$BaseUrl/agent-executions/$($agent.id)/advance"
        if($agent.status -in @('COMPLETED','FAILED','LIMIT_REACHED')) {
            if($agent.status -ne 'LIMIT_REACHED' -or $agent.terminationReason -ne 'MAX_ESTIMATED_COST'){throw 'Expected fallback cost reservation to be blocked. Check the fresh worker script/configuration.'}
            $agent | ConvertTo-Json -Depth 10 | Write-Output
            $iterations=@(Invoke-RestMethod -Uri "$BaseUrl/agent-executions/$($agent.id)/iterations")
            Invoke-RestMethod -Uri "$BaseUrl/workflow-runs/$($iterations[0].modelRunId)/model-routes" | ConvertTo-Json -Depth 14 | Write-Output
            return
        }
        Start-Sleep -Milliseconds 250
    }
    throw 'Agent/demo deadline reached.'
}
try {Invoke-RestMethod -Method Post -Uri "$BaseUrl/workflow-definitions" -ContentType 'application/json' -Body (Get-Content -LiteralPath (Join-Path $repoRoot 'examples/routed-model-v1.json') -Raw) | Out-Null}
catch {if($_.Exception.Response.StatusCode.value__ -ne 409){throw}}
$first=Start-RoutedRun
$output=($first.activities | Where-Object activityId -eq 'model').output
if(-not $output.routing.fallbackUsed -or $output.provider -ne 'fake-b'){throw 'Expected frozen primary A -> fallback B. Start a fresh single worker with the documented script.'}
$output | ConvertTo-Json -Depth 12 | Write-Output
if($Scenario -eq 'circuit') {
    $second=Start-RoutedRun
    $next=($second.activities | Where-Object activityId -eq 'model').output
    if($next.routing.primaryProvider -ne 'fake-b' -or $next.routing.fallbackUsed){throw 'Expected this same worker instance to skip open A on the next selection.'}
    $next | ConvertTo-Json -Depth 12 | Write-Output
}
