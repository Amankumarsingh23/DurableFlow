param([ValidateSet('all','leases','effects','advancement','timers','signals','approvals','models','agents','telemetry')][string]$Scenario='all')
$ErrorActionPreference='Stop'
$repoRoot=Split-Path -Parent $PSScriptRoot
# Deterministic test hooks/context restarts: this runner does not kill user processes or a real database.
$cases=[ordered]@{
    leases='TaskLeaseIntegrationTest'
    effects='ToolExecutionIntegrationTest#crashAfterEffectBeforeCompletionReclaimsSameKeyAndFencesOldResult'
    advancement='PostgresTaskQueueIntegrationTest'
    timers='DurableTimerIntegrationTest'
    signals='DurableSignalIntegrationTest#bufferedSignalSurvivesFreshContextAndIsConsumedAfterUpstreamWork'
    approvals='ToolExecutionIntegrationTest#duplicateApprovalIsHarmlessButChangedDeliveryConflicts'
    models='ModelRoutingIntegrationTest'
    agents='BoundedAgentIntegrationTest#runawayStopsAtMaxIterationsAndConcurrentRestartCannotExceedIt'
    telemetry='TelemetryFailureIntegrationTest'
}
$selection=if($Scenario -eq 'all'){($cases.Values -join ',')}else{$cases[$Scenario]}
Push-Location -LiteralPath $repoRoot
try {
    Get-Command java,mvn,docker -ErrorAction Stop | Out-Null
    & mvn -B "-Dtest=$selection" test
    if($LASTEXITCODE -ne 0){throw 'Chaos regression failed. Inspect target/surefire-reports; no successful recovery claim is implied.'}
    Write-Output "Selected chaos regressions passed: $Scenario. Evidence: target/surefire-reports. These are deterministic simulations, not process/network fault injection."
}
finally {Pop-Location}
