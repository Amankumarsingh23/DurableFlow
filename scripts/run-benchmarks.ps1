param(
    [ValidateSet('start','queue','dag','timer','signal')][string]$Scenario='queue',
    [ValidateRange(1,10000)][int]$Workflows=20,
    [ValidateRange(1,32)][int]$Workers=1,
    [ValidateRange(1,32)][int]$Concurrency=4,
    [ValidateRange(1,64)][int]$Fanout=8,
    [ValidateRange(1,600)][int]$DeadlineSeconds=60,
    [switch]$Scaling
)
$ErrorActionPreference='Stop'
$repoRoot=Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $repoRoot
try {
    Get-Command java,mvn,docker -ErrorAction Stop | Out-Null
    $counts=if($Scaling){@(1,4,16)}else{@($Workers)}
    foreach($count in $counts) {
        & mvn -B '-Dtest=BenchmarkIntegrationTest' '-Ddurableflow.benchmark=true' "-Dbench.scenario=$Scenario" "-Dbench.workflows=$Workflows" "-Dbench.workers=$count" "-Dbench.concurrency=$Concurrency" "-Dbench.fanout=$Fanout" "-Dbench.deadlineSeconds=$DeadlineSeconds" test
        if($LASTEXITCODE -ne 0){throw "Benchmark failed/incomplete for $count workers. Inspect target/surefire-reports and any benchmark report."}
        Get-Content -LiteralPath "target/benchmark-$Scenario-w$count.json" -Raw | Write-Output
    }
}
finally {Pop-Location}
