# Runtime verification board - 2026-10-26

The local correctness gates pass on tested commit `c07b1ad`; feature freeze remains in force. The full default test selection ran through Maven's package lifecycle, including executable repackaging. [Machine-readable evidence](verification/runtime-2026-10-26/regression.json) records versions, hardware, per-suite counts, artifact hash and earlier failures. The earlier unit-only evidence remains historical.

| Gate | Actual result |
| --- | --- |
| Java / Maven | PASS - Microsoft OpenJDK 21.0.13.0.2 / Maven 3.9.16 |
| Docker / PostgreSQL | PASS - Docker Server 29.9.2, Linux engine, postgres:17.6 healthy |
| Compile / test compile / package | PASS - executable JAR produced |
| Full default regression | PASS - 369 selected, 368 passed, zero failures/errors; benchmark intentionally skipped |
| Flyway / Hibernate | PASS - V1-V13 applied/validated in test containers and shared Compose database |
| API startup / health | PASS - separate API JVM, HTTP health UP |
| Independent worker coordination | PASS - two worker JVMs; worker 1 completed 17 tasks, worker 2 completed 11 plus the intentional failed Saga shipment |
| Live core demos | PASS - parallel DAG COMPLETED; failing-shipment Saga COMPENSATED |
| Live AI demos | PASS - retrieval checkpoint, exact request/fingerprint-bound synthetic approval, one refund effect, FINAL_ANSWER; runaway stops at MAX_ITERATIONS |
| Controlled chaos | PASS within full regression - leases/reclaim/fencing, effect replay, timer/signal recovery, budgets/fallback and telemetry isolation |
| Golden evaluation | PASS - 12/12 synthetic cases; [report](verification/runtime-2026-10-26/eval-report.md) |
| Opt-in benchmark smoke | PASS - seven separate opt-in runs, zero errors; queue 1/4/16 threads plus start/DAG/timer/signal; [evidence and limits](benchmarks.md) |
| Production readiness | NOT CLAIMED - deferred items below |

## Corrections discovered by execution

- Three invalid try-with-resources uses of SimpleMeterRegistry prevented test compilation; explicit finally cleanup fixed them without changing assertions.
- This Windows JVM defaults to legacy Asia/Calcutta, rejected by the PostgreSQL image at JDBC startup. Explicit UTC in every verification/API/worker JVM resolves it. Hibernate's JDBC time zone alone does not control driver startup.
- The persistence test still expected migration 10; it now requires 13 and no pending migrations.
- Cached Spring test contexts kept pollers/pools alive after class-owned databases stopped. Integration classes now close their contexts after each class.
- The golden quality-floor scenario dereferenced absent output from a correctly failed activity. It now records NONE without changing route/quality expectations.
- A persisted refund amount assertion expected scientific notation. It now compares the exact numeric amount 4500; immutable-request and decision-mutation rejection checks remain intact.
- README/demo history URLs used /history; the implemented endpoint is /events. Corrected and exercised live.

The first complete run selected 369 tests and exposed two assertion/fixture failures; both were fixed before the clean full rerun. Two diagnostic runs were deliberately interrupted to apply observed corrections; neither is represented as a pass. Raw logs and a preserved full-suite Surefire archive remain under ignored target/. Only sanitized summaries and synthetic evaluation/benchmark reports are committed.

## What remains outside this completed local correctness pass

Real LLM/vector/MCP transports, production authentication and reviewer authorization, tenant quotas, history pagination/retention, general automatic run reconciliation/agent driving, cluster-wide circuit state, production ingress/monitoring, backup/PITR/restore/failover drills, actual OS-kill/network-partition drills, sustained HTTP/soak load and multi-host capacity are deferred. Two local worker processes prove functional coordination, not scaling capacity. Controlled test hooks/context restarts are distinct from OS or database disaster recovery.

The Compose database is retained. Temporary API/worker JVMs started for this verification were stopped by their recorded process IDs after evidence collection. Their termination was cleanup, not a crash-recovery experiment. PowerShell execution policy was not changed; equivalent direct Maven/HTTP calls were used.

## Local portable tool setup

Tools live outside the repository in `C:/Users/aman8/.codex/tools/durableflow-runtime`. No machine-wide installation, registry PATH change or execution-policy change was made. Use the following in a local PowerShell session to reproduce the verification shell (or use your own installed JDK 21/Maven):

```powershell
$runtimeRoot=Join-Path $env:USERPROFILE '.codex/tools/durableflow-runtime'
$env:JAVA_HOME=(Get-ChildItem -LiteralPath $runtimeRoot -Directory | Where-Object Name -like 'jdk-21*' | Select-Object -First 1).FullName
$mavenHome=(Get-ChildItem -LiteralPath $runtimeRoot -Directory | Where-Object Name -like 'apache-maven-*' | Select-Object -First 1).FullName
$env:PATH="$env:JAVA_HOME/bin;$mavenHome/bin;$env:PATH"
# This machine needs its Windows-trusted certificate chain for Maven HTTPS.
$env:MAVEN_OPTS='-Djavax.net.ssl.trustStoreType=Windows-ROOT'
# Apply to Maven, forked tests, API and worker JVMs in this shell.
$env:JAVA_TOOL_OPTIONS='-Duser.timezone=UTC'
$env:DEBUG='false'
java -version
mvn -version
mvn -B '-Dtest=*Test,!*IntegrationTest' test
```

The first dependency download failed with PKIX certificate-chain validation. Windows-ROOT resolved it while preserving certificate/hostname verification; no insecure TLS bypass or certificate-validation disable flag was used. Portable archives came from Microsoft's JDK download and Apache Maven Central, with vendor-published checksums compared before extraction. They are not committed.
