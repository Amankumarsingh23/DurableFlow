# Runtime verification board - 2026-10-26

Feature freeze remains in force. This verification pass starts from `e68e8aa`, fixes the test-resource cleanup mismatch described below, and records actual execution evidence. No push has been performed: the user authorized pushing only after the intended checks pass, and Docker-backed verification is still blocked.

| Gate | Evidence / status |
| --- | --- |
| Java | PASS - Microsoft OpenJDK 21.0.13.0.2, portable vendor archive with published SHA-256 verified |
| Maven | PASS - 3.9.16, portable Apache Maven archive with published SHA-512 verified |
| Main compilation | PASS - `mvn -B -U -DskipTests compile`, 210 main source files compiled with release 21 |
| Test compilation | PASS after cleanup fix - all 49 test source files compiled |
| Unit/non-integration selection | PASS - 141 tests, zero failures/errors/skips across 23 suites |
| Executable packaging | PASS - `mvn -B -ntp -DskipTests package`; this command skips execution, it does not imply full-suite success |
| Docker / WSL | BLOCKED - Docker Desktop absent; wsl --status reports Class not registered, Wsl/CallMsi/Install/REGDB_E_CLASSNOTREG; user is installing/repairing Docker Desktop |
| Flyway V1-V13 / PostgreSQL integration | BLOCKED - PersistenceIntegrationTest probe reports one setup error: Could not find a valid Docker environment; no DB assertions or migrations ran |
| Application startup / core durability demo | PENDING PostgreSQL and integration validation |
| DAG/Saga and AI refund demos | PENDING |
| Chaos / independent worker verification | PENDING |
| Benchmarks / measured scaling | PENDING - no numbers generated |
| README claims fully verified / resume finalized | PENDING complete evidence |
| Push | NOT PERFORMED - conditional checks incomplete |

## First failure and correction

Main compilation succeeded. Test compilation then reported three errors in `AiTelemetryTest` and `TelemetryBoundaryTest`: the pinned Micrometer `SimpleMeterRegistry` has a close method but does not implement AutoCloseable. Replaced the invalid try-with-resources declarations with explicit finally cleanup. No assertions were removed, weakened or disabled. A subsequent real `PersistenceIntegrationTest` attempt failed in Testcontainers setup with one Docker-environment error, before PostgreSQL or its assertions could run. The complete selected unit set was rerun and passed. Raw logs/XML remain in ignored `target/`; a sanitized machine-readable summary is committed at [verification/unit-results-2026-10-26.json](verification/unit-results-2026-10-26.json).

## Local portable tool setup

Tools live outside the repository in `C:/Users/aman8/.codex/tools/durableflow-runtime`. No machine-wide installation, registry PATH change or execution-policy change was made. Use the following in a local PowerShell session to reproduce the verification shell (or use your own installed JDK 21/Maven):

```powershell
$runtimeRoot=Join-Path $env:USERPROFILE '.codex/tools/durableflow-runtime'
$env:JAVA_HOME=(Get-ChildItem -LiteralPath $runtimeRoot -Directory | Where-Object Name -like 'jdk-21*' | Select-Object -First 1).FullName
$mavenHome=(Get-ChildItem -LiteralPath $runtimeRoot -Directory | Where-Object Name -like 'apache-maven-*' | Select-Object -First 1).FullName
$env:PATH="$env:JAVA_HOME/bin;$mavenHome/bin;$env:PATH"
# This machine needs its Windows-trusted certificate chain for Maven HTTPS.
$env:MAVEN_OPTS='-Djavax.net.ssl.trustStoreType=Windows-ROOT'
java -version
mvn -version
mvn -B '-Dtest=*Test,!*IntegrationTest' test
```

The first dependency download failed with PKIX certificate-chain validation. Windows-ROOT resolved it while preserving certificate/hostname verification; no insecure TLS bypass or certificate-validation disable flag was used. Portable archives came from Microsoft's JDK download and Apache Maven Central, with vendor-published checksums compared before extraction. They are not committed.

Next required external step: finish Docker Desktop/WSL2 repair, start Linux containers and obtain successful `docker version` client/server output. This may need administrator access or a restart. Then continue the [runtime verification checklist](runtime-verification-checklist.md) from PostgreSQL onward, fix failures and rerun the full suite. A Java build and unit pass alone do not establish migrations, Spring wiring, SQL, concurrency, approvals, chaos or capacity.
