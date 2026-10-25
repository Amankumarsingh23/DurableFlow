# Session 26 verification record

Environment: Windows PowerShell, 2026-10-26. This is a verification-status record, **not a passing evaluation report**.

| Attempt | Observed result |
| --- | --- |
| `java -version` | Command not found on PATH |
| `mvn -B -DskipTests compile` | Maven command not found; compilation did not run |
| `mvn -B test` | Maven command not found; tests did not run |
| `mvn -B -Dtest=GoldenEvalIntegrationTest test` | Maven command not found; no golden observations or report generated |
| Selected telemetry/failure-isolation Maven test command | Maven command not found; scenario did not run |
| `docker version` | Docker command not found on PATH |
| GET localhost:8080/actuator/metrics, 3-second timeout | Timed out; endpoint not verified |
| `scripts/run-evaluation.ps1` | PowerShell execution policy rejected script loading; policy was not changed |
| Java tree-sitter syntax parsing | 253 source/test Java files parsed, no syntax errors; this is not compilation/type checking |
| JSON parsing | All repository JSON artifacts parsed, including 12 unique golden cases with 55 independent expectations |
| `git diff --check` | Passed before commit |

The new tests cover telemetry cardinality/sanitization, transaction-thread isolation, rollback suppression, saturation/failures, retrieval/provider measurements, objective evaluator negative cases, fingerprints, retrieval scoring, versions/report arithmetic, the real engine golden suite and a throwing sink during a real agent workflow. Their assertions are authored but runtime results remain unknown.

No database migration or dependency change. No real provider, exported tracing, collector, judge, dashboard or feedback warehouse. The optional schedule/child golden fixture remains represented by the existing full regression tests rather than duplicated in the new dataset. Legacy manually constructed test fixtures retain no-op telemetry constructors.

To finish runtime verification on a configured machine, use the commands in [evaluation-observability.md](evaluation-observability.md#running-and-inspecting). A successful golden invocation generates `target/eval-report.json` and `.md`; a failing case also generates a report before the JUnit assertion fails. Reports include only measured aggregates and objective check results, never synthetic claims of successful execution.
