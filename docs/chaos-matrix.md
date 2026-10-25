# Deterministic chaos and recovery matrix

These tests simulate failure points with controlled hooks, expired database deadlines and fresh application contexts. They are not evidence of an actual OS kill, network partition or database failover. No recovery scenario is claimed to have run in this environment.

```powershell
.\scripts\run-chaos.ps1 -Scenario all
.\scripts\run-chaos.ps1 -Scenario effects
mvn -B '-Dtest=TaskLeaseIntegrationTest,PostgresTaskQueueIntegrationTest,DurableTimerIntegrationTest,DurableSignalIntegrationTest,ToolExecutionIntegrationTest,ModelRoutingIntegrationTest,BoundedAgentIntegrationTest,TelemetryFailureIntegrationTest' test
```

| Failure | Persisted state at failure | Recovery mechanism | Expected final state | Invariant / executable evidence |
| --- | --- | --- | --- | --- |
| Worker dies after claim | CLAIMED task, RUNNING execution, owner/generation/expiry | Expired lease claim transfers generation under lock | Valid replacement can complete | One owner per generation; TaskLeaseIntegrationTest |
| Worker dies after provider effect | External fake ledger committed, local request APPROVED and task not completed | Replacement invokes same immutable idempotency identity | One refund ledger effect; request EXECUTED | Stable semantics/key; ToolExecutionIntegrationTest#crashAfterEffectBeforeCompletionReclaimsSameKeyAndFencesOldResult |
| Zombie returns after replacement | New owner/generation persisted | Every completion/failure/renewal checks current nonexpired ownership | Old result rejected, new result accepted | No stale accepted commit; TaskLeaseIntegrationTest |
| Completion commits before advance | Completed task/activity/output/history; downstream still PENDING | Explicit repeat `/advance` reconstructs readiness | Only remaining task materialized once | Completed work not rerun; PostgresTaskQueueIntegrationTest |
| Timer passes deadline during downtime | WAITING timer with stable wake_at | Restart poller selects overdue row and advances atomically | FIRED, downstream READY once, never early | Durable time; DurableTimerIntegrationTest |
| Signal arrives before wait | Durable delivery, not yet consumed | Later advancement matches exact signal node and consumes atomically | One received wait/downstream task | No lost early delivery; DurableSignalIntegrationTest#bufferedSignalSurvivesFreshContextAndIsConsumedAfterUpstreamWork |
| Approval duplicated | Accepted receipt bound to request/fingerprint | Same delivery identity replays; changed identity/payload conflicts | One logical approval and execution | No approval substitution/double effect; ToolExecutionIntegrationTest#duplicateApprovalIsHarmlessButChangedDeliveryConflicts |
| Primary provider outage | Frozen route + failure attempts/reservations | Bounded same-route retries, then prevalidated fallback; local breaker opens | Fallback succeeds or route exhausts/budget blocks | No infinite hopping/unauthorized downgrade; ModelRoutingIntegrationTest |
| Agent runs indefinitely | Persisted iteration/call/token/cost reservations | Driver checks frozen budgets before next work | LIMIT_REACHED | Iterations beyond cap = 0; BoundedAgentIntegrationTest#runawayStopsAtMaxIterationsAndConcurrentRestartCannotExceedIt |
| Telemetry sink throws | Engine state/history independently committed | Catch failures on bounded background consumer | Workflow/agent completes despite sink loss | Diagnostics cannot roll back truth; TelemetryFailureIntegrationTest |

The runner offers `leases`, `effects`, `advancement`, `timers`, `signals`, `approvals`, `models`, `agents`, `telemetry`, or `all`; each maps to the tests above, with class-wide coverage where several related failure points matter. Surefire XML/text is the actual evidence, not the script's presence. Full regressions also cover late siblings during Saga compensation, timeout/shutdown, child reconciliation and schedule occurrence uniqueness. `GoldenEvalIntegrationTest` combines representative AI/safety/durability contracts in a versioned report.

For a later manual OS crash exercise: use disposable local data and two uniquely named worker processes; identify the exact process you started, stop only that process after observing its claim/effect checkpoint, wait for the database lease to expire, and let the replacement reclaim. Inspect generation, durable history and provider ledger count. Do not kill arbitrary Java processes or a shared PostgreSQL server. Record that experiment separately from controlled test-hook results. True database connection loss/network partition, restore/failover, sustained overload and multi-host failure tests remain deferred.
