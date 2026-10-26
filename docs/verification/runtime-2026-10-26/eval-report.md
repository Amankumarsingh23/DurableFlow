# DurableFlow offline evaluation

Suite: durableflow-golden / 1

Total: 12; passed: 12; failed: 0; pass rate: 1.0

| Case | Result | Failed categories |
| --- | --- | --- |
| A-final-agent | PASS | [] |
| A-read-only | PASS | [] |
| B-approved-refund | PASS | [] |
| C-denied-refund | PASS | [] |
| D-exact-identifier | PASS | [] |
| E-hybrid-policy | PASS | [] |
| F-hostile-document | PASS | [] |
| G-unknown-tool | PASS | [] |
| H-provider-fallback | PASS | [] |
| I-no-quality-downgrade | PASS | [] |
| J-runaway-bound | PASS | [] |
| K-crash-dedup | PASS | [] |

Versions: {retrievalStrategy=per-case-v1, prompt=agent-context-v1, toolPolicy=demo-policy-v1, workflowDefinition=session26-fixtures-v1, retrievalIndex=policies-v1/local-v1, modelRoutingPolicy=cost-quality-v1, dataset=golden-v1}

Measured aggregates (not billing): {costMicrosAverage=333.3333333333333, costMicrosMeasuredCases=9.0, costMicrosTotal=3000.0, fallbacksAverage=0.1111111111111111, fallbacksMeasuredCases=9.0, fallbacksTotal=1.0, iterationsAverage=1.5714285714285714, iterationsMeasuredCases=7.0, iterationsTotal=11.0, modelCallsAverage=1.4444444444444444, modelCallsMeasuredCases=9.0, modelCallsTotal=13.0, providerAttemptsAverage=1.7777777777777777, providerAttemptsMeasuredCases=9.0, providerAttemptsTotal=16.0, recallAtKAverage=1.0, recallAtKMeasuredCases=2.0, recallAtKTotal=2.0, reciprocalRankAverage=1.0, reciprocalRankMeasuredCases=2.0, reciprocalRankTotal=2.0, toolCallsAverage=1.1428571428571428, toolCallsMeasuredCases=7.0, toolCallsTotal=8.0}
