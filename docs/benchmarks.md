# Benchmark plan and harness

No throughput/latency numbers are claimed. Run correctness tests first. `BenchmarkIntegrationTest` is opt-in with `-Ddurableflow.benchmark=true` and uses a fresh Testcontainers PostgreSQL 17.6 database. It calls actual workflow/queue/timer/signal services. It never connects to a production database or manufactures latency samples.

```powershell
mvn -B '-Dtest=BenchmarkIntegrationTest' '-Ddurableflow.benchmark=true' '-Dbench.scenario=queue' '-Dbench.workflows=20' '-Dbench.workers=1' test
.\scripts\run-benchmarks.ps1 -Scenario queue -Workflows 1000 -Scaling
.\scripts\run-benchmarks.ps1 -Scenario dag -Workflows 100 -Workers 4 -Fanout 32
.\scripts\run-benchmarks.ps1 -Scenario timer -Workflows 100
.\scripts\run-benchmarks.ps1 -Scenario signal -Workflows 100
.\scripts\run-benchmarks.ps1 -Scenario start -Workflows 1000 -Concurrency 8
```

Use a trusted local shell under its normal execution policy; if scripts are disabled, use the equivalent Maven command. Do not silently bypass policy. Parameters/system properties: scenario `start|queue|dag|timer|signal`, workflows 1..10,000 (default 20), workers 1..32 (default 1), start concurrency 1..32 (default 4), fanout 1..64 (default 8), deadlineSeconds 1..600 (default 60). Scaling runs 1, 4 and 16 worker threads, each with an isolated container. Duration is a completion deadline, not an open-loop fixed-rate load generator.

| Mode | Workload | Observations |
| --- | --- | --- |
| start | Concurrent starts of a pinned one-step workflow, no advancement | Start throughput and p50/p95/p99 service latency |
| queue | Independent one-step runs; worker threads claim, complete, advance | Claim/complete throughput, separate successful/empty claim latency, completion/advance latency and queue wait |
| dag | Root -> N static branches -> join, repeated for each workflow | Same queue metrics; advancement includes fan-out/join scan/materialization cost |
| timer | 25ms durable timer -> one activity | Deadline-to-fire lag, task processing and queue wait; harness polls at 1ms when idle |
| signal | Public receipt on waiting SIGNAL -> one activity | Receipt call-to-downstream-commit latency and DB receipt-to-READY timestamp difference |

This is a **service/SQL protocol benchmark**, not HTTP endpoint capacity or full worker-fleet measurement. Manual no-op handler completion deliberately excludes real providers, TaskWorker executor, heartbeat, network and HTTP costs. Worker count means concurrent queue consumer threads in one JVM. Multi-process validation is the next benchmark stage, not a result established by this harness. Production poll interval is different from the tight timer benchmark loop. Start and processing phases are separate; processing throughput explicitly includes initial advancement/timer/signal setup, consumer startup and downstream advancement. Queue wait is `claimed_at - available_at` for completed first-attempt tasks in the fresh fixture; no timestamp is artificially backdated. Claim ownership identities are tracked and repeats count as errors. Database correctness remains primary.

Reports are written only after execution to `target/benchmark-<scenario>-w<workers>.json`, including harness/JDK/OS/PostgreSQL versions, requested/started/completed workload, worker/concurrency/deadline parameters, errors, validity, throughput and nearest-rank p50/p95/p99 milliseconds. Empty sample sets are `NO_OBSERVATIONS`, not zero latency. Partial processing/deadline exhaustion is invalid; setup/container/uncaught failures may produce only Surefire diagnostics and no new report. Never treat an old target file as this invocation's result. A sample of 20 is a smoke check, not a statistically useful p99.

For credible comparisons: record Git commit, hardware/CPU/memory, Docker resource caps, DB/JVM settings, pool size, workload shape/input size, telemetry/poller settings and competing load. Warm up, repeat runs, retain raw reports and uncertainty, test 1/4/16 workers with identical workloads, and inspect tail latency plus error/duplicate counts. Increase concurrency until throughput plateaus or latency/SLO fails. Do not extrapolate a local no-op result to model or payment capacity. HTTP/load-balancer and multi-machine tests, sustained arrival rates, real providers and saturation/soak tests are deferred.

PostgreSQL/manual observations: run the exact source queries in [SQL inspection](sql-inspection.md), capture EXPLAIN (ANALYZE, BUFFERS), pg_stat_activity wait events, locks, transaction age, pg_stat_database commits/rollbacks/deadlocks/temp files, connection utilization, host CPU/IO and table/index/vacuum growth. `pg_stat_statements` requires environment-specific setup and is not assumed installed. More connections/indexes are not automatically better.

Results status: **NOT RUN in this workspace**. All runtime measurements await JDK/Maven/Docker availability. Do not copy the teaching session's illustrative scaling numbers into reports or resume bullets.
