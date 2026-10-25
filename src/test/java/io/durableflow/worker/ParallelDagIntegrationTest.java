package io.durableflow.worker;

import io.durableflow.DurableFlowApplication;
import io.durableflow.worker.retry.*;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.compensation.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.history.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.signal.*;
import io.durableflow.workflow.task.*;
import io.durableflow.workflow.timer.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.util.*;
import java.util.concurrent.*;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
@SpringBootTest(properties = "durableflow.timers.polling-enabled=false")
class ParallelDagIntegrationTest {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.6");
    @DynamicPropertySource static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
    @Autowired WorkflowDefinitionService definitions;
    @Autowired WorkflowRunService runs;
    @Autowired WorkflowTaskOrchestrator orchestrator;
    @Autowired ActivityExecutionRepository activities;
    @Autowired CompensationExecutionRepository compensations;
    @Autowired ActivityTaskRepository tasks;
    @Autowired TaskTransactions transactions;
    @Autowired TaskWorker demoWorker;
    @Autowired WorkflowHistoryService history;
    @Autowired SignalRepository signals;
    @Autowired SignalReceiver receiver;
    @Autowired WorkflowTimerRepository timers;
    @Autowired TimerTransactions dispatcher;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach void clean() {
        jdbc.execute("TRUNCATE compensation_execution, workflow_signal_delivery, workflow_signal_wait, workflow_timer, workflow_event, activity_task, activity_execution, workflow_run, workflow_definition");
    }

    @Test
    void oneAdvanceFansOutAllBranchesAndJoinWaitsForFinalDependency() {
        UUID run = fanOut();
        assertThat(readyNodes(run)).containsExactlyInAnyOrder("B", "C", "D");
        var branches = claimBranches(3);
        transactions.complete(branches.get("B"), Map.of("branch", "B")); orchestrator.advanceRun(run);
        transactions.complete(branches.get("C"), Map.of("branch", "C")); orchestrator.advanceRun(run);
        assertThat(task(run, "E")).isEmpty();
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        transactions.complete(branches.get("D"), Map.of("branch", "D")); orchestrator.advanceRun(run);
        orchestrator.advanceRun(run); orchestrator.advanceRun(run);
        assertThat(readyNodes(run)).containsExactly("E");
        assertThat(scheduled(run, "E")).isEqualTo(1);
        transactions.complete(transactions.claim("join").orElseThrow(), Map.of("joined", true)); orchestrator.advanceRun(run);
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        assertThat(runs.get(run).activities()).filteredOn(a -> a.activityId().equals("B"))
                .allMatch(a -> a.output().get("branch").equals("B"));
    }

    @Test
    void simultaneousBranchCompletionsAndAdvanceProduceOneJoinAndContiguousHistory() throws Exception {
        UUID run = fanOut(); var branches = claimBranches(3);
        race(branches.values().stream().<Callable<Void>>map(claim -> () -> {
            transactions.complete(claim, Map.of("done", true));
            for (int i = 0; i < 3; i++) orchestrator.advanceRun(run);
            return null;
        }).toList());
        assertThat(readyNodes(run)).containsExactly("E");
        assertThat(scheduled(run, "E")).isEqualTo(1);
        assertSequence(run);
    }

    @Test
    void independentWorkersClaimDistinctBranchesAndRunHandlersAtTheSameTime() throws Exception {
        UUID run = fanOut();
        var entered = new CountDownLatch(2); var release = new CountDownLatch(1);
        ActivityHandler handler = new ActivityHandler() {
            public String type() { return "branch"; }
            public Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input) throws Exception {
                entered.countDown(); assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
                return Map.of("execution", context.activityExecutionId().toString());
            }
        };
        var pool = Executors.newFixedThreadPool(2);
        try (var a = new Harness("worker-A", handler); var b = new Harness("worker-B", handler)) {
            var first = pool.submit(a.worker::runOne); var second = pool.submit(b.worker::runOne);
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue(); // Both external handlers entered before either may finish.
            var claimed = tasks.findByRunId(run).stream().filter(t -> t.getStatus() == ActivityTaskStatus.CLAIMED).toList();
            assertThat(claimed).hasSize(2);
            assertThat(claimed).extracting(ActivityTaskEntity::getActivityExecutionId).doesNotHaveDuplicates();
            assertThat(claimed).extracting(ActivityTaskEntity::getWorkerId).containsExactlyInAnyOrder("worker-A", "worker-B");
            release.countDown(); assertThat(first.get(10, TimeUnit.SECONDS)).isTrue(); assertThat(second.get(10, TimeUnit.SECONDS)).isTrue();
            assertThat(task(run, "E")).isEmpty(); // Third branch still READY.
        } finally { release.countDown(); pool.shutdownNow(); }
    }

    @Test
    void mixedFanOutAndFanInMaterializeExactlyOnceUnderConcurrentAdvancement() throws Exception {
        UUID run = start(List.of(step("E", "B", "T", "S"), signal("S", "A"), timer("T", "A"), step("B", "A"), step("A")));
        orchestrator.advanceRun(run); completeNext(run);
        race(List.of(() -> { orchestrator.advanceRun(run); return null; }, () -> { orchestrator.advanceRun(run); return null; },
                () -> { orchestrator.advanceRun(run); return null; }));
        assertThat(readyNodes(run)).containsExactly("B");
        assertThat(timers.findByRun(run)).hasSize(1); assertThat(signals.waits(run)).hasSize(1);
        assertThat(events(run, WorkflowEventType.TIMER_SCHEDULED)).isEqualTo(1);
        assertThat(events(run, WorkflowEventType.SIGNAL_WAIT_STARTED)).isEqualTo(1);
        completeNext(run); assertThat(task(run, "E")).isEmpty();
        receiver.receive(run, new ReceiveSignalRequest("approval", "S", Map.of("approved", true)));
        assertThat(task(run, "E")).isEmpty(); assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        jdbc.update("UPDATE workflow_timer SET wake_at=clock_timestamp()-INTERVAL '1 second' WHERE workflow_run_id=?", run);
        assertThat(dispatcher.fireOne()).isTrue();
        orchestrator.advanceRun(run);
        assertThat(readyNodes(run)).containsExactly("E"); assertThat(scheduled(run, "E")).isEqualTo(1);
        completeNext(run); assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
    }

    @Test
    void bufferedSignalInReverseListedDagUnlocksAllEligibleBranchesWithinOneAdvance() {
        UUID run = start(List.of(step("E", "B", "C"), step("C", "S"), step("B", "S"), signal("S", "A"), step("A")));
        receiver.receive(run, new ReceiveSignalRequest("early", "S", Map.of("approved", true)));
        completeNext(run);
        assertThat(readyNodes(run)).containsExactlyInAnyOrder("B", "C");
        orchestrator.advanceRun(run);
        assertThat(scheduled(run, "B")).isEqualTo(1); assertThat(scheduled(run, "C")).isEqualTo(1);
        assertThat(events(run, WorkflowEventType.SIGNAL_RECEIVED)).isEqualTo(1);
    }

    @Test
    void partiallyCompletedJoinRecoversWithFreshServiceContextsAndNoMemoryBarrier() {
        UUID run = fanOut(); var branches = claimBranches(3);
        transactions.complete(branches.get("B"), Map.of("persisted", true)); orchestrator.advanceRun(run);
        transactions.complete(branches.get("C"), Map.of("persisted", true)); orchestrator.advanceRun(run);
        expire(branches.get("D").taskId()); // The restarted worker must recover the lost process's lease.
        try (var fresh = freshContext()) {
            var tx = fresh.getBean(TaskTransactions.class);
            assertThat(fresh.getBean(WorkflowRunService.class).get(run).status()).isEqualTo(WorkflowRunStatus.RUNNING);
            var recovered = tx.claim("restart-branch").orElseThrow();
            assertThat(recovered.activityExecutionId()).isEqualTo(branches.get("D").activityExecutionId());
            tx.complete(recovered, Map.of("afterRestart", true));
            // Simulate another lost completion callback; no advancement before this context closes.
        }
        try (var fresh = freshContext()) {
            var advance = fresh.getBean(WorkflowTaskOrchestrator.class);
            advance.advanceRun(run); advance.advanceRun(run);
            assertThat(scheduled(run, "E")).isEqualTo(1);
            var tx = fresh.getBean(TaskTransactions.class);
            tx.complete(tx.claim("restart-join").orElseThrow(), Map.of("joined", true)); advance.advanceRun(run);
        }
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
    }

    @Test
    void delayedRetryDoesNotBlockUnrelatedBranchesOrPrematurelyCompleteRun() {
        UUID run = fanOut(); var retry = transactions.claim("retrying").orElseThrow();
        transactions.handleFailure(retry, new RetryableActivityException("temporary"));
        jdbc.update("UPDATE activity_task SET available_at=clock_timestamp()+INTERVAL '1 hour' WHERE id=?", retry.taskId());
        completeNext(run); completeNext(run);
        assertThat(task(run, "E")).isEmpty(); assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        due(retry.taskId()); var next = transactions.claim("retry-2").orElseThrow();
        assertThat(next.activityExecutionId()).isEqualTo(retry.activityExecutionId());
        transactions.complete(next, Map.of("recovered", true)); orchestrator.advanceRun(run);
        assertThat(scheduled(run, "E")).isEqualTo(1);
    }

    @Test
    void lateSiblingCompletionIsIncludedBeforeAnyAncestorCompensation() {
        UUID run = sagaFanOut();
        var b = transactions.claim("first").orElseThrow(); var c = transactions.claim("sibling").orElseThrow();
        transactions.fail(b, "permanent branch failure");
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATING);
        assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(run)).isEmpty();
        orchestrator.advanceRun(run);
        assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(run)).isEmpty();
        assertThat(task(run, "E")).isEmpty();
        assertThat(transactions.claim("no-pending-D")).isEmpty(); // D is READY/PENDING and must never start now.
        transactions.complete(c, Map.of("lateExternalEffect", true)); // Valid lease, same transaction schedules undo.
        var undoC = transactions.claim("undo-late").orElseThrow();
        assertThat(undoC.isCompensation()).isTrue(); assertThat(undoC.activityExecutionId()).isEqualTo(c.activityExecutionId());
        assertThat(undoC.context().originalOutput()).containsEntry("lateExternalEffect", true);
        transactions.complete(undoC, Map.of("undone", true));
        var undoA = transactions.claim("undo-root").orElseThrow(); assertThat(node(undoA)).isEqualTo("A");
        transactions.complete(undoA, Map.of("undone", true));
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATED);
        assertThat(transactions.claim("never-forward")).isEmpty();
        assertThat(scheduled(run, "E")).isZero(); assertThat(readyNodes(run)).hasSize(1);
    }

    @Test
    void lateSiblingLeaseExpiryRecoversAndStaleOutcomeCannotChangeSagaPlan() {
        UUID run = sagaFanOut(); var failed = transactions.claim("fail").orElseThrow(); var old = transactions.claim("old").orElseThrow();
        transactions.fail(failed, "permanent"); expire(old.taskId());
        assertThatThrownBy(() -> transactions.complete(old, Map.of("stale", true))).isInstanceOf(OwnershipLostException.class);
        var recovered = transactions.claim("reclaim-started-sibling").orElseThrow();
        assertThat(recovered.isCompensation()).isFalse(); assertThat(recovered.activityExecutionId()).isEqualTo(old.activityExecutionId());
        assertThat(recovered.context().idempotencyKey()).isEqualTo(old.context().idempotencyKey());
        transactions.complete(recovered, Map.of("reconciled", true));
        assertThat(transactions.claim("undo").orElseThrow().activityExecutionId()).isEqualTo(old.activityExecutionId());
    }

    @Test
    void settlingSiblingRetainsBoundedTimeoutRetriesWhileNewPendingWorkCannotStart() {
        UUID run = sagaFanOut(); var b = transactions.claim("fail").orElseThrow(); var c = transactions.claim("timeout").orElseThrow();
        transactions.fail(b, "terminal");
        transactions.handleFailure(c, new ActivityTimeoutException(100));
        jdbc.update("UPDATE activity_task SET available_at=clock_timestamp()+INTERVAL '1 hour' WHERE id=?", c.taskId());
        assertThat(transactions.claim("no-fresh-D")).isEmpty();
        due(c.taskId()); var retry = transactions.claim("retry-started-C").orElseThrow();
        assertThat(retry.activityExecutionId()).isEqualTo(c.activityExecutionId());
        transactions.handleFailure(retry, new RetryableActivityException("still unavailable")); due(c.taskId());
        var last = transactions.claim("last-C").orElseThrow();
        transactions.handleFailure(last, new RetryableActivityException("exhausted"));
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATING);
        var undo = transactions.claim("undo-A").orElseThrow(); assertThat(node(undo)).isEqualTo("A");
        transactions.complete(undo, Map.of("ok", true));
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATED);
    }

    @Test
    void crashedFinalSettlingAttemptTerminatesWithoutFourthInvocationAndUnblocksUndo() {
        UUID run = sagaFanOut(); var b = transactions.claim("fail").orElseThrow(); var c = transactions.claim("crash-1").orElseThrow();
        transactions.fail(b, "terminal"); expire(c.taskId());
        for (int attempt = 2; attempt <= 3; attempt++) { var next = transactions.claim("crash-" + attempt).orElseThrow(); expire(next.taskId()); }
        assertThat(transactions.claim("budget-recovery")).isEmpty();
        var undo = transactions.claim("undo-A").orElseThrow(); assertThat(node(undo)).isEqualTo("A");
        assertThat(activities.findById(c.activityExecutionId()).orElseThrow().getAttemptCount()).isEqualTo(3);
    }

    @Test
    void lateEffectWithoutEarlierCompensatableSuccessIsStillCompensated() {
        UUID run = start(List.of(step("A"), step("B", "A"), undoable("C", "A")));
        orchestrator.advanceRun(run); completeNext(run);
        var first = transactions.claim("one").orElseThrow(); var second = transactions.claim("two").orElseThrow();
        var b = node(first).equals("B") ? first : second; var c = b == first ? second : first;
        transactions.fail(b, "failed");
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATING);
        transactions.complete(c, Map.of("effect", true));
        var undo = transactions.claim("undo-C").orElseThrow(); assertThat(node(undo)).isEqualTo("C");
        transactions.complete(undo, Map.of("ok", true));
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATED);
    }

    @Test
    void settlingWithNoCompletedCompensatableEffectEndsFailedRatherThanFakeCompensated() {
        UUID run = start(List.of(step("A"), step("B", "A"), undoable("C", "A")));
        orchestrator.advanceRun(run); completeNext(run);
        var first = transactions.claim("one").orElseThrow(); var second = transactions.claim("two").orElseThrow();
        transactions.fail(first, "failed"); transactions.fail(second, "also failed");
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.FAILED);
        assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(run)).isEmpty();
        assertThat(events(run, WorkflowEventType.WORKFLOW_COMPENSATED)).isZero();
    }

    @Test
    void reverseTopologicalCompensationIsDeterministicAndAncestorFollowsAllCompletedDescendants() {
        UUID run = start(List.of(step("F", "D", "E"), undoable("E", "C"), undoable("D", "B"),
                undoable("C", "A"), undoable("B", "A"), undoable("A")));
        orchestrator.advanceRun(run); completeNext(run);
        var branches = claimBranches(2);
        // Intentionally complete C before B: timestamps must not determine compensation order.
        transactions.complete(branches.get("C"), Map.of("done", true));
        transactions.complete(branches.get("B"), Map.of("done", true)); orchestrator.advanceRun(run);
        var leaves = claimBranches(2);
        transactions.complete(leaves.get("E"), Map.of("done", true));
        transactions.complete(leaves.get("D"), Map.of("done", true)); orchestrator.advanceRun(run);
        transactions.fail(transactions.claim("F-fail").orElseThrow(), "terminal");
        var order = new ArrayList<String>();
        while (runs.get(run).status() == WorkflowRunStatus.COMPENSATING) {
            var undo = transactions.claim("undo-" + order.size()).orElseThrow();
            assertThat(undo.isCompensation()).isTrue(); order.add(node(undo));
            assertThat(transactions.claim("no-parallel-undo")).isEmpty();
            transactions.complete(undo, Map.of("ok", true));
        }
        assertThat(order).containsExactly("E", "D", "C", "B", "A");
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATED); assertSequence(run);
    }

    @Test
    void compensationOrderPreservesTransitivePathThroughNonCompensatableActivityAndSignal() {
        UUID run = start(List.of(step("failure", "A"), undoable("A", "gate"), signal("gate", "middle"),
                step("middle", "Z"), undoable("Z")));
        orchestrator.advanceRun(run); completeNext(run); completeNext(run);
        receiver.receive(run, new ReceiveSignalRequest("gate", "gate", Map.of())); completeNext(run);
        transactions.fail(transactions.claim("fail").orElseThrow(), "terminal");
        var first = transactions.claim("undo-descendant").orElseThrow(); assertThat(node(first)).isEqualTo("A");
        transactions.complete(first, Map.of("ok", true));
        assertThat(node(transactions.claim("undo-ancestor").orElseThrow())).isEqualTo("Z");
    }

    @Test
    void concurrentLateCompletionAndAdvanceCannotFinalizeEarlyOrDuplicateUndo() throws Exception {
        UUID run = sagaFanOut(); var b = transactions.claim("fail").orElseThrow(); var c = transactions.claim("late").orElseThrow();
        transactions.fail(b, "terminal");
        race(List.of(() -> { transactions.complete(c, Map.of("late", true)); return null; },
                () -> { for (int i = 0; i < 5; i++) orchestrator.advanceRun(run); return null; }));
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATING);
        assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(run)).hasSize(1)
                .allMatch(e -> e.getOriginalActivityExecutionId().equals(c.activityExecutionId()));
        assertThat(events(run, WorkflowEventType.COMPENSATION_SCHEDULED)).isEqualTo(1); assertSequence(run);
    }

    @Test
    void demoHandlersCompleteFanInThenShipmentFailureUnwindsInventoryBeforePayment() {
        String name = "Parallel-demo-" + UUID.randomUUID();
        definitions.register(new RegisterWorkflowDefinitionRequest(name, 1, List.of(
                new StepDefinition("payment", "chargePayment", List.of(), NodeType.ACTIVITY, null, "refundPayment"),
                new StepDefinition("fraud", "checkFraud", List.of("payment")),
                new StepDefinition("inventory", "reserveInventory", List.of("payment"), NodeType.ACTIVITY, null, "releaseInventory"),
                new StepDefinition("risk", "scoreRisk", List.of("payment")),
                new StepDefinition("finalize", "finalizeRequest", List.of("fraud", "inventory", "risk")),
                new StepDefinition("shipment", "createShipment", List.of("finalize")))));
        UUID run = runs.start(new StartWorkflowRunRequest(name, 1, Map.of("orderId", "demo", "simulateShipmentFailure", true))).runId();
        orchestrator.advanceRun(run);
        assertThat(demoWorker.runOne()).isTrue();
        assertThat(readyNodes(run)).containsExactlyInAnyOrder("fraud", "inventory", "risk");
        assertThat(task(run, "finalize")).isEmpty();
        for (int i = 0; i < 5; i++) assertThat(demoWorker.runOne()).isTrue();
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATING);
        assertThat(demoWorker.runOne()).isTrue(); assertThat(demoWorker.runOne()).isTrue();
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATED);
        assertThat(history.read(run).stream().filter(e -> e.eventType() == WorkflowEventType.COMPENSATION_COMPLETED))
                .extracting(WorkflowEventResponse::activityId).containsExactly("inventory", "payment");
    }

    private UUID fanOut() {
        UUID run = start(List.of(step("E", "B", "C", "D"), step("D", "A"), step("C", "A"), step("B", "A"), step("A")));
        orchestrator.advanceRun(run); completeNext(run); return run;
    }
    private UUID sagaFanOut() {
        UUID run = start(List.of(undoable("A"), undoable("B", "A"), undoable("C", "A"), step("D", "A"), step("E", "B", "C", "D")));
        orchestrator.advanceRun(run); completeNext(run);
        // Distinct durable queue deadlines make B/C selection deterministic even on coarse clocks.
        for (int i = 0; i < 3; i++) {
            var queued = task(run, List.of("B", "C", "D").get(i)).orElseThrow();
            jdbc.update("UPDATE activity_task SET available_at=clock_timestamp()-CAST(? AS double precision)*INTERVAL '1 second' WHERE id=?", 3 - i, queued.getId());
        }
        return run;
    }
    private UUID start(List<StepDefinition> steps) {
        String name = "DAG-" + UUID.randomUUID();
        definitions.register(new RegisterWorkflowDefinitionRequest(name, 1, steps));
        return runs.start(new StartWorkflowRunRequest(name, 1, Map.of("orderId", "dag-demo"))).runId();
    }
    private StepDefinition step(String id, String... parents) { return new StepDefinition(id, "branch", List.of(parents)); }
    private StepDefinition undoable(String id, String... parents) { return new StepDefinition(id, "branch", List.of(parents), NodeType.ACTIVITY, null, "undo"); }
    private StepDefinition signal(String id, String... parents) { return new StepDefinition(id, null, List.of(parents), NodeType.SIGNAL, null); }
    private StepDefinition timer(String id, String... parents) { return new StepDefinition(id, null, List.of(parents), NodeType.TIMER, 3_600_000L); }
    private void completeNext(UUID run) { transactions.complete(transactions.claim("forward").orElseThrow(), Map.of("done", true)); orchestrator.advanceRun(run); }
    private String node(ClaimedTask claim) { return activities.findById(claim.activityExecutionId()).orElseThrow().getActivityId(); }
    private Map<String, ClaimedTask> claimBranches(int count) {
        var claims = new HashMap<String, ClaimedTask>();
        for (int i = 0; i < count; i++) { var claim = transactions.claim("branch-" + i).orElseThrow(); claims.put(node(claim), claim); }
        assertThat(claims).hasSize(count); return claims;
    }
    private List<String> readyNodes(UUID run) {
        return tasks.findByRunId(run).stream().filter(t -> !t.isCompensation() && t.getStatus() == ActivityTaskStatus.READY)
                .map(t -> activities.findById(t.getActivityExecutionId()).orElseThrow().getActivityId()).sorted().toList();
    }
    private Optional<ActivityTaskEntity> task(UUID run, String node) {
        return activities.findByWorkflowRunIdOrderByActivityIdAsc(run).stream().filter(a -> a.getActivityId().equals(node)).findFirst()
                .flatMap(a -> tasks.findByActivityExecutionId(a.getId()));
    }
    private long scheduled(UUID run, String node) { return history.read(run).stream().filter(e -> e.eventType() == WorkflowEventType.ACTIVITY_SCHEDULED && node.equals(e.activityId())).count(); }
    private long events(UUID run, WorkflowEventType type) { return history.read(run).stream().filter(e -> e.eventType() == type).count(); }
    private void assertSequence(UUID run) { var events = history.read(run); for (int i = 0; i < events.size(); i++) assertThat(events.get(i).sequence()).isEqualTo(i + 1L); }
    private void due(UUID task) { jdbc.update("UPDATE activity_task SET available_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?", task); }
    private void expire(UUID task) { jdbc.update("UPDATE activity_task SET lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?", task); }
    private void race(List<Callable<Void>> actions) throws Exception {
        var gate = new CountDownLatch(1); var pool = Executors.newFixedThreadPool(actions.size());
        try {
            var futures = new ArrayList<Future<Void>>();
            for (var action : actions) futures.add(pool.submit(() -> { assertThat(gate.await(10, TimeUnit.SECONDS)).isTrue(); return action.call(); }));
            gate.countDown(); for (var future : futures) future.get(10, TimeUnit.SECONDS);
        } finally { gate.countDown(); pool.shutdownNow(); }
    }
    private ConfigurableApplicationContext freshContext() {
        return new SpringApplicationBuilder(DurableFlowApplication.class).web(WebApplicationType.NONE).run(
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(), "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(), "--durableflow.timers.polling-enabled=false");
    }
    private class Harness implements AutoCloseable {
        final TaskWorker worker;
        Harness(String id, ActivityHandler handler) {
            var settings = new WorkerExecutionSettings(60000, 1000);
            worker = new TaskWorker(transactions, new ActivityRegistry(List.of(handler)), orchestrator, new WorkerIdentity(id),
                    new WorkerHeartbeats(transactions, new WorkerLeaseSettings(30000, 10000)), new HandlerExecutions(settings), settings);
        }
        public void close() { worker.stop(); }
    }
}
