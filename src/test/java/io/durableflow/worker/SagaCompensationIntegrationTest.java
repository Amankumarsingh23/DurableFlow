package io.durableflow.worker;

import io.durableflow.api.ApiException;
import io.durableflow.worker.retry.*;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.activity.demo.*;
import io.durableflow.workflow.compensation.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.engine.WorkflowEngineState;
import io.durableflow.workflow.history.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.task.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.*;
import static org.assertj.core.api.Assertions.*;

// Close pollers/pools before this class's disposable PostgreSQL container stops.
@org.springframework.test.annotation.DirtiesContext(classMode = org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@SpringBootTest(properties = "durableflow.timers.polling-enabled=false")
class SagaCompensationIntegrationTest {
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
    @Autowired TaskWorker worker;
    @Autowired WorkflowHistoryService history;
    @Autowired WorkflowEngineState local;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach void clean() {
        jdbc.execute("TRUNCATE model_route_attempt, model_route, agent_iteration, agent_execution, fake_tool_effect, tool_request, workflow_schedule_occurrence, workflow_schedule, child_workflow_execution, compensation_execution, workflow_signal_delivery, workflow_signal_wait, workflow_timer, workflow_event, activity_task, activity_execution, workflow_run, workflow_definition");
    }

    @Test
    void reverseDependencyOrderIsSequentialAndCompletionSchedulesNextInSameCommit() {
        UUID run = failedSaga();
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATING);
        assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(run)).hasSize(1);
        var b = transactions.claim("B-comp").orElseThrow();
        assertThat(b.activityType()).isEqualTo("releaseInventory");
        assertThat(b.context().originalOutput()).containsEntry("reservationId", "res-B");
        assertThat(b.context().originalInput()).containsEntry("orderId", "saga-demo");
        assertThat(transactions.claim("cannot-start-A")).isEmpty();
        transactions.complete(b, Map.of("released", true));
        var a = transactions.claim("A-comp").orElseThrow();
        assertThat(a.activityType()).isEqualTo("refundPayment");
        assertThat(a.context().originalOutput()).containsEntry("paymentId", "pay-A");
        transactions.complete(a, Map.of("refunded", true));
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATED);
        assertThat(runs.get(run).compensations()).hasSize(2).allMatch(c -> c.status() == ActivityExecutionStatus.COMPLETED);
        assertThat(activities.findByWorkflowRunIdOrderByActivityIdAsc(run))
                .filteredOn(e -> e.getActivityId().equals("C")).allMatch(e -> e.getStatus() == ActivityExecutionStatus.FAILED);
        orchestrator.advanceRun(run);
        assertThat(transactions.claim("no-forward-resume")).isEmpty();
        var sagaEvents = history.read(run).stream().filter(e -> e.eventType().name().contains("COMPENSAT")).toList();
        assertThat(sagaEvents).extracting(WorkflowEventResponse::eventType).containsExactly(
                WorkflowEventType.WORKFLOW_COMPENSATING, WorkflowEventType.COMPENSATION_SCHEDULED,
                WorkflowEventType.COMPENSATION_COMPLETED, WorkflowEventType.COMPENSATION_SCHEDULED,
                WorkflowEventType.COMPENSATION_COMPLETED, WorkflowEventType.WORKFLOW_COMPENSATED);
        assertThat(sagaEvents.get(1).activityId()).isEqualTo("B");
        assertThat(sagaEvents.get(3).activityId()).isEqualTo("A");
        assertSequence(run);
    }

    @Test
    void onlyDurablyCompletedForwardEffectsEnterPlanAndRetryDoesNotStartCompensation() {
        UUID run = startSaga(); orchestrator.advanceRun(run);
        var a = transactions.claim("A").orElseThrow();
        orchestrator.advanceRun(run);
        assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(run)).isEmpty();
        transactions.handleFailure(a, new RetryableActivityException("temporary"));
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(run)).isEmpty();
        due(a.taskId());
        transactions.fail(transactions.claim("retry-A").orElseThrow(), "permanent");
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.FAILED);
        assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(run)).isEmpty();
    }

    @Test
    void refundEffectBeforeCompletionCrashReclaimsWithDistinctStableKeyAndFencesOldWorker() {
        UUID run = failedSaga();
        transactions.complete(transactions.claim("release").orElseThrow(), Map.of("released", true));
        var provider = new FakePaymentProvider();
        var refund = new RefundPaymentActivity(provider);
        var old = transactions.claim("crashed-refund").orElseThrow();
        String forwardKey = new ActivityExecutionContext(run, old.activityExecutionId()).idempotencyKey();
        assertThat(old.context().idempotencyKey()).isEqualTo("df:v1:compensate:" + old.activityExecutionId()).isNotEqualTo(forwardKey);
        var effect = refund.execute(old.context(), old.input()); // External effect committed, queue outcome absent.
        assertThat(transactions.heartbeat(old)).isTrue();
        expire(old.taskId());
        var replacement = transactions.claim("recovery").orElseThrow();
        assertThat(replacement.context().idempotencyKey()).isEqualTo(old.context().idempotencyKey());
        assertThat(replacement.leaseGeneration()).isEqualTo(old.leaseGeneration() + 1);
        assertThat(refund.execute(replacement.context(), replacement.input())).isEqualTo(effect);
        assertThat(provider.refundEffectCount()).isEqualTo(1);
        assertThat(transactions.heartbeat(old)).isFalse();
        assertThatThrownBy(() -> transactions.complete(old, effect)).isInstanceOf(OwnershipLostException.class);
        assertThatThrownBy(() -> transactions.handleFailure(old, new IllegalArgumentException("late")))
                .isInstanceOf(OwnershipLostException.class);
        transactions.complete(replacement, effect);
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATED);
    }

    @Test
    void compensationRetryAndTimeoutUseDurableBackoffAndPreserveKey() {
        UUID run = failedSaga();
        var first = transactions.claim("first").orElseThrow();
        transactions.handleFailure(first, new ActivityTimeoutException(100));
        var task = tasks.findById(first.taskId()).orElseThrow();
        assertThat(task.getStatus()).isEqualTo(ActivityTaskStatus.READY);
        assertThat(task.getLeaseExpiresAt()).isNull();
        assertThat(task.getLeaseGeneration()).isEqualTo(1);
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATING);
        var retryEvent = history.read(run).stream().filter(e -> e.eventType() == WorkflowEventType.RETRY_SCHEDULED).findFirst().orElseThrow();
        assertThat(((Number) retryEvent.metadata().get("delayMillis")).longValue()).isBetween(0L, 1000L);
        assertThat(retryEvent.metadata()).containsEntry("taskKind", "COMPENSATION").containsEntry("code", "ACTIVITY_TIMEOUT");
        assertThat(OffsetDateTime.parse(retryEvent.metadata().get("availableAt").toString()).toInstant()).isEqualTo(task.getAvailableAt());
        due(task.getId());
        var second = transactions.claim("second").orElseThrow();
        assertThat(second.context().idempotencyKey()).isEqualTo(first.context().idempotencyKey());
        assertThat(compensations.findById(second.compensationExecutionId()).orElseThrow().getAttemptCount()).isEqualTo(2);
        transactions.complete(second, Map.of("ok", true));
        assertThat(history.read(run)).filteredOn(e -> e.eventType() == WorkflowEventType.ACTIVITY_TIMED_OUT).hasSize(1);
        assertSequence(run);
    }

    @Test
    void terminalCompensationFailureStopsEarlierCompensations() {
        UUID run = failedSaga();
        var b = transactions.claim("bad-release").orElseThrow();
        transactions.handleFailure(b, new IllegalArgumentException("permanent"));
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATION_FAILED);
        orchestrator.advanceRun(run);
        assertThat(transactions.claim("no-refund-after-failed-release")).isEmpty();
        assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(run)).hasSize(1)
                .allMatch(e -> e.getStatus() == ActivityExecutionStatus.FAILED);
        assertThat(history.read(run)).extracting(WorkflowEventResponse::eventType)
                .contains(WorkflowEventType.COMPENSATION_FAILED, WorkflowEventType.WORKFLOW_COMPENSATION_FAILED)
                .doesNotContain(WorkflowEventType.WORKFLOW_COMPENSATED);
    }

    @Test
    void exhaustedRetryBudgetTerminatesCompensation() {
        UUID run = failedSaga();
        for (int attempt = 1; attempt <= 3; attempt++) {
            var claim = transactions.claim("attempt-" + attempt).orElseThrow();
            transactions.handleFailure(claim, new RetryableActivityException("unavailable"));
            if (attempt < 3) due(claim.taskId());
        }
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATION_FAILED);
        var execution = compensations.findByWorkflowRunIdOrderByCreatedAtAsc(run).getFirst();
        assertThat(execution.getAttemptCount()).isEqualTo(3);
        assertThat(history.read(run)).filteredOn(e -> e.eventType() == WorkflowEventType.RETRY_SCHEDULED).hasSize(2);
    }

    @Test
    void crashedFinalCompensationAttemptDoesNotInvokeFourthHandler() {
        UUID run = failedSaga();
        for (int attempt = 1; attempt <= 3; attempt++) {
            var claim = transactions.claim("crash-" + attempt).orElseThrow();
            expire(claim.taskId());
        }
        assertThat(transactions.claim("budget-recovery")).isEmpty();
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATION_FAILED);
        assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(run).getFirst().getAttemptCount()).isEqualTo(3);
    }

    @Test
    void forwardRetryExhaustionStartsSagaRatherThanFailed() {
        UUID run = startSaga(); orchestrator.advanceRun(run);
        completeForward("A", Map.of("paymentId", "pay-A"), run);
        for (int attempt = 1; attempt <= 3; attempt++) {
            var claim = transactions.claim("B-retry-" + attempt).orElseThrow();
            transactions.handleFailure(claim, new RetryableActivityException("inventory unavailable"));
            if (attempt < 3) due(claim.taskId());
        }
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATING);
        assertThat(transactions.claim("undo-A").orElseThrow().activityType()).isEqualTo("refundPayment");
    }

    @Test
    void crashedFinalForwardAttemptStartsSagaWithoutAnotherForwardInvocation() {
        UUID run = startSaga(); orchestrator.advanceRun(run);
        completeForward("A", Map.of("paymentId", "pay-A"), run);
        for (int attempt = 1; attempt <= 3; attempt++) {
            var claim = transactions.claim("B-crash-" + attempt).orElseThrow();
            expire(claim.taskId());
        }
        assertThat(transactions.claim("budget-recovery")).isEmpty();
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATING);
        assertThat(transactions.claim("undo-payment").orElseThrow().activityType()).isEqualTo("refundPayment");
    }

    @Test
    void staleReadyForwardWorkCannotClaimOrMaterializeAfterCompensating() {
        UUID run = startSaga(); orchestrator.advanceRun(run);
        completeForward("A", Map.of("paymentId", "pay-A"), run);
        var b = transactions.claim("B-failure").orElseThrow();
        var c = activities.findByWorkflowRunIdOrderByActivityIdAsc(run).stream()
                .filter(e -> e.getActivityId().equals("C")).findFirst().orElseThrow();
        tasks.saveAndFlush(ActivityTaskEntity.ready(c.getId(), java.time.Instant.EPOCH)); // Deliberately stale pre-authorized row.
        transactions.fail(b, "permanent");
        var count = tasks.findByRunId(run).size();
        orchestrator.advanceRun(run); orchestrator.advanceRun(run);
        assertThat(tasks.findByRunId(run)).hasSize(count);
        var undo = transactions.claim("comp-only").orElseThrow();
        assertThat(undo.isCompensation()).isTrue();
        assertThat(transactions.claim("must-not-start-C")).isEmpty();
        assertThat(tasks.findByActivityExecutionId(c.getId()).orElseThrow().getStatus()).isEqualTo(ActivityTaskStatus.READY);
        transactions.complete(undo, Map.of("ok", true));
        assertThat(transactions.claim("still-no-C")).isEmpty();
    }

    @Test
    void concurrentAdvanceAndCompletionCannotDuplicateCompensations() throws Exception {
        UUID run = failedSaga();
        try (var pool = Executors.newFixedThreadPool(4)) {
            var gate = new CountDownLatch(1);
            var futures = new ArrayList<Future<?>>();
            for (int i = 0; i < 4; i++) futures.add(pool.submit(() -> {
                gate.await(); for (int j = 0; j < 4; j++) orchestrator.advanceRun(run); return null;
            }));
            gate.countDown();
            for (var future : futures) future.get(10, TimeUnit.SECONDS);
        }
        assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(run)).hasSize(1);
        var claim = transactions.claim("winner").orElseThrow();
        try (var pool = Executors.newFixedThreadPool(2)) {
            var gate = new CountDownLatch(1);
            var completion = pool.submit(() -> { gate.await(); transactions.complete(claim, Map.of("done", true)); return null; });
            var advancement = pool.submit(() -> { gate.await(); orchestrator.advanceRun(run); return null; });
            gate.countDown(); completion.get(10, TimeUnit.SECONDS); advancement.get(10, TimeUnit.SECONDS);
        }
        assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(run)).hasSize(2);
        assertThatThrownBy(() -> transactions.fail(claim, "duplicate failure")).isInstanceOf(OwnershipLostException.class);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM activity_task WHERE task_kind = 'COMPENSATION'", Long.class)).isEqualTo(2L);
        assertSequence(run);
    }

    @Test
    void realDemoWorkerRunsForwardAndReverseHandlersThroughOnePipeline() {
        UUID run = startSaga(); orchestrator.advanceRun(run);
        assertThat(worker.runOne()).isTrue(); // Charge.
        assertThat(worker.runOne()).isTrue(); // Reserve.
        assertThat(worker.runOne()).isTrue(); // Deliberately fail shipment.
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATING);
        assertThat(worker.runOne()).isTrue(); // Release.
        assertThat(worker.runOne()).isTrue(); // Refund.
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATED);
        assertThat(worker.runOne()).isFalse();
    }

    @Test
    void compensationTimeoutIsSupervisedByExistingHandlerExecutor() throws Exception {
        UUID run = failedSaga();
        var entered = new CountDownLatch(1);
        var released = new CountDownLatch(1);
        var settings = new WorkerExecutionSettings(60000, 0);
        var heartbeats = new WorkerHeartbeats(transactions, new WorkerLeaseSettings(30000, 10000));
        var executor = Executors.newSingleThreadExecutor(r -> { var t = new Thread(r); t.setDaemon(true); return t; });
        var executions = new HandlerExecutions(settings, executor, (future, millis) -> {
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue(); throw new TimeoutException();
        });
        var handler = new ActivityHandler() {
            public String type() { return "releaseInventory"; }
            public Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input) throws Exception {
                assertThat(context.compensation()).isTrue(); entered.countDown();
                released.await(); return Map.of("tooLate", true);
            }
        };
        var timed = new TaskWorker(transactions, new ActivityRegistry(List.of(handler)), orchestrator,
                new WorkerIdentity("timeout-comp"), heartbeats, executions, settings);
        try {
            assertThat(timed.runOne()).isTrue();
            var compensation = compensations.findByWorkflowRunIdOrderByCreatedAtAsc(run).getFirst();
            assertThat(tasks.findByCompensationExecutionId(compensation.getId()).orElseThrow().getStatus()).isEqualTo(ActivityTaskStatus.READY);
            assertThat(compensation.getErrorMessage()).contains("ActivityTimeoutException");
            assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATING);
        } finally { released.countDown(); timed.stop(); }
    }

    @Test
    void localExecutorRejectsSagaDefinitionsBeforeStartingAnyWork() {
        UUID run = startSaga();
        assertThatThrownBy(() -> local.next(run, true)).isInstanceOf(ApiException.class).hasMessageContaining("queued");
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        assertThat(tasks.findByRunId(run)).isEmpty();
    }

    @Test
    void historyFailureRollsBackForwardFailureAndEntireSagaTransition() {
        UUID run = startSaga(); orchestrator.advanceRun(run);
        completeForward("A", Map.of("paymentId", "pay-A"), run);
        completeForward("B", Map.of("reservationId", "res-B"), run);
        var c = transactions.claim("C").orElseThrow();
        long before = history.read(run).size();
        rejectHistory("WORKFLOW_COMPENSATING");
        try {
            assertThatThrownBy(() -> transactions.fail(c, "failed")).isInstanceOf(RuntimeException.class);
            assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.RUNNING);
            assertThat(activities.findById(c.activityExecutionId()).orElseThrow().getStatus()).isEqualTo(ActivityExecutionStatus.RUNNING);
            assertThat(tasks.findById(c.taskId()).orElseThrow().getStatus()).isEqualTo(ActivityTaskStatus.CLAIMED);
            assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(run)).isEmpty();
            assertThat(history.read(run)).hasSize((int) before);
        } finally { allowHistory(); }
        transactions.fail(c, "retry same outcome");
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPENSATING);
        assertSequence(run);
    }

    @Test
    void nextCompensationHistoryFailureRollsBackPreviousCompletion() {
        UUID run = failedSaga();
        var b = transactions.claim("B-comp").orElseThrow();
        int before = history.read(run).size();
        rejectHistory("COMPENSATION_SCHEDULED");
        try {
            assertThatThrownBy(() -> transactions.complete(b, Map.of("ok", true))).isInstanceOf(RuntimeException.class);
            assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(run)).hasSize(1)
                    .allMatch(e -> e.getStatus() == ActivityExecutionStatus.RUNNING);
            assertThat(tasks.findById(b.taskId()).orElseThrow().getStatus()).isEqualTo(ActivityTaskStatus.CLAIMED);
            assertThat(history.read(run)).hasSize(before);
        } finally { allowHistory(); }
        transactions.complete(b, Map.of("ok", true));
        assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(run)).hasSize(2);
        assertSequence(run);
    }

    private void rejectHistory(String eventType) {
        // Test-only fixed enum values, never user input.
        jdbc.execute("CREATE FUNCTION reject_saga_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_type = '"
                + eventType + "' THEN RAISE EXCEPTION 'injected history failure'; END IF; RETURN NEW; END $$");
        jdbc.execute("CREATE TRIGGER reject_saga_history BEFORE INSERT ON workflow_event FOR EACH ROW EXECUTE FUNCTION reject_saga_history()");
    }
    private void allowHistory() {
        jdbc.execute("DROP TRIGGER reject_saga_history ON workflow_event");
        jdbc.execute("DROP FUNCTION reject_saga_history()");
    }

    private UUID startSaga() {
        String name = "Saga-" + UUID.randomUUID();
        definitions.register(new RegisterWorkflowDefinitionRequest(name, 1, List.of(
                new StepDefinition("C", "createShipment", List.of("B")), // JSON order intentionally reversed.
                new StepDefinition("B", "reserveInventory", List.of("A"), NodeType.ACTIVITY, null, "releaseInventory"),
                new StepDefinition("A", "chargePayment", List.of(), NodeType.ACTIVITY, null, "refundPayment"))));
        return runs.start(new StartWorkflowRunRequest(name, 1, Map.of("orderId", "saga-demo", "simulateShipmentFailure", true))).runId();
    }
    private UUID failedSaga() {
        UUID run = startSaga(); orchestrator.advanceRun(run);
        completeForward("A", Map.of("paymentId", "pay-A"), run);
        completeForward("B", Map.of("reservationId", "res-B"), run);
        transactions.fail(transactions.claim("C-failure").orElseThrow(), "shipment failed");
        return run;
    }
    private void completeForward(String id, Map<String, Object> output, UUID run) {
        var claim = transactions.claim(id).orElseThrow();
        assertThat(claim.isCompensation()).isFalse();
        transactions.complete(claim, output); orchestrator.advanceRun(run);
    }
    private void due(UUID task) { jdbc.update("UPDATE activity_task SET available_at = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", task); }
    private void expire(UUID task) { jdbc.update("UPDATE activity_task SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", task); }
    private void assertSequence(UUID run) {
        var events = history.read(run);
        for (int i = 0; i < events.size(); i++) assertThat(events.get(i).sequence()).isEqualTo(i + 1L);
    }
}
