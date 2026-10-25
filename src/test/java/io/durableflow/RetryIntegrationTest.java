package io.durableflow;

import io.durableflow.worker.*;
import io.durableflow.worker.retry.*;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.activity.demo.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.task.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.*;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
@SpringBootTest(properties = {"durableflow.worker.retry.initial-delay-ms=60000", "durableflow.worker.retry.max-delay-ms=240000"})
@Import(RetryIntegrationTest.DeterministicJitter.class)
class RetryIntegrationTest {
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.6");
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
    @TestConfiguration(proxyBeanMethods = false)
    static class DeterministicJitter {
        @Bean @Primary RetryDelayCalculator deterministicRetryDelays() { return new RetryDelayCalculator(() -> 1); }
    }

    @Autowired TaskTransactions transactions;
    @Autowired WorkflowDefinitionService definitions;
    @Autowired WorkflowRunService runs;
    @Autowired WorkflowTaskOrchestrator orchestrator;
    @Autowired ActivityExecutionRepository activities;
    @Autowired ActivityTaskRepository tasks;
    @Autowired WorkerHeartbeats heartbeats;
    @Autowired HandlerExecutions executions;
    @Autowired WorkerExecutionSettings executionSettings;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void clean() { jdbc.execute("TRUNCATE fake_tool_effect, tool_request, workflow_schedule_occurrence, workflow_schedule, child_workflow_execution, compensation_execution, workflow_signal_delivery, workflow_signal_wait, workflow_timer, workflow_event, activity_task, activity_execution, workflow_run, workflow_definition"); }

    @Test
    void transientFailuresPersistDelayedRetriesThenSucceedWithOneEffectAndStableKey() {
        var provider = new FakePaymentProvider();
        var calls = new AtomicInteger();
        var keys = new ArrayList<String>();
        var handler = handler((context, input) -> {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            keys.add(context.idempotencyKey());
            var result = provider.charge(context.idempotencyKey(), input);
            if (calls.incrementAndGet() <= 2) throw new RetryableActivityException("temporary response loss");
            return result;
        });
        UUID runId = ready();
        Instant before = now();
        assertThat(worker("A", handler).runOne()).isTrue();
        var first = task(runId);
        assertThat(first.getStatus()).isEqualTo(ActivityTaskStatus.READY);
        assertThat(first.getWorkerId()).isNull();
        assertThat(first.getClaimedAt()).isNull();
        assertThat(first.getLeaseExpiresAt()).isNull();
        assertThat(first.getLeaseGeneration()).isEqualTo(1);
        assertThat(first.getAvailableAt()).isBetween(before.plusSeconds(60), now().plusSeconds(60));
        assertWaiting(runId, 1);
        assertThat(transactions.claim("too-early")).isEmpty();
        orchestrator.advanceRun(runId); // Retry wait is tracked work, never an orphan.
        assertWaiting(runId, 1);

        due(first.getId());
        before = now();
        assertThat(worker("B", handler).runOne()).isTrue();
        assertWaiting(runId, 2);
        assertThat(task(runId).getAvailableAt()).isBetween(before.plusSeconds(120), now().plusSeconds(120));
        due(first.getId());
        // A new worker object has no retry timer/state to recover; only PostgreSQL is consulted.
        var c = worker("C", handler);
        assertThat(c.runOne()).isTrue();
        var payment = activities.findById(first.getActivityExecutionId()).orElseThrow();
        assertThat(payment.getStatus()).isEqualTo(ActivityExecutionStatus.COMPLETED);
        assertThat(payment.getAttemptCount()).isEqualTo(3);
        assertThat(payment.getOutputJson()).containsKey("paymentId");
        assertThat(task(runId).getLeaseGeneration()).isEqualTo(3);
        assertThat(keys).hasSize(3).containsOnly("df:v1:" + payment.getId());
        assertThat(provider.charge(keys.getFirst(), payment.getInputJson())).isEqualTo(payment.getOutputJson());
        assertThat(provider.effectCount()).isEqualTo(1);
        assertThat(c.runOne()).isTrue();
        assertThat(runs.get(runId).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        assertThat(calls.get()).isEqualTo(3);
    }

    @Test
    void unknownProgrammingFailureIsTerminalOnFirstAttempt() {
        var runId = ready();
        assertThat(worker("A", handler((context, input) -> { throw new IllegalStateException("broken code"); })).runOne()).isTrue();
        assertFailed(runId, 1);
        assertThat(transactions.claim("B")).isEmpty();
    }

    @Test
    void providerIdempotencyMisuseIsNeverRetried() {
        var provider = new FakePaymentProvider();
        var runId = ready();
        var handler = handler((context, input) -> {
            provider.charge(context.idempotencyKey(), input);
            return provider.charge(context.idempotencyKey(), Map.of("different", "payload"));
        });
        assertThat(worker("A", handler).runOne()).isTrue();
        assertFailed(runId, 1);
        assertThat(provider.effectCount()).isEqualTo(1);
        assertThat(activities.findById(task(runId).getActivityExecutionId()).orElseThrow().getErrorMessage())
                .contains("Idempotency key reused");
    }

    @Test
    void maximumAttemptsIncludesInitialExecutionAndFailsExactlyOnThirdFailure() {
        var runId = ready();
        var calls = new AtomicInteger();
        var worker = worker("A", handler((context, input) -> {
            calls.incrementAndGet();
            throw new RetryableActivityException("still unavailable");
        }));
        for (int attempt = 1; attempt <= 3; attempt++) {
            assertThat(worker.runOne()).isTrue();
            if (attempt < 3) { assertWaiting(runId, attempt); due(task(runId).getId()); }
        }
        assertFailed(runId, 3);
        assertThat(calls.get()).isEqualTo(3);
        assertThat(worker.runOne()).isFalse();
    }

    @Test
    void heartbeatAndReschedulingDoNotIncrementAttemptsAndOldOwnerCannotScheduleRetry() {
        var runId = ready();
        var a = transactions.claim("A").orElseThrow();
        assertThat(transactions.heartbeat(a)).isTrue();
        assertThat(activities.findById(a.activityExecutionId()).orElseThrow().getAttemptCount()).isEqualTo(1);
        transactions.handleFailure(a, new RetryableActivityException("retry"));
        assertWaiting(runId, 1);
        var before = snapshot();
        assertThatThrownBy(() -> transactions.handleFailure(a, new RetryableActivityException("stale")))
                .isInstanceOf(OwnershipLostException.class);
        assertThat(snapshot()).isEqualTo(before);
        due(a.taskId());
        var b = transactions.claim("B").orElseThrow();
        before = snapshot();
        assertThatThrownBy(() -> transactions.handleFailure(a, new RetryableActivityException("stale")))
                .isInstanceOf(OwnershipLostException.class);
        assertThat(snapshot()).isEqualTo(before);
        assertThat(b.leaseGeneration()).isEqualTo(2);
        transactions.complete(b, Map.of("ok", true));
    }

    @Test
    void expiredOwnerCannotRescheduleEvenWithoutReplacementAndFailureMetadataRollsBack() {
        ready();
        var a = transactions.claim("A").orElseThrow();
        expire(a.taskId());
        var before = snapshot();
        assertThatThrownBy(() -> transactions.handleFailure(a, new RetryableActivityException("late")))
                .isInstanceOf(OwnershipLostException.class);
        assertThat(snapshot()).isEqualTo(before);
    }

    @Test
    void retryRechecksLeaseAfterFlushingLogicalFailure() {
        ready();
        var a = transactions.claim("A").orElseThrow();
        var before = snapshot();
        jdbc.execute("""
                CREATE FUNCTION expire_before_retry() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    UPDATE activity_task SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'
                    WHERE activity_execution_id = NEW.id;
                    RETURN NEW;
                END $$
                """);
        jdbc.execute("CREATE TRIGGER expire_before_retry BEFORE UPDATE ON activity_execution "
                + "FOR EACH ROW EXECUTE FUNCTION expire_before_retry()");
        try {
            assertThatThrownBy(() -> transactions.handleFailure(a, new RetryableActivityException("late")))
                    .isInstanceOf(OwnershipLostException.class);
            assertThat(snapshot()).isEqualTo(before);
        } finally {
            jdbc.execute("DROP TRIGGER expire_before_retry ON activity_execution");
            jdbc.execute("DROP FUNCTION expire_before_retry()");
        }
    }

    @Test
    void finalCrashedAttemptCannotEvadeBudgetThroughReclamation() {
        var runId = ready();
        UUID taskId = null;
        for (int attempt = 1; attempt <= 3; attempt++) {
            var claim = transactions.claim("worker-" + attempt).orElseThrow();
            taskId = claim.taskId();
            assertThat(activities.findById(claim.activityExecutionId()).orElseThrow().getAttemptCount()).isEqualTo(attempt);
            expire(taskId);
        }
        assertThat(transactions.claim("fourth")).isEmpty();
        assertFailed(runId, 3);
        assertThat(tasks.findById(taskId).orElseThrow().getLeaseGeneration()).isEqualTo(4); // Recovery bookkeeping, no execution.
    }

    private void assertWaiting(UUID runId, int attempts) {
        var task = task(runId);
        var activity = activities.findById(task.getActivityExecutionId()).orElseThrow();
        assertThat(task.getStatus()).isEqualTo(ActivityTaskStatus.READY);
        assertThat(activity.getStatus()).isEqualTo(ActivityExecutionStatus.RUNNING);
        assertThat(activity.getAttemptCount()).isEqualTo(attempts);
        assertThat(activity.getErrorMessage()).contains("RetryableActivityException");
        assertThat(runs.get(runId).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        assertThat(tasks.findByRunId(runId)).hasSize(1);
        assertThat(activities.findByWorkflowRunIdOrderByActivityIdAsc(runId))
                .filteredOn(a -> a.getActivityId().equals("shipment"))
                .allSatisfy(a -> assertThat(a.getStatus()).isEqualTo(ActivityExecutionStatus.PENDING));
    }

    private void assertFailed(UUID runId, int attempts) {
        assertThat(task(runId).getStatus()).isEqualTo(ActivityTaskStatus.FAILED);
        var activity = activities.findById(task(runId).getActivityExecutionId()).orElseThrow();
        assertThat(activity.getStatus()).isEqualTo(ActivityExecutionStatus.FAILED);
        assertThat(activity.getAttemptCount()).isEqualTo(attempts);
        assertThat(runs.get(runId).status()).isEqualTo(WorkflowRunStatus.FAILED);
    }

    private UUID ready() {
        String name = "Retry-" + UUID.randomUUID();
        definitions.register(new RegisterWorkflowDefinitionRequest(name, 1, List.of(
                new StepDefinition("payment", "transient-demo", List.of()),
                new StepDefinition("shipment", "createShipment", List.of("payment")))));
        var run = runs.start(new StartWorkflowRunRequest(name, 1, Map.of("orderId", "RETRY-DEMO")));
        orchestrator.advanceRun(run.runId());
        return run.runId();
    }

    private ActivityTaskEntity task(UUID runId) {
        return tasks.findByRunId(runId).stream().filter(t -> activities.findById(t.getActivityExecutionId()).orElseThrow()
                .getActivityId().equals("payment")).findFirst().orElseThrow();
    }
    private Instant now() { return Objects.requireNonNull(jdbc.queryForObject("SELECT clock_timestamp()", OffsetDateTime.class)).toInstant(); }
    private void due(UUID id) { jdbc.update("UPDATE activity_task SET available_at = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", id); }
    private void expire(UUID id) { jdbc.update("UPDATE activity_task SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", id); }
    private List<Map<String, Object>> snapshot() {
        return jdbc.queryForList("SELECT t.*, a.status AS logical_status, a.error_message, a.attempt_count, a.updated_at "
                + "FROM activity_task t JOIN activity_execution a ON a.id=t.activity_execution_id ORDER BY t.id");
    }
    private TaskWorker worker(String id, ActivityHandler handler) {
        return new TaskWorker(transactions, new ActivityRegistry(List.of(handler, new CreateShipmentActivity())),
                orchestrator, new WorkerIdentity(id), heartbeats, executions, executionSettings);
    }
    @FunctionalInterface
    interface Body { Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input); }
    private ActivityHandler handler(Body body) {
        return new ActivityHandler() {
            public String type() { return "transient-demo"; }
            public Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input) { return body.execute(context, input); }
        };
    }
}
