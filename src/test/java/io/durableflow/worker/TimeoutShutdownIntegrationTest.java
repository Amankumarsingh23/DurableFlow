package io.durableflow.worker;

import io.durableflow.worker.retry.*;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.activity.demo.FakePaymentProvider;
import io.durableflow.workflow.definition.*;
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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@Testcontainers
@SpringBootTest
class TimeoutShutdownIntegrationTest {
    private static final int WAIT = 10;
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.6");
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
    @Autowired TaskTransactions transactions;
    @Autowired WorkflowDefinitionService definitions;
    @Autowired WorkflowRunService runs;
    @Autowired WorkflowTaskOrchestrator orchestrator;
    @Autowired ActivityExecutionRepository activities;
    @Autowired ActivityTaskRepository tasks;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach void clean() { jdbc.execute("TRUNCATE agent_iteration, agent_execution, fake_tool_effect, tool_request, workflow_schedule_occurrence, workflow_schedule, child_workflow_execution, compensation_execution, workflow_signal_delivery, workflow_signal_wait, workflow_timer, workflow_event, activity_task, activity_execution, workflow_run, workflow_definition"); }

    @Test
    void timeoutRetriesPaymentWithSameKeyAndFencesLateOldCompletion() throws Exception {
        var run = ready();
        var provider = new FakePaymentProvider();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var returned = new CountDownLatch(1);
        var old = new AtomicReference<ClaimedTask>();
        var original = new AtomicReference<Map<String, Object>>();
        var key = new AtomicReference<String>();
        try (var a = new Harness("A", handler((context, input) -> {
            key.set(context.idempotencyKey());
            old.set(claim(run, context));
            original.set(provider.charge(context.idempotencyKey(), input));
            entered.countDown();
            ignoreInterruptsUntil(release);
            returned.countDown();
            return original.get();
        }), 0, timeoutAfter(entered))) {
            assertThat(a.worker.runOne()).isTrue();
            var task = task(run);
            assertThat(task.getStatus()).isEqualTo(ActivityTaskStatus.READY);
            assertThat(task.getLeaseExpiresAt()).isNull();
            assertThat(transactions.heartbeat(old.get())).isFalse();
            var activity = activities.findById(task.getActivityExecutionId()).orElseThrow();
            assertThat(activity.getStatus()).isEqualTo(ActivityExecutionStatus.RUNNING);
            assertThat(activity.getAttemptCount()).isEqualTo(1);
            assertThat(activity.getErrorMessage()).contains("ActivityTimeoutException");
            assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.RUNNING);
            assertThat(a.executions.reserve()).isEmpty(); // Ignoring interruption cannot spawn more local handlers.
            due(task.getId());
            try (var b = new Harness("B", handler((context, input) -> {
                assertThat(context.idempotencyKey()).isEqualTo(key.get());
                return provider.charge(context.idempotencyKey(), input);
            }), 1000, normalWait())) {
                assertThat(b.worker.runOne()).isTrue();
            }
            release.countDown();
            assertThat(returned.await(WAIT, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> transactions.complete(old.get(), original.get())).isInstanceOf(OwnershipLostException.class);
            assertThat(task(run).getLeaseGeneration()).isEqualTo(2);
            assertThat(activities.findById(task.getActivityExecutionId()).orElseThrow().getAttemptCount()).isEqualTo(2);
            assertThat(activities.findById(task.getActivityExecutionId()).orElseThrow().getOutputJson()).isEqualTo(original.get());
            assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
            assertThat(provider.effectCount()).isEqualTo(1);
        } finally { release.countDown(); }
    }

    @Test
    void timeoutBudgetExhaustsExactlyAndNeverChangesLogicalKey() {
        var run = ready();
        var keys = new ArrayList<String>();
        for (int attempt = 1; attempt <= 3; attempt++) {
            var entered = new CountDownLatch(1);
            try (var worker = new Harness("attempt-" + attempt, handler((context, input) -> {
                keys.add(context.idempotencyKey());
                entered.countDown();
                new CountDownLatch(1).await();
                return Map.of();
            }), 0, timeoutAfter(entered))) {
                assertThat(worker.worker.runOne()).isTrue();
            }
            var task = task(run);
            assertThat(activities.findById(task.getActivityExecutionId()).orElseThrow().getAttemptCount()).isEqualTo(attempt);
            if (attempt < 3) { assertThat(task.getStatus()).isEqualTo(ActivityTaskStatus.READY); due(task.getId()); }
        }
        assertThat(keys).hasSize(3).containsOnly("df:v1:" + task(run).getActivityExecutionId());
        assertThat(task(run).getStatus()).isEqualTo(ActivityTaskStatus.FAILED);
        assertThat(activities.findById(task(run).getActivityExecutionId()).orElseThrow().getStatus()).isEqualTo(ActivityExecutionStatus.FAILED);
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.FAILED);
        assertThat(transactions.claim("extra")).isEmpty();
    }

    @Test
    void staleTimeoutCannotRescheduleReplacementOwner() {
        var run = ready();
        var entered = new CountDownLatch(1);
        var replacement = new AtomicReference<ClaimedTask>();
        try (var a = new Harness("A", handler((context, input) -> {
            entered.countDown();
            new CountDownLatch(1).await();
            return Map.of();
        }), 0, (future, millis) -> {
            assertThat(entered.await(WAIT, TimeUnit.SECONDS)).isTrue();
            expire(task(run).getId());
            replacement.set(transactions.claim("B").orElseThrow());
            throw new TimeoutException();
        })) {
            assertThatThrownBy(a.worker::runOne).isInstanceOf(OwnershipLostException.class);
            assertThat(task(run).getStatus()).isEqualTo(ActivityTaskStatus.CLAIMED);
            assertThat(task(run).getWorkerId()).isEqualTo("B");
            assertThat(task(run).getLeaseGeneration()).isEqualTo(2);
            assertThat(activities.findById(task(run).getActivityExecutionId()).orElseThrow().getErrorMessage()).isNull();
            transactions.complete(replacement.get(), Map.of("current", true));
        }
    }

    @Test
    void drainClosesClaimGateButKeepsValidHeartbeatsAndAllowsCompletion() throws Exception {
        var run = ready();
        var another = ready();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try (var worker = new Harness("A", handler((context, input) -> {
            entered.countDown(); assertThat(release.await(WAIT, TimeUnit.SECONDS)).isTrue();
            return Map.of("drained", true);
        }), 5000, normalWait())) {
            var work = executor.submit(worker.worker::runOne);
            assertThat(entered.await(WAIT, TimeUnit.SECONDS)).isTrue();
            var claim = claim(run, new ActivityExecutionContext(run, task(run).getActivityExecutionId()));
            var shutdown = executor.submit(worker.worker::stop);
            await().atMost(WAIT, TimeUnit.SECONDS).until(worker.worker::isDraining);
            assertThat(worker.worker.runOne()).isFalse();
            assertThat(shutdown.isDone()).isFalse();
            assertThat(transactions.heartbeat(claim)).isTrue();
            assertThat(task(another).getStatus()).isEqualTo(ActivityTaskStatus.READY);
            release.countDown();
            assertThat(work.get(WAIT, TimeUnit.SECONDS)).isTrue();
            shutdown.get(WAIT, TimeUnit.SECONDS);
            assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
            assertThat(task(another).getStatus()).isEqualTo(ActivityTaskStatus.READY);
            assertThat(worker.executions.reserve()).isEmpty();
            assertThatThrownBy(() -> worker.heartbeats.start(claim)).isInstanceOf(OwnershipLostException.class);
        } finally { release.countDown(); executor.shutdownNow(); }
    }

    @Test
    void exhaustedDrainAbandonsWithoutFailureAndLeaseExpiryRecovers() throws Exception {
        var run = ready();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var old = new AtomicReference<ClaimedTask>();
        var executor = Executors.newSingleThreadExecutor();
        try (var a = new Harness("A", handler((context, input) -> {
            old.set(claim(run, context)); entered.countDown();
            ignoreInterruptsUntil(release);
            return Map.of("late", true);
        }), 0, normalWait())) {
            var work = executor.submit(a.worker::runOne);
            assertThat(entered.await(WAIT, TimeUnit.SECONDS)).isTrue();
            a.worker.stop();
            assertThatThrownBy(() -> work.get(WAIT, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(OwnershipLostException.class);
            assertThat(task(run).getStatus()).isEqualTo(ActivityTaskStatus.CLAIMED);
            assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.RUNNING);
            assertThat(activities.findById(old.get().activityExecutionId()).orElseThrow().getErrorMessage()).isNull();
            expire(old.get().taskId());
            try (var b = new Harness("B", handler((context, input) -> Map.of("recovered", true)), 1000, normalWait())) {
                assertThat(b.worker.runOne()).isTrue();
            }
            release.countDown();
            assertThatThrownBy(() -> transactions.complete(old.get(), Map.of("late", true))).isInstanceOf(OwnershipLostException.class);
            assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        } finally { release.countDown(); executor.shutdownNow(); }
    }

    private class Harness implements AutoCloseable {
        final WorkerHeartbeats heartbeats = new WorkerHeartbeats(transactions, new WorkerLeaseSettings(30000, 10000));
        final HandlerExecutions executions;
        final TaskWorker worker;
        Harness(String id, ActivityHandler handler, long grace, HandlerExecutions.Awaiter awaiter) {
            var settings = new WorkerExecutionSettings(60000, grace);
            var executor = Executors.newSingleThreadExecutor(runnable -> {
                var thread = new Thread(runnable, "test-handler"); thread.setDaemon(true); return thread;
            });
            executions = new HandlerExecutions(settings, executor, awaiter);
            worker = new TaskWorker(transactions, new ActivityRegistry(List.of(handler)), orchestrator,
                    new WorkerIdentity(id), heartbeats, executions, settings);
        }
        public void close() { worker.stop(); }
    }

    private UUID ready() {
        String name = "Timeout-" + UUID.randomUUID();
        definitions.register(new RegisterWorkflowDefinitionRequest(name, 1, List.of(new StepDefinition("activity", "timed", List.of()))));
        var run = runs.start(new StartWorkflowRunRequest(name, 1, Map.of("orderId", "TIMEOUT-DEMO")));
        orchestrator.advanceRun(run.runId());
        return run.runId();
    }
    private ActivityTaskEntity task(UUID run) { return tasks.findByRunId(run).getFirst(); }
    private ClaimedTask claim(UUID run, ActivityExecutionContext context) {
        var task = task(run);
        return new ClaimedTask(task.getId(), run, context.activityExecutionId(), "timed",
                activities.findById(context.activityExecutionId()).orElseThrow().getInputJson(),
                task.getWorkerId(), task.getLeaseGeneration(), task.getLeaseExpiresAt());
    }
    private void due(UUID id) { jdbc.update("UPDATE activity_task SET available_at = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", id); }
    private void expire(UUID id) { jdbc.update("UPDATE activity_task SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", id); }
    private HandlerExecutions.Awaiter normalWait() { return (future, millis) -> future.get(millis, TimeUnit.MILLISECONDS); }
    private HandlerExecutions.Awaiter timeoutAfter(CountDownLatch entered) {
        return (future, millis) -> { assertThat(entered.await(WAIT, TimeUnit.SECONDS)).isTrue(); throw new TimeoutException(); };
    }
    private static void ignoreInterruptsUntil(CountDownLatch release) {
        while (release.getCount() != 0) {
            try { release.await(); } catch (InterruptedException ignored) { /* Deliberate zombie fixture. */ }
        }
    }
    @FunctionalInterface interface Body { Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input) throws Exception; }
    private ActivityHandler handler(Body body) {
        return new ActivityHandler() {
            public String type() { return "timed"; }
            public Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input) throws Exception { return body.execute(context, input); }
        };
    }
}
