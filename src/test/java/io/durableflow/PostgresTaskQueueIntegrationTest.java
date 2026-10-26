package io.durableflow;

import io.durableflow.api.ApiException;
import io.durableflow.worker.*;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.engine.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.task.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import org.testcontainers.postgresql.PostgreSQLContainer;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.*;

// Close pollers/pools before this class's disposable PostgreSQL container stops.
@org.springframework.test.annotation.DirtiesContext(classMode = org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@SpringBootTest
class PostgresTaskQueueIntegrationTest {
    private static final int WAIT = 15;
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.6");

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired WorkflowDefinitionService definitions;
    @Autowired WorkflowRunService runs;
    @Autowired WorkflowEngine engine;
    @Autowired WorkflowTaskOrchestrator orchestrator;
    @Autowired ActivityTaskRepository tasks;
    @Autowired ActivityExecutionRepository activities;
    @Autowired TaskTransactions transactions;
    @Autowired PlatformTransactionManager transactionManager;
    @Autowired JdbcTemplate jdbc;
    @Autowired ActivityRegistry serverRegistry;
    @Autowired WorkerHeartbeats heartbeats;
    @Autowired HandlerExecutions executions;
    @Autowired WorkerExecutionSettings executionSettings;

    @BeforeEach
    void resetIsolatedTestDatabase() {
        // This class owns its own container, and no poller runs in the server test profile.
        jdbc.execute("TRUNCATE model_route_attempt, model_route, agent_iteration, agent_execution, fake_tool_effect, tool_request, workflow_schedule_occurrence, workflow_schedule, child_workflow_execution, compensation_execution, workflow_signal_delivery, workflow_signal_wait, workflow_timer, workflow_event, activity_task, activity_execution, workflow_run, workflow_definition");
    }

    @Test
    void concurrentRepeatedAdvancementCreatesOnlyOneTaskAndCannotMixWithLocalExecution() throws Exception {
        var run = start(List.of(step("payment", "chargePayment")));
        var together = new CyclicBarrier(2);
        Callable<WorkflowRunResponse> advance = () -> { await(together); return engine.advanceRun(run.runId()); };
        race(advance, advance);
        engine.advanceRun(run.runId());
        assertThat(tasks.findAll()).hasSize(1).allSatisfy(t -> assertThat(t.getStatus()).isEqualTo(ActivityTaskStatus.READY));
        assertThat(activities.findByWorkflowRunIdOrderByActivityIdAsc(run.runId()).getFirst().getStatus())
                .isEqualTo(ActivityExecutionStatus.PENDING);
        assertThatThrownBy(() -> engine.executeRun(run.runId())).isInstanceOf(ApiException.class)
                .hasMessageContaining("queued tasks");
    }

    @Test
    void simultaneousWorkersCannotClaimTheSameReadyTask() throws Exception {
        var run = start(List.of(step("payment", "chargePayment")));
        engine.advanceRun(run.runId());
        var together = new CyclicBarrier(2);
        var claims = race(() -> { await(together); return transactions.claim("A"); },
                () -> { await(together); return transactions.claim("B"); });
        assertThat(claims).filteredOn(Optional::isPresent).hasSize(1);
        assertThat(claims).filteredOn(Optional::isEmpty).hasSize(1);
        assertThat(tasks.findAll()).hasSize(1).allSatisfy(t -> {
            assertThat(t.getStatus()).isEqualTo(ActivityTaskStatus.CLAIMED);
            assertThat(t.getWorkerId()).isIn("A", "B");
            assertThat(t.getClaimedAt()).isNotNull();
        });
        var activity = activities.findByWorkflowRunIdOrderByActivityIdAsc(run.runId()).getFirst();
        assertThat(activity.getStatus()).isEqualTo(ActivityExecutionStatus.RUNNING);
        assertThat(activity.getAttemptCount()).isEqualTo(1);
    }

    @Test
    void independentEligibleRootsAreClaimedAsDifferentTasks() throws Exception {
        var run = start(List.of(step("A", "chargePayment"), step("B", "reserveInventory"),
                step("join", "createShipment", "A", "B")));
        engine.advanceRun(run.runId());
        assertThat(tasks.count()).isEqualTo(2);
        var together = new CyclicBarrier(2);
        var claims = race(() -> { await(together); return transactions.claim("A").orElseThrow(); },
                () -> { await(together); return transactions.claim("B").orElseThrow(); });
        assertThat(claims).extracting(ClaimedTask::taskId).doesNotHaveDuplicates();
        assertThat(claims).extracting(ClaimedTask::activityExecutionId).doesNotHaveDuplicates();
        engine.advanceRun(run.runId()); // Recognized CLAIMED tasks are in flight, never reset/retried.
        assertThat(tasks.count()).isEqualTo(2);
        transactions.complete(claims.getFirst(), Map.of("ok", true));
        engine.advanceRun(run.runId());
        assertThat(tasks.count()).isEqualTo(2); // The join still requires the other root.
        transactions.complete(claims.get(1), Map.of("ok", true));
        engine.advanceRun(run.runId());
        assertThat(tasks.count()).isEqualTo(3);
        assertThat(runs.get(run.runId()).status()).isEqualTo(WorkflowRunStatus.RUNNING);
    }

    @Test
    void skipLockedClaimsSecondTaskWhileFirstTaskLockIsStillHeld() throws Exception {
        var run = start(List.of(step("A", "chargePayment"), step("B", "reserveInventory")));
        engine.advanceRun(run.runId());
        UUID firstId = jdbc.queryForObject("SELECT id FROM activity_task ORDER BY available_at, created_at, id LIMIT 1", UUID.class);
        var locked = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var holder = executor.submit(() -> new TransactionTemplate(transactionManager).executeWithoutResult(tx -> {
                tasks.findByIdForUpdate(firstId).orElseThrow();
                locked.countDown();
                await(release);
            }));
            await(locked);
            var other = executor.submit(() -> transactions.claim("skip-worker").orElseThrow());
            var claim = other.get(WAIT, TimeUnit.SECONDS); // Must finish BEFORE the first lock is released.
            assertThat(claim.taskId()).isNotEqualTo(firstId);
            assertThat(holder.isDone()).isFalse();
            assertThat(tasks.findById(firstId).orElseThrow().getStatus()).isEqualTo(ActivityTaskStatus.READY);
            release.countDown();
            holder.get(WAIT, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            shutdown(executor);
        }
    }

    @Test
    void workerExecutesOutsideTransactionsAndAdvancesSequentialWorkflowToCompletion() {
        var run = start(List.of(step("payment", "payment"), step("inventory", "inventory", "payment"),
                step("shipment", "shipment", "inventory")));
        var calls = new ArrayList<String>();
        var handlers = List.of("payment", "inventory", "shipment").stream().map(type -> handler(type, input -> {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            var claimed = tasks.findByRunId(run.runId()).stream()
                    .filter(t -> t.getStatus() == ActivityTaskStatus.CLAIMED).findFirst().orElseThrow();
            // Another transaction can lock both rows during the handler: the claim committed and released them.
            new TransactionTemplate(transactionManager).executeWithoutResult(tx -> {
                jdbc.execute("SET LOCAL lock_timeout = '3s'");
                tasks.findByIdForUpdate(claimed.getId()).orElseThrow();
                jdbc.queryForObject("SELECT id FROM workflow_run WHERE id = ? FOR UPDATE", UUID.class, run.runId());
            });
            calls.add(type);
            return Map.<String, Object>of("executed", type);
        })).toList();
        var worker = worker("test-worker", handlers);
        engine.advanceRun(run.runId());
        assertThat(worker.runOne()).isTrue();
        assertThat(tasks.count()).isEqualTo(2);
        assertThat(runs.get(run.runId()).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        assertThat(worker.runOne()).isTrue();
        assertThat(runs.get(run.runId()).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        assertThat(worker.runOne()).isTrue();
        assertThat(worker.runOne()).isFalse();
        assertThat(calls).containsExactly("payment", "inventory", "shipment");
        assertThat(runs.get(run.runId()).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        assertThat(tasks.findAll()).hasSize(3).allSatisfy(t -> {
            assertThat(t.getStatus()).isEqualTo(ActivityTaskStatus.COMPLETED);
            assertThat(t.getWorkerId()).isEqualTo("test-worker");
        });
        assertThat(activities.findByWorkflowRunIdOrderByActivityIdAsc(run.runId())).allSatisfy(a -> {
            assertThat(a.getStatus()).isEqualTo(ActivityExecutionStatus.COMPLETED);
            assertThat(a.getAttemptCount()).isEqualTo(1);
            assertThat(a.getOutputJson()).containsEntry("executed", a.getActivityId());
        });
    }

    @Test
    void thrownHandlerAtomicallyFailsTaskActivityAndRunWithoutRetry() {
        var run = start(List.of(step("first", "throws"), step("blocked", "chargePayment", "first")));
        var calls = new AtomicInteger();
        var worker = worker("failure-worker", List.of(handler("throws", input -> {
            calls.incrementAndGet(); throw new IllegalStateException("injected handler failure");
        })));
        engine.advanceRun(run.runId());
        assertThat(worker.runOne()).isTrue();
        assertThat(worker.runOne()).isFalse();
        assertThat(calls.get()).isEqualTo(1);
        assertThat(tasks.findAll()).hasSize(1).allSatisfy(t -> assertThat(t.getStatus()).isEqualTo(ActivityTaskStatus.FAILED));
        assertThat(runs.get(run.runId()).status()).isEqualTo(WorkflowRunStatus.FAILED);
        assertThat(runs.get(run.runId()).activities()).anySatisfy(a -> {
            assertThat(a.activityId()).isEqualTo("first");
            assertThat(a.status()).isEqualTo(ActivityExecutionStatus.FAILED);
            assertThat(a.errorMessage()).contains("injected handler failure");
        });
        assertThat(tasks.count()).isEqualTo(1);
    }

    @Test
    void simultaneousWorkerServicesExecuteOneHandlerExactlyOnce() throws Exception {
        var run = start(List.of(step("payment", "once")));
        engine.advanceRun(run.runId());
        var calls = new AtomicInteger();
        var handlers = List.of(handler("once", input -> {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            calls.incrementAndGet();
            return Map.of("result", "once");
        }));
        var a = worker("A", handlers);
        var b = worker("B", handlers);
        var together = new CyclicBarrier(2);
        assertThat(race(() -> { await(together); return a.runOne(); },
                () -> { await(together); return b.runOne(); })).containsExactlyInAnyOrder(true, false);
        assertThat(calls.get()).isEqualTo(1);
        assertThat(tasks.findAll()).hasSize(1).allSatisfy(t -> assertThat(t.getStatus()).isEqualTo(ActivityTaskStatus.COMPLETED));
        assertThat(runs.get(run.runId()).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
    }

    @Test
    void outcomeWriteFailureRollsBackBothTaskAndActivityChanges() {
        var run = start(List.of(step("payment", "chargePayment")));
        engine.advanceRun(run.runId());
        var claim = transactions.claim("owner").orElseThrow();
        jdbc.execute("""
                CREATE FUNCTION reject_task_completion() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  IF NEW.status = 'COMPLETED' THEN RAISE EXCEPTION 'injected task completion failure'; END IF;
                  RETURN NEW;
                END; $$
                """);
        jdbc.execute("CREATE TRIGGER reject_task_completion BEFORE UPDATE ON activity_task "
                + "FOR EACH ROW EXECUTE FUNCTION reject_task_completion()");
        try {
            assertThatThrownBy(() -> transactions.complete(claim, Map.of("result", "ok")))
                    .isInstanceOf(org.springframework.dao.DataAccessException.class);
            assertThat(tasks.findById(claim.taskId()).orElseThrow().getStatus()).isEqualTo(ActivityTaskStatus.CLAIMED);
            var activity = activities.findById(claim.activityExecutionId()).orElseThrow();
            assertThat(activity.getStatus()).isEqualTo(ActivityExecutionStatus.RUNNING);
            assertThat(activity.getOutputJson()).isNull();
        } finally {
            jdbc.execute("DROP TRIGGER reject_task_completion ON activity_task");
            jdbc.execute("DROP FUNCTION reject_task_completion()");
        }
    }

    @Test
    void missingHandlerFailsControlledAndWrongWorkerCannotFinishClaim() {
        var run = start(List.of(step("A", "unregistered")));
        engine.advanceRun(run.runId());
        var claim = transactions.claim("owner").orElseThrow();
        var wrong = new ClaimedTask(claim.taskId(), claim.runId(), claim.activityExecutionId(), claim.activityType(),
                claim.input(), "other", claim.leaseGeneration(), claim.leaseExpiresAt());
        assertThatThrownBy(() -> transactions.complete(wrong, Map.of())).isInstanceOf(ApiException.class);
        assertThat(tasks.findById(claim.taskId()).orElseThrow().getStatus()).isEqualTo(ActivityTaskStatus.CLAIMED);
        transactions.fail(claim, "controlled owner failure");
        assertThat(runs.get(run.runId()).status()).isEqualTo(WorkflowRunStatus.FAILED);

        var missing = start(List.of(step("B", "unregistered")));
        engine.advanceRun(missing.runId());
        assertThat(worker("missing-handler", List.of()).runOne()).isTrue();
        assertThat(runs.get(missing.runId()).activities().getFirst().errorMessage()).contains("No activity handler");
    }

    @Test
    void futureTaskIsNotClaimedAndOrphanedLegacyStateIsNotReenqueued() {
        var run = start(List.of(step("A", "chargePayment")));
        engine.advanceRun(run.runId());
        jdbc.update("UPDATE activity_task SET available_at = ?", java.sql.Timestamp.from(Instant.now().plusSeconds(3600)));
        assertThat(transactions.claim("early-worker")).isEmpty();
        var orphan = start(List.of(step("B", "reserveInventory")));
        jdbc.update("UPDATE activity_execution SET status = 'RUNNING' WHERE workflow_run_id = ?", orphan.runId());
        assertThatThrownBy(() -> engine.advanceRun(orphan.runId())).isInstanceOf(OrphanedActivityException.class);
        assertThat(tasks.findByRunId(orphan.runId())).isEmpty();
        assertThat(runs.get(orphan.runId()).status()).isEqualTo(WorkflowRunStatus.RUNNING);
    }

    @Test
    void alreadyClaimedSiblingCanRecordItsOutcomeAfterAnotherRootFails() {
        var run = start(List.of(step("A", "chargePayment"), step("B", "reserveInventory"),
                step("join", "createShipment", "A", "B")));
        engine.advanceRun(run.runId());
        var a = transactions.claim("A").orElseThrow();
        var b = transactions.claim("B").orElseThrow();
        transactions.fail(a, "confirmed root failure");
        transactions.complete(b, Map.of("result", "completed in flight"));
        engine.advanceRun(run.runId());
        assertThat(runs.get(run.runId()).status()).isEqualTo(WorkflowRunStatus.FAILED);
        assertThat(tasks.findByRunId(run.runId())).extracting(ActivityTaskEntity::getStatus)
                .containsExactlyInAnyOrder(ActivityTaskStatus.FAILED, ActivityTaskStatus.COMPLETED);
        assertThat(transactions.claim("C")).isEmpty();
    }

    @Test
    void workerApplicationContextConsumesPersistedTasksWithoutAnyHttpApi() {
        var run = start(List.of(step("payment", "chargePayment")));
        engine.advanceRun(run.runId());
        try (var workerContext = new SpringApplicationBuilder(DurableFlowApplication.class).run(
                "--spring.profiles.active=worker", "--durableflow.worker.polling-enabled=false",
                "--durableflow.worker.id=context-worker",
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword())) {
            assertThat(workerContext).isNotInstanceOf(org.springframework.web.context.WebApplicationContext.class);
            assertThat(workerContext.containsBean("workflowRunController")).isFalse();
            assertThat(workerContext.containsBean("workflowDefinitionController")).isFalse();
            assertThat(workerContext.containsBean("workflowExecutionController")).isFalse();
            assertThat(workerContext.containsBean("signalController")).isFalse();
            assertThat(workerContext.containsBean("scheduleController")).isFalse();
            assertThat(workerContext.getBeansOfType(io.durableflow.config.CompositionPollingConfiguration.ChildPoller.class)).isEmpty();
            assertThat(workerContext.getBeansOfType(io.durableflow.config.CompositionPollingConfiguration.SchedulePoller.class)).isEmpty();
            assertThat(workerContext.getBeansOfType(io.durableflow.workflow.timer.TimerPollingConfiguration.TimerPoller.class)).isEmpty();
            assertThat(workerContext.getBean(ActivityRegistry.class)).isNotSameAs(serverRegistry);
            assertThat(workerContext.getBean(TaskWorker.class).runOne()).isTrue();
        }
        assertThat(tasks.findAll().getFirst().getWorkerId()).isEqualTo("context-worker");
        assertThat(tasks.findAll().getFirst().getStatus()).isEqualTo(ActivityTaskStatus.COMPLETED);
        assertThat(runs.get(run.runId()).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
    }

    @Test
    void queuedRecoverySkipsPreviouslyCompletedLegacySteps() {
        var run = start(List.of(step("payment", "chargePayment"), step("inventory", "reserveInventory", "payment"),
                step("shipment", "createShipment", "inventory")));
        engine.executeRun(run.runId(), 1);
        var paymentBefore = activities.findByWorkflowRunIdOrderByActivityIdAsc(run.runId()).stream()
                .filter(a -> a.getActivityId().equals("payment")).findFirst().orElseThrow();
        engine.advanceRun(run.runId());
        var worker = worker("resume-worker", List.of(new io.durableflow.workflow.activity.demo.ReserveInventoryActivity(),
                new io.durableflow.workflow.activity.demo.CreateShipmentActivity()));
        assertThat(worker.runOne()).isTrue();
        assertThat(worker.runOne()).isTrue();
        assertThat(runs.get(run.runId()).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        assertThat(tasks.findByRunId(run.runId())).hasSize(2);
        var after = activities.findById(paymentBefore.getId()).orElseThrow();
        assertThat(after.getAttemptCount()).isEqualTo(1);
        assertThat(after.getCompletedAt()).isEqualTo(paymentBefore.getCompletedAt());
        assertThat(after.getOutputJson()).isEqualTo(paymentBefore.getOutputJson());
    }

    private WorkflowRunResponse start(List<StepDefinition> steps) {
        String name = "Queue-" + UUID.randomUUID();
        definitions.register(new RegisterWorkflowDefinitionRequest(name, 1, steps));
        return runs.start(new StartWorkflowRunRequest(name, 1, Map.of("orderId", "ORD-QUEUE")));
    }

    private static StepDefinition step(String id, String type, String... dependencies) {
        return new StepDefinition(id, type, List.of(dependencies));
    }

    private ActivityHandler handler(String type, Function<Map<String, Object>, Map<String, Object>> body) {
        return new ActivityHandler() {
            public String type() { return type; }
            public Map<String, Object> execute(io.durableflow.workflow.activity.ActivityExecutionContext context, Map<String, Object> input) { return body.apply(input); }
        };
    }

    private TaskWorker worker(String id, List<ActivityHandler> handlers) {
        return new TaskWorker(transactions, new ActivityRegistry(handlers), orchestrator, new WorkerIdentity(id), heartbeats, executions, executionSettings);
    }

    private <T> List<T> race(Callable<T> a, Callable<T> b) throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        try {
            var left = executor.submit(a);
            var right = executor.submit(b);
            return List.of(left.get(WAIT, TimeUnit.SECONDS), right.get(WAIT, TimeUnit.SECONDS));
        } finally { shutdown(executor); }
    }

    private static void await(CyclicBarrier barrier) {
        try { barrier.await(WAIT, TimeUnit.SECONDS); }
        catch (Exception failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            throw new AssertionError("Concurrent operation did not reach barrier", failure);
        }
    }

    private static void await(CountDownLatch latch) {
        try { assertThat(latch.await(WAIT, TimeUnit.SECONDS)).isTrue(); }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); throw new AssertionError(interrupted);
        }
    }

    private static void shutdown(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(WAIT, TimeUnit.SECONDS)).isTrue();
    }
}
