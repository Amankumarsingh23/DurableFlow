package io.durableflow;

import io.durableflow.api.ApiException;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.engine.*;
import io.durableflow.workflow.run.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

@Testcontainers
@SpringBootTest
class WorkflowConcurrencyIntegrationTest {
    private static final int WAIT_SECONDS = 15;
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.6");

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired WorkflowDefinitionService definitions;
    @Autowired WorkflowRunService runService;
    @Autowired WorkflowRunRepository runs;
    @Autowired ActivityExecutionRepository activities;
    @Autowired WorkflowEngineState state;
    @Autowired PlatformTransactionManager transactionManager;
    @Autowired JdbcTemplate jdbc;

    @Test
    void legacyReadThenWriteDemonstratesTwoClaimantsAndLostAttemptIncrement() throws Exception {
        var run = start();
        UUID activityId = rows(run.runId()).getFirst().getId();
        var bothReadPending = new CyclicBarrier(2);
        var claimed = new AtomicInteger();
        // Deliberately bypass the new protocol ONLY to reproduce the old Session 8 race.
        // Each task has its own transaction/persistence context and reads before either can write.
        Callable<Boolean> unsafeClaim = () -> {
            transactions().executeWithoutResult(transaction -> {
                var activity = activities.findById(activityId).orElseThrow();
                assertThat(activity.getStatus()).isEqualTo(ActivityExecutionStatus.PENDING);
                await(bothReadPending);
                activity.markRunning(Map.of(), Instant.now());
                activities.saveAndFlush(activity);
            });
            claimed.incrementAndGet();
            return true;
        };
        race(unsafeClaim, unsafeClaim);
        assertThat(claimed.get()).isEqualTo(2);
        var stored = rows(run.runId()).getFirst();
        assertThat(stored.getStatus()).isEqualTo(ActivityExecutionStatus.RUNNING);
        assertThat(stored.getAttemptCount()).isEqualTo(1); // Both overwrote the same old counter.
    }

    @Test
    void exactlyOneConcurrentCheckpointCanClaimTheSamePendingActivity() throws Exception {
        var run = start();
        var startTogether = new CyclicBarrier(2);
        Callable<Claim> claim = () -> {
            await(startTogether);
            try {
                return new Claim(state.next(run.runId(), true).work(), null);
            } catch (OrphanedActivityException conflict) {
                return new Claim(null, conflict);
            }
        };
        var outcomes = race(claim, claim);
        assertThat(outcomes).filteredOn(result -> result.work() != null).hasSize(1);
        assertThat(outcomes).filteredOn(result -> result.conflict() != null).hasSize(1);
        var stored = rows(run.runId());
        assertThat(stored).hasSize(1);
        assertThat(stored.getFirst().getStatus()).isEqualTo(ActivityExecutionStatus.RUNNING);
        assertThat(stored.getFirst().getAttemptCount()).isEqualTo(1);
        assertThat(runService.get(run.runId()).status()).isEqualTo(WorkflowRunStatus.RUNNING);
    }

    @Test
    void independentEnginesInvokeHandlerOnceAndLoserCannotFailTheActiveRun() throws Exception {
        var run = start();
        var startTogether = new CyclicBarrier(2);
        var handlerEntered = new CountDownLatch(1);
        var releaseHandler = new CountDownLatch(1);
        var loserExited = new CountDownLatch(1);
        var calls = new AtomicInteger();
        ActivityHandler handler = new ActivityHandler() {
            public String type() { return "concurrent-demo"; }
            public Map<String, Object> execute(Map<String, Object> input) {
                calls.incrementAndGet();
                assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
                // A separate short transaction can acquire the run lock during the handler.
                // This would block/fail if the claim transaction kept its lock open.
                transactions().executeWithoutResult(transaction -> {
                    jdbc.execute("SET LOCAL lock_timeout = '3s'");
                    assertThat(runs.findByIdForUpdate(run.runId()).orElseThrow().getStatus())
                            .isEqualTo(WorkflowRunStatus.RUNNING);
                    assertThat(rows(run.runId()).getFirst().getStatus()).isEqualTo(ActivityExecutionStatus.RUNNING);
                });
                handlerEntered.countDown();
                await(releaseHandler);
                return Map.of("result", "once");
            }
        };
        var registry = new ActivityRegistry(List.of(handler));
        // Separate objects have separate in-memory guards; the DB must supply the coordination.
        var engineA = new WorkflowLocalExecutor(state, registry, runService);
        var engineB = new WorkflowLocalExecutor(state, registry, runService);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var a = executor.submit(() -> execute(engineA, run.runId(), startTogether, loserExited));
            var b = executor.submit(() -> execute(engineB, run.runId(), startTogether, loserExited));
            await(handlerEntered);
            await(loserExited); // Winner is still inside handler; loser must finish without altering its state.
            assertThat(calls.get()).isEqualTo(1);
            assertThat(runService.get(run.runId()).status()).isEqualTo(WorkflowRunStatus.RUNNING);
            assertThat(rows(run.runId()).getFirst().getStatus()).isEqualTo(ActivityExecutionStatus.RUNNING);
            releaseHandler.countDown();
            var outcomes = List.of(a.get(WAIT_SECONDS, TimeUnit.SECONDS), b.get(WAIT_SECONDS, TimeUnit.SECONDS));
            assertThat(outcomes).filteredOn(result -> result.response() != null).hasSize(1);
            assertThat(outcomes).filteredOn(result -> result.conflict() instanceof OrphanedActivityException).hasSize(1);
            assertThat(calls.get()).isEqualTo(1);
            assertThat(runService.get(run.runId()).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
            assertThat(rows(run.runId())).hasSize(1).allSatisfy(row -> {
                assertThat(row.getStatus()).isEqualTo(ActivityExecutionStatus.COMPLETED);
                assertThat(row.getAttemptCount()).isEqualTo(1);
                assertThat(row.getOutputJson()).containsEntry("result", "once");
            });
        } finally {
            releaseHandler.countDown();
            shutdown(executor);
        }
    }

    @Test
    void completionAndFailureCannotOverwriteEachOthersActivityOutcome() throws Exception {
        var run = start();
        UUID activityId = state.next(run.runId(), true).work().activityExecutionId();
        var startTogether = new CyclicBarrier(2);
        var outcomes = race(
                () -> transition(startTogether, () -> state.complete(run.runId(), activityId, Map.of("result", "ok"))),
                () -> transition(startTogether, () -> state.fail(run.runId(), activityId, "confirmed failure")));
        assertThat(outcomes).containsExactlyInAnyOrder(true, false);
        var row = rows(run.runId()).getFirst();
        if (row.getStatus() == ActivityExecutionStatus.COMPLETED) {
            // Completion is a separate checkpoint from run finalization.
            state.next(run.runId(), false);
            assertThat(runService.get(run.runId()).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
            assertThat(row.getOutputJson()).containsEntry("result", "ok");
            assertThat(row.getErrorMessage()).isNull();
        } else {
            assertThat(row.getStatus()).isEqualTo(ActivityExecutionStatus.FAILED);
            assertThat(runService.get(run.runId()).status()).isEqualTo(WorkflowRunStatus.FAILED);
            assertThat(row.getErrorMessage()).isEqualTo("confirmed failure");
            assertThat(row.getOutputJson()).isNull();
        }
        assertThat(row.getAttemptCount()).isEqualTo(1);
        assertThat(runs.findById(run.runId()).orElseThrow().getCompletedAt()).isNotNull();
    }

    @Test
    void competingFinalizersCannotOverwriteATerminalRun() throws Exception {
        var run = start();
        UUID activityId = state.next(run.runId(), true).work().activityExecutionId();
        state.complete(run.runId(), activityId, Map.of("result", "ok"));
        var startTogether = new CyclicBarrier(2);
        Callable<Boolean> finalizeRun = () -> transition(startTogether, () -> state.next(run.runId(), false));
        assertThat(race(finalizeRun, finalizeRun)).containsExactlyInAnyOrder(true, false);
        var terminal = runs.findById(run.runId()).orElseThrow();
        assertThat(terminal.getStatus()).isEqualTo(WorkflowRunStatus.COMPLETED);
        var completedAt = terminal.getCompletedAt();
        assertThatThrownBy(() -> state.next(run.runId(), false)).isInstanceOf(ApiException.class);
        assertThat(runs.findById(run.runId()).orElseThrow().getCompletedAt()).isEqualTo(completedAt);
        assertThat(rows(run.runId())).hasSize(1);
    }

    private WorkflowRunResponse start() {
        String name = "Concurrency-" + UUID.randomUUID();
        definitions.register(new RegisterWorkflowDefinitionRequest(name, 1,
                List.of(new StepDefinition("payment", "concurrent-demo", List.of()))));
        return runService.start(new StartWorkflowRunRequest(name, 1, Map.of("orderId", "ORD-RACE")));
    }

    private List<ActivityExecutionEntity> rows(UUID runId) {
        return activities.findByWorkflowRunIdOrderByActivityIdAsc(runId);
    }

    private TransactionTemplate transactions() {
        var template = new TransactionTemplate(transactionManager);
        template.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        template.setTimeout(WAIT_SECONDS);
        return template;
    }

    private record Claim(WorkflowEngineState.Work work, OrphanedActivityException conflict) {}
    private record Execution(WorkflowRunResponse response, ApiException conflict) {}

    private Execution execute(WorkflowLocalExecutor engine, UUID runId, CyclicBarrier barrier, CountDownLatch loserExited) {
        await(barrier);
        try {
            return new Execution(engine.executeRun(runId), null);
        } catch (ApiException conflict) {
            assertThat(conflict.status()).isEqualTo(HttpStatus.CONFLICT);
            loserExited.countDown();
            return new Execution(null, conflict);
        }
    }

    private boolean transition(CyclicBarrier barrier, Runnable change) {
        await(barrier);
        try {
            change.run();
            return true;
        } catch (ApiException conflict) {
            assertThat(conflict.status()).isEqualTo(HttpStatus.CONFLICT);
            return false;
        }
    }

    private <T> List<T> race(Callable<T> a, Callable<T> b) throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        try {
            var left = executor.submit(a);
            var right = executor.submit(b);
            return List.of(left.get(WAIT_SECONDS, TimeUnit.SECONDS), right.get(WAIT_SECONDS, TimeUnit.SECONDS));
        } finally {
            shutdown(executor);
        }
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await(WAIT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while coordinating competing transactions", interrupted);
        } catch (BrokenBarrierException | TimeoutException failure) {
            throw new AssertionError("Competing transaction did not reach the barrier", failure);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(WAIT_SECONDS, TimeUnit.SECONDS)).as("Expected concurrent operation to finish").isTrue();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while coordinating handler execution", interrupted);
        }
    }

    private static void shutdown(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }
}
