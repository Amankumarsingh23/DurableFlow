package io.durableflow;

import io.durableflow.worker.*;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.task.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

@Testcontainers
@SpringBootTest
class TaskLeaseIntegrationTest {
    private static final int WAIT = 15;
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.6");

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
    @Autowired ActivityTaskRepository tasks;
    @Autowired ActivityExecutionRepository activities;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager transactionManager;

    @BeforeEach
    void cleanOwnContainer() {
        jdbc.execute("TRUNCATE workflow_timer, workflow_event, activity_task, activity_execution, workflow_run, workflow_definition");
    }

    @Test
    void initialClaimGetsDatabaseTimeLeaseAndCannotBeReclaimedWhileUnexpired() {
        var runId = ready();
        Instant before = dbNow();
        var claim = transactions.claim("A").orElseThrow();
        var task = tasks.findById(claim.taskId()).orElseThrow();
        assertThat(task.getClaimedAt()).isBetween(before, dbNow());
        assertThat(task.getLeaseExpiresAt()).isEqualTo(task.getClaimedAt().plusSeconds(30));
        assertThat(task.getLeaseGeneration()).isEqualTo(1);
        assertThat(claim.leaseGeneration()).isEqualTo(1);
        assertThat(transactions.claim("B")).isEmpty();
        assertThat(activities.findByWorkflowRunIdOrderByActivityIdAsc(runId).getFirst().getStatus())
                .isEqualTo(ActivityExecutionStatus.RUNNING);
    }

    @Test
    void ownerHeartbeatExtendsDeadlineWithoutChangingAttemptStartOrGeneration() {
        ready();
        var claim = transactions.claim("A").orElseThrow();
        // Set a deterministic near-future deadline; no test waits for a wall-clock sleep.
        jdbc.update("UPDATE activity_task SET lease_expires_at = clock_timestamp() + INTERVAL '10 seconds' WHERE id = ?", claim.taskId());
        var before = tasks.findById(claim.taskId()).orElseThrow();
        assertThat(transactions.heartbeat(claim)).isTrue();
        var renewed = tasks.findById(claim.taskId()).orElseThrow();
        assertThat(renewed.getLeaseExpiresAt()).isAfter(before.getLeaseExpiresAt());
        assertThat(renewed.getClaimedAt()).isEqualTo(before.getClaimedAt());
        assertThat(renewed.getLeaseGeneration()).isEqualTo(before.getLeaseGeneration());
        assertThat(transactions.claim("B")).isEmpty(); // A healthy renewed lease is not a reclaim candidate.
    }

    @Test
    void wrongWorkerAndExpiredOwnerCannotRenew() {
        ready();
        var owner = transactions.claim("A").orElseThrow();
        var wrong = copyOwner(owner, "B");
        Instant expiry = tasks.findById(owner.taskId()).orElseThrow().getLeaseExpiresAt();
        assertThat(transactions.heartbeat(wrong)).isFalse();
        assertThat(tasks.findById(owner.taskId()).orElseThrow().getLeaseExpiresAt()).isEqualTo(expiry);
        expire(owner.taskId());
        assertThat(transactions.heartbeat(owner)).isFalse(); // No resurrection even before a replacement claims.
    }

    @Test
    void expiredClaimTransfersOwnershipWithoutResettingLogicalExecution() {
        var runId = ready();
        var a = transactions.claim("A").orElseThrow();
        var before = activities.findById(a.activityExecutionId()).orElseThrow();
        expire(a.taskId());
        var b = transactions.claim("B").orElseThrow();
        assertThat(b.taskId()).isEqualTo(a.taskId());
        assertThat(b.activityExecutionId()).isEqualTo(a.activityExecutionId());
        assertThat(b.workerId()).isEqualTo("B");
        assertThat(b.leaseGeneration()).isEqualTo(2);
        assertThat(b.leaseExpiresAt()).isAfter(dbNow());
        var logical = activities.findById(a.activityExecutionId()).orElseThrow();
        assertThat(logical.getStatus()).isEqualTo(ActivityExecutionStatus.RUNNING);
        assertThat(logical.getAttemptCount()).isEqualTo(2);
        assertThat(logical.getStartedAt()).isEqualTo(before.getStartedAt());
        assertThat(logical.getInputJson()).isEqualTo(before.getInputJson());
        assertThat(tasks.count()).isEqualTo(1);
        assertThat(activities.count()).isEqualTo(1);
        assertThat(runs.get(runId).status()).isEqualTo(WorkflowRunStatus.RUNNING);
    }

    @Test
    void exactlyOneWorkerWinsConcurrentReclamation() throws Exception {
        ready();
        var a = transactions.claim("A").orElseThrow();
        expire(a.taskId());
        var together = new CyclicBarrier(2);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var b = executor.submit(() -> { together.await(WAIT, TimeUnit.SECONDS); return transactions.claim("B"); });
            var c = executor.submit(() -> { together.await(WAIT, TimeUnit.SECONDS); return transactions.claim("C"); });
            var outcomes = List.of(b.get(WAIT, TimeUnit.SECONDS), c.get(WAIT, TimeUnit.SECONDS));
            assertThat(outcomes).filteredOn(Optional::isPresent).hasSize(1);
            assertThat(outcomes).filteredOn(Optional::isEmpty).hasSize(1);
            var task = tasks.findById(a.taskId()).orElseThrow();
            assertThat(task.getWorkerId()).isIn("B", "C");
            assertThat(task.getLeaseGeneration()).isEqualTo(2);
            assertThat(activities.findById(a.activityExecutionId()).orElseThrow().getAttemptCount()).isEqualTo(2);
        } finally { shutdown(executor); }
    }

    @Test
    void staleOwnerCannotCompleteFailOrRenewButCurrentOwnerCanComplete() {
        var runId = ready();
        var a = transactions.claim("A").orElseThrow();
        expire(a.taskId());
        var b = transactions.claim("B").orElseThrow();
        var before = rows();
        assertThatThrownBy(() -> transactions.complete(a, Map.of("result", "stale")))
                .isInstanceOf(OwnershipLostException.class);
        assertThatThrownBy(() -> transactions.fail(a, "stale failure")).isInstanceOf(OwnershipLostException.class);
        assertThat(transactions.heartbeat(a)).isFalse();
        assertThat(rows()).isEqualTo(before);
        assertThat(runs.get(runId).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        transactions.complete(b, Map.of("result", "current"));
        orchestrator.advanceRun(runId);
        assertThat(tasks.findById(b.taskId()).orElseThrow().getStatus()).isEqualTo(ActivityTaskStatus.COMPLETED);
        assertThat(runs.get(runId).activities().getFirst().output()).containsEntry("result", "current");
        assertThat(runs.get(runId).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
    }

    @Test
    void generationRejectsOldAttemptEvenWhenWorkerIdIsReused() {
        ready();
        var a = transactions.claim("same-id").orElseThrow();
        expire(a.taskId());
        var b = transactions.claim("same-id").orElseThrow();
        assertThat(b.leaseGeneration()).isGreaterThan(a.leaseGeneration());
        assertThat(transactions.heartbeat(a)).isFalse();
        assertThatThrownBy(() -> transactions.complete(a, Map.of())).isInstanceOf(OwnershipLostException.class);
        assertThat(transactions.heartbeat(b)).isTrue();
        transactions.complete(b, Map.of("generation", 2));
        assertThat(tasks.findById(b.taskId()).orElseThrow().getStatus()).isEqualTo(ActivityTaskStatus.COMPLETED);
    }

    @Test
    void expiredOwnerOutcomeRollsBackAlreadyFlushedLogicalChanges() {
        var runId = ready();
        var a = transactions.claim("A").orElseThrow();
        expire(a.taskId());
        var before = rows();
        // Same worker and generation still match, so only the final DB-time predicate can reject these.
        assertThatThrownBy(() -> transactions.complete(a, Map.of("result", "too late")))
                .isInstanceOf(OwnershipLostException.class);
        assertThatThrownBy(() -> transactions.fail(a, "too late")).isInstanceOf(OwnershipLostException.class);
        assertThat(rows()).isEqualTo(before);
        assertThat(runs.get(runId).status()).isEqualTo(WorkflowRunStatus.RUNNING);
    }

    @Test
    void ownershipIsRecheckedAfterLogicalFlushRatherThanOnlyAtTransactionEntry() {
        ready();
        var claim = transactions.claim("A").orElseThrow();
        var before = rows();
        // Force expiry between the logical write and final task write without sleeping.
        jdbc.execute("""
                CREATE FUNCTION expire_during_outcome() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    UPDATE activity_task SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'
                    WHERE activity_execution_id = NEW.id;
                    RETURN NEW;
                END $$
                """);
        jdbc.execute("CREATE TRIGGER expire_during_outcome BEFORE UPDATE ON activity_execution "
                + "FOR EACH ROW EXECUTE FUNCTION expire_during_outcome()");
        try {
            assertThatThrownBy(() -> transactions.complete(claim, Map.of("result", "late")))
                    .isInstanceOf(OwnershipLostException.class);
            assertThat(rows()).isEqualTo(before); // Even trigger changes roll back with the stale outcome.
        } finally {
            jdbc.execute("DROP TRIGGER expire_during_outcome ON activity_execution");
            jdbc.execute("DROP FUNCTION expire_during_outcome()");
        }
    }

    @Test
    void lockedExpiredHeadIsSkippedInFavorOfIndependentReadyWork() throws Exception {
        ready();
        var expired = transactions.claim("A").orElseThrow();
        expire(expired.taskId());
        var otherRun = ready();
        var locked = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var holder = executor.submit(() -> new TransactionTemplate(transactionManager).executeWithoutResult(tx -> {
                tasks.findByIdForUpdate(expired.taskId()).orElseThrow();
                locked.countDown();
                await(release);
            }));
            await(locked);
            var claimant = executor.submit(() -> transactions.claim("B").orElseThrow());
            var claim = claimant.get(WAIT, TimeUnit.SECONDS);
            assertThat(claim.runId()).isEqualTo(otherRun);
            assertThat(holder.isDone()).isFalse();
            release.countDown();
            holder.get(WAIT, TimeUnit.SECONDS);
        } finally { release.countDown(); shutdown(executor); }
    }

    private UUID ready() {
        String name = "Lease-" + UUID.randomUUID();
        definitions.register(new RegisterWorkflowDefinitionRequest(name, 1,
                List.of(new StepDefinition("payment", "chargePayment", List.of()))));
        var run = runs.start(new StartWorkflowRunRequest(name, 1, Map.of("orderId", "LEASE-DEMO")));
        orchestrator.advanceRun(run.runId());
        return run.runId();
    }

    private void expire(UUID taskId) {
        jdbc.update("UPDATE activity_task SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", taskId);
    }

    private Instant dbNow() {
        return Objects.requireNonNull(jdbc.queryForObject("SELECT clock_timestamp()", OffsetDateTime.class)).toInstant();
    }

    private List<Map<String, Object>> rows() {
        return jdbc.queryForList("SELECT t.*, a.status AS logical_status, a.output_json, a.error_message, "
                + "a.attempt_count, a.updated_at AS logical_updated_at, a.completed_at "
                + "FROM activity_task t JOIN activity_execution a ON a.id = t.activity_execution_id ORDER BY t.id");
    }

    private ClaimedTask copyOwner(ClaimedTask claim, String worker) {
        return new ClaimedTask(claim.taskId(), claim.runId(), claim.activityExecutionId(), claim.activityType(),
                claim.input(), worker, claim.leaseGeneration(), claim.leaseExpiresAt());
    }

    private static void await(CountDownLatch latch) {
        try { assertThat(latch.await(WAIT, TimeUnit.SECONDS)).isTrue(); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
    }

    private static void shutdown(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(WAIT, TimeUnit.SECONDS)).isTrue();
    }
}
