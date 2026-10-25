package io.durableflow;

import io.durableflow.api.ApiException;
import io.durableflow.worker.*;
import io.durableflow.worker.retry.*;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.engine.WorkflowEngine;
import io.durableflow.workflow.history.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.task.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.net.URI;
import java.net.http.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;
import static org.assertj.core.api.Assertions.*;
import static io.durableflow.workflow.history.WorkflowEventType.*;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WorkflowHistoryIntegrationTest {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.6");
    @DynamicPropertySource static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
    @Autowired WorkflowDefinitionService definitions;
    @Autowired WorkflowRunService runs;
    @Autowired WorkflowRunRepository runRows;
    @Autowired ActivityExecutionRepository activities;
    @Autowired ActivityTaskRepository tasks;
    @Autowired WorkflowTaskOrchestrator orchestrator;
    @Autowired TaskTransactions transactions;
    @Autowired WorkflowHistoryService history;
    @Autowired WorkflowEventRepository events;
    @Autowired WorkflowHistoryWriter writer;
    @Autowired WorkflowEngine engine;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager transactionManager;
    @LocalServerPort int port;

    @BeforeEach void clean() { jdbc.execute("TRUNCATE model_route_attempt, model_route, agent_iteration, agent_execution, fake_tool_effect, tool_request, workflow_schedule_occurrence, workflow_schedule, child_workflow_execution, compensation_execution, workflow_signal_delivery, workflow_signal_wait, workflow_timer, workflow_event, activity_task, activity_execution, workflow_run, workflow_definition"); }

    @Test
    void timeoutRetryReclaimAndSuccessProduceOrderedImmutableDurableTimeline() {
        UUID run = start(List.of(step("payment", "chargePayment"), step("shipment", "createShipment", "payment")));
        assertThat(types(run)).containsExactly(WORKFLOW_STARTED);
        var first = history.read(run);
        orchestrator.advanceRun(run);
        orchestrator.advanceRun(run);
        assertThat(types(run)).containsExactly(WORKFLOW_STARTED, ACTIVITY_SCHEDULED);
        var a = transactions.claim("A").orElseThrow();
        int count = history.read(run).size();
        assertThat(transactions.heartbeat(a)).isTrue();
        assertThat(transactions.claim("no-work")).isEmpty();
        assertThat(history.read(run)).hasSize(count);
        transactions.handleFailure(a, new ActivityTimeoutException(60000));
        var retry = history.read(run).getLast();
        assertThat(retry.eventType()).isEqualTo(RETRY_SCHEDULED);
        assertThat(retry.metadata()).containsKeys("availableAt", "delayMillis", "attemptCount", "classification");
        jdbc.update("UPDATE activity_task SET available_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?", a.taskId());
        var b = transactions.claim("B").orElseThrow();
        jdbc.update("UPDATE activity_task SET lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?", b.taskId());
        var c = transactions.claim("C").orElseThrow();
        assertThat(history.read(run).getLast().metadata()).containsEntry("previousWorkerId", "B").containsEntry("workerId", "C");
        var afterReclaim = history.read(run);
        assertThatThrownBy(() -> transactions.complete(b, Map.of("stale", true))).isInstanceOf(OwnershipLostException.class);
        assertThatThrownBy(() -> transactions.handleFailure(b, new ActivityTimeoutException(60000)))
                .isInstanceOf(OwnershipLostException.class);
        assertThat(history.read(run)).isEqualTo(afterReclaim);
        transactions.complete(c, Map.of("paymentId", "safe-result"));
        orchestrator.advanceRun(run);
        var shipment = transactions.claim("D").orElseThrow();
        transactions.complete(shipment, Map.of("shipmentId", "safe-shipment"));
        orchestrator.advanceRun(run);
        orchestrator.advanceRun(run);
        assertThat(types(run)).containsExactly(WORKFLOW_STARTED, ACTIVITY_SCHEDULED, TASK_CLAIMED,
                ACTIVITY_TIMED_OUT, RETRY_SCHEDULED, TASK_CLAIMED, TASK_RECLAIMED, ACTIVITY_COMPLETED,
                ACTIVITY_SCHEDULED, TASK_CLAIMED, ACTIVITY_COMPLETED, WORKFLOW_COMPLETED);
        var timeline = history.read(run);
        assertThat(timeline).extracting(WorkflowEventResponse::sequence)
                .containsExactlyElementsOf(LongStream.rangeClosed(1, timeline.size()).boxed().toList());
        assertThat(timeline.subList(0, first.size())).isEqualTo(first);
        assertThat(timeline.get(2).activityId()).isEqualTo("payment");
        assertThat(timeline).allSatisfy(event -> assertThat(event.timestamp()).isNotNull());
        assertThat(timeline.toString()).doesNotContain("secret-test-token", "safe-result", "safe-shipment");
        var fresh = new WorkflowHistoryService(runRows, activities, events);
        assertThat(fresh.read(run)).isEqualTo(timeline);
    }

    @Test
    void terminalFailuresEmitActivityAndWorkflowFailureOnceWithoutRawMessages() {
        UUID run = start(List.of(step("payment", "chargePayment")));
        orchestrator.advanceRun(run);
        var claim = transactions.claim("A").orElseThrow();
        transactions.handleFailure(claim, new IllegalArgumentException("secret-test-token"));
        orchestrator.advanceRun(run);
        assertThat(types(run)).containsExactly(WORKFLOW_STARTED, ACTIVITY_SCHEDULED, TASK_CLAIMED, ACTIVITY_FAILED, WORKFLOW_FAILED);
        assertThat(history.read(run).toString()).doesNotContain("secret-test-token");
    }

    @Test
    void concurrentClaimsAndCompletionsSerializePerRunSequences() throws Exception {
        UUID run = start(List.of(step("A", "chargePayment"), step("B", "reserveInventory")));
        orchestrator.advanceRun(run);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var together = new CyclicBarrier(2);
            var a = executor.submit(() -> { together.await(10, TimeUnit.SECONDS); return transactions.claim("A").orElseThrow(); });
            var b = executor.submit(() -> { together.await(10, TimeUnit.SECONDS); return transactions.claim("B").orElseThrow(); });
            var claimA = a.get(10, TimeUnit.SECONDS); var claimB = b.get(10, TimeUnit.SECONDS);
            var finish = new CyclicBarrier(2);
            var doneA = executor.submit(() -> { finish.await(10, TimeUnit.SECONDS); transactions.complete(claimA, Map.of()); return true; });
            var doneB = executor.submit(() -> { finish.await(10, TimeUnit.SECONDS); transactions.complete(claimB, Map.of()); return true; });
            assertThat(doneA.get(10, TimeUnit.SECONDS)).isTrue(); assertThat(doneB.get(10, TimeUnit.SECONDS)).isTrue();
            orchestrator.advanceRun(run);
            var timeline = history.read(run);
            assertThat(timeline).hasSize(8);
            assertThat(timeline).extracting(WorkflowEventResponse::sequence).doesNotHaveDuplicates().isSorted();
            assertThat(timeline).filteredOn(e -> e.eventType() == TASK_CLAIMED).hasSize(2);
            assertThat(timeline).filteredOn(e -> e.eventType() == ACTIVITY_COMPLETED).hasSize(2);
        } finally { executor.shutdownNow(); assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue(); }
    }

    @Test
    void outerStateRollbackRemovesStartedEventAndNewRun() {
        String name = register(List.of(step("payment", "chargePayment")));
        var id = new AtomicReference<UUID>();
        assertThatThrownBy(() -> new TransactionTemplate(transactionManager).executeWithoutResult(tx -> {
            id.set(runs.start(new StartWorkflowRunRequest(name, 1, Map.of())).runId());
            assertThat(history.read(id.get())).hasSize(1);
            throw new IllegalStateException("rollback after event insert");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(runRows.existsById(id.get())).isFalse();
        assertThat(events.findByWorkflowRunIdOrderBySequenceNumberAsc(id.get())).isEmpty();
    }

    @Test
    void eventInsertionFailureRollsBackClaimAndCounter() {
        UUID run = start(List.of(step("payment", "chargePayment")));
        orchestrator.advanceRun(run);
        var before = history.read(run);
        rejectEvent(TASK_CLAIMED);
        try {
            assertThatThrownBy(() -> transactions.claim("A")).isInstanceOf(RuntimeException.class);
            assertThat(tasks.findByRunId(run).getFirst().getStatus()).isEqualTo(ActivityTaskStatus.READY);
            assertThat(activities.findByWorkflowRunIdOrderByActivityIdAsc(run).getFirst().getAttemptCount()).isZero();
            assertThat(history.read(run)).isEqualTo(before);
            assertThat(jdbc.queryForObject("SELECT history_sequence FROM workflow_run WHERE id=?", Long.class, run)).isEqualTo(2);
        } finally { dropRejection(); }
        transactions.claim("A").orElseThrow();
        assertThat(history.read(run).getLast().sequence()).isEqualTo(3);
    }

    @Test
    void eventFailureRollsBackCompletionAndTimeoutRetryIncludingEarlierTimeoutEvent() {
        UUID run = start(List.of(step("payment", "chargePayment")));
        orchestrator.advanceRun(run);
        var claim = transactions.claim("A").orElseThrow();
        var before = history.read(run);
        rejectEvent(ACTIVITY_COMPLETED);
        try {
            assertThatThrownBy(() -> transactions.complete(claim, Map.of("result", "uncommitted"))).isInstanceOf(RuntimeException.class);
            assertThat(tasks.findById(claim.taskId()).orElseThrow().getStatus()).isEqualTo(ActivityTaskStatus.CLAIMED);
            assertThat(activities.findById(claim.activityExecutionId()).orElseThrow().getOutputJson()).isNull();
            assertThat(history.read(run)).isEqualTo(before);
        } finally { dropRejection(); }
        rejectEvent(RETRY_SCHEDULED);
        try {
            assertThatThrownBy(() -> transactions.handleFailure(claim, new ActivityTimeoutException(60000))).isInstanceOf(RuntimeException.class);
            assertThat(tasks.findById(claim.taskId()).orElseThrow().getStatus()).isEqualTo(ActivityTaskStatus.CLAIMED);
            assertThat(activities.findById(claim.activityExecutionId()).orElseThrow().getErrorMessage()).isNull();
            assertThat(history.read(run)).isEqualTo(before);
        } finally { dropRejection(); }
    }

    @Test
    void deferredStateFailureAtCommitLeavesNoPhantomClaimEvent() {
        UUID run = start(List.of(step("payment", "chargePayment")));
        orchestrator.advanceRun(run);
        var before = history.read(run);
        jdbc.execute("CREATE FUNCTION reject_claim_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected state failure'; END $$");
        jdbc.execute("CREATE CONSTRAINT TRIGGER reject_claim_commit AFTER UPDATE ON activity_task DEFERRABLE INITIALLY DEFERRED "
                + "FOR EACH ROW WHEN (NEW.status = 'CLAIMED') EXECUTE FUNCTION reject_claim_commit()");
        try {
            assertThatThrownBy(() -> transactions.claim("A")).isInstanceOf(RuntimeException.class);
            assertThat(history.read(run)).isEqualTo(before);
            assertThat(tasks.findByRunId(run).getFirst().getStatus()).isEqualTo(ActivityTaskStatus.READY);
        } finally {
            jdbc.execute("DROP TRIGGER reject_claim_commit ON activity_task");
            jdbc.execute("DROP FUNCTION reject_claim_commit()");
        }
    }

    @Test
    void failedStartedEventRollsBackRunAndAllLogicalActivities() {
        String name = register(List.of(step("payment", "chargePayment")));
        rejectEvent(WORKFLOW_STARTED);
        try {
            assertThatThrownBy(() -> runs.start(new StartWorkflowRunRequest(name, 1, Map.of())))
                    .isInstanceOf(RuntimeException.class);
            assertThat(runRows.count()).isZero();
            assertThat(activities.count()).isZero();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_event", Long.class)).isZero();
        } finally { dropRejection(); }
    }

    @Test
    void failedSiblingOutcomeDoesNotRepeatWorkflowFailure() {
        UUID run = start(List.of(step("A", "chargePayment"), step("B", "reserveInventory")));
        orchestrator.advanceRun(run);
        var a = transactions.claim("A").orElseThrow();
        var b = transactions.claim("B").orElseThrow();
        transactions.fail(a, "first failure");
        transactions.fail(b, "second failure");
        assertThat(types(run)).filteredOn(type -> type == WORKFLOW_FAILED).hasSize(1);
        assertThat(types(run)).filteredOn(type -> type == ACTIVITY_FAILED).hasSize(2);
    }

    @Test
    void historyEndpointReturnsDtosAndExistingSnapshotRemainsSeparate() throws Exception {
        UUID run = start(List.of(step("payment", "chargePayment")));
        orchestrator.advanceRun(run);
        var response = get("/api/workflow-runs/" + run + "/events");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"sequence\":1", "WORKFLOW_STARTED", "timestamp", "metadata");
        assertThat(response.body().indexOf("\"sequence\":1")).isLessThan(response.body().indexOf("\"sequence\":2"));
        assertThat(get("/api/workflow-runs/" + run).body()).doesNotContain("WORKFLOW_STARTED", "sequence");
        assertThat(get("/api/workflow-runs/" + UUID.randomUUID() + "/events").statusCode()).isEqualTo(404);
        assertThatThrownBy(() -> history.read(UUID.randomUUID())).isInstanceOfSatisfying(ApiException.class,
                failure -> assertThat(failure.status()).isEqualTo(HttpStatus.NOT_FOUND));
    }

    @Test
    void historyAppendCannotOpenAnIndependentTransaction() {
        assertThatThrownBy(() -> writer.append(UUID.randomUUID(), null, WORKFLOW_STARTED, Map.of()))
                .isInstanceOf(org.springframework.transaction.IllegalTransactionStateException.class);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_event", Long.class)).isZero();
    }

    @Test
    void legacyLocalExecutionAlsoRecordsItsActualTransitions() {
        UUID run = start(List.of(step("payment", "chargePayment")));
        engine.executeRun(run);
        assertThat(types(run)).containsExactly(WORKFLOW_STARTED, ACTIVITY_STARTED, ACTIVITY_COMPLETED, WORKFLOW_COMPLETED);
    }

    private UUID start(List<StepDefinition> steps) {
        return runs.start(new StartWorkflowRunRequest(register(steps), 1, Map.of("token", "secret-test-token"))).runId();
    }
    private String register(List<StepDefinition> steps) {
        String name = "History-" + UUID.randomUUID();
        definitions.register(new RegisterWorkflowDefinitionRequest(name, 1, steps)); return name;
    }
    private StepDefinition step(String id, String activity, String... dependencies) { return new StepDefinition(id, activity, List.of(dependencies)); }
    private List<WorkflowEventType> types(UUID run) { return history.read(run).stream().map(WorkflowEventResponse::eventType).toList(); }
    private HttpResponse<String> get(String path) throws Exception {
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }
    private void rejectEvent(WorkflowEventType type) {
        jdbc.execute("CREATE FUNCTION reject_history_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "IF NEW.event_type = '" + type.name() + "' THEN RAISE EXCEPTION 'injected history failure'; END IF; RETURN NEW; END $$");
        jdbc.execute("CREATE TRIGGER reject_history_event BEFORE INSERT ON workflow_event FOR EACH ROW EXECUTE FUNCTION reject_history_event()");
    }
    private void dropRejection() {
        jdbc.execute("DROP TRIGGER reject_history_event ON workflow_event"); jdbc.execute("DROP FUNCTION reject_history_event()");
    }
}
