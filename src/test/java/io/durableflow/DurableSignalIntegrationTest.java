package io.durableflow;

import io.durableflow.api.ApiException;
import io.durableflow.worker.*;
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
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.*;
import java.util.*;
import java.util.concurrent.*;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "durableflow.timers.polling-enabled=false")
class DurableSignalIntegrationTest {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.6");
    @DynamicPropertySource static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
    @Autowired WorkflowDefinitionService definitions;
    @Autowired WorkflowRunService runs;
    @Autowired WorkflowTaskOrchestrator orchestrator;
    @Autowired SignalReceiver receiver;
    @Autowired SignalRepository signals;
    @Autowired WorkflowHistoryService history;
    @Autowired ActivityTaskRepository tasks;
    @Autowired TaskTransactions transactions;
    @Autowired TaskWorker worker;
    @Autowired JdbcTemplate jdbc;
    @Autowired TimerTransactions timerDispatcher;
    @Autowired WorkflowTimerRepository timers;
    @LocalServerPort int port;
    @BeforeEach void clean() {
        jdbc.execute("TRUNCATE workflow_signal_delivery, workflow_signal_wait, workflow_timer, workflow_event, activity_task, activity_execution, workflow_run, workflow_definition");
    }

    @Test
    void waitingSignalBlocksWorkersThenReceiptMaterializesDownstreamExactlyOnce() {
        UUID run = approval(); orchestrator.advanceRun(run);
        var waiting = signals.waits(run).getFirst();
        assertThat(waiting.status()).isEqualTo(SignalWait.Status.WAITING);
        assertThat(worker.runOne()).isFalse(); assertThat(tasks.findByRunId(run)).isEmpty();
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        var request = request("delivery-1", "approval");
        var receipt = receiver.receive(run, request);
        assertThat(receipt.status()).isEqualTo(SignalReceipt.Status.CONSUMED);
        assertThat(signals.waits(run).getFirst().id()).isEqualTo(waiting.id());
        assertThat(signals.waits(run).getFirst().receivedAt()).isNotNull();
        assertThat(payloadDecision(run, "approval")).isEqualTo("approved");
        assertThat(tasks.findByRunId(run)).hasSize(1);
        var before = history.read(run);
        assertThat(receiver.receive(run, request)).isEqualTo(receipt);
        orchestrator.advanceRun(run);
        assertThat(history.read(run)).isEqualTo(before);
        assertThat(tasks.findByRunId(run)).hasSize(1);
        assertThat(worker.runOne()).isTrue();
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        assertOneReceipt(run);
    }

    @Test
    void earlySignalPersistsBeforeWaitAndLaterConsumptionHandlesReverseDefinitionOrder() {
        UUID run = earlyApproval();
        var receipt = receiver.receive(run, request("early", "approval"));
        assertThat(receipt.status()).isEqualTo(SignalReceipt.Status.BUFFERED);
        assertThat(signals.waits(run)).isEmpty();
        assertThat(signals.delivery(run, "early").orElseThrow().consumedAt()).isNull();
        assertThat(worker.runOne()).isTrue(); // Submit completed; its advancement must consume the buffered signal.
        assertThat(signals.waits(run).getFirst().status()).isEqualTo(SignalWait.Status.RECEIVED);
        assertThat(signals.delivery(run, "early").orElseThrow().id()).isEqualTo(receipt.id());
        assertThat(signals.delivery(run, "early").orElseThrow().consumedAt()).isNotNull();
        assertThat(payloadDecision(run, "approval")).isEqualTo("approved");
        assertThat(tasks.findByRunId(run)).hasSize(2); // Completed submit plus READY finalize, despite reverse node order.
        assertThat(worker.runOne()).isTrue(); assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        assertThat(history.read(run)).filteredOn(e -> e.eventType() == WorkflowEventType.SIGNAL_BUFFERED).hasSize(1);
        assertOneReceipt(run);
    }

    @Test
    void canonicalPayloadDedupesAndDifferentPayloadTargetOrDeliveryIdentityCannotReplaceSignal() {
        UUID run = start(List.of(signal("approval"), signal("other")));
        orchestrator.advanceRun(run);
        var original = receiver.receive(run, new ReceiveSignalRequest("K", "approval", Map.of("amount", 1, "decision", "approved")));
        assertThat(receiver.receive(run, new ReceiveSignalRequest("K", "approval", Map.of("decision", "approved", "amount", new BigDecimal("1.00")))))
                .isEqualTo(original);
        conflict(() -> receiver.receive(run, new ReceiveSignalRequest("K", "approval", Map.of("amount", 2))));
        conflict(() -> receiver.receive(run, request("K", "other")));
        conflict(() -> receiver.receive(run, request("replacement", "approval")));
        assertThat(payloadDecision(run, "approval")).isEqualTo("approved");
        assertThat(signals.waits(run)).filteredOn(wait -> wait.status() == SignalWait.Status.RECEIVED).hasSize(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_signal_delivery WHERE workflow_run_id=?", Long.class, run)).isEqualTo(1);
        assertThat(history.read(run)).filteredOn(e -> e.eventType() == WorkflowEventType.SIGNAL_RECEIVED).hasSize(1);
    }

    @Test
    void concurrentDuplicateDeliveriesHaveOneDurableReceiptEvenIfFirstCompletesRun() throws Exception {
        UUID run = start(List.of(signal("approval"))); orchestrator.advanceRun(run);
        var results = race(() -> receiver.receive(run, request("K", "approval")), () -> receiver.receive(run, request("K", "approval")));
        assertThat(results.getFirst()).isEqualTo(results.getLast());
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED); assertOneReceipt(run);
    }

    @Test
    void concurrentAdvanceAndDeliveryCannotLoseSignalOrDuplicateWait() throws Exception {
        UUID run = approval();
        race(() -> { orchestrator.advanceRun(run); return true; }, () -> { receiver.receive(run, request("K", "approval")); return true; });
        assertThat(signals.waits(run)).hasSize(1).allSatisfy(wait -> assertThat(wait.status()).isEqualTo(SignalWait.Status.RECEIVED));
        assertThat(tasks.findByRunId(run)).hasSize(1); assertOneReceipt(run);
    }

    @Test
    void deliveryRacingUpstreamCompletionCannotFallBetweenWaitCreationAndConsumption() throws Exception {
        UUID run = earlyApproval(); orchestrator.advanceRun(run);
        var claim = transactions.claim("submit-worker").orElseThrow();
        race(() -> { transactions.complete(claim, Map.of("submitted", true)); orchestrator.advanceRun(run); return true; },
                () -> { receiver.receive(run, request("K", "approval")); return true; });
        assertThat(signals.waits(run).getFirst().status()).isEqualTo(SignalWait.Status.RECEIVED);
        assertThat(tasks.findByRunId(run)).hasSize(2); assertOneReceipt(run);
    }

    @Test
    void twoBufferedSignalsUnlockReverseListedChainInOneAdvanceWithoutDuplicateTasks() {
        UUID run = start(List.of(activity("finish", "finalizeRequest", "second"), signal("second", "first"),
                signal("first", "submit"), activity("submit", "submitRequest")));
        receiver.receive(run, request("second-delivery", "second")); receiver.receive(run, request("first-delivery", "first"));
        assertThat(worker.runOne()).isTrue();
        assertThat(signals.waits(run)).hasSize(2).allSatisfy(wait -> assertThat(wait.status()).isEqualTo(SignalWait.Status.RECEIVED));
        assertThat(tasks.findByRunId(run)).hasSize(2);
        assertThat(worker.runOne()).isTrue(); assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
    }

    @Test
    void earlySignalBehindTimerIsConsumedAtomicallyWhenTimerFires() {
        UUID run = start(List.of(activity("finish", "finalizeRequest", "approval"), signal("approval", "wait"),
                new StepDefinition("wait", null, List.of(), NodeType.TIMER, 3600000L)));
        assertThat(receiver.receive(run, request("early", "approval")).status()).isEqualTo(SignalReceipt.Status.BUFFERED);
        assertThat(signals.waits(run)).isEmpty(); assertThat(worker.runOne()).isFalse();
        var timer = timers.findByRun(run).getFirst();
        jdbc.update("UPDATE workflow_timer SET wake_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?", timer.id());
        assertThat(timerDispatcher.fireOne()).isTrue();
        assertThat(signals.waits(run).getFirst().status()).isEqualTo(SignalWait.Status.RECEIVED);
        assertThat(tasks.findByRunId(run)).hasSize(1);
        assertThat(worker.runOne()).isTrue(); assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        assertOneReceipt(run);
    }

    @Test
    void newSignalsCannotResumeTerminalRunButAcceptedDeliveryCanBeReplayed() {
        UUID completed = start(List.of(signal("approval"))); var request = request("K", "approval");
        var accepted = receiver.receive(completed, request);
        var before = history.read(completed);
        assertThat(receiver.receive(completed, request)).isEqualTo(accepted);
        conflict(() -> receiver.receive(completed, request("new", "approval")));
        assertThat(history.read(completed)).isEqualTo(before);
        UUID failed = start(List.of(signal("approval"), activity("submit", "submitRequest")));
        orchestrator.advanceRun(failed); transactions.fail(transactions.claim("A").orElseThrow(), "permanent failure");
        conflict(() -> receiver.receive(failed, request("late", "approval")));
        assertThat(signals.delivery(failed, "late")).isEmpty();
        assertThat(signals.waits(failed).getFirst().status()).isEqualTo(SignalWait.Status.WAITING);
    }

    @Test
    void waitingStateSurvivesFreshContextAndReceivesWithoutLocalSubscription() {
        UUID run = approval(); orchestrator.advanceRun(run); var waiting = signals.waits(run).getFirst();
        try (var fresh = freshContext()) {
            assertThat(fresh.getBean(SignalRepository.class).waits(run)).containsExactly(waiting);
            assertThat(fresh.getBean(SignalReceiver.class).receive(run, request("after-restart", "approval")).status())
                    .isEqualTo(SignalReceipt.Status.CONSUMED);
        }
        assertThat(signals.waits(run).getFirst().id()).isEqualTo(waiting.id());
        assertThat(tasks.findByRunId(run)).hasSize(1); assertOneReceipt(run);
    }

    @Test
    void bufferedSignalSurvivesFreshContextAndIsConsumedAfterUpstreamWork() {
        UUID run = earlyApproval(); var buffered = receiver.receive(run, request("early", "approval"));
        try (var fresh = freshContext()) {
            assertThat(fresh.getBean(SignalRepository.class).delivery(run, "early").orElseThrow()).isEqualTo(buffered);
            assertThat(fresh.getBean(TaskWorker.class).runOne()).isTrue();
            assertThat(fresh.getBean(SignalRepository.class).waits(run).getFirst().status()).isEqualTo(SignalWait.Status.RECEIVED);
            assertThat(fresh.getBean(TaskWorker.class).runOne()).isTrue();
        }
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED); assertOneReceipt(run);
    }

    @Test
    void receiptOrDownstreamEventFailureRollsBackDeliveryWaitPayloadAndHistory() {
        UUID run = approval(); orchestrator.advanceRun(run); var before = history.read(run);
        for (var event : List.of(WorkflowEventType.SIGNAL_RECEIVED, WorkflowEventType.ACTIVITY_SCHEDULED)) {
            reject(event);
            try {
                assertThatThrownBy(() -> receiver.receive(run, request("K", "approval"))).isInstanceOf(RuntimeException.class);
                assertThat(signals.delivery(run, "K")).isEmpty();
                assertThat(signals.waits(run).getFirst().status()).isEqualTo(SignalWait.Status.WAITING);
                assertThat(payloadDecision(run, "approval")).isNull();
                assertThat(tasks.findByRunId(run)).isEmpty(); assertThat(history.read(run)).isEqualTo(before);
            } finally { dropRejection(); }
        }
        receiver.receive(run, request("K", "approval")); assertOneReceipt(run);
    }

    @Test
    void failedWaitMaterializationDoesNotLoseAlreadyBufferedDelivery() {
        UUID run = earlyApproval(); receiver.receive(run, request("early", "approval"));
        reject(WorkflowEventType.SIGNAL_RECEIVED);
        try {
            assertThatThrownBy(worker::runOne).isInstanceOf(RuntimeException.class);
            assertThat(signals.waits(run)).isEmpty();
            assertThat(signals.delivery(run, "early").orElseThrow().status()).isEqualTo(SignalReceipt.Status.BUFFERED);
            assertThat(tasks.findByRunId(run)).hasSize(1); // Submit outcome committed before its advance callback.
        } finally { dropRejection(); }
        orchestrator.advanceRun(run);
        assertThat(tasks.findByRunId(run)).hasSize(2); assertOneReceipt(run);
    }

    @Test
    void httpApiReturnsTypedReceiptRejectsInvalidTargetsAndKeepsPayloadOutOfStateAndHistory() throws Exception {
        UUID run = approval(); orchestrator.advanceRun(run);
        String body = "{\"signalId\":\"http-1\",\"signalNodeId\":\"approval\",\"payload\":{\"reference\":\"reference-42\"}}";
        var accepted = post(run, body);
        assertThat(accepted.statusCode()).isEqualTo(200); assertThat(accepted.body()).contains("CONSUMED", "consumedAt", "http-1");
        var before = history.read(run); assertThat(post(run, body).statusCode()).isEqualTo(200); assertThat(history.read(run)).isEqualTo(before);
        var snapshot = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/workflow-runs/" + run)).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(snapshot.body()).contains("signalWaits", "RECEIVED", "receivedAt").doesNotContain("reference-42");
        assertThat(history.read(run).toString()).doesNotContain("reference-42");
        assertThat(post(UUID.randomUUID(), body).statusCode()).isEqualTo(404);
        assertThat(post(run, "{\"signalId\":\"bad\",\"signalNodeId\":\"finish\",\"payload\":{}}").statusCode()).isEqualTo(400);
        assertThat(post(run, "{\"signalId\":\"missing\",\"signalNodeId\":\"unknown\",\"payload\":{}}").statusCode()).isEqualTo(400);
        assertThat(post(run, "{\"signalId\":\"bad\",\"signalNodeId\":\"approval\",\"payload\":null}").statusCode()).isEqualTo(400);
    }

    private UUID approval() { return start(List.of(activity("finish", "finalizeRequest", "approval"), signal("approval"))); }
    private UUID earlyApproval() { return start(List.of(activity("finish", "finalizeRequest", "approval"), signal("approval", "submit"), activity("submit", "submitRequest"))); }
    private UUID start(List<StepDefinition> steps) {
        String name = "Signal-" + UUID.randomUUID(); definitions.register(new RegisterWorkflowDefinitionRequest(name, 1, steps));
        return runs.start(new StartWorkflowRunRequest(name, 1, Map.of("request", "DEMO"))).runId();
    }
    private StepDefinition activity(String id, String type, String... dependencies) { return new StepDefinition(id, type, List.of(dependencies)); }
    private StepDefinition signal(String id, String... dependencies) { return new StepDefinition(id, null, List.of(dependencies), NodeType.SIGNAL, null); }
    private ReceiveSignalRequest request(String id, String node) { return new ReceiveSignalRequest(id, node, Map.of("decision", "approved")); }
    private String payloadDecision(UUID run, String node) { return jdbc.queryForObject("SELECT payload_json->>'decision' FROM workflow_signal_wait WHERE workflow_run_id=? AND signal_node_id=?", String.class, run, node); }
    private void assertOneReceipt(UUID run) {
        assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_signal_delivery WHERE workflow_run_id=?", Long.class, run)).isEqualTo(1);
        assertThat(history.read(run)).filteredOn(e -> e.eventType() == WorkflowEventType.SIGNAL_WAIT_STARTED).hasSize(1);
        assertThat(history.read(run)).filteredOn(e -> e.eventType() == WorkflowEventType.SIGNAL_RECEIVED).hasSize(1);
    }
    private void conflict(Runnable action) { assertThatThrownBy(action::run).isInstanceOfSatisfying(ApiException.class, failure -> assertThat(failure.status()).isEqualTo(HttpStatus.CONFLICT)); }
    private <T> List<T> race(Callable<T> a, Callable<T> b) throws Exception {
        var together = new CyclicBarrier(2); var executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> { together.await(10, TimeUnit.SECONDS); return a.call(); });
            var second = executor.submit(() -> { together.await(10, TimeUnit.SECONDS); return b.call(); });
            return List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS));
        } finally { executor.shutdownNow(); }
    }
    private ConfigurableApplicationContext freshContext() {
        return new SpringApplicationBuilder(DurableFlowApplication.class).web(WebApplicationType.NONE).run(
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(), "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(), "--durableflow.timers.polling-enabled=false");
    }
    private HttpResponse<String> post(UUID run, String body) throws Exception {
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/workflow-runs/" + run + "/signals"))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
    private void reject(WorkflowEventType type) {
        jdbc.execute("CREATE FUNCTION reject_signal_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "IF NEW.event_type='" + type.name() + "' THEN RAISE EXCEPTION 'injected signal failure'; END IF; RETURN NEW; END $$");
        jdbc.execute("CREATE TRIGGER reject_signal_event BEFORE INSERT ON workflow_event FOR EACH ROW EXECUTE FUNCTION reject_signal_event()");
    }
    private void dropRejection() { jdbc.execute("DROP TRIGGER reject_signal_event ON workflow_event"); jdbc.execute("DROP FUNCTION reject_signal_event()"); }
}
