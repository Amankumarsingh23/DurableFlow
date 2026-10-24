package io.durableflow;

import io.durableflow.api.ApiException;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.engine.WorkflowEngine;
import io.durableflow.workflow.engine.OrphanedActivityException;
import io.durableflow.workflow.run.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

@Testcontainers
@Import(WorkflowEngineIntegrationTest.Handlers.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WorkflowEngineIntegrationTest {
    static final Instant NOW = Instant.parse("2026-10-25T10:00:00Z");
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.6");

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired WorkflowEngine engine;
    @Autowired WorkflowDefinitionService definitions;
    @Autowired WorkflowRunService runService;
    @Autowired WorkflowRunRepository runs;
    @Autowired ActivityExecutionRepository activities;
    @Autowired JdbcTemplate jdbc;
    @Autowired Probe probe;
    @LocalServerPort int port;

    @BeforeEach
    void resetProbe() { probe.calls.clear(); probe.dependencies = Map.of(); }

    @Test
    void sequentialWorkflowPersistsCheckpointsAndOutputsInDependencyOrder() {
        probe.dependencies = Map.of("recordB", List.of("payment"), "recordC", List.of("inventory"));
        var run = start(List.of(step("shipment", "recordC", "inventory"),
                step("inventory", "recordB", "payment"), step("payment", "recordA")));
        assertThat(run.activities()).allSatisfy(a -> assertThat(a.status()).isEqualTo(ActivityExecutionStatus.PENDING));
        assertThat(probe.calls).isEmpty();
        var result = engine.executeRun(run.runId());
        assertThat(probe.calls).containsExactly("recordA", "recordB", "recordC");
        assertCompleted(result);
        assertThat(runService.get(run.runId()).activities()).allSatisfy(a -> assertThat(a.output()).isNotEmpty());
        assertThat(activities.findByWorkflowRunIdOrderByActivityIdAsc(run.runId())).allSatisfy(a -> {
            assertThat(a.getAttemptCount()).isEqualTo(1);
            assertThat(a.getStartedAt()).isEqualTo(NOW);
            assertThat(a.getCompletedAt()).isEqualTo(NOW);
            assertThat(a.getInputJson()).isEqualTo(run.input());
        });
        assertThatThrownBy(() -> engine.executeRun(run.runId())).isInstanceOf(ApiException.class)
                .hasMessageContaining("Only RUNNING");
        assertThat(probe.calls).hasSize(3);
    }

    @Test
    void branchingWorkflowWaitsForBothRootsAndExecutesThemSequentially() {
        probe.dependencies = Map.of("recordC", List.of("A", "B"));
        var run = start(List.of(step("join", "recordC", "A", "B"),
                step("B", "recordB"), step("A", "recordA")));
        assertCompleted(engine.executeRun(run.runId()));
        assertThat(probe.calls).containsExactly("recordB", "recordA", "recordC");
    }

    @Test
    void executionBudgetStopsBeforeClaimingMoreWorkAndCanFinalizeAtExactLimit() {
        probe.dependencies = Map.of("recordB", List.of("A"));
        var run = start(List.of(step("B", "recordB", "A"), step("A", "recordA")));
        var paused = engine.executeRun(run.runId(), 1);
        assertThat(paused.status()).isEqualTo(WorkflowRunStatus.RUNNING);
        assertThat(probe.calls).containsExactly("recordA");
        var pending = activities.findByWorkflowRunIdOrderByActivityIdAsc(run.runId()).get(1);
        assertThat(pending.getActivityId()).isEqualTo("B");
        assertThat(pending.getStatus()).isEqualTo(ActivityExecutionStatus.PENDING);
        assertThat(pending.getStartedAt()).isNull();
        assertThat(pending.getAttemptCount()).isZero();
        assertCompleted(engine.executeRun(run.runId(), 1));
        assertThat(probe.calls).containsExactly("recordA", "recordB");
    }

    @Test
    void invalidExecutionBudgetDoesNotChangeDurableState() {
        var run = start(List.of(step("A", "recordA")));
        assertThatThrownBy(() -> engine.executeRun(run.runId(), 0)).isInstanceOf(ApiException.class)
                .hasMessageContaining("positive");
        assertThatThrownBy(() -> engine.executeRun(run.runId(), -1)).isInstanceOf(ApiException.class);
        assertThat(probe.calls).isEmpty();
        var row = activities.findByWorkflowRunIdOrderByActivityIdAsc(run.runId()).getFirst();
        assertThat(row.getStatus()).isEqualTo(ActivityExecutionStatus.PENDING);
        assertThat(row.getAttemptCount()).isZero();
    }

    @Test
    void persistedFailedActivityIsNeverRetriedDuringRecovery() {
        var run = start(List.of(step("A", "recordA"), step("B", "recordB", "A")));
        jdbc.update("UPDATE activity_execution SET status = 'FAILED', error_message = 'previous failure' "
                + "WHERE workflow_run_id = ? AND activity_id = 'A'", run.runId());
        assertThatThrownBy(() -> engine.executeRun(run.runId())).isInstanceOf(ApiException.class)
                .hasMessageContaining("automatic retry is disabled");
        assertThat(runService.get(run.runId()).status()).isEqualTo(WorkflowRunStatus.FAILED);
        assertThat(probe.calls).isEmpty();
    }

    @Test
    void missingHandlerRecordsControlledFailureAndDoesNotExecuteDependents() {
        var run = start(List.of(step("missing", "unregistered"), step("blocked", "recordA", "missing")));
        var result = engine.executeRun(run.runId());
        assertFailed(result, "missing", "No activity handler registered");
        assertThat(probe.calls).isEmpty();
        assertThat(result.activities().stream().filter(a -> a.activityId().equals("blocked")).findFirst().orElseThrow().status())
                .isEqualTo(ActivityExecutionStatus.PENDING);
        assertThatThrownBy(() -> engine.executeRun(run.runId())).isInstanceOf(ApiException.class);
    }

    @Test
    void thrownHandlerExceptionFailsActivityAndRunWithoutRetry() {
        var run = start(List.of(step("failure", "throws"), step("blocked", "recordA", "failure")));
        assertFailed(engine.executeRun(run.runId()), "failure", "demo failure");
        assertThat(probe.calls).containsExactly("throws");
        assertThat(activities.findByWorkflowRunIdOrderByActivityIdAsc(run.runId()).stream()
                .filter(a -> a.getActivityId().equals("failure")).findFirst().orElseThrow().getAttemptCount()).isEqualTo(1);
    }

    @Test
    void interruptedStateRequiresRecoveryWithoutChangingItsUncertainOutcome() {
        var run = start(List.of(step("A", "recordA")));
        jdbc.update("UPDATE activity_execution SET status = 'RUNNING' WHERE workflow_run_id = ?", run.runId());
        assertThatThrownBy(() -> engine.executeRun(run.runId())).isInstanceOf(OrphanedActivityException.class)
                .hasMessageContaining("cannot safely determine");
        assertThat(runs.findById(run.runId()).orElseThrow().getStatus()).isEqualTo(WorkflowRunStatus.RUNNING);
        assertThat(activities.findByWorkflowRunIdOrderByActivityIdAsc(run.runId()).getFirst().getStatus())
                .isEqualTo(ActivityExecutionStatus.RUNNING);
        assertThat(probe.calls).isEmpty();
    }

    @Test
    void missingRowsAndCorruptStoredDefinitionsCannotCompleteRun() {
        var missing = start(List.of(step("A", "recordA")));
        jdbc.update("DELETE FROM activity_execution WHERE workflow_run_id = ?", missing.runId());
        assertThatThrownBy(() -> engine.executeRun(missing.runId())).isInstanceOf(ApiException.class)
                .hasMessageContaining("do not match");
        assertThat(runs.findById(missing.runId()).orElseThrow().getStatus()).isEqualTo(WorkflowRunStatus.FAILED);
        var corrupt = start(List.of(step("A", "recordA")));
        var definitionId = runs.findById(corrupt.runId()).orElseThrow().getWorkflowDefinitionId();
        jdbc.update("UPDATE workflow_definition SET definition_json = '{}'::jsonb WHERE id = ?", definitionId);
        assertThatThrownBy(() -> engine.executeRun(corrupt.runId())).isInstanceOf(ApiException.class)
                .hasMessageContaining("definition is invalid");
        assertThat(runs.findById(corrupt.runId()).orElseThrow().getStatus()).isEqualTo(WorkflowRunStatus.FAILED);
        assertThat(probe.calls).isEmpty();
    }

    @Test
    void temporaryExecutionEndpointRunsDemoAndGetExposesPersistedOutput() throws Exception {
        var run = start(List.of(step("payment", "chargePayment"), step("inventory", "reserveInventory", "payment"),
                step("shipment", "createShipment", "inventory")));
        var client = HttpClient.newHttpClient();
        var response = client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port
                        + "/api/workflow-runs/" + run.runId() + "/execute"))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("COMPLETED", "pay-demo-123", "res-demo-456", "ship-demo-789");
        var get = client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port
                + "/api/workflow-runs/" + run.runId())).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(get.statusCode()).isEqualTo(200);
        assertThat(get.body()).contains("pay-demo-123", "COMPLETED");
        var again = client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port
                        + "/api/workflow-runs/" + run.runId() + "/execute"))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(again.statusCode()).isEqualTo(409);
        var missing = client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port
                        + "/api/workflow-runs/" + UUID.randomUUID() + "/execute"))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(missing.statusCode()).isEqualTo(404);
    }

    private WorkflowRunResponse start(List<StepDefinition> steps) {
        String name = "Engine-" + UUID.randomUUID();
        definitions.register(new RegisterWorkflowDefinitionRequest(name, 1, steps));
        return runService.start(new StartWorkflowRunRequest(name, 1, Map.of("testKey", UUID.randomUUID().toString())));
    }

    private static StepDefinition step(String id, String handler, String... dependencies) {
        return new StepDefinition(id, handler, List.of(dependencies));
    }

    private void assertCompleted(WorkflowRunResponse result) {
        assertThat(result.status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        assertThat(result.activities()).allSatisfy(a -> assertThat(a.status()).isEqualTo(ActivityExecutionStatus.COMPLETED));
        assertThat(runs.findById(result.runId()).orElseThrow().getCompletedAt()).isEqualTo(NOW);
    }

    private void assertFailed(WorkflowRunResponse result, String id, String diagnostic) {
        assertThat(result.status()).isEqualTo(WorkflowRunStatus.FAILED);
        var failed = result.activities().stream().filter(a -> a.activityId().equals(id)).findFirst().orElseThrow();
        assertThat(failed.status()).isEqualTo(ActivityExecutionStatus.FAILED);
        assertThat(failed.errorMessage()).contains(diagnostic);
        assertThat(runs.findById(result.runId()).orElseThrow().getCompletedAt()).isEqualTo(NOW);
        assertThat(runService.get(result.runId()).activities()).contains(failed);
    }

    static class Probe {
        final JdbcTemplate jdbc;
        final List<String> calls = new ArrayList<>();
        Map<String, List<String>> dependencies = Map.of();
        Probe(JdbcTemplate jdbc) { this.jdbc = jdbc; }

        Map<String, Object> execute(String type, Map<String, Object> input) {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            UUID runId = jdbc.queryForObject("SELECT id FROM workflow_run WHERE input_json ->> 'testKey' = ?",
                    UUID.class, input.get("testKey"));
            assertThat(jdbc.queryForObject("SELECT status FROM workflow_run WHERE id = ?", String.class, runId))
                    .isEqualTo("RUNNING");
            assertThat(jdbc.queryForObject("SELECT count(*) FROM activity_execution WHERE workflow_run_id = ? AND status = 'RUNNING'",
                    Integer.class, runId)).isEqualTo(1);
            for (String dependency : dependencies.getOrDefault(type, List.of())) {
                assertThat(jdbc.queryForObject("SELECT status FROM activity_execution WHERE workflow_run_id = ? AND activity_id = ?",
                        String.class, runId, dependency)).isEqualTo("COMPLETED");
            }
            calls.add(type);
            if (type.equals("throws")) throw new IllegalStateException("demo failure");
            return Map.of("executed", type);
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Handlers {
        @Bean @Primary Clock testClock() { return Clock.fixed(NOW, ZoneOffset.UTC); }
        @Bean Probe probe(JdbcTemplate jdbc) { return new Probe(jdbc); }
        @Bean ActivityHandler recordA(Probe probe) { return handler("recordA", probe); }
        @Bean ActivityHandler recordB(Probe probe) { return handler("recordB", probe); }
        @Bean ActivityHandler recordC(Probe probe) { return handler("recordC", probe); }
        @Bean ActivityHandler throwing(Probe probe) { return handler("throws", probe); }

        private ActivityHandler handler(String type, Probe probe) {
            return new ActivityHandler() {
                public String type() { return type; }
                public Map<String, Object> execute(Map<String, Object> input) { return probe.execute(type, input); }
            };
        }
    }
}
