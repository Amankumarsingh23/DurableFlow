package io.durableflow;

import com.jayway.jsonpath.JsonPath;
import io.durableflow.api.ApiException;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.engine.*;
import io.durableflow.workflow.run.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.net.URI;
import java.net.http.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.*;

/** Real context/pool/registry restarts against one DB, not an actual JVM kill or process-crash test. */
@Testcontainers
class WorkflowRecoveryIntegrationTest {
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.6");

    @Test
    void restartedApplicationResumesOnlyPendingShipmentUsingPostgres() throws Exception {
        UUID runId;
        UUID oldRecorderId;
        List<Checkpoint> completedBeforeRestart;
        try (var first = application(true)) {
            var run = start(first);
            runId = run.runId();
            var recorder = first.getBean(Invocations.class);
            oldRecorderId = recorder.instanceId;
            assertThat(post(first, "/api/dev/workflow-runs/" + runId + "/execute?maxActivities=0").statusCode())
                    .isEqualTo(400);
            assertThat(post(first, "/api/dev/workflow-runs/" + runId + "/execute").statusCode()).isEqualTo(400);
            assertThat(recorder.calls).isEmpty();
            assertThat(checkpoints(first, runId)).allSatisfy(row -> {
                assertThat(row.status()).isEqualTo(ActivityExecutionStatus.PENDING);
                assertThat(row.attempts()).isZero();
            });

            var paused = post(first, "/api/dev/workflow-runs/" + runId + "/execute?maxActivities=2");
            assertThat(paused.statusCode()).isEqualTo(200);
            assertThat(JsonPath.<String>read(paused.body(), "$.status")).isEqualTo("RUNNING");
            assertThat(recorder.calls).containsExactly("payment", "inventory");
            assertStates(first, runId, ActivityExecutionStatus.COMPLETED,
                    ActivityExecutionStatus.COMPLETED, ActivityExecutionStatus.PENDING);
            completedBeforeRestart = checkpoints(first, runId).stream()
                    .filter(row -> row.status() == ActivityExecutionStatus.COMPLETED).toList();
            assertThat(completedBeforeRestart).hasSize(2);
            assertThat(first.getBean(WorkflowRunRepository.class).findById(runId).orElseThrow().getCompletedAt()).isNull();
        } // Closes the HTTP server, connection pool, persistence context, services, registry and engine.

        // Only identifiers and immutable assertion snapshots cross the boundary, not engine state.
        try (var restarted = application(false)) {
            var recorder = restarted.getBean(Invocations.class);
            assertThat(recorder.instanceId).isNotEqualTo(oldRecorderId);
            assertThat(recorder.calls).isEmpty();
            assertStates(restarted, runId, ActivityExecutionStatus.COMPLETED,
                    ActivityExecutionStatus.COMPLETED, ActivityExecutionStatus.PENDING);
            assertThat(checkpoints(restarted, runId)).containsAll(completedBeforeRestart);

            var resumed = post(restarted, "/api/workflow-runs/" + runId + "/execute");
            assertThat(resumed.statusCode()).isEqualTo(200);
            assertThat(JsonPath.<String>read(resumed.body(), "$.status")).isEqualTo("COMPLETED");
            assertThat(recorder.calls).containsExactly("shipment");
            assertStates(restarted, runId, ActivityExecutionStatus.COMPLETED,
                    ActivityExecutionStatus.COMPLETED, ActivityExecutionStatus.COMPLETED);
            // Completed outputs, timestamps, attempts and row identities have not been rewritten.
            assertThat(checkpoints(restarted, runId)).containsAll(completedBeforeRestart)
                    .allSatisfy(row -> assertThat(row.attempts()).isEqualTo(1));
            var completed = restarted.getBean(WorkflowRunRepository.class).findById(runId).orElseThrow();
            assertThat(completed.getStatus()).isEqualTo(WorkflowRunStatus.COMPLETED);
            assertThat(completed.getCompletedAt()).isNotNull();
            assertThat(post(restarted, "/api/workflow-runs/" + runId + "/execute").statusCode()).isEqualTo(409);
            assertThat(recorder.calls).containsExactly("shipment");
            // The checkpoint API is absent from the ordinary application profile.
            assertThat(post(restarted, "/api/dev/workflow-runs/" + runId + "/execute?maxActivities=2").statusCode())
                    .isEqualTo(404);
        }
    }

    @Test
    void restartedApplicationPreservesOrphanedRunningAndRefusesAllFurtherWork() throws Exception {
        UUID runId;
        List<Checkpoint> beforeRestart;
        try (var first = application(false)) {
            var run = start(first);
            runId = run.runId();
            first.getBean(WorkflowEngine.class).executeRun(runId, 1);
            // Commit the real RUNNING checkpoint, then close before invoking its handler.
            // This creates the same durable uncertainty shape; it does not claim a literal crash.
            var work = first.getBean(WorkflowEngineState.class).next(runId, true).work();
            assertThat(work.activityType()).isEqualTo("recovery-inventory");
            assertThat(first.getBean(Invocations.class).calls).containsExactly("payment");
            assertStates(first, runId, ActivityExecutionStatus.COMPLETED,
                    ActivityExecutionStatus.RUNNING, ActivityExecutionStatus.PENDING);
            beforeRestart = checkpoints(first, runId);
        }

        try (var restarted = application(false)) {
            assertThatThrownBy(() -> restarted.getBean(WorkflowEngine.class).executeRun(runId))
                    .isInstanceOfSatisfying(OrphanedActivityException.class, error -> {
                        assertThat(error.runId()).isEqualTo(runId);
                        assertThat(error.activityIds()).containsExactly("inventory");
                        assertThat(error.getMessage()).contains("cannot safely determine", "side effect");
                    });
            var rejected = post(restarted, "/api/workflow-runs/" + runId + "/execute");
            assertThat(rejected.statusCode()).isEqualTo(409);
            assertThat(JsonPath.<String>read(rejected.body(), "$.code")).isEqualTo("RECOVERY_REQUIRED");
            assertThat(JsonPath.<String>read(rejected.body(), "$.runId")).isEqualTo(runId.toString());
            assertThat(JsonPath.<List<String>>read(rejected.body(), "$.activityIds")).containsExactly("inventory");
            assertThat(rejected.body()).doesNotContain("stackTrace", "org.hibernate");
            assertThat(restarted.getBean(Invocations.class).calls).isEmpty();
            assertThat(checkpoints(restarted, runId)).isEqualTo(beforeRestart);
            var inspected = restarted.getBean(WorkflowRunService.class).get(runId);
            assertThat(inspected.status()).isEqualTo(WorkflowRunStatus.RUNNING);
            assertThat(restarted.getBean(WorkflowRunRepository.class).findById(runId).orElseThrow().getCompletedAt()).isNull();
            assertThat(inspected.activities()).hasSize(3);
        }
    }

    private ConfigurableApplicationContext application(boolean dev) {
        // CLI arguments override application.yml; both contexts use the same container database.
        return new SpringApplicationBuilder(DurableFlowApplication.class, RecoveryHandlers.class)
                .web(WebApplicationType.SERVLET)
                .run("--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                        "--spring.datasource.username=" + POSTGRES.getUsername(),
                        "--spring.datasource.password=" + POSTGRES.getPassword(),
                        "--spring.profiles.active=" + (dev ? "dev" : "test"),
                        "--server.port=0", "--spring.main.banner-mode=off");
    }

    private WorkflowRunResponse start(ConfigurableApplicationContext context) {
        String name = "Recovery-" + UUID.randomUUID();
        context.getBean(WorkflowDefinitionService.class).register(new RegisterWorkflowDefinitionRequest(name, 1,
                List.of(new StepDefinition("shipment", "recovery-shipment", List.of("inventory")),
                        new StepDefinition("inventory", "recovery-inventory", List.of("payment")),
                        new StepDefinition("payment", "recovery-payment", List.of()))));
        return context.getBean(WorkflowRunService.class).start(new StartWorkflowRunRequest(name, 1, Map.of("orderId", "ORD-42")));
    }

    private HttpResponse<String> post(ConfigurableApplicationContext context, String path) throws Exception {
        int port = context.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
        try (var client = HttpClient.newHttpClient()) {
            return client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                    .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
        }
    }

    private List<Checkpoint> checkpoints(ConfigurableApplicationContext context, UUID runId) {
        return context.getBean(ActivityExecutionRepository.class).findByWorkflowRunIdOrderByActivityIdAsc(runId).stream()
                .map(row -> new Checkpoint(row.getId(), row.getActivityId(), row.getStatus(), row.getAttemptCount(),
                        row.getStartedAt(), row.getCompletedAt(), row.getUpdatedAt(), row.getOutputJson(), row.getErrorMessage()))
                .toList();
    }

    private void assertStates(ConfigurableApplicationContext context, UUID runId, ActivityExecutionStatus payment,
                              ActivityExecutionStatus inventory, ActivityExecutionStatus shipment) {
        // Repository order is alphabetical, unrelated to dependency order.
        var rows = checkpoints(context, runId);
        assertThat(rows).extracting(Checkpoint::activityId).containsExactly("inventory", "payment", "shipment");
        assertThat(rows).extracting(Checkpoint::status).containsExactly(inventory, payment, shipment);
    }

    private record Checkpoint(UUID id, String activityId, ActivityExecutionStatus status, int attempts,
                              Instant startedAt, Instant completedAt, Instant updatedAt,
                              Map<String, Object> output, String errorMessage) {
        private Checkpoint {
            if (output != null) output = Map.copyOf(output);
        }
    }

    static class Invocations {
        final UUID instanceId = UUID.randomUUID();
        final List<String> calls = new CopyOnWriteArrayList<>();
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class RecoveryHandlers {
        @Bean Invocations invocations() { return new Invocations(); }
        @Bean ActivityHandler recoveryPayment(Invocations calls) { return handler("payment", calls); }
        @Bean ActivityHandler recoveryInventory(Invocations calls) { return handler("inventory", calls); }
        @Bean ActivityHandler recoveryShipment(Invocations calls) { return handler("shipment", calls); }

        private ActivityHandler handler(String step, Invocations recorder) {
            return new ActivityHandler() {
                public String type() { return "recovery-" + step; }
                public Map<String, Object> execute(io.durableflow.workflow.activity.ActivityExecutionContext context, Map<String, Object> input) {
                    assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
                    recorder.calls.add(step);
                    return Map.of("executed", step);
                }
            };
        }
    }
}
