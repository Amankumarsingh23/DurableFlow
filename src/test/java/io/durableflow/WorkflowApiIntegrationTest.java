package io.durableflow;

import com.jayway.jsonpath.JsonPath;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.run.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.net.URI;
import java.net.http.*;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WorkflowApiIntegrationTest {
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.6");

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @LocalServerPort int port;
    @Autowired WorkflowDefinitionService definitionService;
    @Autowired WorkflowRunService runService;
    @Autowired WorkflowRunRepository runs;
    @Autowired ActivityExecutionRepository activities;
    @Autowired JdbcTemplate jdbc;
    final HttpClient client = HttpClient.newHttpClient();

    @Test
    void registersRetrievesAndStartsWorkflowWithoutExecutingActivities() throws Exception {
        String name = "Order-" + UUID.randomUUID();
        String body = """
                {"name":"%s","version":1,"steps":[
                  {"id":"payment","activity":"chargePayment"},
                  {"id":"inventory","activity":"reserveInventory","dependsOn":["payment"]},
                  {"id":"shipment","activity":"createShipment","dependsOn":["inventory"]}]}
                """.formatted(name);
        var created = post("/api/workflow-definitions", body);
        assertThat(created.statusCode()).isEqualTo(201);
        var definition = get("/api/workflow-definitions/" + name + "/versions/1");
        assertThat(definition.statusCode()).isEqualTo(200);
        assertThat((String) JsonPath.read(definition.body(), "$.name")).isEqualTo(name);
        assertThat((List<String>) JsonPath.<List<String>>read(definition.body(), "$.steps[*].id"))
                .containsExactly("payment", "inventory", "shipment");
        assertThat((List<String>) JsonPath.<List<String>>read(definition.body(), "$.steps[0].dependsOn"))
                .isEmpty();
        assertThat((List<String>) JsonPath.<List<String>>read(definition.body(), "$.steps[2].dependsOn"))
                .containsExactly("inventory");
        assertThat(post("/api/workflow-definitions", body).statusCode()).isEqualTo(409);
        assertThat(get("/api/workflow-definitions/" + name + "/versions/1").body()).isEqualTo(definition.body());

        long runsBefore = runs.count();
        var started = post("/api/workflow-runs",
                """
                {"workflowName":"%s","version":1,"input":{"orderId":"ORD-42"}}
                """.formatted(name));
        assertThat(started.statusCode()).isEqualTo(201);
        UUID runId = UUID.fromString(JsonPath.read(started.body(), "$.runId"));
        assertThat(runs.count()).isEqualTo(runsBefore + 1);
        var persistedRun = runs.findById(runId).orElseThrow();
        assertThat(persistedRun.getStatus()).isEqualTo(WorkflowRunStatus.RUNNING);
        assertThat(persistedRun.getWorkflowDefinitionId())
                .isEqualTo(definitionService.get(name, 1).id());
        assertThat(persistedRun.getStartedAt()).isNotNull();
        assertThat(persistedRun.getCompletedAt()).isNull();
        var rows = activities.findByWorkflowRunIdOrderByActivityIdAsc(runId);
        assertThat(rows).hasSize(3);
        assertThat(rows).extracting(ActivityExecutionEntity::getActivityId)
                .containsExactly("inventory", "payment", "shipment");
        assertThat(rows).allSatisfy(row -> {
            assertThat(row.getWorkflowRunId()).isEqualTo(runId);
            assertThat(row.getStatus()).isEqualTo(ActivityExecutionStatus.PENDING);
            assertThat(row.getAttemptCount()).isZero();
            assertThat(row.getStartedAt()).isNull();
        });
        var fetched = get("/api/workflow-runs/" + runId);
        assertThat(fetched.statusCode()).isEqualTo(200);
        assertThat((String) JsonPath.read(fetched.body(), "$.workflowName")).isEqualTo(name);
        assertThat((String) JsonPath.read(fetched.body(), "$.status")).isEqualTo("RUNNING");
        assertThat((String) JsonPath.read(fetched.body(), "$.input.orderId")).isEqualTo("ORD-42");
        assertThat(JsonPath.<List<String>>read(fetched.body(), "$.activities[*].status"))
                .containsExactly("PENDING", "PENDING", "PENDING");
    }

    @Test
    void returnsConsistentSafeErrorsForInvalidAndMissingResources() throws Exception {
        long before = runs.count();
        var missing = post("/api/workflow-runs",
                """
                {"workflowName":"missing","version":1,"input":{}}
                """);
        assertError(missing, 404);
        assertThat(runs.count()).isEqualTo(before);
        assertError(get("/api/workflow-runs/" + UUID.randomUUID()), 404);
        assertError(get("/api/workflow-definitions/missing/versions/1"), 404);
        assertError(get("/api/workflow-runs/not-a-uuid"), 400);
        assertError(post("/api/workflow-definitions", "{}"), 400);
        assertError(post("/api/workflow-definitions", "{invalid-json"), 400);
        assertError(post("/api/workflow-definitions",
                """
                {"name":"bad","version":1,"steps":[{"id":"A","activity":"a","dependsOn":["missing"]}]}
                """), 400);
        assertError(post("/api/workflow-runs",
                """
                {"workflowName":"missing","version":1}
                """), 400);
    }

    @Test
    void activityInsertFailureRollsBackRunAndAllPreviouslyInsertedActivities() {
        String name = "Atomic-" + UUID.randomUUID();
        definitionService.register(new RegisterWorkflowDefinitionRequest(name, 1, List.of(
                new StepDefinition("first", "first", List.of()),
                new StepDefinition("reject-me", "second", List.of("first")))));
        long runCount = runs.count();
        long activityCount = activities.count();
        // Test-only failure at the database boundary, after the workflow_run insert.
        jdbc.execute("""
                CREATE FUNCTION reject_test_activity() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  IF NEW.activity_id = 'reject-me' THEN
                    RAISE EXCEPTION 'injected activity insert failure';
                  END IF;
                  RETURN NEW;
                END;
                $$
                """);
        jdbc.execute("""
                CREATE TRIGGER reject_test_activity BEFORE INSERT ON activity_execution
                FOR EACH ROW EXECUTE FUNCTION reject_test_activity()
                """);
        try {
            assertThatThrownBy(() -> runService.start(new StartWorkflowRunRequest(name, 1, Map.of())))
                    .isInstanceOf(org.springframework.dao.DataAccessException.class)
                    .hasStackTraceContaining("injected activity insert failure");
            // Service invocation has ended: counts are observed outside the rolled-back transaction.
            assertThat(runs.count()).isEqualTo(runCount);
            assertThat(activities.count()).isEqualTo(activityCount);
        } finally {
            jdbc.execute("DROP TRIGGER reject_test_activity ON activity_execution");
            jdbc.execute("DROP FUNCTION reject_test_activity()");
        }
    }

    private void assertError(HttpResponse<String> response, int status) {
        assertThat(response.statusCode()).isEqualTo(status);
        assertThat(JsonPath.<Integer>read(response.body(), "$.status")).isEqualTo(status);
        assertThat(JsonPath.<String>read(response.body(), "$.message")).isNotBlank();
        assertThat(response.body()).doesNotContain("SQLException", "Hibernate", "stackTrace", "org.springframework");
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .GET().build(), HttpResponse.BodyHandlers.ofString());
    }
}
