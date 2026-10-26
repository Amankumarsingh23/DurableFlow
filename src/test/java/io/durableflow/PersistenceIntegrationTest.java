package io.durableflow;

import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.activity.*;
import jakarta.persistence.EntityManager;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

// Close pollers/pools before this class's disposable PostgreSQL container stops.
@org.springframework.test.annotation.DirtiesContext(classMode = org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PersistenceIntegrationTest {
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.6");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired WorkflowDefinitionRepository definitions;
    @Autowired WorkflowRunRepository runs;
    @Autowired ActivityExecutionRepository activities;
    @Autowired EntityManager entityManager;
    @Autowired Flyway flyway;
    @LocalServerPort int port;

    @Test
    void applicationStartsWithMigratedValidatedSchemaAndHealthyHttpEndpoint() throws Exception {
        assertThat(flyway.info().current().getVersion().getVersion()).isEqualTo("13");
        assertThat(flyway.info().pending()).isEmpty();
        var response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/actuator/health"))
                        .GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"status\":\"UP\"");
    }

    @Test
    @Transactional
    void persistsAndReloadsDefinitionRunAndActivitiesFromPostgres() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        var definition = new WorkflowDefinitionEntity();
        definition.setName("OrderFulfillment");
        definition.setVersion(1);
        definition.setDefinitionJson(Map.of("activities", java.util.List.of("payment", "shipment")));
        definition.setCreatedAt(now);
        definitions.saveAndFlush(definition);

        var run = new WorkflowRunEntity();
        run.setWorkflowDefinitionId(definition.getId());
        run.setStatus(WorkflowRunStatus.PENDING);
        run.setInputJson(Map.of("orderId", "order-123"));
        run.setCreatedAt(now);
        run.setUpdatedAt(now);
        runs.saveAndFlush(run);

        var payment = activity(run, "payment", now);
        payment.setStatus(ActivityExecutionStatus.COMPLETED);
        payment.setInputJson(Map.of("amount", 100));
        payment.setOutputJson(Map.of("receipt", "receipt-1"));
        payment.setAttemptCount(1);
        payment.setStartedAt(now);
        payment.setCompletedAt(now);
        activities.saveAndFlush(payment);
        var shipment = activities.saveAndFlush(activity(run, "shipment", now));

        entityManager.clear(); // Force database reads rather than first-level cache hits.
        var storedDefinition = definitions.findById(definition.getId()).orElseThrow();
        assertThat(storedDefinition.getDefinitionJson()).isEqualTo(definition.getDefinitionJson());
        assertThat(storedDefinition.getCreatedAt()).isEqualTo(now);
        var storedRun = runs.findById(run.getId()).orElseThrow();
        assertThat(storedRun.getWorkflowDefinitionId()).isEqualTo(definition.getId());
        assertThat(storedRun.getStatus()).isEqualTo(WorkflowRunStatus.PENDING);
        assertThat(storedRun.getInputJson()).isEqualTo(run.getInputJson());
        assertThat(storedRun.getStartedAt()).isNull();
        assertThat(storedRun.getCompletedAt()).isNull();
        var storedPayment = activities.findById(payment.getId()).orElseThrow();
        assertThat(storedPayment.getWorkflowRunId()).isEqualTo(run.getId());
        assertThat(storedPayment.getOutputJson()).isEqualTo(payment.getOutputJson());
        assertThat(storedPayment.getAttemptCount()).isEqualTo(1);
        assertThat(storedPayment.getStatus()).isEqualTo(ActivityExecutionStatus.COMPLETED);
        assertThat(storedPayment.getCompletedAt()).isEqualTo(now);
        var storedShipment = activities.findById(shipment.getId()).orElseThrow();
        assertThat(storedShipment.getInputJson()).isNull();
        assertThat(storedShipment.getOutputJson()).isNull();
        assertThat(storedShipment.getAttemptCount()).isZero();
    }

    private ActivityExecutionEntity activity(WorkflowRunEntity run, String activityId, Instant now) {
        var activity = new ActivityExecutionEntity();
        activity.setWorkflowRunId(run.getId());
        activity.setActivityId(activityId);
        activity.setStatus(ActivityExecutionStatus.PENDING);
        activity.setCreatedAt(now);
        activity.setUpdatedAt(now);
        return activity;
    }
}
