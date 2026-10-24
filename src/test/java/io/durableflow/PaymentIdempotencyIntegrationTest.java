package io.durableflow;

import io.durableflow.worker.*;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.activity.demo.FakePaymentProvider;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.task.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;

@Testcontainers
@SpringBootTest
class PaymentIdempotencyIntegrationTest {
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
    @Autowired ActivityExecutionRepository activities;
    @Autowired ActivityTaskRepository tasks;
    @Autowired ActivityRegistry registry;
    @Autowired FakePaymentProvider provider;
    @Autowired WorkerHeartbeats heartbeats;
    @Autowired HandlerExecutions executions;
    @Autowired WorkerExecutionSettings executionSettings;
    @Autowired JdbcTemplate jdbc;

    @Test
    void crashAfterPaymentBeforeCompletionReusesOriginalEffectAndContinuesWorkflow() throws Exception {
        recoverPayment(false);
    }

    @Test
    void zombieCompletionIsFencedWhileReplacementPaymentIsDeduplicated() throws Exception {
        recoverPayment(true);
    }

    private void recoverPayment(boolean zombieWakes) throws Exception {
        String name = "Idempotency-" + UUID.randomUUID();
        definitions.register(new RegisterWorkflowDefinitionRequest(name, 1, List.of(
                new StepDefinition("payment", "chargePayment", List.of()),
                new StepDefinition("shipment", "createShipment", List.of("payment")))));
        var run = runs.start(new StartWorkflowRunRequest(name, 1,
                Map.of("orderId", "IDEMPOTENCY-DEMO", "amount", 1000, "currency", "INR")));
        orchestrator.advanceRun(run.runId());
        int effectsBefore = provider.effectCount();
        var a = transactions.claim("A").orElseThrow();
        var contextA = new ActivityExecutionContext(a.runId(), a.activityExecutionId());
        assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
        var original = registry.require("chargePayment").execute(contextA, a.input());
        assertThat(provider.effectCount()).isEqualTo(effectsBefore + 1);
        // A's external effect succeeded, but no DurableFlow outcome transaction has happened.
        assertThat(tasks.findById(a.taskId()).orElseThrow().getStatus()).isEqualTo(ActivityTaskStatus.CLAIMED);
        assertThat(activities.findById(a.activityExecutionId()).orElseThrow().getStatus()).isEqualTo(ActivityExecutionStatus.RUNNING);
        assertThat(activities.findById(a.activityExecutionId()).orElseThrow().getOutputJson()).isNull();
        jdbc.update("UPDATE activity_task SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE id = ?", a.taskId());

        var seen = new AtomicReference<ActivityExecutionContext>();
        var payment = new ActivityHandler() {
            public String type() { return "chargePayment"; }
            public Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input) throws Exception {
                assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
                seen.set(context);
                var current = tasks.findById(a.taskId()).orElseThrow();
                assertThat(current.getWorkerId()).isEqualTo("B");
                assertThat(current.getLeaseGeneration()).isEqualTo(a.leaseGeneration() + 1);
                var logical = activities.findById(a.activityExecutionId()).orElseThrow();
                assertThat(logical.getAttemptCount()).isEqualTo(2);
                assertThat(logical.getStatus()).isEqualTo(ActivityExecutionStatus.RUNNING);
                var repeated = registry.require(type()).execute(context, input);
                assertThat(repeated).isEqualTo(original);
                if (zombieWakes) {
                    // Even a late external request reuses K; fencing independently rejects A's DB write.
                    assertThat(registry.require(type()).execute(contextA, a.input())).isEqualTo(original);
                    assertThatThrownBy(() -> transactions.complete(a, original)).isInstanceOf(OwnershipLostException.class);
                    assertThatThrownBy(() -> transactions.fail(a, "stale failure")).isInstanceOf(OwnershipLostException.class);
                    assertThat(activities.findById(a.activityExecutionId()).orElseThrow().getOutputJson()).isNull();
                }
                return repeated;
            }
        };
        var b = new TaskWorker(transactions, new ActivityRegistry(List.of(payment,
                registry.require("createShipment"))), orchestrator, new WorkerIdentity("B"), heartbeats, executions, executionSettings);
        assertThat(b.runOne()).isTrue();
        assertThat(seen.get().idempotencyKey()).isEqualTo(contextA.idempotencyKey());
        assertThat(seen.get().activityExecutionId()).isEqualTo(a.activityExecutionId());
        assertThat(provider.effectCount()).isEqualTo(effectsBefore + 1);
        assertThat(activities.findById(a.activityExecutionId()).orElseThrow().getOutputJson()).isEqualTo(original);
        assertThat(tasks.findById(a.taskId()).orElseThrow().getStatus()).isEqualTo(ActivityTaskStatus.COMPLETED);
        assertThat(b.runOne()).isTrue(); // Successor was durably queued by B's advancement.
        assertThat(runs.get(run.runId()).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        assertThat(provider.effectCount()).isEqualTo(effectsBefore + 1);
    }
}
