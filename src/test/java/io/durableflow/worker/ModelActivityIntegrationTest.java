package io.durableflow.worker;

import io.durableflow.DurableFlowApplication;
import io.durableflow.ai.activity.ModelCallActivity;
import io.durableflow.ai.model.*;
import io.durableflow.ai.provider.*;
import io.durableflow.ai.demo.FakeApprovedActionProvider;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.history.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.signal.*;
import io.durableflow.workflow.task.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
@SpringBootTest(properties={"durableflow.timers.polling-enabled=false", "durableflow.children.polling-enabled=false", "durableflow.schedules.polling-enabled=false"})
class ModelActivityIntegrationTest {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.6");
    @DynamicPropertySource static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl); registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
    @Autowired WorkflowDefinitionService definitions;
    @Autowired WorkflowRunService runs;
    @Autowired WorkflowTaskOrchestrator orchestrator;
    @Autowired ActivityExecutionRepository activities;
    @Autowired ActivityTaskRepository tasks;
    @Autowired TaskTransactions transactions;
    @Autowired WorkflowHistoryService history;
    @Autowired TaskWorker worker;
    @Autowired FakeModelProvider defaultProvider;
    @Autowired SignalRepository signals;
    @Autowired JdbcTemplate jdbc;
    @BeforeEach void clean() { jdbc.execute("TRUNCATE model_route_attempt, model_route, agent_iteration, agent_execution, fake_tool_effect, tool_request, workflow_schedule_occurrence, workflow_schedule, child_workflow_execution, compensation_execution, workflow_signal_delivery, workflow_signal_wait, workflow_timer, workflow_event, activity_task, activity_execution, workflow_run, workflow_definition"); }

    @Test
    void modelCallPersistsResultUsageRequestIdAndOnlyGenericHistory() {
        UUID run = single(); var provider = new FakeModelProvider();
        try (var harness = new Harness(provider)) { assertThat(harness.worker.runOne()).isTrue(); }
        var output = model(run).getOutputJson();
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        assertThat(output).containsEntry("provider","fake").containsEntry("model","fake-recommendation-v1");
        assertThat(output.get("structuredJson")).isInstanceOf(Map.class);
        assertThat(((Map<?,?>)output.get("usage")).get("inputTokens")).isInstanceOf(Number.class);
        assertThat(((Number)((Map<?,?>)output.get("usage")).get("outputTokens")).longValue()).isPositive();
        assertThat(output.get("providerRequestId")).asString().startsWith("fake-");
        assertThat(((Number)output.get("latencyMillis")).longValue()).isEqualTo(7);
        assertThat(provider.callCount()).isEqualTo(1);
        assertThat(history.read(run)).extracting(WorkflowEventResponse::eventType).containsExactly(
                WorkflowEventType.WORKFLOW_STARTED, WorkflowEventType.ACTIVITY_SCHEDULED, WorkflowEventType.TASK_CLAIMED,
                WorkflowEventType.ACTIVITY_COMPLETED, WorkflowEventType.WORKFLOW_COMPLETED);
        assertThat(history.read(run)).allMatch(e -> !e.metadata().toString().contains("FROZEN-PRIVATE-PROMPT")
                && !e.metadata().containsKey("structuredJson") && !e.metadata().containsKey("providerRequestId"));
    }

    @Test
    void transientFailureUsesDurableRetryAndFrozenRequestSurvivesFreshContextAndChangedRunInput() {
        UUID run = single(); var provider = new FakeModelProvider(List.of(FakeModelProvider.Outcome.RATE_LIMITED));
        try (var harness = new Harness(provider)) { assertThat(harness.worker.runOne()).isTrue(); }
        var execution = model(run); var task = tasks.findByActivityExecutionId(execution.getId()).orElseThrow();
        var frozen = ModelRequest.fromInput(execution.getInputJson());
        assertThat(task.getStatus()).isEqualTo(ActivityTaskStatus.READY); assertThat(task.getLeaseExpiresAt()).isNull();
        assertThat(execution.getAttemptCount()).isEqualTo(1); assertThat(execution.getOutputJson()).isNull();
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        assertThat(history.read(run)).filteredOn(e -> e.eventType() == WorkflowEventType.RETRY_SCHEDULED).hasSize(1);
        // Simulate a changed external/default source. Retries must still read the execution snapshot.
        jdbc.update("UPDATE workflow_run SET input_json=jsonb_set(input_json,'{modelRequest,userContent}',to_jsonb(CAST(? AS text))) WHERE id=?", "changed-after-restart",run);
        due(task.getId());
        try (var fresh = freshContext()) {
            assertThat(fresh.getBean(TaskWorker.class).runOne()).isTrue();
            var recovered = fresh.getBean(FakeModelProvider.class);
            assertThat(recovered.callCount()).isEqualTo(1);
            assertThat(recovered.invocations().getFirst().request()).isEqualTo(frozen);
            assertThat(recovered.invocations().getFirst().operationKey()).isEqualTo(provider.invocations().getFirst().operationKey());
        }
        assertThat(model(run).getAttemptCount()).isEqualTo(2);
        assertThat(ModelRequest.fromInput(model(run).getInputJson())).isEqualTo(frozen);
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
    }

    @Test
    void invalidProviderRequestIsTerminalAndUsesNormalFailureState() {
        UUID run = single(); var provider = new FakeModelProvider(List.of(FakeModelProvider.Outcome.INVALID_REQUEST));
        try (var harness = new Harness(provider)) { assertThat(harness.worker.runOne()).isTrue(); }
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.FAILED);
        assertThat(model(run).getAttemptCount()).isEqualTo(1); assertThat(model(run).getErrorMessage()).contains("MODEL_INVALID_REQUEST");
        assertThat(history.read(run)).filteredOn(e -> e.eventType() == WorkflowEventType.RETRY_SCHEDULED).isEmpty();
        assertThat(provider.callCount()).isEqualTo(1);
    }

    @Test
    void malformedStructuredResponseDoesNotBecomeDurableSuccess() {
        UUID run = single();
        try (var harness = new Harness(new FakeModelProvider(List.of(FakeModelProvider.Outcome.MALFORMED_STRUCTURED)))) {
            assertThat(harness.worker.runOne()).isTrue();
        }
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.FAILED);
        assertThat(model(run).getOutputJson()).isNull(); assertThat(model(run).getErrorMessage()).contains("MALFORMED_RESPONSE");
    }

    @Test
    void completedOutputIsNeverRegeneratedByOrdinaryFreshContextRecovery() {
        UUID run = single(); assertThat(worker.runOne()).isTrue(); var output = model(run).getOutputJson();
        try (var fresh = freshContext()) {
            fresh.getBean(WorkflowTaskOrchestrator.class).advanceRun(run);
            assertThat(fresh.getBean(TaskWorker.class).runOne()).isFalse();
            assertThat(fresh.getBean(FakeModelProvider.class).callCount()).isZero();
            assertThat(fresh.getBean(ActivityExecutionRepository.class).findById(model(run).getId()).orElseThrow().getOutputJson()).isEqualTo(output);
        }
        assertThat(model(run).getAttemptCount()).isEqualTo(1);
    }

    @Test
    void modelActivityParticipatesInFanOutAndDurableAndJoin() {
        UUID run = start(List.of(new StepDefinition("join","finalizeRequest",List.of("model","check")),
                new StepDefinition("model","modelCall",List.of()),new StepDefinition("check","submitRequest",List.of())));
        int before = defaultProvider.callCount(); orchestrator.advanceRun(run);
        assertThat(tasks.findByRunId(run)).hasSize(2); assertThat(worker.runOne()).isTrue();
        assertThat(activities.findByWorkflowRunIdOrderByActivityIdAsc(run)).filteredOn(a -> a.getActivityId().equals("join"))
                .allMatch(a -> tasks.findByActivityExecutionId(a.getId()).isEmpty());
        assertThat(worker.runOne()).isTrue(); assertThat(worker.runOne()).isTrue();
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        assertThat(defaultProvider.callCount()-before).isEqualTo(1);
        assertThat(model(run).getOutputJson()).containsKey("structuredJson");
        assertThat(history.read(run)).filteredOn(e -> e.eventType()==WorkflowEventType.ACTIVITY_SCHEDULED && "join".equals(e.activityId())).hasSize(1);
    }

    @Test
    void agentWaitSurvivesRestartAndActionRunsOnlyAfterSignalWithoutRegeneratingModel() {
        UUID run = start(List.of(new StepDefinition("prepare","submitRequest",List.of()),
                new StepDefinition("model","modelCall",List.of("prepare")),
                new StepDefinition("approval",null,List.of("model"),NodeType.SIGNAL,null),
                new StepDefinition("action","applyApprovedAction",List.of("approval"))));
        orchestrator.advanceRun(run); assertThat(worker.runOne()).isTrue(); assertThat(worker.runOne()).isTrue();
        var output = model(run).getOutputJson();
        assertThat(signals.waits(run).getFirst().status()).isEqualTo(SignalWait.Status.WAITING);
        assertThat(worker.runOne()).isFalse(); assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        try (var fresh = freshContext()) {
            fresh.getBean(WorkflowTaskOrchestrator.class).advanceRun(run);
            assertThat(fresh.getBean(TaskWorker.class).runOne()).isFalse();
            assertThat(fresh.getBean(FakeModelProvider.class).callCount()).isZero();
            assertThat(fresh.getBean(FakeApprovedActionProvider.class).effectCount()).isZero();
            fresh.getBean(SignalReceiver.class).receive(run,new ReceiveSignalRequest("human-review-1","approval",Map.of("approved",true)));
            assertThat(fresh.getBean(TaskWorker.class).runOne()).isTrue();
            assertThat(fresh.getBean(FakeApprovedActionProvider.class).effectCount()).isEqualTo(1);
            assertThat(fresh.getBean(FakeModelProvider.class).callCount()).isZero();
        }
        assertThat(model(run).getOutputJson()).isEqualTo(output); assertThat(model(run).getAttemptCount()).isEqualTo(1);
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
    }

    @Test
    void crashAfterGenerationMayGenerateDifferentOutputOnReclaimAndOldWorkerIsFenced() throws Exception {
        UUID run = single(); var old = transactions.claim("crashed-model-worker").orElseThrow();
        var firstProvider = new FakeModelProvider(); var firstHandler = new ModelCallActivity(new ModelGateway(List.of(firstProvider)));
        var lostOutput = firstHandler.execute(old.context(),old.input()); // Provider returned; no completion committed.
        expire(old.taskId()); var calls = new AtomicInteger();
        ModelProvider changedGeneration = new ModelProvider() {
            public String id() { return "fake"; }
            public ModelResponse generate(ModelRequest request,String key) {
                calls.incrementAndGet(); assertThat(request).isEqualTo(ModelRequest.fromInput(old.input()));
                assertThat(key).isEqualTo(old.context().idempotencyKey());
                return new ModelResponse(null,Map.of("recommendation","A DIFFERENT GENERATION"),"fake",request.model(),new ModelUsage(10,8),4,"second-provider-request");
            }
        };
        try (var harness = new Harness(changedGeneration)) { assertThat(harness.worker.runOne()).isTrue(); }
        assertThat(firstProvider.callCount()).isEqualTo(1); assertThat(calls.get()).isEqualTo(1);
        assertThat(model(run).getOutputJson().get("structuredJson")).isNotEqualTo(lostOutput.get("structuredJson"));
        assertThat(model(run).getOutputJson()).containsEntry("providerRequestId","second-provider-request");
        assertThatThrownBy(() -> transactions.complete(old,lostOutput)).isInstanceOf(OwnershipLostException.class);
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        assertThat(history.read(run)).filteredOn(e -> e.eventType()==WorkflowEventType.ACTIVITY_COMPLETED).hasSize(1);
    }

    @Test
    void slowModelUsesExistingLocalTimeoutSupervisionAndDurableRetry() throws Exception {
        UUID run = single(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var provider = new FakeModelProvider(List.of(FakeModelProvider.Outcome.SUCCESS),(request,key,attempt) -> {
            if (attempt==1) { entered.countDown(); release.await(); }
        });
        try (var timed = new Harness(provider,(future,millis) -> {
            assertThat(entered.await(10,TimeUnit.SECONDS)).isTrue(); throw new TimeoutException();
        })) { assertThat(timed.worker.runOne()).isTrue(); }
        finally { release.countDown(); }
        var task = tasks.findByActivityExecutionId(model(run).getId()).orElseThrow();
        assertThat(task.getStatus()).isEqualTo(ActivityTaskStatus.READY); assertThat(model(run).getOutputJson()).isNull();
        assertThat(model(run).getErrorMessage()).contains("ActivityTimeoutException");
        assertThat(history.read(run)).filteredOn(e -> e.eventType()==WorkflowEventType.ACTIVITY_TIMED_OUT).hasSize(1);
        due(task.getId());
        try (var recovered = new Harness(provider)) { assertThat(recovered.worker.runOne()).isTrue(); }
        assertThat(provider.callCount()).isEqualTo(2); assertThat(model(run).getAttemptCount()).isEqualTo(2);
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
    }

    private UUID single() { UUID run = start(List.of(new StepDefinition("model","modelCall",List.of()))); orchestrator.advanceRun(run); return run; }
    private UUID start(List<StepDefinition> steps) {
        String name="Model-"+UUID.randomUUID(); definitions.register(new RegisterWorkflowDefinitionRequest(name,1,steps));
        return runs.start(new StartWorkflowRunRequest(name,1,input())).runId();
    }
    private Map<String,Object> input() {
        return Map.of("modelRequest",Map.of("requestVersion",1,"provider","fake","model","fake-recommendation-v1",
                "systemInstructions","FROZEN-PRIVATE-PROMPT","userContent","Frozen user content","temperature",0.0,"maxOutputTokens",256,"structuredOutput",true),
                "approvedAction",Map.of("name","markReviewed","target","ORDER-21"));
    }
    private ActivityExecutionEntity model(UUID run) { return activities.findByWorkflowRunIdOrderByActivityIdAsc(run).stream().filter(a -> a.getActivityId().equals("model")).findFirst().orElseThrow(); }
    private void due(UUID task) { jdbc.update("UPDATE activity_task SET available_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?",task); }
    private void expire(UUID task) { jdbc.update("UPDATE activity_task SET lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?",task); }
    private ConfigurableApplicationContext freshContext() {
        return new SpringApplicationBuilder(DurableFlowApplication.class).web(WebApplicationType.NONE).run(
                "--spring.datasource.url="+POSTGRES.getJdbcUrl(),"--spring.datasource.username="+POSTGRES.getUsername(),"--spring.datasource.password="+POSTGRES.getPassword(),
                "--durableflow.timers.polling-enabled=false","--durableflow.children.polling-enabled=false","--durableflow.schedules.polling-enabled=false");
    }
    private class Harness implements AutoCloseable {
        final TaskWorker worker;
        Harness(ModelProvider provider) { this(provider,(future,millis) -> future.get(millis,TimeUnit.MILLISECONDS)); }
        Harness(ModelProvider provider,HandlerExecutions.Awaiter awaiter) {
            var settings=new WorkerExecutionSettings(60000,0);
            var executor=Executors.newSingleThreadExecutor(r -> { var t=new Thread(r,"test-model-handler"); t.setDaemon(true); return t; });
            worker=new TaskWorker(transactions,new ActivityRegistry(List.of(new ModelCallActivity(new ModelGateway(List.of(provider))))),
                    orchestrator,new WorkerIdentity("model-test-worker"),new WorkerHeartbeats(transactions,new WorkerLeaseSettings(30000,10000)),
                    new HandlerExecutions(settings,executor,awaiter),settings);
        }
        public void close() { worker.stop(); }
    }
}
