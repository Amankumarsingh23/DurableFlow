package io.durableflow.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.durableflow.DurableFlowApplication;
import io.durableflow.ai.model.*;
import io.durableflow.ai.provider.FakeModelProvider;
import io.durableflow.ai.retrieval.*;
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
import static org.assertj.core.api.Assertions.*;

@Testcontainers
@SpringBootTest(properties={"durableflow.timers.polling-enabled=false","durableflow.children.polling-enabled=false","durableflow.schedules.polling-enabled=false"})
class RagRetrievalIntegrationTest {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:17.6");
    @DynamicPropertySource static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",POSTGRES::getJdbcUrl); registry.add("spring.datasource.username",POSTGRES::getUsername);
        registry.add("spring.datasource.password",POSTGRES::getPassword);
    }
    @Autowired WorkflowDefinitionService definitions;
    @Autowired WorkflowRunService runs;
    @Autowired WorkflowTaskOrchestrator orchestrator;
    @Autowired ActivityExecutionRepository activities;
    @Autowired ActivityTaskRepository tasks;
    @Autowired TaskTransactions transactions;
    @Autowired TaskWorker worker;
    @Autowired FakeRetriever defaultRetriever;
    @Autowired FakeModelProvider models;
    @Autowired WorkflowHistoryService history;
    @Autowired SignalRepository signals;
    @Autowired JdbcTemplate jdbc;
    @BeforeEach void clean() { jdbc.execute("TRUNCATE workflow_schedule_occurrence, workflow_schedule, child_workflow_execution, compensation_execution, workflow_signal_delivery, workflow_signal_wait, workflow_timer, workflow_event, activity_task, activity_execution, workflow_run, workflow_definition"); }

    @Test
    void retrievalOutputPersistsMetadataContextAndSurvivesFreshContextWithoutRerunning() {
        UUID run=single(); var retriever=new FakeRetriever();
        try (var harness=new Harness(retriever)) { assertThat(harness.worker.runOne()).isTrue(); }
        var output=activity(run,"retrieve").getOutputJson();
        assertThat(output).containsKeys("retrievalVersion","corpusVersion","rankingVersion","strategy","selectedChunks","context","filters");
        assertThat((List<?>)output.get("selectedChunks")).isNotEmpty();
        assertThat(output.toString()).doesNotContain("PRIVATE-B-777","embedding","vector");
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        try (var fresh=freshContext()) {
            fresh.getBean(WorkflowTaskOrchestrator.class).advanceRun(run);
            assertThat(fresh.getBean(TaskWorker.class).runOne()).isFalse();
            assertThat(fresh.getBean(FakeRetriever.class).callCount()).isZero();
            assertThat(fresh.getBean(ActivityExecutionRepository.class).findById(activity(run,"retrieve").getId()).orElseThrow().getOutputJson()).isEqualTo(output);
        }
        assertThat(retriever.callCount()).isEqualTo(1);
        assertThat(history.read(run)).allMatch(e -> !e.metadata().toString().contains("POL-RFD-042") && !e.metadata().containsKey("context"));
    }

    @Test
    void transientRetrievalFailureUsesExistingBackoffAttemptAndFencingPipeline() {
        UUID run=single(); var retriever=new FakeRetriever(FakeCorpus.policiesV1(),List.of(FakeRetriever.Outcome.UNAVAILABLE,FakeRetriever.Outcome.SUCCESS));
        try (var harness=new Harness(retriever)) {
            assertThat(harness.worker.runOne()).isTrue();
            var task=tasks.findByActivityExecutionId(activity(run,"retrieve").getId()).orElseThrow();
            assertThat(task.getStatus()).isEqualTo(ActivityTaskStatus.READY); assertThat(task.getLeaseExpiresAt()).isNull();
            assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.RUNNING);
            assertThat(history.read(run)).filteredOn(e -> e.eventType()==WorkflowEventType.RETRY_SCHEDULED).hasSize(1);
            due(task.getId()); assertThat(harness.worker.runOne()).isTrue();
        }
        assertThat(activity(run,"retrieve").getAttemptCount()).isEqualTo(2); assertThat(retriever.callCount()).isEqualTo(2);
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
    }

    @Test
    void invalidBackendConfigurationAndInvalidTenantFilterFailTerminally() {
        for (boolean missingTenant:List.of(false,true)) {
            var input=input(false); var retrieval=new LinkedHashMap<String,Object>((Map<String,Object>)input.get("retrievalRequest"));
            if (missingTenant) retrieval.put("filters",Map.of("documentType","policy")); else retrieval.put("corpusVersion","missing-version");
            input.put("retrievalRequest",retrieval);
            UUID run=start(List.of(new StepDefinition("retrieve","retrieveContext",List.of())),input); orchestrator.advanceRun(run);
            assertThat(worker.runOne()).isTrue(); assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.FAILED);
            assertThat(history.read(run)).filteredOn(e -> e.eventType()==WorkflowEventType.RETRY_SCHEDULED).isEmpty();
        }
    }

    @Test
    void groundedWorkflowPassesAuthorizedCheckpointIntoModelThenWaitsAcrossRestartForHumanSignal() throws Exception {
        UUID run=rag(); assertThat(worker.runOne()).isTrue(); var retrieval=activity(run,"retrieve").getOutputJson();
        assertThat(worker.runOne()).isTrue(); var execution=activity(run,"model"); var modelOutput=execution.getOutputJson();
        var actual=ModelRequest.fromInput(execution.getInputJson());
        var reference=new ObjectMapper().readTree(actual.userContent()).path("referenceData");
        assertThat(reference.path("context").asText()).isEqualTo(((Map<?,?>)retrieval.get("context")).get("text"));
        assertThat(actual.userContent()).contains("POL-RFD-042","UNTRUSTED_DATA","S1").doesNotContain("PRIVATE-B-777");
        assertThat(actual.systemInstructions()).contains("UNTRUSTED DATA").doesNotContain("Refund requests are eligible");
        assertThat(models.invocations()).anyMatch(i -> i.request().equals(actual));
        assertThat(((Map<?,?>)modelOutput.get("grounding")).get("sourceActivityExecutionId")).isEqualTo(activity(run,"retrieve").getId().toString());
        assertThat(signals.waits(run).getFirst().status()).isEqualTo(SignalWait.Status.WAITING); assertThat(worker.runOne()).isFalse();
        try (var fresh=freshContext()) {
            fresh.getBean(WorkflowTaskOrchestrator.class).advanceRun(run); assertThat(fresh.getBean(TaskWorker.class).runOne()).isFalse();
            assertThat(fresh.getBean(FakeRetriever.class).callCount()).isZero(); assertThat(fresh.getBean(FakeModelProvider.class).callCount()).isZero();
            fresh.getBean(SignalReceiver.class).receive(run,new ReceiveSignalRequest("review-1","approval",Map.of("approved",true)));
            assertThat(fresh.getBean(TaskWorker.class).runOne()).isTrue();
            assertThat(fresh.getBean(FakeRetriever.class).callCount()).isZero(); assertThat(fresh.getBean(FakeModelProvider.class).callCount()).isZero();
        }
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        assertThat(activity(run,"retrieve").getOutputJson()).isEqualTo(retrieval); assertThat(activity(run,"model").getOutputJson()).isEqualTo(modelOutput);
        assertThat(history.read(run)).allMatch(e -> !e.metadata().toString().contains("POL-RFD-042") && !e.metadata().containsKey("selectedChunks"));
    }

    @Test
    void groundedModelRequestIsFrozenOnClaimBeforeProviderAndReusedAfterRestart() {
        UUID run=rag(); assertThat(worker.runOne()).isTrue();
        var claim=transactions.claim("crashed-before-model-call").orElseThrow();
        assertThat(claim.activityType()).isEqualTo("modelCall"); var frozen=ModelRequest.fromInput(claim.input());
        assertThat(frozen.userContent()).contains("POL-RFD-042");
        jdbc.update("UPDATE workflow_run SET input_json=jsonb_set(input_json,'{grounding,sourceNodeId}',to_jsonb(CAST(? AS text))) WHERE id=?","wrong-source-after-claim",run);
        expire(claim.taskId());
        try (var fresh=freshContext()) {
            assertThat(fresh.getBean(TaskWorker.class).runOne()).isTrue();
            assertThat(fresh.getBean(FakeModelProvider.class).invocations().getFirst().request()).isEqualTo(frozen);
            assertThat(fresh.getBean(FakeRetriever.class).callCount()).isZero();
        }
        assertThat(activity(run,"model").getAttemptCount()).isEqualTo(2);
        assertThat(ModelRequest.fromInput(activity(run,"model").getInputJson())).isEqualTo(frozen);
        assertThat(signals.waits(run).getFirst().status()).isEqualTo(SignalWait.Status.WAITING);
    }

    @Test
    void invalidGroundingIsDurablyClaimedThenFailsNormallyWithoutCallingModel() {
        UUID run=rag(); assertThat(worker.runOne()).isTrue(); int before=models.callCount();
        jdbc.update("UPDATE workflow_run SET input_json=jsonb_set(input_json,'{retrievalRequest,filters,tenantId}',to_jsonb(CAST(? AS text))) WHERE id=?","tenant-b",run);
        assertThat(worker.runOne()).isTrue(); assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.FAILED);
        assertThat(models.callCount()).isEqualTo(before);
        assertThat(activity(run,"model").getErrorMessage()).contains("MODEL_INVALID_GROUNDING");
        assertThat(activity(run,"model").getInputJson()).containsKey("groundingPreparation");
        assertThat(tasks.findByActivityExecutionId(activity(run,"model").getId()).orElseThrow().getStatus()).isEqualTo(ActivityTaskStatus.FAILED);
    }

    @Test
    void retrievalCrashBeforeCheckpointMayReturnChangedCorpusAndOldWorkerCannotOverwrite() throws Exception {
        UUID run=single(); var old=transactions.claim("old-retriever").orElseThrow();
        var first=new RetrieveContextActivity(new RetrievalGateway(List.of(new FakeRetriever())),new ContextBuilder());
        var lost=first.execute(old.context(),old.input()); expire(old.taskId());
        var doc=new DocumentMetadata("refund","Updated refund policy","demo://documents/refund","v2","tenant-a","policy",Map.of());
        var changed=new RetrievedChunk("refund:v2:000",doc,0,"POL-RFD-042: Updated refund policy requires review within 14 days.",0,RetrievalStrategy.SPARSE,Set.of());
        try (var harness=new Harness(new FakeRetriever(List.of(changed),List.of(FakeRetriever.Outcome.SUCCESS)))) { assertThat(harness.worker.runOne()).isTrue(); }
        assertThat(activity(run,"retrieve").getOutputJson().get("selectedChunks")).isNotEqualTo(lost.get("selectedChunks"));
        assertThat(activity(run,"retrieve").getOutputJson().toString()).contains("refund:v2:000");
        assertThatThrownBy(() -> transactions.complete(old,lost)).isInstanceOf(OwnershipLostException.class);
        assertThat(history.read(run)).filteredOn(e -> e.eventType()==WorkflowEventType.ACTIVITY_COMPLETED).hasSize(1);
    }

    @Test
    void retrievalAndModelWorkInStaticDagWithAnotherIndependentBranch() {
        UUID run=start(List.of(new StepDefinition("model","modelCall",List.of("retrieve","check")),
                new StepDefinition("retrieve","retrieveContext",List.of()),new StepDefinition("check","submitRequest",List.of())),input(true));
        orchestrator.advanceRun(run); assertThat(worker.runOne()).isTrue();
        assertThat(tasks.findByActivityExecutionId(activity(run,"model").getId())).isEmpty();
        assertThat(worker.runOne()).isTrue(); assertThat(worker.runOne()).isTrue();
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        assertThat(activity(run,"model").getOutputJson()).containsKey("grounding");
    }

    @Test
    void retrievalLocalTimeoutUsesExistingSupervisionAndRetry() throws Exception {
        UUID run=single(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
        var retriever=new FakeRetriever(FakeCorpus.policiesV1(),List.of(FakeRetriever.Outcome.SUCCESS),(request,call) -> {
            if (call==1) { entered.countDown(); release.await(); }
        });
        try (var timed=new Harness(retriever,(future,millis) -> {
            assertThat(entered.await(10,TimeUnit.SECONDS)).isTrue(); throw new TimeoutException();
        })) { assertThat(timed.worker.runOne()).isTrue(); }
        finally { release.countDown(); }
        var task=tasks.findByActivityExecutionId(activity(run,"retrieve").getId()).orElseThrow();
        assertThat(task.getStatus()).isEqualTo(ActivityTaskStatus.READY);
        assertThat(activity(run,"retrieve").getErrorMessage()).contains("ActivityTimeoutException");
        assertThat(history.read(run)).filteredOn(e -> e.eventType()==WorkflowEventType.ACTIVITY_TIMED_OUT).hasSize(1);
        due(task.getId()); try (var recovered=new Harness(retriever)) { assertThat(recovered.worker.runOne()).isTrue(); }
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED); assertThat(retriever.callCount()).isEqualTo(2);
    }

    private UUID single() { UUID run=start(List.of(new StepDefinition("retrieve","retrieveContext",List.of())),input(false)); orchestrator.advanceRun(run); return run; }
    private UUID rag() {
        UUID run=start(List.of(new StepDefinition("retrieve","retrieveContext",List.of()),new StepDefinition("model","modelCall",List.of("retrieve")),
                new StepDefinition("approval",null,List.of("model"),NodeType.SIGNAL,null),new StepDefinition("action","applyApprovedAction",List.of("approval"))),input(true));
        orchestrator.advanceRun(run); return run;
    }
    private UUID start(List<StepDefinition> steps,Map<String,Object> input) {
        String name="RAG-"+UUID.randomUUID(); definitions.register(new RegisterWorkflowDefinitionRequest(name,1,steps));
        return runs.start(new StartWorkflowRunRequest(name,1,input)).runId();
    }
    private Map<String,Object> input(boolean grounding) {
        var input=new LinkedHashMap<String,Object>();
        input.put("retrievalRequest",Map.of("requestVersion",1,"retriever","fake","corpusVersion","policies-v1","rankingVersion","local-v1",
                "query","refund POL-RFD-042","strategy","HYBRID","topK",3,"maxContextChars",8000,"filters",Map.of("tenantId","tenant-a","documentType","policy")));
        input.put("modelRequest",Map.of("requestVersion",1,"provider","fake","model","fake-recommendation-v1","systemInstructions","Recommend a safe reviewed next step.",
                "userContent","Can order RAG-22 be refunded?","temperature",0.0,"maxOutputTokens",256,"structuredOutput",true));
        if (grounding) input.put("grounding",Map.of("groundingVersion",1,"sourceNodeId","retrieve"));
        input.put("approvedAction",Map.of("name","markReviewed","target","ORDER-RAG-22")); return input;
    }
    private ActivityExecutionEntity activity(UUID run,String id) { return activities.findByWorkflowRunIdOrderByActivityIdAsc(run).stream().filter(a -> a.getActivityId().equals(id)).findFirst().orElseThrow(); }
    private void due(UUID id) { jdbc.update("UPDATE activity_task SET available_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?",id); }
    private void expire(UUID id) { jdbc.update("UPDATE activity_task SET lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?",id); }
    private ConfigurableApplicationContext freshContext() {
        return new SpringApplicationBuilder(DurableFlowApplication.class).web(WebApplicationType.NONE).run(
                "--spring.datasource.url="+POSTGRES.getJdbcUrl(),"--spring.datasource.username="+POSTGRES.getUsername(),"--spring.datasource.password="+POSTGRES.getPassword(),
                "--durableflow.timers.polling-enabled=false","--durableflow.children.polling-enabled=false","--durableflow.schedules.polling-enabled=false");
    }
    private class Harness implements AutoCloseable {
        final TaskWorker worker;
        Harness(Retriever retriever) { this(retriever,(future,millis) -> future.get(millis,TimeUnit.MILLISECONDS)); }
        Harness(Retriever retriever,HandlerExecutions.Awaiter awaiter) {
            var settings=new WorkerExecutionSettings(60000,0); var executor=Executors.newSingleThreadExecutor(r -> { var t=new Thread(r,"test-retrieval"); t.setDaemon(true); return t; });
            worker=new TaskWorker(transactions,new ActivityRegistry(List.of(new RetrieveContextActivity(new RetrievalGateway(List.of(retriever)),new ContextBuilder()))),
                    orchestrator,new WorkerIdentity("retrieval-test"),new WorkerHeartbeats(transactions,new WorkerLeaseSettings(30000,10000)),new HandlerExecutions(settings,executor,awaiter),settings);
        }
        public void close() { worker.stop(); }
    }
}
