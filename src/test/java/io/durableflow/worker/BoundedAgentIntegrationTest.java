package io.durableflow.worker;

import io.durableflow.DurableFlowApplication;
import io.durableflow.agent.*;
import io.durableflow.ai.activity.ModelCallActivity;
import io.durableflow.ai.model.ModelGateway;
import io.durableflow.ai.provider.FakeModelProvider;
import io.durableflow.tools.*;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
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

// Close pollers/pools before this class's disposable PostgreSQL container stops.
@org.springframework.test.annotation.DirtiesContext(classMode = org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@SpringBootTest(properties={"durableflow.timers.polling-enabled=false","durableflow.children.polling-enabled=false","durableflow.schedules.polling-enabled=false"})
class BoundedAgentIntegrationTest {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:17.6");
    @DynamicPropertySource static void database(DynamicPropertyRegistry r) {r.add("spring.datasource.url",POSTGRES::getJdbcUrl);r.add("spring.datasource.username",POSTGRES::getUsername);r.add("spring.datasource.password",POSTGRES::getPassword);}
    @Autowired AgentStartService start;
    @Autowired AgentLoopService loop;
    @Autowired AgentRepository agents;
    @Autowired TaskWorker worker;
    @Autowired TaskTransactions transactions;
    @Autowired ActivityExecutionRepository activities;
    @Autowired ActivityTaskRepository tasks;
    @Autowired ToolRequestRepository requests;
    @Autowired FakeModelProvider models;
    @Autowired SignalReceiver receiver;
    @Autowired WorkflowTaskOrchestrator orchestrator;
    @Autowired WorkflowDefinitionService definitions;
    @Autowired WorkflowRunService runs;
    @Autowired JdbcTemplate jdbc;
    @BeforeEach void clean() {jdbc.execute("TRUNCATE model_route_attempt, model_route, agent_iteration, agent_execution, fake_tool_effect, tool_request, workflow_schedule_occurrence, workflow_schedule, child_workflow_execution, compensation_execution, workflow_signal_delivery, workflow_signal_wait, workflow_timer, workflow_event, activity_task, activity_execution, workflow_run, workflow_definition");}
    @Test void concurrentAdvanceCreatesFirstIterationAndLogicalModelCallOnce() throws Exception {
        var a=create("runaway",AgentLimits.defaults());concurrentAdvance(a.id());
        assertThat(agents.iterations(a.id())).hasSize(1);assertThat(agents.get(a.id()).logicalModelCalls()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM activity_task",Integer.class)).isEqualTo(1);
    }
    @Test void modelDecisionCheckpointSurvivesCrashBeforeExactlyOneToolRequest() {
        var a=create("refund",AgentLimits.defaults());decision(a.id());var i=latest(a.id());var output=activities.findById(i.modelActivityId()).orElseThrow().getOutputJson();
        assertThat(i.decision()).containsEntry("type","TOOL_CALL").containsEntry("toolName","getOrder");
        int before=models.callCount();
        try(var fresh=freshContext()) {
            fresh.getBean(AgentLoopService.class).advance(a.id());fresh.getBean(AgentLoopService.class).advance(a.id());
            assertThat(fresh.getBean(FakeModelProvider.class).callCount()).isZero();
        }
        var persisted=latest(a.id());assertThat(persisted.toolRequestId()).isNotNull();assertThat(agents.get(a.id()).logicalToolCalls()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM tool_request",Integer.class)).isEqualTo(1);
        assertThat(activities.findById(i.modelActivityId()).orElseThrow().getOutputJson()).isEqualTo(output);assertThat(models.callCount()).isEqualTo(before);
    }
    @Test void persistedModelActivityOutputIsRecoveredBeforeDecisionAccounting() {
        var a=create("final",AgentLimits.defaults());loop.advance(a.id());assertThat(worker.runOne()).isTrue();
        assertThat(latest(a.id()).status()).isEqualTo(AgentIteration.Status.MODEL_PENDING);
        try(var fresh=freshContext()) {fresh.getBean(AgentLoopService.class).advance(a.id());assertThat(fresh.getBean(FakeModelProvider.class).callCount()).isZero();}
        assertThat(latest(a.id()).decision()).containsEntry("type","FINAL");assertThat(agents.get(a.id()).inputTokens()).isPositive();
    }
    @Test void toolResultFeedsNextDeterministicFrozenModelRequest() {
        var a=create("refund",AgentLimits.defaults());decision(a.id());loop.advance(a.id());assertThat(worker.runOne()).isTrue();loop.advance(a.id());
        var first=latest(a.id());assertThat(first.toolResult()).containsEntry("toolName","getOrder");loop.advance(a.id());
        assertThat(worker.runOne()).isTrue();var second=latest(a.id());var actual=activities.findById(second.modelActivityId()).orElseThrow().getInputJson();
        assertThat(ToolJson.encode(actual)).contains("priorFacts","getOrder","PAID","AUTHORIZED_AND_EXECUTED","UNTRUSTED_DATA");
    }
    @Test void refundDemoWaitsAcrossRestartApprovesExactRequestExecutesOnceAndFinalizes() {
        var a=create("refund",AgentLimits.defaults());pump(a.id());var waiting=agents.get(a.id());var i=latest(a.id());
        assertThat(waiting.status()).isEqualTo(AgentExecution.Status.WAITING_APPROVAL);assertThat(waiting.currentIteration()).isEqualTo(3);
        var request=requests.require(i.toolRequestId());assertThat(request.status()).isEqualTo(ToolRequest.Status.AWAITING_APPROVAL);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM fake_tool_effect WHERE tool_name='refundOrder'",Integer.class)).isZero();
        var delivery=approval(i,true);int calls=models.callCount();
        try(var fresh=freshContext()) {
            for(int n=0;n<5;n++)fresh.getBean(AgentLoopService.class).advance(a.id());
            assertThat(fresh.getBean(AgentRepository.class).iterations(a.id())).hasSize(3);
            assertThat(fresh.getBean(FakeModelProvider.class).callCount()).isZero();
            assertThat(fresh.getBean(ToolRequestRepository.class).require(request.id()).status()).isEqualTo(ToolRequest.Status.AWAITING_APPROVAL);
            fresh.getBean(SignalReceiver.class).receive(i.toolRunId(),delivery);fresh.getBean(SignalReceiver.class).receive(i.toolRunId(),delivery);
        }
        assertThat(models.callCount()).isEqualTo(calls);pump(a.id());var done=agents.get(a.id());
        assertThat(done.status()).isEqualTo(AgentExecution.Status.COMPLETED);assertThat(done.terminationReason()).isEqualTo("FINAL_ANSWER");
        assertThat(done.currentIteration()).isEqualTo(4);assertThat(done.logicalToolCalls()).isEqualTo(3);assertThat(done.logicalModelCalls()).isEqualTo(4);
        assertThat((String)done.finalOutput().get("answer")).contains("Refund fake-","issued");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM fake_tool_effect WHERE tool_name='refundOrder'",Integer.class)).isEqualTo(1);
        assertThat(requests.require(request.id()).status()).isEqualTo(ToolRequest.Status.EXECUTED);
    }
    @Test void finalAndRepeatedTerminalAdvanceNeverMaterializeMoreWork() {
        var a=create("final",AgentLimits.defaults());pump(a.id());var done=agents.get(a.id());int calls=models.callCount();
        for(int n=0;n<6;n++)assertThat(loop.advance(a.id())).isEqualTo(done);
        assertThat(done.status()).isEqualTo(AgentExecution.Status.COMPLETED);assertThat(done.logicalToolCalls()).isZero();
        assertThat(agents.iterations(a.id())).hasSize(1);assertThat(models.callCount()).isEqualTo(calls);assertThat(worker.runOne()).isFalse();
    }
    @Test void runawayStopsAtMaxIterationsAndConcurrentRestartCannotExceedIt() throws Exception {
        var limits=new AgentLimits(2,8,8,500000,1000000,12000,128);var a=create("runaway",limits);pump(a.id());
        assertThat(agents.get(a.id()).terminationReason()).isEqualTo("MAX_ITERATIONS");concurrentAdvance(a.id());
        try(var fresh=freshContext()) {fresh.getBean(AgentLoopService.class).advance(a.id());}
        assertThat(agents.iterations(a.id())).hasSize(2);assertThat(agents.get(a.id()).logicalModelCalls()).isEqualTo(2);
    }
    @Test void modelCallLimitIsIndependentFromIterationLimit() {
        var a=create("runaway",new AgentLimits(8,1,8,500000,1000000,12000,128));pump(a.id());
        assertThat(agents.get(a.id()).terminationReason()).isEqualTo("MAX_MODEL_CALLS");assertThat(agents.get(a.id()).currentIteration()).isEqualTo(1);
    }
    @Test void toolBudgetPreventsFourthToolAfterPersistedDecisionAndRestart() {
        var a=create("runaway",new AgentLimits(5,5,3,500000,1000000,12000,128));
        for(int n=0;n<3;n++) {decision(a.id());loop.advance(a.id());assertThat(worker.runOne()).isTrue();loop.advance(a.id());}
        decision(a.id());assertThat(latest(a.id()).number()).isEqualTo(4);assertThat(latest(a.id()).status()).isEqualTo(AgentIteration.Status.DECIDED);
        int calls=models.callCount();
        try(var fresh=freshContext()) {fresh.getBean(AgentLoopService.class).advance(a.id());assertThat(fresh.getBean(FakeModelProvider.class).callCount()).isZero();}
        assertThat(agents.get(a.id()).terminationReason()).isEqualTo("MAX_TOOL_CALLS");assertThat(latest(a.id()).toolRequestId()).isNull();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM tool_request",Integer.class)).isEqualTo(3);assertThat(models.callCount()).isEqualTo(calls);
    }
    @Test void zeroToolBudgetAllowsModelButNoToolRequest() {
        var a=create("runaway",new AgentLimits(5,5,0,500000,1000000,12000,128));pump(a.id());
        assertThat(agents.get(a.id()).terminationReason()).isEqualTo("MAX_TOOL_CALLS");assertThat(agents.get(a.id()).logicalModelCalls()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM tool_request",Integer.class)).isZero();
    }
    @Test void tokenAndCostLimitsRejectSemanticWorkBeforeQueueMaterialization() {
        for(var limits:List.of(new AgentLimits(5,5,3,1,1000000,12000,128),new AgentLimits(5,5,3,500000,1,12000,128))) {
            var a=create("runaway",limits);loop.advance(a.id());var done=agents.get(a.id());
            assertThat(done.status()).isEqualTo(AgentExecution.Status.LIMIT_REACHED);assertThat(done.currentIteration()).isZero();assertThat(agents.iterations(a.id())).isEmpty();
            assertThat(done.terminationReason()).isEqualTo(limits.maxTotalTokens()==1?"MAX_TOTAL_TOKENS":"MAX_ESTIMATED_COST");
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM activity_task",Integer.class)).isZero();
    }
    @Test void durableReservationAndKnownUsageCannotBeResetAcrossRestartOrByConfigMutation() {
        var a=create("runaway",new AgentLimits(5,5,5,10000,1000000,12000,128));decision(a.id());var saved=agents.get(a.id());
        assertThat(saved.reservedTokens()).isPositive();assertThat(saved.estimatedCostMicros()).isEqualTo(saved.inputTokens()+2*saved.outputTokens());
        assertThatThrownBy(() -> jdbc.update("UPDATE agent_execution SET reserved_tokens=0 WHERE id=?",a.id())).hasMessageContaining("AGENT_COUNTER_REGRESSION");
        assertThatThrownBy(() -> jdbc.update("UPDATE agent_execution SET limits_json='{}'::jsonb WHERE id=?",a.id())).hasMessageContaining("AGENT_IMMUTABLE_EXECUTION");
        try(var fresh=freshContext()) {fresh.getBean(AgentLoopService.class).advance(a.id());assertThat(fresh.getBean(AgentRepository.class).get(a.id()).reservedTokens()).isEqualTo(saved.reservedTokens());}
        assertThat(agents.get(a.id()).inputTokens()).isEqualTo(saved.inputTokens());
    }
    @Test void logicalModelCountDiffersFromRetryAttemptCountAndUsageIsAccountedOnce() {
        var a=create("final",AgentLimits.defaults());loop.advance(a.id());var provider=new FakeModelProvider(List.of(FakeModelProvider.Outcome.UNAVAILABLE,FakeModelProvider.Outcome.SUCCESS));
        try(var harness=new ModelHarness(provider)) {
            assertThat(harness.worker.runOne()).isTrue();var task=tasks.findByActivityExecutionId(latest(a.id()).modelActivityId()).orElseThrow();
            jdbc.update("UPDATE activity_task SET available_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?",task.getId());assertThat(harness.worker.runOne()).isTrue();
        }
        loop.advance(a.id());var saved=agents.get(a.id());assertThat(saved.logicalModelCalls()).isEqualTo(1);assertThat(saved.modelAttempts()).isEqualTo(2);
        assertThat(provider.callCount()).isEqualTo(2);loop.advance(a.id());loop.advance(a.id());assertThat(agents.get(a.id()).inputTokens()).isEqualTo(saved.inputTokens());
    }
    @Test void malformedDecisionFailsWithoutPersistingPrivateReasoningOrCreatingTool() {
        var a=create("malformed",AgentLimits.defaults());pump(a.id());var done=agents.get(a.id());
        assertThat(done.status()).isEqualTo(AgentExecution.Status.FAILED);assertThat(done.terminationReason()).isEqualTo("INVALID_MODEL_DECISION");assertThat(done.inputTokens()).isPositive();
        var output=activities.findById(latest(a.id()).modelActivityId()).orElseThrow().getOutputJson();
        assertThat(ToolJson.encode(output)).doesNotContain("must-never-be-checkpointed","privateReasoning","UNKNOWN");assertThat(latest(a.id()).decision()).isEmpty();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM tool_request",Integer.class)).isZero();
    }
    @Test void rejectionTerminatesWithoutReformulationOrWriteEffect() {
        var a=create("repeat-write",AgentLimits.defaults());pump(a.id());var i=latest(a.id());receiver.receive(i.toolRunId(),approval(i,false));loop.advance(a.id());
        assertThat(agents.get(a.id()).terminationReason()).isEqualTo("TOOL_REJECTED");assertThat(agents.iterations(a.id())).hasSize(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM fake_tool_effect",Integer.class)).isZero();
        assertThat(worker.runOne()).isTrue();assertThat(jdbc.queryForObject("SELECT count(*) FROM fake_tool_effect",Integer.class)).isZero();
    }
    @Test void duplicateRefundProposalIsRejectedInsteadOfCreatingAnotherWriteIdentity() {
        var a=create("repeat-write",AgentLimits.defaults());pump(a.id());var i=latest(a.id());receiver.receive(i.toolRunId(),approval(i,true));pump(a.id());
        assertThat(agents.get(a.id()).terminationReason()).isEqualTo("DUPLICATE_WRITE");assertThat(agents.get(a.id()).logicalToolCalls()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM fake_tool_effect WHERE tool_name='refundOrder'",Integer.class)).isEqualTo(1);
    }
    @Test void parentCompletionSignalCannotBeSpoofedByPublicCaller() {
        var a=create("final",AgentLimits.defaults());assertThatThrownBy(() -> receiver.receive(a.workflowRunId(),new ReceiveSignalRequest("spoof","agentFinished",Map.of())))
                .hasMessage("AGENT_HOST_SIGNAL_IS_INTERNAL");assertThat(agents.get(a.id()).status()).isEqualTo(AgentExecution.Status.RUNNING);
    }
    @Test void completedModelUsageAndDecisionCannotBeCountedTwiceUnderConcurrentAdvance() throws Exception {
        var a=create("final",AgentLimits.defaults());loop.advance(a.id());assertThat(worker.runOne()).isTrue();concurrentAdvance(a.id());
        var done=agents.get(a.id());assertThat(done.status()).isEqualTo(AgentExecution.Status.COMPLETED);assertThat(done.modelAttempts()).isEqualTo(1);
        var output=activities.findById(latest(a.id()).modelActivityId()).orElseThrow().getOutputJson();var usage=ToolJson.object(output.get("usage"));
        assertThat(done.inputTokens()).isEqualTo(((Number)usage.get("inputTokens")).longValue());
    }
    @Test void oversizedContextStopsBeforeAnyModelCall() {
        var a=start.start(new StartAgentRequest("x".repeat(1800),"final",new AgentLimits(5,5,3,500000,1000000,1000,128),null));loop.advance(a.id());
        assertThat(agents.get(a.id()).terminationReason()).isEqualTo("CONTEXT_LIMIT");assertThat(agents.iterations(a.id())).isEmpty();
    }
    @Test void optionalRetrievalEvidenceIsFrozenFromCompletedScopedCheckpoint() {
        String name="AgentRetrieval-"+UUID.randomUUID();definitions.register(new RegisterWorkflowDefinitionRequest(name,1,List.of(new StepDefinition("retrieve","retrieveContext",List.of()))));
        var input=Map.<String,Object>of("retrievalRequest",Map.of("requestVersion",1,"retriever","fake","corpusVersion","policies-v1","rankingVersion","local-v1","query","refund POL-RFD-042",
                "strategy","HYBRID","topK",1,"maxContextChars",1500,"filters",Map.of("tenantId","tenant-a","documentType","policy")));
        var run=runs.start(new StartWorkflowRunRequest(name,1,input));orchestrator.advanceRun(run.runId());assertThat(worker.runOne()).isTrue();
        var retrieval=activities.findByWorkflowRunIdOrderByActivityIdAsc(run.runId()).getFirst();
        var a=start.start(new StartAgentRequest("Review O-42","final",AgentLimits.defaults(),retrieval.getId()));
        assertThat(ToolJson.encode(a.frozenContext())).contains("POL-RFD-042","S1");
        jdbc.update("UPDATE activity_execution SET output_json='{}'::jsonb WHERE id=?",retrieval.getId());
        loop.advance(a.id());assertThat(worker.runOne()).isTrue();
        assertThat(ToolJson.encode(activities.findById(latest(a.id()).modelActivityId()).orElseThrow().getInputJson())).contains("POL-RFD-042","UNTRUSTED_DATA");
    }
    @Test void spentReservationStopsNextModelAfterRestartForBothTokenAndCostCaps() {
        var probe=create("runaway",AgentLimits.defaults());var builder=new AgentContextBuilder();var initial=builder.build(probe,List.of());
        long firstTokens=builder.reservedTokens(initial,probe.maxModelAttempts()),firstCost=builder.reservedCost(initial,probe.maxModelAttempts());
        for(boolean tokenLimit:List.of(true,false)) {
            var a=create("runaway",new AgentLimits(5,5,5,tokenLimit?firstTokens:500000,tokenLimit?1000000:firstCost,12000,128));
            decision(a.id());loop.advance(a.id());assertThat(worker.runOne()).isTrue();loop.advance(a.id());
            try(var fresh=freshContext()) {fresh.getBean(AgentLoopService.class).advance(a.id());}
            assertThat(agents.get(a.id()).terminationReason()).isEqualTo(tokenLimit?"MAX_TOTAL_TOKENS":"MAX_ESTIMATED_COST");
            assertThat(agents.get(a.id()).logicalModelCalls()).isEqualTo(1);assertThat(agents.iterations(a.id())).hasSize(1);
        }
    }
    @Test void increasedWorkerRetryConfigurationCannotExceedFrozenProviderAttemptCap() {
        var a=create("final",AgentLimits.defaults());loop.advance(a.id());var old=transactions.claim("crashed-model-worker").orElseThrow();
        jdbc.update("UPDATE activity_execution SET attempt_count=3 WHERE id=?",old.activityExecutionId());
        jdbc.update("UPDATE activity_task SET lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?",old.taskId());
        try(var fresh=new SpringApplicationBuilder(DurableFlowApplication.class).web(WebApplicationType.NONE).run(
            "--spring.datasource.url="+POSTGRES.getJdbcUrl(),"--spring.datasource.username="+POSTGRES.getUsername(),"--spring.datasource.password="+POSTGRES.getPassword(),
            "--durableflow.worker.retry.max-attempts=5","--durableflow.timers.polling-enabled=false","--durableflow.children.polling-enabled=false","--durableflow.schedules.polling-enabled=false")) {
            assertThat(fresh.getBean(TaskWorker.class).runOne()).isTrue();assertThat(fresh.getBean(FakeModelProvider.class).callCount()).isZero();
            fresh.getBean(AgentLoopService.class).advance(a.id());
        }
        assertThat(agents.get(a.id()).terminationReason()).isEqualTo("MODEL_ACTIVITY_FAILED");assertThat(agents.get(a.id()).logicalModelCalls()).isEqualTo(1);
    }
    private AgentExecution create(String scenario,AgentLimits limits) {return start.start(new StartAgentRequest("Determine refund eligibility for O-42 and process it if allowed.",scenario,limits,null));}
    private AgentIteration latest(UUID id) {return agents.iterations(id).getLast();}
    private void decision(UUID id) {loop.advance(id);assertThat(worker.runOne()).isTrue();loop.advance(id);assertThat(latest(id).status()).isEqualTo(AgentIteration.Status.DECIDED);}
    private void pump(UUID id) {
        for(int n=0;n<96;n++) {var a=loop.advance(id);if(a.terminal() || a.status()==AgentExecution.Status.WAITING_APPROVAL)return;worker.runOne();}
        fail("Agent did not reach a terminal state or human wait within the test transition bound");
    }
    private ReceiveSignalRequest approval(AgentIteration i,boolean allowed) {var r=requests.require(i.toolRequestId());return new ReceiveSignalRequest("agent-review-1","approval",Map.of("toolRequestId",r.id().toString(),"fingerprint",r.fingerprint(),"approved",allowed));}
    private void concurrentAdvance(UUID id) throws Exception {
        var pool=Executors.newFixedThreadPool(4);
        try {var futures=new ArrayList<Future<?>>();for(int n=0;n<12;n++)futures.add(pool.submit(() -> loop.advance(id)));for(var f:futures)f.get(20,TimeUnit.SECONDS);}
        finally {pool.shutdownNow();}
    }
    private ConfigurableApplicationContext freshContext() {return new SpringApplicationBuilder(DurableFlowApplication.class).web(WebApplicationType.NONE).run(
        "--spring.datasource.url="+POSTGRES.getJdbcUrl(),"--spring.datasource.username="+POSTGRES.getUsername(),"--spring.datasource.password="+POSTGRES.getPassword(),
        "--durableflow.timers.polling-enabled=false","--durableflow.children.polling-enabled=false","--durableflow.schedules.polling-enabled=false");}
    private class ModelHarness implements AutoCloseable {
        final TaskWorker worker;
        ModelHarness(FakeModelProvider provider) {
            var settings=new WorkerExecutionSettings(60000,0);var executor=Executors.newSingleThreadExecutor(r -> {var t=new Thread(r,"agent-model-test");t.setDaemon(true);return t;});
            worker=new TaskWorker(transactions,new ActivityRegistry(List.of(new AgentModelActivity(new ModelCallActivity(new ModelGateway(List.of(provider))),agents,activities,new ToolRegistry(List.of())))),
                orchestrator,new WorkerIdentity("agent-model-test"),new WorkerHeartbeats(transactions,new WorkerLeaseSettings(30000,10000)),
                new HandlerExecutions(settings,executor,(future,millis) -> future.get(millis,TimeUnit.MILLISECONDS)),settings);
        }
        public void close() {worker.stop();}
    }
}
