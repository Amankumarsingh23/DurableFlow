package io.durableflow.worker;

import io.durableflow.DurableFlowApplication;
import io.durableflow.tools.*;
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

// Close pollers/pools before this class's disposable PostgreSQL container stops.
@org.springframework.test.annotation.DirtiesContext(classMode = org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@SpringBootTest(properties={"durableflow.timers.polling-enabled=false","durableflow.children.polling-enabled=false","durableflow.schedules.polling-enabled=false"})
class ToolExecutionIntegrationTest {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:17.6");
    @DynamicPropertySource static void database(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url",POSTGRES::getJdbcUrl);r.add("spring.datasource.username",POSTGRES::getUsername);r.add("spring.datasource.password",POSTGRES::getPassword);
    }
    @Autowired WorkflowDefinitionService definitions;
    @Autowired WorkflowRunService runs;
    @Autowired WorkflowTaskOrchestrator orchestrator;
    @Autowired ActivityExecutionRepository activities;
    @Autowired ActivityTaskRepository tasks;
    @Autowired TaskTransactions transactions;
    @Autowired TaskWorker worker;
    @Autowired ToolRequestRepository requests;
    @Autowired ToolRequestService proposalService;
    @Autowired ExecuteToolActivity execution;
    @Autowired FakeToolLedger ledger;
    @Autowired SignalReceiver receiver;
    @Autowired SignalRepository signals;
    @Autowired WorkflowHistoryService history;
    @Autowired JdbcTemplate jdbc;
    @Autowired io.durableflow.workflow.engine.WorkflowLocalExecutor local;
    @BeforeEach void clean() {
        jdbc.execute("TRUNCATE model_route_attempt, model_route, agent_iteration, agent_execution, fake_tool_effect, tool_request, workflow_schedule_occurrence, workflow_schedule, child_workflow_execution, compensation_execution, workflow_signal_delivery, workflow_signal_wait, workflow_timer, workflow_event, activity_task, activity_execution, workflow_run, workflow_definition");
    }
    @Test void groundedRefundWaitsForExactHumanApprovalThenPersistsOneResult() throws Exception {
        UUID run=ready(false); var request=requests.forRun(run).orElseThrow();
        assertThat(request.status()).isEqualTo(ToolRequest.Status.AWAITING_APPROVAL); assertThat(effects()).isZero();
        assertThat(signals.waits(run).getFirst().status()).isEqualTo(SignalWait.Status.WAITING); assertThat(worker.runOne()).isFalse();
        assertThat(activity(run,"recommendation").getOutputJson()).containsKeys("grounding","structuredJson");
        assertThat(request.descriptor().get("riskLevel")).isEqualTo("HIGH_RISK_WRITE");
        assertThat(request.decision()).containsEntry("policyVersion","demo-policy-v1").containsEntry("permission","ALLOWED");
        approve(run,true); assertThat(worker.runOne()).isTrue();
        var completed=requests.forRun(run).orElseThrow(); assertThat(completed.status()).isEqualTo(ToolRequest.Status.EXECUTED);
        assertThat(completed.result()).containsKeys("data","providerReference","executionMetadata");assertThat(effects()).isEqualTo(1);
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        assertThat(ToolJson.freeze(ToolJson.object(activity(run,"execute").getOutputJson().get("toolResult")))).isEqualTo(completed.result());
        assertThat(history.read(run)).filteredOn(e -> e.eventType()==WorkflowEventType.TOOL_EXECUTED).hasSize(1);
        assertThat(history.read(run)).allMatch(e -> !e.metadata().containsKey("arguments") && !e.metadata().containsKey("toolResult"));
    }
    @Test void readOnlyToolAutoApprovesAndNeedsNoSignal() {
        UUID run=ready(true); assertThat(requests.forRun(run).orElseThrow().status()).isEqualTo(ToolRequest.Status.APPROVED);
        assertThat(signals.waits(run)).isEmpty(); assertThat(worker.runOne()).isTrue();assertThat(effects()).isEqualTo(1);
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
    }
    @Test void wrongRequestIdOrFingerprintCannotConsumeApproval() {
        UUID run=ready(false); var r=requests.forRun(run).orElseThrow();
        for(var payload:List.of(Map.<String,Object>of("toolRequestId",UUID.randomUUID().toString(),"fingerprint",r.fingerprint(),"approved",true),
                Map.<String,Object>of("toolRequestId",r.id().toString(),"fingerprint","0".repeat(64),"approved",true),
                Map.<String,Object>of("toolRequestId",r.id().toString(),"fingerprint",r.fingerprint(),"approved",true,"amount",45000)))
            assertThatThrownBy(() -> receiver.receive(run,new ReceiveSignalRequest("bad-"+UUID.randomUUID(),"approval",payload))).hasMessage("TOOL_APPROVAL_BINDING_MISMATCH");
        assertThat(requests.require(r.id()).status()).isEqualTo(ToolRequest.Status.AWAITING_APPROVAL);
        assertThat(signals.forNode(run,"approval")).isEmpty();assertThat(effects()).isZero();
    }
    @Test void earlyBufferedApprovalCannotBypassRequestBinding() {
        UUID run=start(false); assertThatThrownBy(() -> receiver.receive(run,new ReceiveSignalRequest("early","approval",Map.of("approved",true))))
                .hasMessage("TOOL_REQUEST_NOT_READY"); assertThat(signals.forNode(run,"approval")).isEmpty();
    }
    @Test void duplicateApprovalIsHarmlessButChangedDeliveryConflicts() {
        UUID run=ready(false); var signal=approval(run,true); receiver.receive(run,signal);receiver.receive(run,signal);
        assertThat(history.read(run)).filteredOn(e -> e.eventType()==WorkflowEventType.TOOL_APPROVED).hasSize(1);
        assertThatThrownBy(() -> receiver.receive(run,new ReceiveSignalRequest(signal.signalId(),signal.signalNodeId(),Map.of("approved",false))))
                .hasMessageContaining("Signal id reused");
        assertThat(worker.runOne()).isTrue();receiver.receive(run,signal);assertThat(worker.runOne()).isFalse(); assertThat(effects()).isEqualTo(1);
    }
    @Test void humanRejectionNeverExecutesRefund() {
        UUID run=ready(false); approve(run,false); assertThat(requests.forRun(run).orElseThrow().status()).isEqualTo(ToolRequest.Status.REJECTED);
        assertThat(worker.runOne()).isTrue();assertThat(effects()).isZero();assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.FAILED);
    }
    @Test void approvedPayloadAndDecisionsAreImmutableEvenForDirectDatabaseUpdates() {
        UUID run=ready(false);approve(run,true);var r=requests.forRun(run).orElseThrow();
        assertThatThrownBy(() -> jdbc.update("UPDATE tool_request SET arguments_json=CAST(? AS jsonb) WHERE id=?",
                ToolJson.encode(Map.of("orderId","O-42","amount",45000)),r.id())).hasMessageContaining("TOOL_IMMUTABLE_REQUEST");
        assertThatThrownBy(() -> jdbc.update("UPDATE tool_request SET decision_json='{}'::jsonb WHERE id=?",r.id())).hasMessageContaining("TOOL_IMMUTABLE_REQUEST");
        assertThatThrownBy(() -> r.requireIdentity("refundOrder",Map.of("orderId","O-42","amount",45000))).hasMessage("TOOL_IDEMPOTENCY_MISUSE");
        assertThat(worker.runOne()).isTrue();assertThat(effects()).isEqualTo(1);
        assertThat(ToolJson.object(requests.require(r.id()).result().get("data")).get("amount").toString()).isEqualTo("4.5E+3");
    }
    @Test void transientProviderFailureUsesExistingRetryQueueAndStableRequestKey() {
        UUID run=ready(false);approve(run,true);var r=requests.forRun(run).orElseThrow();
        var provider=new FakeMcpToolProvider(ledger,List.of(FakeMcpToolProvider.Outcome.UNAVAILABLE,FakeMcpToolProvider.Outcome.SUCCESS));
        try(var harness=new Harness(provider)) {
            assertThat(harness.worker.runOne()).isTrue();assertThat(effects()).isZero();
            assertThat(requests.require(r.id()).status()).isEqualTo(ToolRequest.Status.APPROVED);
            var task=tasks.findByActivityExecutionId(activity(run,"execute").getId()).orElseThrow();assertThat(task.getStatus()).isEqualTo(ActivityTaskStatus.READY);
            due(task.getId());assertThat(harness.worker.runOne()).isTrue();
        }
        assertThat(provider.callCount()).isEqualTo(2); assertThat(activity(run,"execute").getAttemptCount()).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT idempotency_key FROM fake_tool_effect",String.class)).isEqualTo(r.idempotencyKey());
        assertThat(history.read(run)).filteredOn(e -> e.eventType()==WorkflowEventType.RETRY_SCHEDULED).hasSize(1);
    }
    @Test void responseLostAfterProviderEffectDoesNotDoubleRefundOnRetry() {
        UUID run=ready(false);approve(run,true);var r=requests.forRun(run).orElseThrow();
        var provider=new FakeMcpToolProvider(ledger,List.of(FakeMcpToolProvider.Outcome.AFTER_EFFECT_FAILURE,FakeMcpToolProvider.Outcome.SUCCESS));
        try(var harness=new Harness(provider)) {
            assertThat(harness.worker.runOne()).isTrue(); assertThat(effects()).isEqualTo(1);
            assertThat(requests.require(r.id()).status()).isEqualTo(ToolRequest.Status.APPROVED);
            due(tasks.findByActivityExecutionId(activity(run,"execute").getId()).orElseThrow().getId()); assertThat(harness.worker.runOne()).isTrue();
        }
        assertThat(provider.callCount()).isEqualTo(2);assertThat(effects()).isEqualTo(1);assertThat(requests.require(r.id()).status()).isEqualTo(ToolRequest.Status.EXECUTED);
    }
    @Test void crashAfterEffectBeforeCompletionReclaimsSameKeyAndFencesOldResult() throws Exception {
        UUID run=ready(false);approve(run,true);var r=requests.forRun(run).orElseThrow();
        var old=transactions.claim("old-tool-worker").orElseThrow();var output=execution.execute(old.context(),old.input());
        assertThat(effects()).isEqualTo(1);assertThat(requests.require(r.id()).status()).isEqualTo(ToolRequest.Status.APPROVED);
        expire(old.taskId());
        try(var fresh=freshContext("demo-agent")) {
            var newer=fresh.getBean(TaskTransactions.class).claim("new-tool-worker").orElseThrow();
            var replay=fresh.getBean(ExecuteToolActivity.class).execute(newer.context(),newer.input());assertThat(replay).isEqualTo(output);
            assertThatThrownBy(() -> transactions.complete(old,output)).isInstanceOf(OwnershipLostException.class);
            assertThat(requests.require(r.id()).status()).isEqualTo(ToolRequest.Status.APPROVED);
            fresh.getBean(TaskTransactions.class).complete(newer,replay);fresh.getBean(WorkflowTaskOrchestrator.class).advanceRun(run);
        }
        assertThat(effects()).isEqualTo(1);assertThat(requests.require(r.id()).status()).isEqualTo(ToolRequest.Status.EXECUTED);
        assertThat(history.read(run)).filteredOn(e -> e.eventType()==WorkflowEventType.TOOL_EXECUTED).hasSize(1);
    }
    @Test void completedRequestAndToolResultSurviveFreshContextWithoutProviderCall() throws Exception {
        UUID run=ready(false);approve(run,true);assertThat(worker.runOne()).isTrue();var r=requests.forRun(run).orElseThrow();
        try(var fresh=freshContext("demo-agent")) {
            fresh.getBean(WorkflowTaskOrchestrator.class).advanceRun(run);assertThat(fresh.getBean(TaskWorker.class).runOne()).isFalse();
            // Even explicit handler replay returns the durable fact and makes zero provider calls.
            var output=fresh.getBean(ExecuteToolActivity.class).execute(new ActivityExecutionContext(run,activity(run,"execute").getId()),Map.of());
            assertThat(output.get("toolResult")).isEqualTo(r.result());assertThat(fresh.getBean(FakeMcpToolProvider.class).callCount()).isZero();
        }
        assertThat(effects()).isEqualTo(1);
    }
    @Test void permissionDenialAndPolicyDenialArePersistedAndPreventExecution() {
        UUID denied=start(false);assertThat(worker.runOne()).isTrue();assertThat(worker.runOne()).isTrue();
        try(var fresh=freshContext("demo-reader")) { assertThat(fresh.getBean(TaskWorker.class).runOne()).isTrue(); }
        var deniedRequest=requests.forRun(denied).orElseThrow();assertThat(deniedRequest.status()).isEqualTo(ToolRequest.Status.REJECTED);
        assertThat(deniedRequest.decision()).containsEntry("permission","DENIED");assertThat(runs.get(denied).status()).isEqualTo(WorkflowRunStatus.FAILED);
        UUID capped=start(false);assertThat(worker.runOne()).isTrue();assertThat(worker.runOne()).isTrue();
        replaceProposal(capped,Map.of("toolName","refundOrder","arguments",Map.of("orderId","O-42","amount",45000)));
        assertThat(worker.runOne()).isTrue();assertThat(requests.forRun(capped).orElseThrow().decision()).containsEntry("policy","DENY");assertThat(effects()).isZero();
    }
    @Test void unknownToolsInvalidSchemaAndRiskOverridesAreTerminalBeforeRequestOrEffect() {
        for(var proposal:List.of(Map.<String,Object>of("toolName","deleteAllOrders","arguments",Map.of()),
                Map.<String,Object>of("toolName","refundOrder","arguments",Map.of("orderId","O-42","amount","bad")),
                Map.<String,Object>of("toolName","refundOrder","arguments",Map.of("orderId","O-42","amount",4500),"riskLevel","READ_ONLY"))) {
            UUID run=start(false);assertThat(worker.runOne()).isTrue();assertThat(worker.runOne()).isTrue();replaceProposal(run,proposal);
            assertThat(worker.runOne()).isTrue();assertThat(requests.forRun(run)).isEmpty();assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.FAILED);
        }
        assertThat(effects()).isZero();
    }
    @Test void proposalRecoveryReusesDecisionAndRejectsSameIdentityChangedArguments() {
        UUID run=start(false);assertThat(worker.runOne()).isTrue();assertThat(worker.runOne()).isTrue();
        var claim=transactions.claim("proposal-crash").orElseThrow();var first=proposalService.propose(claim.context(),claim.input());
        // No completion checkpoint yet; a changed deployment must not reinterpret this already-decided request.
        expire(claim.taskId());
        try(var fresh=freshContext("demo-reader")) { assertThat(fresh.getBean(TaskWorker.class).runOne()).isTrue(); }
        assertThat(requests.require(first.id()).decision()).isEqualTo(first.decision());
        replaceProposal(run,Map.of("toolName","refundOrder","arguments",Map.of("orderId","O-42","amount",45000)));
        // Test the semantic identity guard directly while preserving the original proposal execution identity.
        jdbc.update("UPDATE activity_execution SET status='RUNNING' WHERE id=?",claim.activityExecutionId());
        assertThatThrownBy(() -> proposalService.propose(claim.context(),claim.input())).hasMessage("TOOL_IDEMPOTENCY_MISUSE");
        assertThat(effects()).isZero();
    }
    @Test void providerLedgerRejectsSameKeyWithDifferentSemantics() {
        var args=Map.<String,Object>of("orderId","O-42","amount",4500);String key="df:v1:tool:"+UUID.randomUUID();
        ledger.apply("refundOrder",args,ToolJson.fingerprint("refundOrder",args),key);
        var changed=Map.<String,Object>of("orderId","O-42","amount",45000);
        assertThatThrownBy(() -> ledger.apply("refundOrder",changed,ToolJson.fingerprint("refundOrder",changed),key)).hasMessage("TOOL_PROVIDER_IDEMPOTENCY_MISUSE");
        assertThat(effects()).isEqualTo(1);
    }
    @Test void invalidCompletionRollsBackRequestTaskActivityAndSemanticAuditTogether() throws Exception {
        UUID run=ready(false);approve(run,true);var request=requests.forRun(run).orElseThrow();
        var claim=transactions.claim("completion-test").orElseThrow();var output=execution.execute(claim.context(),claim.input());
        var invalid=new HashMap<String,Object>(output);invalid.put("fingerprint","0".repeat(64));
        assertThatThrownBy(() -> transactions.complete(claim,invalid)).hasMessage("TOOL_COMPLETION_IDENTITY");
        assertThat(requests.require(request.id()).status()).isEqualTo(ToolRequest.Status.APPROVED);
        assertThat(activity(run,"execute").getStatus()).isEqualTo(ActivityExecutionStatus.RUNNING);
        assertThat(tasks.findById(claim.taskId()).orElseThrow().getStatus()).isEqualTo(ActivityTaskStatus.CLAIMED);
        assertThat(history.read(run)).noneMatch(e -> e.eventType()==WorkflowEventType.TOOL_EXECUTED);
        transactions.complete(claim,output);orchestrator.advanceRun(run);
        assertThat(requests.require(request.id()).status()).isEqualTo(ToolRequest.Status.EXECUTED);assertThat(effects()).isEqualTo(1);
    }
    private UUID ready(boolean read) { var run=start(read);for(int i=0;i<(read?2:3);i++)assertThat(worker.runOne()).isTrue();return run; }
    @Test void legacySynchronousRunnerCannotInvokeToolRequestOrExecutionHandlers() {
        UUID run=start(true,false);assertThat(local.executeRun(run).status()).isEqualTo(WorkflowRunStatus.FAILED);
        assertThat(requests.forRun(run)).isEmpty();assertThat(effects()).isZero();
        assertThat(activity(run,"proposal").getErrorMessage()).contains("requires advance and durable task workers");
    }
    private UUID start(boolean read) { return start(read,true); }
    private UUID start(boolean read,boolean queued) {
        var steps=new ArrayList<StepDefinition>();if(!read)steps.add(new StepDefinition("policies","retrieveContext",List.of()));
        steps.add(new StepDefinition("recommendation","modelCall",read?List.of():List.of("policies")));
        steps.add(new StepDefinition("proposal","createToolRequest",List.of("recommendation")));
        if(!read)steps.add(new StepDefinition("approval",null,List.of("proposal"),NodeType.SIGNAL,null));
        steps.add(new StepDefinition("execute","executeTool",List.of(read?"proposal":"approval")));
        String name="Tools-"+UUID.randomUUID();definitions.register(new RegisterWorkflowDefinitionRequest(name,1,steps));
        var input=new LinkedHashMap<String,Object>();input.put("modelRequest",Map.of("requestVersion",1,"provider","fake","model",read?"fake-tool-read-v1":"fake-tool-refund-v1",
                "systemInstructions","Propose one bounded request; do not execute.","userContent","Order O-42 refund 4500.","temperature",0.0,"maxOutputTokens",256,"structuredOutput",true));
        input.put("toolBinding",read?Map.of("sourceNodeId","recommendation","executeNodeId","execute"):Map.of("sourceNodeId","recommendation","executeNodeId","execute","approvalNodeId","approval"));
        if(!read) { input.put("grounding",Map.of("groundingVersion",1,"sourceNodeId","policies"));
            input.put("retrievalRequest",Map.of("requestVersion",1,"retriever","fake","corpusVersion","policies-v1","rankingVersion","local-v1","query","refund POL-RFD-042",
                    "strategy","HYBRID","topK",3,"maxContextChars",8000,"filters",Map.of("tenantId","tenant-a","documentType","policy"))); }
        UUID run=runs.start(new StartWorkflowRunRequest(name,1,input)).runId();if(queued)orchestrator.advanceRun(run);return run;
    }
    private ActivityExecutionEntity activity(UUID run,String node) { return activities.findByWorkflowRunIdOrderByActivityIdAsc(run).stream().filter(a -> a.getActivityId().equals(node)).findFirst().orElseThrow(); }
    private int effects() { return Objects.requireNonNull(jdbc.queryForObject("SELECT count(*) FROM fake_tool_effect",Integer.class)); }
    private void due(UUID task) { jdbc.update("UPDATE activity_task SET available_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?",task); }
    private void expire(UUID task) { jdbc.update("UPDATE activity_task SET lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?",task); }
    private void replaceProposal(UUID run,Map<String,Object> proposal) {
        jdbc.update("UPDATE activity_execution SET output_json=CAST(? AS jsonb) WHERE id=?",ToolJson.encode(Map.of("structuredJson",Map.of("toolProposal",proposal))),activity(run,"recommendation").getId());
    }
    private ReceiveSignalRequest approval(UUID run,boolean approved) { var r=requests.forRun(run).orElseThrow();return new ReceiveSignalRequest("review-1","approval",Map.of("toolRequestId",r.id().toString(),"fingerprint",r.fingerprint(),"approved",approved)); }
    private void approve(UUID run,boolean approved) { receiver.receive(run,approval(run,approved)); }
    private ConfigurableApplicationContext freshContext(String actor) {
        return new SpringApplicationBuilder(DurableFlowApplication.class).web(WebApplicationType.NONE).run("--spring.datasource.url="+POSTGRES.getJdbcUrl(),
                "--spring.datasource.username="+POSTGRES.getUsername(),"--spring.datasource.password="+POSTGRES.getPassword(),"--durableflow.tools.actor-id="+actor,
                "--durableflow.timers.polling-enabled=false","--durableflow.children.polling-enabled=false","--durableflow.schedules.polling-enabled=false");
    }
    private class Harness implements AutoCloseable {
        final TaskWorker worker;
        Harness(ToolProvider provider) {
            var settings=new WorkerExecutionSettings(60000,0);var executor=Executors.newSingleThreadExecutor(r -> {var t=new Thread(r,"test-tools");t.setDaemon(true);return t;});
            worker=new TaskWorker(transactions,new ActivityRegistry(List.of(new ExecuteToolActivity(requests,activities,new ToolRegistry(List.of(provider))))),
                    orchestrator,new WorkerIdentity("tool-test"),new WorkerHeartbeats(transactions,new WorkerLeaseSettings(30000,10000)),
                    new HandlerExecutions(settings,executor,(future,millis) -> future.get(millis,TimeUnit.MILLISECONDS)),settings);
        }
        public void close() { worker.stop(); }
    }
}
