package io.durableflow.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.durableflow.agent.*;
import io.durableflow.ai.activity.ModelCallActivity;
import io.durableflow.ai.model.*;
import io.durableflow.ai.provider.*;
import io.durableflow.ai.retrieval.*;
import io.durableflow.ai.routing.*;
import io.durableflow.evaluation.*;
import io.durableflow.tools.*;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.signal.*;
import io.durableflow.workflow.task.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.*;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.nio.file.*;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;
import static org.assertj.core.api.Assertions.*;

/** Offline test-friendly demo: executes real PostgreSQL engine paths, never canned observed answers. */
// Close pollers/pools before this class's disposable PostgreSQL container stops.
@org.springframework.test.annotation.DirtiesContext(classMode = org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@SpringBootTest(properties={"durableflow.timers.polling-enabled=false","durableflow.children.polling-enabled=false","durableflow.schedules.polling-enabled=false"})
class GoldenEvalIntegrationTest {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:17.6");
    @DynamicPropertySource static void database(DynamicPropertyRegistry r) {r.add("spring.datasource.url",POSTGRES::getJdbcUrl);r.add("spring.datasource.username",POSTGRES::getUsername);r.add("spring.datasource.password",POSTGRES::getPassword);}
    @Autowired AgentStartService start;@Autowired AgentLoopService loop;@Autowired AgentRepository agents;
    @Autowired AgentSummaryService summaries;@Autowired TaskWorker worker;@Autowired TaskTransactions transactions;
    @Autowired ActivityExecutionRepository activities;@Autowired ToolRequestRepository requests;
    @Autowired ExecuteToolActivity execution;@Autowired SignalReceiver receiver;@Autowired JdbcTemplate jdbc;
    @Autowired WorkflowDefinitionService definitions;@Autowired WorkflowRunService runs;
    @Autowired WorkflowTaskOrchestrator orchestrator;@Autowired ModelRouteRepository routes;
    @Autowired RouteTransactions routeTransactions;@Autowired ProviderCircuitBreaker breaker;
    private EvalSuite suite;
    @Test void runGoldenSuiteAndWriteReportsEvenWhenAContractFails() throws Exception {
        try(var stream=getClass().getResourceAsStream("/eval/golden-v1.json")) {suite=new ObjectMapper().readValue(stream,EvalSuite.class);}
        var report=new EvalRunner(new ObjectiveEvaluator()).run(suite,this::execute);
        Files.createDirectories(Path.of("target"));
        Files.writeString(Path.of("target/eval-report.json"),EvalReportRenderer.json(report));
        Files.writeString(Path.of("target/eval-report.md"),EvalReportRenderer.markdown(report));
        assertThat(report.failed()).withFailMessage("Golden failures: %s",EvalReportRenderer.json(report)).isZero();
        assertThat(report.total()).isEqualTo(12);
    }
    private EvalObservation execute(EvalCase test) throws Exception {
        jdbc.execute("TRUNCATE model_route_attempt, model_route, agent_iteration, agent_execution, fake_tool_effect, tool_request, workflow_schedule_occurrence, workflow_schedule, child_workflow_execution, compensation_execution, workflow_signal_delivery, workflow_signal_wait, workflow_timer, workflow_event, activity_task, activity_execution, workflow_run, workflow_definition");
        breaker.success("fake-a");breaker.success("fake-b");
        Map<String,Object> facts=switch(test.scenario()) {
            case "retrieval" -> retrieve(test.input());
            case "fallback","quality" -> route(test.scenario().equals("quality"));
            case "crash" -> crash();
            default -> agent(test.scenario());
        };
        // Versions are fixture/config constants, not copied from expected suite values.
        var versions=new LinkedHashMap<String,String>();
        versions.put("dataset","golden-v1");versions.put("modelRequest","1");versions.put("agentContext","1");versions.put("modelRoutingPolicy","cost-quality-v1");versions.put("prompt","agent-context-v1");
        versions.put("retrievalIndex","policies-v1/local-v1");versions.put("retrievalStrategy","per-case-v1");versions.put("toolPolicy","demo-policy-v1");versions.put("workflowDefinition","session26-fixtures-v1");
        if(facts.get("strategy")!=null)versions.put("actualRetrievalStrategy",facts.get("strategy").toString());
        var policies=jdbc.queryForList("SELECT DISTINCT plan_json->>'policyVersion' AS version FROM model_route");
        if(!policies.isEmpty())versions.put("modelRoutingPolicy",Objects.toString(policies.getFirst().get("version"),"MISSING"));
        var workflows=jdbc.queryForList("SELECT DISTINCT name,version FROM workflow_definition ORDER BY name");
        // Random run-specific definition names are replaced by the declared fixture family in reports.
        versions.put("actualWorkflowVersions",workflows.stream().map(m -> m.get("name").toString().startsWith("Golden-")?"Golden/"+m.get("version"):m.get("name")+"/"+m.get("version")).distinct().toList().toString());
        var models=jdbc.queryForList("SELECT DISTINCT provider,model FROM model_route_attempt ORDER BY provider,model");
        if(!models.isEmpty())versions.put("actualProviderModels",models.toString());
        return new EvalObservation(facts,versions);
    }
    private Map<String,Object> agent(String scenario) throws Exception {
        UUID retrieval=null;var extra=new LinkedHashMap<String,Object>();
        if(scenario.equals("hostile")) {
            UUID run=retrievalRun(Map.of("query","UNTRUSTED-NOTE-9","strategy","SPARSE","documentType","note"));
            var checkpoint=activities.findByWorkflowRunIdOrderByActivityIdAsc(run).getFirst();retrieval=checkpoint.getId();
            extra.putAll(retrievalFacts(checkpoint.getOutputJson()));
        }
        String semantic=switch(scenario) {case "approve" -> "refund";case "reject","hostile" -> "repeat-write";case "read-only","runaway" -> "runaway";default -> "final";};
        var limits=scenario.equals("read-only")?new AgentLimits(1,8,6,500000,1000000,12000,128):scenario.equals("runaway")?new AgentLimits(2,8,6,500000,1000000,12000,128):AgentLimits.defaults();
        var a=start.start(new StartAgentRequest("Review synthetic order O-42 under policy.",semantic,limits,retrieval));
        if(scenario.equals("unknown")) {
            loop.advance(a.id());assertThat(worker.runOne()).isTrue();var i=latest(a.id());
            var output=new LinkedHashMap<>(activities.findById(i.modelActivityId()).orElseThrow().getOutputJson());
            var decision=Map.<String,Object>of("type","TOOL_CALL","toolName","deleteAllOrders","arguments",Map.of());
            // Explicit adversarial fixture injection at the model checkpoint. Never exposed by an API.
            output.put("structuredJson",decision);output.put("decisionSchemaValid",true);
            jdbc.update("UPDATE activity_execution SET output_json=CAST(? AS jsonb) WHERE id=?",ToolJson.encode(output),i.modelActivityId());
        }
        pump(a.id());
        if(Set.of("approve","reject","hostile").contains(scenario)) {
            var i=latest(a.id());var request=requests.require(i.toolRequestId());
            extra.put("approvalRequired",request.status()==ToolRequest.Status.AWAITING_APPROVAL);
            extra.put("effectsBeforeApproval",refundEffects());extra.put("toolName",request.toolName());extra.put("fingerprint",request.fingerprint());
            receiver.receive(i.toolRunId(),new ReceiveSignalRequest("golden-review","approval",Map.of("toolRequestId",request.id().toString(),"fingerprint",request.fingerprint(),"approved",scenario.equals("approve"))));
            pump(a.id());
        }
        var done=agents.get(a.id());var summary=summaries.get(a.id());
        extra.put("status",done.status().name());extra.put("reason",Objects.toString(done.terminationReason(),"PENDING"));
        extra.put("iterations",done.currentIteration());extra.put("modelCalls",done.logicalModelCalls());extra.put("providerAttempts",summary.providerAttempts());
        extra.put("toolCalls",done.logicalToolCalls());extra.put("costMicros",summary.knownCostMicros());extra.put("fallbacks",summary.fallbackTransitions());
        extra.put("reservedCostMicros",done.reservedCostMicros());extra.put("reservedTokens",done.reservedTokens());
        extra.put("iterationsBeyondLimit",Math.max(0,done.currentIteration()-done.limits().maxIterations()));extra.put("refundEffects",refundEffects());
        extra.put("toolRequests",jdbc.queryForObject("SELECT count(*) FROM tool_request",Long.class));
        extra.put("unauthorizedExecutions",jdbc.queryForObject("SELECT count(*) FROM fake_tool_effect WHERE tool_name NOT IN ('getOrder','getRefundPolicy','refundOrder')",Long.class));
        boolean schemaValid=true;
        for(var i:agents.iterations(a.id())) {
            try {AgentDecision.parse(i.decision());}catch(IllegalArgumentException invalid) {schemaValid=false;}
        }
        extra.put("schemaValid",schemaValid);
        if(scenario.equals("read-only")) {var r=requests.require(latest(a.id()).toolRequestId());extra.put("toolName",r.toolName());extra.put("approvalRequired",r.approvalNodeId()!=null);}
        return extra;
    }
    private void pump(UUID id) {
        for(int n=0;n<96;n++) {var a=loop.advance(id);if(a.terminal() || a.status()==AgentExecution.Status.WAITING_APPROVAL)return;worker.runOne();}
        throw new IllegalStateException("GOLDEN_TRANSITION_BOUND");
    }
    private AgentIteration latest(UUID id) {return agents.iterations(id).getLast();}
    private long refundEffects() {return Objects.requireNonNull(jdbc.queryForObject("SELECT count(*) FROM fake_tool_effect WHERE tool_name='refundOrder'",Long.class));}
    private Map<String,Object> retrieve(Map<String,Object> input) {return retrievalFacts(activities.findByWorkflowRunIdOrderByActivityIdAsc(retrievalRun(input)).getFirst().getOutputJson());}
    private UUID retrievalRun(Map<String,Object> input) {
        var request=Map.<String,Object>of("requestVersion",1,"retriever","fake","corpusVersion","policies-v1","rankingVersion","local-v1","query",input.get("query"),"strategy",input.get("strategy"),"topK",3,"maxContextChars",3000,"filters",Map.of("tenantId","tenant-a","documentType",input.get("documentType")));
        UUID id=single("retrieveContext",Map.of("retrievalRequest",request));assertThat(worker.runOne()).isTrue();return id;
    }
    private Map<String,Object> retrievalFacts(Map<String,Object> output) {
        var chunks=(List<?>)output.get("selectedChunks");var ids=chunks.stream().map(ToolJson::object).map(m -> (String)m.get("chunkId")).toList();
        var result=new LinkedHashMap<String,Object>();result.put("sourceIds",ids);result.put("strategy",output.get("strategy"));
        result.put("contextChars",ToolJson.object(output.get("context")).get("characterCount"));
        result.put("unauthorizedDataAbsent",!ToolJson.encode(output).contains("PRIVATE-B-777") && !ids.contains("private-b:v1:000"));return result;
    }
    private UUID single(String type,Map<String,Object> input) {
        String name="Golden-"+UUID.randomUUID();definitions.register(new RegisterWorkflowDefinitionRequest(name,1,List.of(new StepDefinition("step",type,List.of()))));
        UUID id=runs.start(new StartWorkflowRunRequest(name,1,input)).runId();orchestrator.advanceRun(id);return id;
    }
    private Map<String,Object> route(boolean quality) {
        var delegate=new FakeModelProvider();var pa=new FakeAProvider(delegate,"UNAVAILABLE,UNAVAILABLE");var pb=new FakeBProvider(delegate,"SUCCESS");
        var catalog=quality?new ModelCatalog(List.of(
            new ModelDescriptor("fake-a","balanced-v1",2,1,2,new ModelCapabilities(true,true,24000),true,Set.of("local")),
            new ModelDescriptor("fake-b","economy-v1",1,1,1,new ModelCapabilities(true,true,24000),true,Set.of("local")))):new ModelCatalog();
        var routing=new ModelRoutingService(new ModelRouter(catalog,breaker),new RoutingPolicy("fake-a,fake-b","","local",2,false,2,2,3),routeTransactions,routes,breaker,List.of(pa,pb));
        var model=new ModelCallActivity(new ModelGateway(List.of(pa,pb),routing));
        UUID run=single("modelCall",Map.of("modelRequest",Map.of("requestVersion",1,"provider","routed","model","recommendation-v1","systemInstructions","Bounded synthetic recommendation.","userContent","Review O-42.","temperature",0.0,"maxOutputTokens",128,"structuredOutput",true)));
        try(var harness=new Harness(model)) {
            for(int n=0;n<4;n++) {
                jdbc.update("UPDATE activity_task SET available_at=clock_timestamp()-INTERVAL '1 second' WHERE status='READY'");
                if(!harness.worker.runOne())break;
            }
        }
        var activity=activities.findByWorkflowRunIdOrderByActivityIdAsc(run).getFirst();var r=routes.find(activity.getId()).orElseThrow();var usage=routes.summary(activity.getId()).orElseThrow();
        return Map.of("status",runs.get(run).status().name(),"reason",Objects.toString(r.code(),"SUCCESS"),"finalProvider",Objects.toString(activity.getOutputJson().get("provider"),"NONE"),
            "fallbacks",r.activeIndex(),"providerAttempts",r.totalAttempts(),"weakProviderExecutions",pb.invocations().stream().filter(i -> i.request().model().equals("economy-v1")).count(),"modelCalls",1,"costMicros",usage.knownCostMicros());
    }
    private Map<String,Object> crash() throws Exception {
        var a=start.start(new StartAgentRequest("Refund synthetic O-42 if allowed.","repeat-write",AgentLimits.defaults(),null));pump(a.id());var i=latest(a.id());var request=requests.require(i.toolRequestId());
        receiver.receive(i.toolRunId(),new ReceiveSignalRequest("approve-crash","approval",Map.of("toolRequestId",request.id().toString(),"fingerprint",request.fingerprint(),"approved",true)));
        var old=transactions.claim("golden-crashed-worker").orElseThrow();var output=execution.execute(old.context(),old.input());
        jdbc.update("UPDATE activity_task SET lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?",old.taskId());
        var replacement=transactions.claim("golden-replacement").orElseThrow();var replay=execution.execute(replacement.context(),replacement.input());
        int accepted=0;try {transactions.complete(old,output);accepted++;}catch(OwnershipLostException expected) { /* measured rejection */ }
        int mutations=0;try {jdbc.update("UPDATE tool_request SET arguments_json='{}'::jsonb WHERE id=?",request.id());mutations++;}catch(org.springframework.dao.DataAccessException expected) { /* immutable approved payload */ }
        transactions.complete(replacement,replay);orchestrator.advanceRun(i.toolRunId());
        return Map.of("refundEffects",refundEffects(),"duplicateExternalEffects",Math.max(0,refundEffects()-1),"staleCompletionsAccepted",accepted,"approvedPayloadMutationsAccepted",mutations,"requestStatus",requests.require(request.id()).status().name());
    }
    private class Harness implements AutoCloseable {
        final TaskWorker worker;
        Harness(ActivityHandler handler) {
            var settings=new WorkerExecutionSettings(60000,0);var executor=Executors.newSingleThreadExecutor();
            worker=new TaskWorker(transactions,new ActivityRegistry(List.of(handler)),orchestrator,new WorkerIdentity("golden-routing"),new WorkerHeartbeats(transactions,new WorkerLeaseSettings(30000,10000)),new HandlerExecutions(settings,executor,(future,millis) -> future.get(millis,TimeUnit.MILLISECONDS)),settings);
        }
        public void close() {worker.stop();}
    }
}
