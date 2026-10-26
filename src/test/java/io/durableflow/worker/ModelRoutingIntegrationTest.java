package io.durableflow.worker;
import io.durableflow.DurableFlowApplication;
import io.durableflow.ai.activity.ModelCallActivity;
import io.durableflow.ai.model.*;
import io.durableflow.ai.routing.*;
import io.durableflow.agent.*;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.run.*;
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
import static org.assertj.core.api.Assertions.*;

// Close pollers/pools before this class's disposable PostgreSQL container stops.
@org.springframework.test.annotation.DirtiesContext(classMode = org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
@SpringBootTest(properties={"durableflow.timers.polling-enabled=false","durableflow.children.polling-enabled=false","durableflow.schedules.polling-enabled=false",
    "durableflow.routing.fake-a-script=UNAVAILABLE,UNAVAILABLE","durableflow.routing.max-provider-attempts=4","durableflow.worker.retry.max-attempts=5"})
class ModelRoutingIntegrationTest {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:17.6");
    @DynamicPropertySource static void database(DynamicPropertyRegistry r){r.add("spring.datasource.url",POSTGRES::getJdbcUrl);r.add("spring.datasource.username",POSTGRES::getUsername);r.add("spring.datasource.password",POSTGRES::getPassword);}
    @Autowired WorkflowDefinitionService definitions;@Autowired WorkflowRunService runs;@Autowired WorkflowTaskOrchestrator orchestrator;
    @Autowired TaskWorker worker;@Autowired TaskTransactions transactions;@Autowired ActivityExecutionRepository activities;@Autowired ActivityTaskRepository tasks;
    @Autowired ModelRouteRepository routes;@Autowired ModelCallActivity model;@Autowired FakeAProvider a;@Autowired FakeBProvider b;@Autowired ProviderCircuitBreaker breaker;
    @Autowired AgentStartService startAgent;@Autowired AgentLoopService loop;@Autowired AgentRepository agents;@Autowired JdbcTemplate jdbc;
    @BeforeEach void clean(){jdbc.execute("TRUNCATE model_route_attempt, model_route, agent_iteration, agent_execution, fake_tool_effect, tool_request, workflow_schedule_occurrence, workflow_schedule, child_workflow_execution, compensation_execution, workflow_signal_delivery, workflow_signal_wait, workflow_timer, workflow_event, activity_task, activity_execution, workflow_run, workflow_definition");breaker.success("fake-a");breaker.success("fake-b");}
    @Test void retriesSamePrimaryTwiceThenDurablyFallsBackAndFreezesSemanticRequest() {
        UUID run=standalone();assertThat(worker.runOne()).isTrue();UUID activity=activity(run).getId();var first=routes.find(activity).orElseThrow();
        assertThat(first.activeIndex()).isZero();assertThat(first.totalAttempts()).isEqualTo(1);due(activity);assertThat(worker.runOne()).isTrue();
        assertThat(routes.find(activity).orElseThrow().activeIndex()).isEqualTo(1);due(activity);assertThat(worker.runOne()).isTrue();
        var output=activity(run).getOutputJson();assertThat(output).containsEntry("provider","fake-b").containsEntry("model","premium-v1");
        var metadata=io.durableflow.tools.ToolJson.object(output.get("routing"));assertThat(metadata).containsKey("logicalModelCallId");assertThat(metadata.get("fallbackUsed")).isEqualTo(true);
        assertThat(routes.attempts(activity)).extracting(m -> m.get("provider")).containsExactly("fake-a","fake-a","fake-b");
        var ai=a.invocations().stream().filter(i -> i.key().contains(activity.toString())).toList();var bi=b.invocations().stream().filter(i -> i.key().contains(activity.toString())).findFirst().orElseThrow();
        assertThat(ai).hasSize(2);assertThat(ai.getFirst().request().userContent()).isEqualTo(bi.request().userContent());assertThat(ai.getFirst().request().systemInstructions()).isEqualTo(bi.request().systemInstructions());
        assertThat(ai.getFirst().key()).isEqualTo(ai.getLast().key());assertThat(activity(run).getAttemptCount()).isEqualTo(3);
    }
    @Test void fallbackOutputPersistsAcrossFreshContextWithoutProviderInvocation() {
        UUID run=standalone();finish(worker,run,3);var output=activity(run).getOutputJson();
        try(var fresh=fresh()) {fresh.getBean(WorkflowTaskOrchestrator.class).advanceRun(run);assertThat(fresh.getBean(TaskWorker.class).runOne()).isFalse();
            assertThat(fresh.getBean(FakeAProvider.class).invocations()).isEmpty();assertThat(fresh.getBean(FakeBProvider.class).invocations()).isEmpty();}
        assertThat(activity(run).getOutputJson()).isEqualTo(output);
    }
    @Test void allPrevalidatedRoutesExhaustWithoutInfiniteHopping() {
        UUID run=standalone();try(var fresh=fresh("--durableflow.routing.fake-b-script=UNAVAILABLE")){finish(fresh.getBean(TaskWorker.class),run,4);}
        var route=routes.find(activity(run).getId()).orElseThrow();assertThat(route.status()).isEqualTo("EXHAUSTED");assertThat(route.totalAttempts()).isEqualTo(4);
        assertThat(activity(run).getErrorMessage()).contains("MODEL_ROUTE_EXHAUSTED");assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.FAILED);
    }
    @Test void requestAndAuthenticationErrorsAreTerminalWithoutBlindFallbackOrBreakerOpening() {
        for(String script:List.of("INVALID_REQUEST","AUTHENTICATION","UNSUPPORTED_CAPABILITY")) {
            UUID run=standalone();try(var fresh=fresh("--durableflow.routing.fake-a-script="+script)) {
                assertThat(fresh.getBean(TaskWorker.class).runOne()).isTrue();assertThat(fresh.getBean(FakeBProvider.class).invocations()).isEmpty();
                assertThat(fresh.getBean(ProviderCircuitBreaker.class).state("fake-a")).isEqualTo(ProviderCircuitBreaker.State.CLOSED);
            }
            assertThat(routes.find(activity(run).getId()).orElseThrow().status()).isEqualTo("TERMINAL");assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.FAILED);
        }
    }
    @Test void rateLimitUsesExistingBackoffThenFallback() {
        UUID run=standalone();try(var fresh=fresh("--durableflow.routing.fake-a-script=RATE_LIMITED")) {finish(fresh.getBean(TaskWorker.class),run,3);}
        assertThat(routes.attempts(activity(run).getId()).getFirst()).containsEntry("failure_code","MODEL_RATE_LIMITED");assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
    }
    @Test void typedProviderTimeoutFollowsSameBoundedHealthPolicy() {
        UUID run=standalone();try(var fresh=fresh("--durableflow.routing.fake-a-script=TIMEOUT")) {finish(fresh.getBean(TaskWorker.class),run,3);}
        assertThat(routes.attempts(activity(run).getId()).getFirst()).containsEntry("failure_code","MODEL_TIMEOUT");
    }
    @Test void openProviderIsSkippedForNewStandaloneSelectionInSameProcess() {
        UUID first=standalone();finish(worker,first,3);assertThat(breaker.state("fake-a")).isEqualTo(ProviderCircuitBreaker.State.OPEN);
        UUID second=standalone();assertThat(worker.runOne()).isTrue();var decision=routes.find(activity(second).getId()).orElseThrow().plan();
        assertThat(decision.primary().provider()).isEqualTo("fake-b");assertThat(routes.attempts(activity(second).getId())).hasSize(1);
    }
    @Test void persistedPlanSurvivesCrashBeforeInvocationAndPolicyChange() {
        var agent=startAgent.start(new StartAgentRequest("Finish safely","final",AgentLimits.defaults(),null));loop.advance(agent.id());
        var iteration=agents.iterations(agent.id()).getFirst();var snapshot=routes.find(iteration.modelActivityId()).orElseThrow();
        var claim=transactions.claim("crash-before-provider").orElseThrow();expire(claim.taskId());
        try(var fresh=fresh("--durableflow.routing.allowed-providers=fake-b","--durableflow.routing.fake-a-script=SUCCESS","--durableflow.routing.required-quality=3")) {
            assertThat(fresh.getBean(TaskWorker.class).runOne()).isTrue();assertThat(fresh.getBean(FakeAProvider.class).invocations()).hasSize(1);
        }
        assertThat(routes.find(iteration.modelActivityId()).orElseThrow().plan()).isEqualTo(snapshot.plan());assertThat(routes.attempts(iteration.modelActivityId()).getFirst()).containsEntry("provider","fake-a");
    }
    @Test void fallbackGenerationLostBeforeWorkerCompletionMayBeGeneratedAgainWithinDurableBounds() throws Exception {
        UUID run=standalone();assertThat(worker.runOne()).isTrue();due(activity(run).getId());assertThat(worker.runOne()).isTrue();due(activity(run).getId());
        var old=transactions.claim("lost-fallback-worker").orElseThrow();var output=model.execute(old.context(),old.input());assertThat(output.get("provider")).isEqualTo("fake-b");
        assertThat(activity(run).getStatus()).isEqualTo(ActivityExecutionStatus.RUNNING);expire(old.taskId());
        try(var fresh=fresh()) {
            assertThat(fresh.getBean(TaskWorker.class).runOne()).isTrue();assertThat(fresh.getBean(FakeAProvider.class).invocations()).isEmpty();assertThat(fresh.getBean(FakeBProvider.class).invocations()).hasSize(1);
        }
        assertThat(routes.find(activity(run).getId()).orElseThrow().totalAttempts()).isEqualTo(4);
        assertThatThrownBy(() -> transactions.complete(old,output)).isInstanceOf(OwnershipLostException.class);
        assertThat(routes.summary(activity(run).getId()).orElseThrow().inputTokens()).isGreaterThan(((Number)((Map<?,?>)output.get("usage")).get("inputTokens")).longValue());
    }
    @Test void agentFallbackRemainsOneLogicalModelCallWithSeparateAttemptUsageAndCost() {
        var agent=startAgent.start(new StartAgentRequest("Return final answer","final",AgentLimits.defaults(),null));loop.advance(agent.id());var i=agents.iterations(agent.id()).getFirst();
        finish(worker,i.modelRunId(),3);loop.advance(agent.id());loop.advance(agent.id());var done=agents.get(agent.id());
        assertThat(done.status()).isEqualTo(AgentExecution.Status.COMPLETED);assertThat(done.logicalModelCalls()).isEqualTo(1);assertThat(done.modelAttempts()).isEqualTo(3);
        assertThat(done.estimatedCostMicros()).isEqualTo(done.inputTokens()*3+done.outputTokens()*4);assertThat(done.reservedCostMicros()).isGreaterThan(i.reservedCostMicros());
        assertThat(done.currentIteration()).isEqualTo(1);assertThat(done.logicalToolCalls()).isZero();
    }
    @Test void agentHardCostCapPreventsFallbackBeforeProviderInvocation() {
        var probe=startAgent.start(new StartAgentRequest("Return final answer","final",AgentLimits.defaults(),null));var builder=new AgentContextBuilder();var request=builder.build(probe,List.of());
        long budget=builder.reservedCost(request,probe.maxModelAttempts());var limits=new AgentLimits(8,8,6,500000,budget,12000,128);
        var agent=startAgent.start(new StartAgentRequest("Return final answer","final",limits,null));loop.advance(agent.id());var i=agents.iterations(agent.id()).getFirst();
        int before=b.invocations().size();finish(worker,i.modelRunId(),3);loop.advance(agent.id());var done=agents.get(agent.id());
        assertThat(done.status()).isEqualTo(AgentExecution.Status.LIMIT_REACHED);assertThat(done.terminationReason()).isEqualTo("MAX_ESTIMATED_COST");
        assertThat(b.invocations()).hasSize(before);assertThat(routes.find(i.modelActivityId()).orElseThrow().status()).isEqualTo("BUDGET_BLOCKED");assertThat(done.reservedCostMicros()).isLessThanOrEqualTo(budget);
    }
    @Test void requestMutationCannotChangeFrozenPromptOrRoute() throws Exception {
        UUID run=standalone();var claim=transactions.claim("request-mutation").orElseThrow();
        assertThatThrownBy(() -> model.execute(claim.context(),claim.input())).isInstanceOf(io.durableflow.worker.retry.RetryableActivityException.class);
        var altered=new HashMap<String,Object>(claim.input());var request=new HashMap<String,Object>((Map<String,Object>)claim.input().get("modelRequest"));request.put("userContent","different semantic task");altered.put("modelRequest",request);
        assertThatThrownBy(() -> model.execute(claim.context(),altered)).hasMessage("MODEL_LOGICAL_REQUEST_CHANGED");
        assertThat(routes.find(claim.activityExecutionId()).orElseThrow().totalAttempts()).isEqualTo(1);
        assertThatThrownBy(() -> jdbc.update("UPDATE model_route SET plan_json='{}'::jsonb WHERE activity_execution_id=?",claim.activityExecutionId())).hasMessageContaining("MODEL_ROUTE_IMMUTABLE_PLAN");
    }
    @Test void staleWorkerCannotCreateRouteOrReserveProviderAttemptAfterReclaim() throws Exception {
        UUID run=standalone();var old=transactions.claim("stale-route-worker").orElseThrow();expire(old.taskId());
        transactions.claim("replacement-route-worker").orElseThrow();
        assertThatThrownBy(() -> model.execute(old.context(),old.input())).hasMessage("MODEL_ROUTE_OWNERSHIP_LOST");
        assertThat(routes.find(old.activityExecutionId())).isEmpty();
        assertThat(a.invocations()).noneMatch(i -> i.key().contains(old.activityExecutionId().toString()));
    }
    private UUID standalone(){String name="Route-"+UUID.randomUUID();definitions.register(new RegisterWorkflowDefinitionRequest(name,1,List.of(new StepDefinition("model","modelCall",List.of()))));
        var request=Map.<String,Object>of("requestVersion",1,"provider","routed","model","recommendation-v1","systemInstructions","Provide a bounded recommendation.","userContent","Review order O-42.","temperature",0.0,"maxOutputTokens",128,"structuredOutput",true);
        UUID id=runs.start(new StartWorkflowRunRequest(name,1,Map.of("modelRequest",request))).runId();orchestrator.advanceRun(id);return id;}
    private ActivityExecutionEntity activity(UUID run){return activities.findByWorkflowRunIdOrderByActivityIdAsc(run).getFirst();}
    private void due(UUID activity){var task=tasks.findByActivityExecutionId(activity).orElseThrow();jdbc.update("UPDATE activity_task SET available_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?",task.getId());}
    private void expire(UUID id){jdbc.update("UPDATE activity_task SET lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?",id);}
    private void finish(TaskWorker worker,UUID run,int count){for(int n=0;n<count;n++){if(n>0)due(activity(run).getId());assertThat(worker.runOne()).isTrue();}}
    private ConfigurableApplicationContext fresh(String... extra){var args=new ArrayList<>(List.of("--spring.datasource.url="+POSTGRES.getJdbcUrl(),"--spring.datasource.username="+POSTGRES.getUsername(),"--spring.datasource.password="+POSTGRES.getPassword(),
        "--durableflow.timers.polling-enabled=false","--durableflow.children.polling-enabled=false","--durableflow.schedules.polling-enabled=false","--durableflow.routing.fake-a-script=UNAVAILABLE,UNAVAILABLE","--durableflow.routing.max-provider-attempts=4","--durableflow.worker.retry.max-attempts=5"));
        for(String value:extra){String prefix=value.substring(0,value.indexOf('='));args.removeIf(x -> x.startsWith(prefix+"="));args.add(value);}return new SpringApplicationBuilder(DurableFlowApplication.class).web(WebApplicationType.NONE).run(args.toArray(String[]::new));}
}
