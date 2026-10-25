package io.durableflow.agent;

import io.durableflow.ai.model.ModelRequest;
import io.durableflow.ai.routing.*;
import io.durableflow.tools.*;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.task.WorkflowTaskOrchestrator;
import io.durableflow.workflow.signal.*;
import io.durableflow.workflow.history.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;

/** One durable orchestration transition per invocation. Never invokes a model or tool provider. */
@Service
public class AgentLoopService {
    private final AgentRepository agents;
    private final WorkflowRunRepository runs;
    private final WorkflowRunService runService;
    private final ActivityExecutionRepository activities;
    private final WorkflowTaskOrchestrator orchestrator;
    private final ToolRegistry registry;
    private final ToolRequestFactory factory;
    private final ToolRequestRepository requests;
    private final AgentContextBuilder contexts;
    private final WorkflowHistoryWriter history;
    private final SignalRepository signals;
    private final ModelRoutingService routing;
    private final ModelRouteRepository routes;
    public AgentLoopService(AgentRepository agents,WorkflowRunRepository runs,WorkflowRunService runService,ActivityExecutionRepository activities,
            WorkflowTaskOrchestrator orchestrator,ToolRegistry registry,ToolRequestFactory factory,ToolRequestRepository requests,
            AgentContextBuilder contexts,WorkflowHistoryWriter history,SignalRepository signals,ModelRoutingService routing,ModelRouteRepository routes) {
        this.agents=agents;this.runs=runs;this.runService=runService;this.activities=activities;this.orchestrator=orchestrator;
        this.registry=registry;this.factory=factory;this.requests=requests;this.contexts=contexts;this.history=history;this.signals=signals;this.routing=routing;this.routes=routes;
    }
    @Transactional(propagation=Propagation.REQUIRES_NEW,isolation=Isolation.READ_COMMITTED)
    public AgentExecution advance(UUID id) {
        var agent=agents.lock(id);
        if(agent.terminal())return agent;
        // Workers lock their own step run and never the agent; driver only reads existing step checkpoints.
        runs.findByIdForUpdate(agent.workflowRunId()).orElseThrow();
        var iterations=agents.iterations(id);
        var iteration=iterations.isEmpty()?null:iterations.getLast();
        if(iteration==null || iteration.status()==AgentIteration.Status.COMPLETED) newModel(agent,iterations);
        else switch(iteration.status()) {
            case MODEL_PENDING -> checkpointModel(agent,iteration);
            case DECIDED -> decide(agent,iteration,iterations);
            case TOOL_PENDING -> checkpointTool(agent,iteration);
            default -> finish(agent,AgentExecution.Status.FAILED,"INVALID_ITERATION_STATE",Map.of());
        }
        return agents.get(id);
    }
    private void newModel(AgentExecution a,List<AgentIteration> prior) {
        if(a.currentIteration()>=a.limits().maxIterations()) {limit(a,"MAX_ITERATIONS");return;}
        if(a.logicalModelCalls()>=a.limits().maxLogicalModelCalls()) {limit(a,"MAX_MODEL_CALLS");return;}
        ModelRequest request;
        try {request=contexts.build(a,prior);}
        catch(IllegalArgumentException tooLarge) {limit(a,"CONTEXT_LIMIT");return;}
        long tokens=contexts.reservedTokens(request,a.maxModelAttempts()),cost=contexts.reservedCost(request,a.maxModelAttempts());
        if(tokens>a.limits().maxTotalTokens()-a.reservedTokens()) {limit(a,"MAX_TOTAL_TOKENS");return;}
        if(cost>a.limits().maxEstimatedCostMicros()-a.reservedCostMicros()) {limit(a,"MAX_ESTIMATED_COST");return;}
        RouteDecision plan;
        try {plan=routing.plan(request,true,a.limits().maxTotalTokens()-a.reservedTokens(),a.limits().maxEstimatedCostMicros()-a.reservedCostMicros(),a.maxModelAttempts());}
        catch(IllegalArgumentException noRoute) {finish(a,AgentExecution.Status.FAILED,"MODEL_NO_ELIGIBLE_ROUTE",Map.of());return;}
        cost=Math.multiplyExact(plan.primary().estimatedCost(request.systemInstructions().length()+request.userContent().length(),request.maxOutputTokens()),a.maxModelAttempts());
        if(cost>a.limits().maxEstimatedCostMicros()-a.reservedCostMicros()) {limit(a,"MAX_ESTIMATED_COST");return;}
        var run=runService.start(new StartWorkflowRunRequest(AgentWorkflows.MODEL,1,contexts.input(request)));
        var activity=activities.findByWorkflowRunIdOrderByActivityIdAsc(run.runId()).getFirst();
        agents.startIteration(a,UUID.randomUUID(),run.runId(),activity.getId(),tokens,cost);
        routing.prepare(activity.getId(),a.id(),request,plan);
        orchestrator.advanceInTransaction(run.runId());
    }
    private void checkpointModel(AgentExecution a,AgentIteration i) {
        var activity=activities.findById(i.modelActivityId()).orElseThrow();
        if(activity.getStatus()==ActivityExecutionStatus.FAILED) {
            var summary=routes.summary(i.modelActivityId());
            int attempts=summary.filter(x -> x.attempts()>0 || x.terminalCode()!=null).map(ModelRouteRepository.Summary::attempts).orElse(activity.getAttemptCount());
            agents.decision(i,null,attempts,summary.map(ModelRouteRepository.Summary::inputTokens).orElse(0L),summary.map(ModelRouteRepository.Summary::outputTokens).orElse(0L),summary.map(ModelRouteRepository.Summary::knownCostMicros).orElse(0L));
            String code=summary.map(ModelRouteRepository.Summary::terminalCode).orElse("MODEL_ACTIVITY_FAILED");
            if(Set.of("MAX_ESTIMATED_COST","MAX_TOTAL_TOKENS").contains(code))limit(a,code);
            else finish(a,AgentExecution.Status.FAILED,code,Map.of());return;
        }
        if(activity.getStatus()!=ActivityExecutionStatus.COMPLETED)return;
        var output=activity.getOutputJson();long input,generated,cost;
        try {
            var usage=ToolJson.object(output.get("usage"));input=number(usage.get("inputTokens"));generated=number(usage.get("outputTokens"));
            cost=Math.addExact(input,Math.multiplyExact(2L,generated));
        } catch(IllegalArgumentException | ArithmeticException malformed) {finish(a,AgentExecution.Status.FAILED,"MODEL_USAGE_INVALID",Map.of());return;}
        AgentDecision decision=null;
        try {if(Boolean.TRUE.equals(output.get("decisionSchemaValid")))decision=AgentDecision.parse(output.get("structuredJson"));}
        catch(IllegalArgumentException invalid) { /* Persist usage, but never invalid/private model content. */ }
        var observed=routes.summary(i.modelActivityId()).filter(x -> x.attempts()>0);
        if(observed.isPresent()) {input=observed.get().inputTokens();generated=observed.get().outputTokens();cost=observed.get().knownCostMicros();}
        agents.decision(i,decision,observed.map(ModelRouteRepository.Summary::attempts).orElse(activity.getAttemptCount()),input,generated,cost);
        long extra=observed.map(ModelRouteRepository.Summary::extraReservedCost).orElse(0L);
        if(input+generated>i.reservedTokens() || cost>i.reservedCostMicros()+extra) {limit(a,"USAGE_RESERVATION_EXCEEDED");return;}
        if(decision==null)finish(a,AgentExecution.Status.FAILED,"INVALID_MODEL_DECISION",Map.of());
    }
    private void decide(AgentExecution a,AgentIteration i,List<AgentIteration> all) {
        var decision=AgentDecision.parse(i.decision());
        if(decision.type()==AgentDecision.Type.FINAL) {
            history.append(a.workflowRunId(),null,WorkflowEventType.AGENT_ITERATION_COMPLETED,Map.of("agentExecutionId",a.id().toString(),"iteration",i.number(),"decisionType","FINAL"));
            finish(a,AgentExecution.Status.COMPLETED,"FINAL_ANSWER",Map.of("answer",decision.payload().get("answer")));return;
        }
        if(a.logicalToolCalls()>=a.limits().maxLogicalToolCalls()) {limit(a,"MAX_TOOL_CALLS");return;}
        String name=(String)decision.payload().get("toolName");ToolDescriptor descriptor;Map<String,Object> args;
        try {descriptor=registry.require(name);args=descriptor.validate(ToolJson.object(decision.payload().get("arguments")));}
        catch(IllegalArgumentException invalid) {finish(a,AgentExecution.Status.FAILED,"INVALID_TOOL_PROPOSAL",Map.of());return;}
        var fingerprint=ToolJson.fingerprint(name,args);
        if(descriptor.sideEffecting() && all.stream().filter(p -> p.status()==AgentIteration.Status.COMPLETED && !p.toolResult().isEmpty())
                .anyMatch(p -> fingerprint.equals(ToolJson.fingerprint((String)p.decision().get("toolName"),ToolJson.object(p.decision().get("arguments")))))) {
            finish(a,AgentExecution.Status.FAILED,"DUPLICATE_WRITE",Map.of());return;
        }
        boolean review=descriptor.riskLevel()!=ToolRiskLevel.READ_ONLY;
        var run=runService.start(new StartWorkflowRunRequest(review?AgentWorkflows.WRITE:AgentWorkflows.READ,1,Map.of()));
        var toolRun=runs.findByIdForUpdate(run.runId()).orElseThrow();
        var activity=activities.findByWorkflowRunIdOrderByActivityIdAsc(run.runId()).getFirst();
        var request=factory.create(run.runId(),i.modelActivityId(),activity.getId(),"execute",review?"approval":null,name,args);
        agents.tool(i,run.runId(),request.id(),request.status()==ToolRequest.Status.AWAITING_APPROVAL);
        if(request.status()==ToolRequest.Status.REJECTED) {
            toolRun.markFailed(Instant.now());history.append(run.runId(),null,WorkflowEventType.WORKFLOW_FAILED,Map.of("code","TOOL_REJECTED"));
            finish(a,AgentExecution.Status.FAILED,"TOOL_REJECTED",Map.of());return;
        }
        orchestrator.advanceInTransaction(run.runId());
    }
    private void checkpointTool(AgentExecution a,AgentIteration i) {
        var request=requests.require(i.toolRequestId());
        if(request.status()==ToolRequest.Status.REJECTED) {finish(a,AgentExecution.Status.FAILED,"TOOL_REJECTED",Map.of());return;}
        if(request.status()==ToolRequest.Status.EXECUTED) {
            agents.iterationCompleted(i,request.result());
            history.append(a.workflowRunId(),null,WorkflowEventType.AGENT_ITERATION_COMPLETED,Map.of("agentExecutionId",a.id().toString(),"iteration",i.number(),"toolRequestId",request.id().toString()));return;
        }
        if(activities.findByWorkflowRunIdOrderByActivityIdAsc(i.toolRunId()).stream().anyMatch(e -> e.getStatus()==ActivityExecutionStatus.FAILED)) {
            finish(a,AgentExecution.Status.FAILED,"TOOL_ACTIVITY_FAILED",Map.of());return;
        }
        if(a.status()==AgentExecution.Status.WAITING_APPROVAL && request.status()==ToolRequest.Status.APPROVED)agents.running(a.id());
    }
    private long number(Object value) {
        if(!(value instanceof Number n))throw new IllegalArgumentException("Invalid usage");
        long result=new BigDecimal(n.toString()).longValueExact();if(result<0)throw new IllegalArgumentException("Invalid usage");return result;
    }
    private void limit(AgentExecution a,String reason) {finish(a,AgentExecution.Status.LIMIT_REACHED,reason,Map.of());}
    private void finish(AgentExecution a,AgentExecution.Status status,String reason,Map<String,Object> output) {
        agents.terminal(a,status,reason,output);
        history.append(a.workflowRunId(),null,status==AgentExecution.Status.LIMIT_REACHED?WorkflowEventType.AGENT_LIMIT_REACHED:WorkflowEventType.AGENT_COMPLETED,
                Map.of("agentExecutionId",a.id().toString(),"status",status.name(),"reason",reason));
        // Internal terminal receipt completes the fixed host workflow without another waiting mechanism.
        // The agent's separate status remains the authoritative business/limit outcome.
        signals.insert(a.workflowRunId(),new ReceiveSignalRequest("agent-finished:"+a.id(),"agentFinished",Map.of("status",status.name(),"reason",reason)));
        orchestrator.advanceInTransaction(a.workflowRunId());
    }
}
