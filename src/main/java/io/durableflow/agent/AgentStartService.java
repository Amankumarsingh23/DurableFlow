package io.durableflow.agent;
import io.durableflow.ai.retrieval.RetrievalRequest;
import io.durableflow.tools.ToolJson;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.task.WorkflowTaskOrchestrator;
import io.durableflow.workflow.history.*;
import io.durableflow.worker.retry.RetryPolicy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.*;

@Service
public class AgentStartService {
    private final AgentRepository agents;
    private final AgentWorkflows templates;
    private final WorkflowRunService runService;
    private final WorkflowRunRepository runs;
    private final ActivityExecutionRepository activities;
    private final WorkflowDefinitionRepository definitions;
    private final WorkflowTaskOrchestrator orchestrator;
    private final WorkflowHistoryWriter history;
    private final RetryPolicy retry;
    private final String tenant;
    public AgentStartService(AgentRepository agents,AgentWorkflows templates,WorkflowRunService runService,WorkflowRunRepository runs,ActivityExecutionRepository activities,
            WorkflowDefinitionRepository definitions,WorkflowTaskOrchestrator orchestrator,WorkflowHistoryWriter history,RetryPolicy retry,
            @Value("${durableflow.tools.tenant-id:tenant-a}") String tenant) {
        this.agents=agents;this.templates=templates;this.runService=runService;this.runs=runs;this.activities=activities;this.definitions=definitions;
        this.orchestrator=orchestrator;this.history=history;this.retry=retry;this.tenant=tenant;
    }
    @Transactional
    public AgentExecution start(StartAgentRequest request) {
        if(request==null || request.goal()==null || request.goal().isBlank() || request.goal().length()>2000)throw new IllegalArgumentException("AGENT_INVALID_GOAL");
        String scenario=request.scenario()==null?"refund":request.scenario();
        if(!Set.of("refund","runaway","final","malformed","repeat-write").contains(scenario) || retry.maxAttempts()>10)throw new IllegalArgumentException("AGENT_INVALID_CONFIGURATION");
        var limits=request.limits()==null?AgentLimits.defaults():request.limits();var context=new LinkedHashMap<String,Object>();
        if(request.retrievalActivityId()!=null) {
            var source=activities.findById(request.retrievalActivityId()).orElseThrow(() -> new IllegalArgumentException("AGENT_RETRIEVAL_NOT_FOUND"));
            var definition=definitions.findById(runs.findById(source.getWorkflowRunId()).orElseThrow().getWorkflowDefinitionId()).orElseThrow();
            boolean retrieval=WorkflowDefinitionDocument.steps(definition.getDefinitionJson()).stream().anyMatch(s -> s.id().equals(source.getActivityId()) && "retrieveContext".equals(s.activity()));
            if(!retrieval || source.getStatus()!=ActivityExecutionStatus.COMPLETED || !tenant.equals(RetrievalRequest.fromInput(source.getInputJson()).filters().tenantId()))
                throw new IllegalArgumentException("AGENT_RETRIEVAL_SCOPE");
            context.put("sourceActivityId",source.getId().toString());context.put("context",source.getOutputJson().get("context"));
        }
        if(ToolJson.encode(context).length()>limits.maxContextChars())throw new IllegalArgumentException("AGENT_CONTEXT_LIMIT");
        templates.ensure();
        var run=runService.start(new StartWorkflowRunRequest(AgentWorkflows.HOST,1,Map.of()));runs.findByIdForUpdate(run.runId()).orElseThrow();
        UUID id=UUID.randomUUID();agents.insert(id,run.runId(),request.goal(),scenario,limits,retry.maxAttempts(),context);
        orchestrator.advanceInTransaction(run.runId());history.append(run.runId(),null,WorkflowEventType.AGENT_STARTED,Map.of("agentExecutionId",id.toString()));
        return agents.get(id);
    }
}
