package io.durableflow.tools;

import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.history.*;
import io.durableflow.workflow.run.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import java.util.*;

@Service
public class ToolRequestService {
    private final WorkflowRunRepository runs;
    private final WorkflowDefinitionRepository definitions;
    private final ActivityExecutionRepository activities;
    private final ToolRequestFactory factory;
    public ToolRequestService(WorkflowRunRepository runs,WorkflowDefinitionRepository definitions,ActivityExecutionRepository activities,ToolRequestFactory factory) {
        this.runs=runs;this.definitions=definitions;this.activities=activities;this.factory=factory;
    }
    /** No provider calls: freeze/decide one bounded proposal under the run lock. */
    @Transactional(propagation=Propagation.REQUIRES_NEW)
    public ToolRequest propose(ActivityExecutionContext context,Map<String,Object> input) {
        var run=runs.findByIdForUpdate(context.workflowRunId()).orElseThrow();
        if(context.compensation() || run.getStatus()!=WorkflowRunStatus.RUNNING) throw new IllegalArgumentException("TOOL_RUN_NOT_RUNNING");
        var config=ToolJson.object(input.get("toolBinding"));
        if(!Set.of("sourceNodeId","executeNodeId","approvalNodeId").containsAll(config.keySet())
                || !(config.get("sourceNodeId") instanceof String sourceNode) || !(config.get("executeNodeId") instanceof String executeNode))
            throw new IllegalArgumentException("TOOL_INVALID_BINDING");
        if(config.get("approvalNodeId")!=null && !(config.get("approvalNodeId") instanceof String)) throw new IllegalArgumentException("TOOL_INVALID_BINDING");
        String approvalNode=(String)config.get("approvalNodeId");
        var steps=WorkflowDefinitionDocument.steps(definitions.findById(run.getWorkflowDefinitionId()).orElseThrow().getDefinitionJson());
        var all=activities.findByWorkflowRunIdOrderByActivityIdAsc(run.getId());
        var proposal=all.stream().filter(a -> a.getId().equals(context.activityExecutionId())).findFirst().orElseThrow();
        var create=step(steps,proposal.getActivityId()); var source=step(steps,sourceNode); var execute=step(steps,executeNode);
        if(proposal.getStatus()!=ActivityExecutionStatus.RUNNING || !"createToolRequest".equals(create.activity())
                || !create.dependsOn().contains(sourceNode) || !"modelCall".equals(source.activity()) || !"executeTool".equals(execute.activity()))
            throw new IllegalArgumentException("TOOL_INVALID_GRAPH");
        if(approvalNode==null) {
            if(!execute.dependsOn().contains(create.id())) throw new IllegalArgumentException("TOOL_EXECUTION_DEPENDENCY");
        } else {
            var approval=step(steps,approvalNode);
            if(approval.type()!=NodeType.SIGNAL || !approval.dependsOn().contains(create.id()) || !execute.dependsOn().contains(approvalNode))
                throw new IllegalArgumentException("TOOL_APPROVAL_DEPENDENCY");
        }
        var sourceExecution=all.stream().filter(a -> a.getActivityId().equals(sourceNode)).findFirst().orElseThrow();
        if(sourceExecution.getStatus()!=ActivityExecutionStatus.COMPLETED) throw new IllegalArgumentException("TOOL_MODEL_CHECKPOINT_REQUIRED");
        var structured=ToolJson.object(sourceExecution.getOutputJson().get("structuredJson"));
        var proposed=ToolJson.object(structured.get("toolProposal"));
        if(!proposed.keySet().equals(Set.of("toolName","arguments")) || !(proposed.get("toolName") instanceof String name))
            throw new IllegalArgumentException("TOOL_UNTRUSTED_METADATA");
        var args=ToolJson.object(proposed.get("arguments"));
        return factory.create(run.getId(),sourceExecution.getId(),proposal.getId(),executeNode,approvalNode,name,args);
    }
    static StepDefinition step(List<StepDefinition> steps,String node) {
        return steps.stream().filter(s -> s.id().equals(node)).findFirst().orElseThrow(() -> new IllegalArgumentException("TOOL_NODE_NOT_FOUND"));
    }
    public static Map<String,Object> audit(ToolRequest r) { return Map.of("toolRequestId",r.id().toString(),"fingerprint",r.fingerprint(),"status",r.status().name()); }
}
