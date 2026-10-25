package io.durableflow.tools;

import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.history.*;
import io.durableflow.workflow.run.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Service
public class ToolRequestService {
    private final ToolRequestRepository requests;
    private final WorkflowRunRepository runs;
    private final WorkflowDefinitionRepository definitions;
    private final ActivityExecutionRepository activities;
    private final ToolRegistry registry;
    private final ToolPermissionService permissions;
    private final ToolPolicyService policy;
    private final WorkflowHistoryWriter history;
    private final ToolPermissionService.Actor actor;
    public ToolRequestService(ToolRequestRepository requests,WorkflowRunRepository runs,WorkflowDefinitionRepository definitions,
            ActivityExecutionRepository activities,ToolRegistry registry,ToolPermissionService permissions,ToolPolicyService policy,
            WorkflowHistoryWriter history,@Value("${durableflow.tools.actor-id:demo-agent}") String actor,
            @Value("${durableflow.tools.tenant-id:tenant-a}") String tenant) {
        this.requests=requests; this.runs=runs; this.definitions=definitions; this.activities=activities; this.registry=registry;
        this.permissions=permissions; this.policy=policy; this.history=history; this.actor=new ToolPermissionService.Actor(actor,tenant);
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
        var existing=requests.forRun(run.getId());
        if(existing.isPresent()) {
            var old=existing.get(); old.requireIdentity(name,args);
            if(!old.proposalActivityId().equals(proposal.getId()) || !old.sourceActivityId().equals(sourceExecution.getId())
                    || !old.executeNodeId().equals(executeNode) || !Objects.equals(old.approvalNodeId(),approvalNode))
                throw new IllegalArgumentException("TOOL_IDEMPOTENCY_MISUSE");
            return old; // Never re-evaluate a persisted permission/policy decision on recovery.
        }
        var descriptor=registry.require(name); args=descriptor.validate(args);
        var permission=permissions.check(actor,descriptor,args); var decision=policy.decide(descriptor,args);
        if(decision==ToolPolicyService.Decision.REQUIRE_HUMAN_APPROVAL && approvalNode==null)
            throw new IllegalArgumentException("TOOL_APPROVAL_NODE_REQUIRED");
        // Auto-approved tools use the simpler graph; never leave an unnecessary approval wait.
        if(decision==ToolPolicyService.Decision.AUTO_APPROVE && approvalNode!=null)
            throw new IllegalArgumentException("TOOL_AUTO_APPROVAL_GRAPH");
        var status=permission==ToolPermissionService.Decision.DENIED || decision==ToolPolicyService.Decision.DENY ? ToolRequest.Status.REJECTED
                : decision==ToolPolicyService.Decision.AUTO_APPROVE ? ToolRequest.Status.APPROVED : ToolRequest.Status.AWAITING_APPROVAL;
        UUID id=UUID.nameUUIDFromBytes(("df:v1:tool-request:"+proposal.getId()).getBytes(StandardCharsets.UTF_8));
        var metadata=Map.<String,Object>of("permission",permission.name(),"policy",decision.name(),"policyVersion",policy.version(),
                "permissionVersion","demo-permissions-v1","actorId",actor.actorId(),"tenantId",actor.tenantId(),"approvalAuthority","demo-local-control-plane");
        var request=new ToolRequest(id,run.getId(),sourceExecution.getId(),proposal.getId(),executeNode,approvalNode,name,args,
                ToolJson.fingerprint(name,args),descriptor.snapshot(),metadata,status,Map.of());
        requests.insert(request);
        history.append(run.getId(),proposal.getId(),WorkflowEventType.TOOL_REQUEST_CREATED,audit(request));
        if(status==ToolRequest.Status.REJECTED) history.append(run.getId(),proposal.getId(),WorkflowEventType.TOOL_REJECTED,audit(request));
        return request;
    }
    static StepDefinition step(List<StepDefinition> steps,String node) {
        return steps.stream().filter(s -> s.id().equals(node)).findFirst().orElseThrow(() -> new IllegalArgumentException("TOOL_NODE_NOT_FOUND"));
    }
    public static Map<String,Object> audit(ToolRequest r) { return Map.of("toolRequestId",r.id().toString(),"fingerprint",r.fingerprint(),"status",r.status().name()); }
}
