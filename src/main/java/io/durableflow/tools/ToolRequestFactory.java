package io.durableflow.tools;
import io.durableflow.workflow.history.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Shared schema/permission/policy/immutable persistence boundary. Caller holds the target run lock. */
@Service
public class ToolRequestFactory {
    private final ToolRequestRepository requests;
    private final ToolRegistry registry;
    private final ToolPermissionService permissions;
    private final ToolPolicyService policy;
    private final WorkflowHistoryWriter history;
    private final ToolPermissionService.Actor actor;
    public ToolRequestFactory(ToolRequestRepository requests,ToolRegistry registry,ToolPermissionService permissions,
            ToolPolicyService policy,WorkflowHistoryWriter history,@Value("${durableflow.tools.actor-id:demo-agent}") String actor,
            @Value("${durableflow.tools.tenant-id:tenant-a}") String tenant) {
        this.requests=requests;this.registry=registry;this.permissions=permissions;this.policy=policy;this.history=history;
        this.actor=new ToolPermissionService.Actor(actor,tenant);
    }
    @Transactional(propagation=Propagation.MANDATORY)
    public ToolRequest create(UUID runId,UUID sourceId,UUID proposalId,String executeNode,String approvalNode,String name,Map<String,Object> args) {
        var existing=requests.forRun(runId);
        if(existing.isPresent()) {
            var old=existing.get(); old.requireIdentity(name,args);
            if(!old.proposalActivityId().equals(proposalId) || !old.sourceActivityId().equals(sourceId)
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
        UUID id=UUID.nameUUIDFromBytes(("df:v1:tool-request:"+proposalId).getBytes(StandardCharsets.UTF_8));
        var metadata=Map.<String,Object>of("permission",permission.name(),"policy",decision.name(),"policyVersion",policy.version(),
                "permissionVersion","demo-permissions-v1","actorId",actor.actorId(),"tenantId",actor.tenantId(),"approvalAuthority","demo-local-control-plane");
        var request=new ToolRequest(id,runId,sourceId,proposalId,executeNode,approvalNode,name,args,
                ToolJson.fingerprint(name,args),descriptor.snapshot(),metadata,status,Map.of());
        requests.insert(request);
        history.append(runId,proposalId,WorkflowEventType.TOOL_REQUEST_CREATED,ToolRequestService.audit(request));
        if(status==ToolRequest.Status.REJECTED) history.append(runId,proposalId,WorkflowEventType.TOOL_REJECTED,ToolRequestService.audit(request));
        return request;
    }
}
