package io.durableflow.tools;
import io.durableflow.api.ApiException;
import io.durableflow.workflow.signal.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.history.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import java.util.*;

@Component
public class ToolApprovalGuard implements SignalAcceptanceGuard {
    private static final org.slf4j.Logger LOG=org.slf4j.LoggerFactory.getLogger(ToolApprovalGuard.class);
    private final ToolRequestRepository requests;
    private final WorkflowRunRepository runs;
    private final WorkflowDefinitionRepository definitions;
    private final WorkflowHistoryWriter history;
    public ToolApprovalGuard(ToolRequestRepository requests,WorkflowRunRepository runs,WorkflowDefinitionRepository definitions,WorkflowHistoryWriter history) {
        this.requests=requests; this.runs=runs; this.definitions=definitions; this.history=history;
    }
    public void beforeAccept(UUID run,ReceiveSignalRequest signal) {
        var definition=definitions.findById(runs.findById(run).orElseThrow().getWorkflowDefinitionId()).orElseThrow();
        var steps=WorkflowDefinitionDocument.steps(definition.getDefinitionJson());
        var node=ToolRequestService.step(steps,signal.signalNodeId());
        // Protect the graph BEFORE a request exists too: early buffered approvals must not bypass binding.
        var bound=requests.forRun(run);
        boolean protectedNode=bound.filter(r -> signal.signalNodeId().equals(r.approvalNodeId())).isPresent() || node.dependsOn().stream().anyMatch(id -> "createToolRequest".equals(ToolRequestService.step(steps,id).activity()));
        if(!protectedNode) return;
        var request=requests.forRun(run).orElseThrow(() -> conflict("TOOL_REQUEST_NOT_READY"));
        var payload=signal.payload();
        if(!signal.signalNodeId().equals(request.approvalNodeId()) || request.status()!=ToolRequest.Status.AWAITING_APPROVAL
                || !payload.keySet().equals(Set.of("toolRequestId","fingerprint","approved"))
                || !request.id().toString().equals(payload.get("toolRequestId")) || !request.fingerprint().equals(payload.get("fingerprint"))
                || !(payload.get("approved") instanceof Boolean)) {
            LOG.warn("TOOL_APPROVAL_BINDING_MISMATCH run={} request={}",run,request.id());
            throw conflict("TOOL_APPROVAL_BINDING_MISMATCH");
        }
        boolean approved=(Boolean)payload.get("approved");
        requests.approve(request,approved);
        history.append(run,request.proposalActivityId(),approved?WorkflowEventType.TOOL_APPROVED:WorkflowEventType.TOOL_REJECTED,
                Map.of("toolRequestId",request.id().toString(),"fingerprint",request.fingerprint(),"status",approved?"APPROVED":"REJECTED"));
    }
    private ApiException conflict(String code) { return new ApiException(HttpStatus.CONFLICT,code); }
}
