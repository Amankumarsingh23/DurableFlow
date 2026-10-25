package io.durableflow.tools;

import java.util.*;

/** Immutable semantic request; status transitions are database operations, never payload mutation. */
public record ToolRequest(UUID id, UUID workflowRunId, UUID sourceActivityId, UUID proposalActivityId,
        String executeNodeId, String approvalNodeId, String toolName, Map<String,Object> arguments,
        String fingerprint, Map<String,Object> descriptor, Map<String,Object> decision, Status status, Map<String,Object> result) {
    public enum Status { AWAITING_APPROVAL, APPROVED, EXECUTED, REJECTED }
    public ToolRequest { arguments=ToolJson.freeze(arguments); descriptor=ToolJson.freeze(descriptor);
        decision=ToolJson.freeze(decision); result=ToolJson.freeze(result);
        if(!fingerprint.equals(ToolJson.fingerprint(toolName,arguments))) throw new IllegalArgumentException("TOOL_IDENTITY_MISMATCH"); }
    public String idempotencyKey() { return "df:v1:tool:"+id; }
    public void requireIdentity(String name,Map<String,Object> args) {
        if(!fingerprint.equals(ToolJson.fingerprint(name,args))) throw new IllegalArgumentException("TOOL_IDEMPOTENCY_MISUSE");
    }
    public Map<String,Object> receipt() {
        return Map.of("toolRequestId",id.toString(),"fingerprint",fingerprint,"toolName",toolName,"status",status.name(),
                "riskLevel",descriptor.get("riskLevel"),"decision",decision);
    }
}
