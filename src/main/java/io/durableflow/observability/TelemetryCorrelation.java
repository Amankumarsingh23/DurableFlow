package io.durableflow.observability;
import java.util.*;

/** High cardinality: logs/trace links only, never meter tags. No business content. */
public record TelemetryCorrelation(UUID workflowRunId, UUID activityExecutionId, UUID agentExecutionId,
        Integer iteration, UUID logicalModelCallId, UUID toolRequestId, String providerRequestId) {
    public Map<String,Object> metadata() {
        var data=new LinkedHashMap<String,Object>();
        if(workflowRunId!=null)data.put("workflowRunId",workflowRunId.toString());
        if(activityExecutionId!=null)data.put("activityExecutionId",activityExecutionId.toString());
        if(agentExecutionId!=null)data.put("agentExecutionId",agentExecutionId.toString());
        if(iteration!=null)data.put("iteration",iteration);
        if(logicalModelCallId!=null)data.put("logicalModelCallId",logicalModelCallId.toString());
        if(toolRequestId!=null)data.put("toolRequestId",toolRequestId.toString());
        if(providerRequestId!=null)data.put("providerRequestId",providerRequestId);
        return SafeMetadata.sanitize(data);
    }
}
