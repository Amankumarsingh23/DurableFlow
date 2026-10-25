package io.durableflow.tools;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.history.*;
import org.springframework.stereotype.Component;
import java.util.*;
@Component
public class ToolCompletionParticipant implements ActivityCompletionParticipant {
    private final ToolRequestRepository requests;
    private final WorkflowHistoryWriter history;
    public ToolCompletionParticipant(ToolRequestRepository requests,WorkflowHistoryWriter history) { this.requests=requests; this.history=history; }
    public void completed(ActivityExecutionEntity activity,Map<String,Object> output) {
        var found=requests.forRun(activity.getWorkflowRunId());
        if(found.isEmpty() || !found.get().executeNodeId().equals(activity.getActivityId())) return;
        var request=found.get();
        if(!request.id().toString().equals(output.get("toolRequestId")) || !request.fingerprint().equals(output.get("fingerprint")))
            throw new IllegalArgumentException("TOOL_COMPLETION_IDENTITY");
        var result=ToolJson.freeze(ToolJson.object(output.get("toolResult")));
        if(!request.toolName().equals(result.get("toolName")) || ToolJson.encode(result).length()>16000)
            throw new IllegalArgumentException("TOOL_COMPLETION_RESULT");
        if(request.status()==ToolRequest.Status.EXECUTED) {
            if(!request.result().equals(result)) throw new IllegalArgumentException("TOOL_RESULT_CHANGED"); return;
        }
        requests.complete(request,result);
        history.append(request.workflowRunId(),activity.getId(),WorkflowEventType.TOOL_EXECUTED,
                Map.of("toolRequestId",request.id().toString(),"fingerprint",request.fingerprint(),"status","EXECUTED"));
    }
}
