package io.durableflow.tools;
import io.durableflow.workflow.activity.*;
import org.springframework.stereotype.Component;
import java.util.*;
@Component
public class ExecuteToolActivity implements ActivityHandler {
    private final ToolRequestRepository requests;
    private final ActivityExecutionRepository activities;
    private final ToolRegistry registry;
    public ExecuteToolActivity(ToolRequestRepository requests,ActivityExecutionRepository activities,ToolRegistry registry) {
        this.requests=requests; this.activities=activities; this.registry=registry;
    }
    public boolean requiresDurableWorker() { return true; }
    public String type() { return "executeTool"; }
    public Map<String,Object> execute(ActivityExecutionContext context,Map<String,Object> input) throws Exception {
        if(context.compensation()) throw new IllegalArgumentException("TOOL_COMPENSATION_UNSUPPORTED");
        var request=requests.forRun(context.workflowRunId()).orElseThrow(() -> new IllegalArgumentException("TOOL_REQUEST_REQUIRED"));
        var activity=activities.findById(context.activityExecutionId()).orElseThrow();
        if(!activity.getWorkflowRunId().equals(request.workflowRunId()) || !activity.getActivityId().equals(request.executeNodeId()))
            throw new IllegalArgumentException("TOOL_WRONG_EXECUTION_NODE");
        if(request.status()==ToolRequest.Status.EXECUTED) return output(request,request.result());
        if(request.status()!=ToolRequest.Status.APPROVED) throw new IllegalArgumentException("TOOL_NOT_APPROVED");
        // Deliberately ignore workflow/model arguments here. Only the frozen request goes to the provider.
        return output(request,registry.execute(request).toMap());
    }
    static Map<String,Object> output(ToolRequest r,Map<String,Object> result) {
        return Map.of("toolRequestId",r.id().toString(),"fingerprint",r.fingerprint(),"toolResult",result);
    }
}
