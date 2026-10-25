package io.durableflow.tools;
import io.durableflow.workflow.activity.*;
import org.springframework.stereotype.Component;
import java.util.*;
import io.durableflow.observability.*;
@Component
public class ExecuteToolActivity implements ActivityHandler {
    private final ToolRequestRepository requests;
    private final ActivityExecutionRepository activities;
    private final ToolRegistry registry;
    private final SafeTelemetry telemetry;
    public ExecuteToolActivity(ToolRequestRepository requests,ActivityExecutionRepository activities,ToolRegistry registry) {
        this(requests,activities,registry,SafeTelemetry.disabled());
    }
    @org.springframework.beans.factory.annotation.Autowired
    public ExecuteToolActivity(ToolRequestRepository requests,ActivityExecutionRepository activities,ToolRegistry registry,SafeTelemetry telemetry) {
        this.telemetry=telemetry;
        this.requests=requests; this.activities=activities; this.registry=registry;
    }
    public boolean requiresDurableWorker() { return true; }
    public String type() { return "executeTool"; }
    public Map<String,Object> execute(ActivityExecutionContext context,Map<String,Object> input) throws Exception {
        long started=System.nanoTime();String status="FAILURE";String tool="other",risk="other";UUID requestId=null;
        try {
        if(context.compensation()) throw new IllegalArgumentException("TOOL_COMPENSATION_UNSUPPORTED");
        var request=requests.forRun(context.workflowRunId()).orElseThrow(() -> new IllegalArgumentException("TOOL_REQUEST_REQUIRED"));
        requestId=request.id();tool=request.toolName();risk=Objects.toString(request.descriptor().get("riskLevel"),"other");
        var activity=activities.findById(context.activityExecutionId()).orElseThrow();
        if(!activity.getWorkflowRunId().equals(request.workflowRunId()) || !activity.getActivityId().equals(request.executeNodeId()))
            throw new IllegalArgumentException("TOOL_WRONG_EXECUTION_NODE");
        if(request.status()==ToolRequest.Status.EXECUTED) {
            telemetry.count(Metric.TOOL_DEDUP,Map.of("tool",tool));status="SUCCESS";return output(request,request.result());
        }
        if(request.status()!=ToolRequest.Status.APPROVED) throw new IllegalArgumentException("TOOL_NOT_APPROVED");
        // Deliberately ignore workflow/model arguments here. Only the frozen request goes to the provider.
        var result=output(request,registry.execute(request).toMap());status="SUCCESS";return result;
        } finally {
            var labels=Map.of("tool",tool,"risk",risk,"status",status);
            telemetry.count(Metric.TOOL_ATTEMPTS,labels);telemetry.duration(Metric.TOOL_DURATION,System.nanoTime()-started,labels);
            var metadata=new LinkedHashMap<String,Object>(new TelemetryCorrelation(context.workflowRunId(),context.activityExecutionId(),null,null,null,requestId,null).metadata());
            metadata.put("tool",tool);metadata.put("risk",risk);metadata.put("status",status);telemetry.event("TOOL_EXECUTION_ATTEMPT",metadata);
        }
    }
    static Map<String,Object> output(ToolRequest r,Map<String,Object> result) {
        return Map.of("toolRequestId",r.id().toString(),"fingerprint",r.fingerprint(),"toolResult",result);
    }
}
