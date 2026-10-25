package io.durableflow.tools;
import io.durableflow.workflow.activity.*;
import org.springframework.stereotype.Component;
import java.util.Map;
@Component
public class CreateToolRequestActivity implements ActivityHandler {
    private final ToolRequestService service;
    public CreateToolRequestActivity(ToolRequestService service) { this.service=service; }
    public boolean requiresDurableWorker() { return true; }
    public String type() { return "createToolRequest"; }
    public Map<String,Object> execute(ActivityExecutionContext context,Map<String,Object> input) {
        var request=service.propose(context,input);
        if(request.status()==ToolRequest.Status.REJECTED) throw new IllegalArgumentException("TOOL_REQUEST_REJECTED");
        return request.receipt();
    }
}
