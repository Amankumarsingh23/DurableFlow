package io.durableflow.tools;
import io.durableflow.api.ApiException;
import org.springframework.http.HttpStatus;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;
import java.util.*;
@RestController @Profile("!worker")
@RequestMapping("/api/workflow-runs")
public class ToolRequestController {
    private final ToolRequestRepository requests;
    public ToolRequestController(ToolRequestRepository requests) { this.requests=requests; }
    @GetMapping("/{runId}/tool-request")
    public Map<String,Object> get(@PathVariable UUID runId) {
        var r=requests.forRun(runId).orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND,"Tool request not found"));
        var view=new LinkedHashMap<String,Object>(r.receipt());
        view.put("arguments",r.arguments()); view.put("descriptor",r.descriptor()); view.put("result",r.result());
        view.put("sourceActivityId",r.sourceActivityId().toString()); view.put("approvalNodeId",r.approvalNodeId());
        view.put("executeNodeId",r.executeNodeId()); view.put("idempotencyKey",r.idempotencyKey()); return view;
    }
}
