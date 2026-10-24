package io.durableflow.workflow.history;

import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestController @Profile("!worker")
@RequestMapping("/api/workflow-runs")
public class WorkflowHistoryController {
    private final WorkflowHistoryService history;
    public WorkflowHistoryController(WorkflowHistoryService history) { this.history = history; }
    @GetMapping("/{runId}/events")
    public List<WorkflowEventResponse> events(@PathVariable UUID runId) { return history.read(runId); }
}
