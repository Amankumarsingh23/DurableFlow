package io.durableflow.workflow.engine;

import io.durableflow.workflow.run.WorkflowRunResponse;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;

/** Temporary manual checkpoint facility; unavailable unless the dev profile is explicitly enabled. */
@Profile("dev")
@RestController
@RequestMapping("/api/dev/workflow-runs")
public class WorkflowDevelopmentController {
    private final WorkflowEngine engine;

    public WorkflowDevelopmentController(WorkflowEngine engine) { this.engine = engine; }

    @PostMapping("/{runId}/execute")
    public WorkflowRunResponse execute(@PathVariable UUID runId, @RequestParam int maxActivities) {
        return engine.executeRun(runId, maxActivities);
    }
}
