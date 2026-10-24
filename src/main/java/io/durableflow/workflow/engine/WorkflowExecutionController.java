package io.durableflow.workflow.engine;

import io.durableflow.workflow.run.WorkflowRunResponse;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;

/** Temporary synchronous learning interface; later replaced by asynchronous dispatch. */
@RestController
@RequestMapping("/api/workflow-runs")
public class WorkflowExecutionController {
    private final WorkflowEngine engine;

    public WorkflowExecutionController(WorkflowEngine engine) { this.engine = engine; }

    @PostMapping("/{runId}/execute")
    public WorkflowRunResponse execute(@PathVariable UUID runId) { return engine.executeRun(runId); }
}
