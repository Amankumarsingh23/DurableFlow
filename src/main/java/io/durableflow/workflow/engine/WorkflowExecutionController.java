package io.durableflow.workflow.engine;

import io.durableflow.workflow.run.WorkflowRunResponse;
import org.springframework.web.bind.annotation.*;
import org.springframework.context.annotation.Profile;
import java.util.UUID;

/** Temporary synchronous learning interface; later replaced by asynchronous dispatch. */
@RestController
@Profile("!worker")
@RequestMapping("/api/workflow-runs")
public class WorkflowExecutionController {
    private final WorkflowEngine engine;

    public WorkflowExecutionController(WorkflowEngine engine) { this.engine = engine; }

    @PostMapping("/{runId}/execute")
    public WorkflowRunResponse execute(@PathVariable UUID runId) { return engine.executeRun(runId); }

    @PostMapping("/{runId}/advance")
    public WorkflowRunResponse advance(@PathVariable UUID runId) { return engine.advanceRun(runId); }
}
