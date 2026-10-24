package io.durableflow.workflow.run;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.context.annotation.Profile;
import java.util.UUID;

@RestController
@Profile("!worker")
@RequestMapping("/api/workflow-runs")
public class WorkflowRunController {
    private final WorkflowRunService service;

    public WorkflowRunController(WorkflowRunService service) { this.service = service; }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public WorkflowRunResponse start(@Valid @RequestBody StartWorkflowRunRequest request) {
        return service.start(request);
    }

    @GetMapping("/{runId}")
    public WorkflowRunResponse get(@PathVariable UUID runId) { return service.get(runId); }
}
