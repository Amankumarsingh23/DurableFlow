package io.durableflow.workflow.definition;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/workflow-definitions")
public class WorkflowDefinitionController {
    private final WorkflowDefinitionService service;

    public WorkflowDefinitionController(WorkflowDefinitionService service) { this.service = service; }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public WorkflowDefinitionResponse register(@Valid @RequestBody RegisterWorkflowDefinitionRequest request) {
        return service.register(request);
    }

    @GetMapping("/{name}/versions/{version}")
    public WorkflowDefinitionResponse get(@PathVariable String name, @PathVariable int version) {
        return service.get(name, version);
    }
}
