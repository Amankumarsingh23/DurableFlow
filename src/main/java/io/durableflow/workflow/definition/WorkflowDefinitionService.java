package io.durableflow.workflow.definition;

import io.durableflow.workflow.child.ChildDefinitionGuard;
import io.durableflow.api.ApiException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;

@Service
public class WorkflowDefinitionService {
    private final WorkflowDefinitionRepository definitions;
    private final WorkflowDefinitionValidator validator;
    private final ChildDefinitionGuard children;

    public WorkflowDefinitionService(WorkflowDefinitionRepository definitions, WorkflowDefinitionValidator validator, ChildDefinitionGuard children) {
        this.definitions = definitions;
        this.validator = validator; this.children = children;
    }

    @Transactional
    public WorkflowDefinitionResponse register(RegisterWorkflowDefinitionRequest request) {
        var normalized = validator.normalizeNewDefinition(request);
        if (definitions.findByNameAndVersion(normalized.name(), normalized.version()).isPresent()) {
            throw new ApiException(HttpStatus.CONFLICT, "Workflow name/version already registered");
        }
        children.validate(normalized);
        var entity = new WorkflowDefinitionEntity();
        entity.setName(normalized.name());
        entity.setVersion(normalized.version());
        entity.setDefinitionJson(WorkflowDefinitionDocument.encode(normalized));
        entity.setCreatedAt(Instant.now());
        // Flush here; the unique DB constraint also handles concurrent registrations.
        return response(definitions.saveAndFlush(entity));
    }

    @Transactional(readOnly = true)
    public WorkflowDefinitionResponse get(String name, int version) {
        return response(definitions.findByNameAndVersion(name, version)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Workflow definition not found")));
    }

    private WorkflowDefinitionResponse response(WorkflowDefinitionEntity entity) {
        return new WorkflowDefinitionResponse(entity.getId(), entity.getName(), entity.getVersion(),
                WorkflowDefinitionDocument.steps(entity.getDefinitionJson()), entity.getCreatedAt());
    }
}
