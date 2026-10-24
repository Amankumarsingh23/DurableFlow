package io.durableflow.workflow.definition;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record WorkflowDefinitionResponse(
        UUID id, String name, int version, List<StepDefinition> steps, Instant createdAt) {}
