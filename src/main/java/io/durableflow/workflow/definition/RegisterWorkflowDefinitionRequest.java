package io.durableflow.workflow.definition;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.List;

public record RegisterWorkflowDefinitionRequest(
        @NotBlank @Size(max = 200) String name,
        @Positive int version,
        @NotEmpty List<@NotNull @Valid StepDefinition> steps) {}
