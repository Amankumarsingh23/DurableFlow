package io.durableflow.workflow.definition;

import jakarta.validation.constraints.*;
import java.util.List;

public record StepDefinition(
        @NotBlank @Size(max = 200) String id,
        @NotBlank String activity,
        List<@NotBlank String> dependsOn) {
    public StepDefinition {
        // Omitted dependencies mean a root. Keep null entries for validation to reject.
        if (dependsOn == null) dependsOn = List.of();
    }
}
