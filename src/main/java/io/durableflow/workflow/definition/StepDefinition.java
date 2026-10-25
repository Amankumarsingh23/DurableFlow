package io.durableflow.workflow.definition;

import jakarta.validation.constraints.*;
import java.util.List;

public record StepDefinition(
        @NotBlank @Size(max = 200) String id,
        String activity,
        List<@NotBlank String> dependsOn, NodeType type, Long delayMillis, String compensation) {
    public StepDefinition {
        // Omitted dependencies mean a root. Keep null entries for validation to reject.
        if (dependsOn == null) dependsOn = List.of();
        if (type == null) type = NodeType.ACTIVITY;
    }
    public StepDefinition(String id, String activity, List<String> dependsOn, NodeType type, Long delayMillis) {
        this(id, activity, dependsOn, type, delayMillis, null);
    }
    public StepDefinition(String id, String activity, List<String> dependsOn) {
        this(id, activity, dependsOn, NodeType.ACTIVITY, null);
    }
}
