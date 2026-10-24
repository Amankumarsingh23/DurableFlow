package io.durableflow.workflow.definition;

import java.util.List;
import java.util.Map;

// Explicit JSON document conversion; no arbitrary Java object serialization.
public final class WorkflowDefinitionDocument {
    private WorkflowDefinitionDocument() {}

    static Map<String, Object> encode(RegisterWorkflowDefinitionRequest definition) {
        return Map.of("name", definition.name(), "version", definition.version(),
                "steps", definition.steps().stream()
                        .map(step -> Map.<String, Object>of("id", step.id(), "activity", step.activity(),
                                "dependsOn", step.dependsOn())).toList());
    }

    public static List<StepDefinition> steps(Map<String, Object> document) {
        if (!(document.get("steps") instanceof List<?> steps)) {
            throw new IllegalStateException("Stored definition has no steps");
        }
        return steps.stream().map(value -> {
            if (!(value instanceof Map<?, ?> step)
                    || !(step.get("id") instanceof String id)
                    || !(step.get("activity") instanceof String activity)
                    || !(step.get("dependsOn") instanceof List<?> dependencies)) {
                throw new IllegalStateException("Invalid stored workflow step");
            }
            return new StepDefinition(id, activity, dependencies.stream().map(String.class::cast).toList());
        }).toList();
    }
}
