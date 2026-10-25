package io.durableflow.workflow.definition;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

// Explicit JSON document conversion; no arbitrary Java object serialization.
public final class WorkflowDefinitionDocument {
    private WorkflowDefinitionDocument() {}

    static Map<String, Object> encode(RegisterWorkflowDefinitionRequest definition) {
        return Map.of("name", definition.name(), "version", definition.version(),
                "steps", definition.steps().stream()
                        .map(step -> {
                            var encoded = new HashMap<String, Object>();
                            encoded.put("id", step.id()); encoded.put("type", step.type().name());
                            encoded.put("dependsOn", step.dependsOn());
                            if (step.type() == NodeType.CHILD_WORKFLOW) {
                                encoded.put("childWorkflow", step.childWorkflow()); encoded.put("childVersion", step.childVersion());
                                if (step.childInput() != null) encoded.put("childInput", step.childInput());
                            }
                            if (step.compensation() != null) encoded.put("compensation", step.compensation());
                            if (step.type() == NodeType.ACTIVITY) encoded.put("activity", step.activity());
                            else if (step.type() == NodeType.TIMER) encoded.put("delayMillis", step.delayMillis());
                            return encoded;
                        }).toList());
    }

    @SuppressWarnings("unchecked")
    public static List<StepDefinition> steps(Map<String, Object> document) {
        if (!(document.get("steps") instanceof List<?> steps)) {
            throw new IllegalStateException("Stored definition has no steps");
        }
        return steps.stream().map(value -> {
            if (!(value instanceof Map<?, ?> step)
                    || !(step.get("id") instanceof String id)
                    || !(step.get("dependsOn") instanceof List<?> dependencies)) {
                throw new IllegalStateException("Invalid stored workflow step");
            }
            var type = step.get("type") == null ? NodeType.ACTIVITY : NodeType.valueOf((String) step.get("type"));
            String activity = (String) step.get("activity");
            Long delay = step.get("delayMillis") == null ? null : new java.math.BigDecimal(step.get("delayMillis").toString()).longValueExact();
            if (type == NodeType.ACTIVITY && (activity == null || activity.isBlank())) throw new IllegalStateException("Invalid activity step");
            if (type == NodeType.TIMER && (delay == null || delay <= 0)) throw new IllegalStateException("Invalid timer step");
            return new StepDefinition(id, activity, dependencies.stream().map(String.class::cast).toList(), type, delay, (String) step.get("compensation"),
                    (String) step.get("childWorkflow"), step.get("childVersion") == null ? null
                            : new java.math.BigDecimal(step.get("childVersion").toString()).intValueExact(),
                    (Map<String, Object>) step.get("childInput"));
        }).toList();
    }
}
