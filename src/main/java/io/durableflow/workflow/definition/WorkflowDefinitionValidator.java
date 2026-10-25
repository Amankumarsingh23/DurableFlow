package io.durableflow.workflow.definition;

import io.durableflow.api.ApiException;
import jakarta.validation.Validator;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import java.util.*;

@Component
public class WorkflowDefinitionValidator {
    private final Validator validator;

    public WorkflowDefinitionValidator(Validator validator) { this.validator = validator; }

    public RegisterWorkflowDefinitionRequest normalizeAndValidate(RegisterWorkflowDefinitionRequest request) {
        if (request == null || !validator.validate(request).isEmpty()) {
            throw invalid("Name, positive version and nonempty valid steps are required");
        }
        for (var step : request.steps()) {
            if (step.compensation() != null && (step.type() != NodeType.ACTIVITY
                    || !step.compensation().strip().matches("[A-Za-z][A-Za-z0-9_.-]{0,199}"))) {
                throw invalid("Compensation must be an ACTIVITY handler name of 1-200 characters");
            }
            if (step.type() == NodeType.ACTIVITY) {
                if (step.activity() == null || step.activity().isBlank() || step.delayMillis() != null) {
                    throw invalid("ACTIVITY requires activity and must not specify delayMillis");
                }
            } else if (step.type() == NodeType.SIGNAL) {
                if (step.delayMillis() != null || (step.activity() != null && !step.activity().isBlank())) {
                    throw invalid("SIGNAL uses its exact node id and must not specify activity or delayMillis");
                }
            } else if (step.delayMillis() == null || step.delayMillis() <= 0
                    || step.delayMillis() > 3_155_760_000_000L
                    || (step.activity() != null && !step.activity().isBlank())) {
                throw invalid("TIMER requires a positive delayMillis of at most 100 years and no activity handler");
            }
        }
        var steps = request.steps().stream()
                .map(step -> new StepDefinition(step.id().strip(), step.type() == NodeType.ACTIVITY ? step.activity().strip() : null,
                        step.dependsOn().stream().map(String::strip).distinct().toList(), step.type(), step.delayMillis(),
                        step.compensation() == null ? null : step.compensation().strip()))
                .toList();
        var normalized = new RegisterWorkflowDefinitionRequest(request.name().strip(), request.version(), steps);
        validateGraph(steps);
        if (steps.stream().anyMatch(s -> s.compensation() != null)) {
            var children = new HashMap<String, Integer>();
            for (var step : steps) {
                if (step.dependsOn().size() > 1) throw invalid("Saga compensation requires a single sequential chain");
                for (var parent : step.dependsOn()) children.merge(parent, 1, Integer::sum);
            }
            if (steps.stream().filter(s -> s.dependsOn().isEmpty()).count() != 1
                    || children.values().stream().anyMatch(count -> count > 1)) {
                throw invalid("Saga compensation requires a single sequential chain");
            }
        }
        return normalized;
    }

    private void validateGraph(List<StepDefinition> steps) {
        Map<String, Integer> indegree = new LinkedHashMap<>();
        Map<String, List<String>> dependents = new HashMap<>();
        for (var step : steps) {
            if (indegree.putIfAbsent(step.id(), step.dependsOn().size()) != null) {
                throw invalid("Step ids must be unique: " + step.id());
            }
            dependents.put(step.id(), new ArrayList<>());
        }
        for (var step : steps) {
            for (var dependency : step.dependsOn()) {
                if (dependency.equals(step.id())) throw invalid("A step cannot depend on itself: " + step.id());
                if (!indegree.containsKey(dependency)) throw invalid("Unknown dependency: " + dependency);
                dependents.get(dependency).add(step.id());
            }
        }
        // Kahn's algorithm: a cycle exists if not every node can be removed.
        Deque<String> ready = new ArrayDeque<>();
        indegree.forEach((id, count) -> { if (count == 0) ready.add(id); });
        int visited = 0;
        while (!ready.isEmpty()) {
            String id = ready.remove();
            visited++;
            for (String dependent : dependents.get(id)) {
                if (indegree.compute(dependent, (key, count) -> count - 1) == 0) ready.add(dependent);
            }
        }
        if (visited != steps.size()) throw invalid("Workflow dependencies must not contain a cycle");
    }

    private ApiException invalid(String message) { return new ApiException(HttpStatus.BAD_REQUEST, message); }
}
