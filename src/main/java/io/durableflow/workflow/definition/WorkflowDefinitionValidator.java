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
        var steps = request.steps().stream()
                .map(step -> new StepDefinition(step.id().strip(), step.activity().strip(),
                        step.dependsOn().stream().map(String::strip).distinct().toList()))
                .toList();
        var normalized = new RegisterWorkflowDefinitionRequest(request.name().strip(), request.version(), steps);
        validateGraph(steps);
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
