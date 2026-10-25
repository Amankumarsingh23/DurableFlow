package io.durableflow.workflow.child;

import io.durableflow.api.ApiException;
import io.durableflow.workflow.definition.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import java.util.*;

/** Forward references are allowed; registering the edge that closes a reachable cycle is rejected. */
@Component
public class ChildDefinitionGuard {
    private final WorkflowDefinitionRepository definitions;
    private final org.springframework.jdbc.core.JdbcTemplate jdbc;
    public ChildDefinitionGuard(WorkflowDefinitionRepository definitions, org.springframework.jdbc.core.JdbcTemplate jdbc) { this.definitions = definitions; this.jdbc = jdbc; }
    @org.springframework.transaction.annotation.Transactional(propagation = org.springframework.transaction.annotation.Propagation.MANDATORY)
    public void validate(RegisterWorkflowDefinitionRequest request) {
        // Serialize definition graph changes across servers, including races between forward references.
        jdbc.execute("SELECT pg_advisory_xact_lock(448210020)");
        visit(new Ref(request.name(), request.version()), request, new HashSet<>(), new HashSet<>());
    }
    private void visit(Ref ref, RegisterWorkflowDefinitionRequest proposed, Set<Ref> path, Set<Ref> visited) {
        if (path.contains(ref)) throw invalid("Recursive child workflow definition cycle");
        if (visited.contains(ref)) return;
        if (path.size() >= 128) throw invalid("Child definition graph exceeds registration traversal depth");
        path.add(ref);
        List<StepDefinition> steps;
        if (ref.equals(new Ref(proposed.name(), proposed.version()))) steps = proposed.steps();
        else steps = definitions.findByNameAndVersion(ref.name(), ref.version())
                .map(d -> WorkflowDefinitionDocument.steps(d.getDefinitionJson())).orElse(List.of());
        for (var step : steps) if (step.type() == NodeType.CHILD_WORKFLOW) {
            visit(new Ref(step.childWorkflow(), step.childVersion()), proposed, path, visited);
        }
        path.remove(ref); visited.add(ref);
    }
    private ApiException invalid(String message) { return new ApiException(HttpStatus.BAD_REQUEST, message); }
    private record Ref(String name, int version) {}
}
