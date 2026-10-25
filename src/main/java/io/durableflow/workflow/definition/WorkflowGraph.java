package io.durableflow.workflow.definition;

import java.util.*;

/** Deterministic topological order of the complete graph, including non-activity gates. */
public final class WorkflowGraph {
    private WorkflowGraph() {}

    public static List<StepDefinition> topologicalOrder(List<StepDefinition> steps) {
        var byId = new HashMap<String, StepDefinition>();
        var remaining = new HashMap<String, Integer>();
        var children = new HashMap<String, List<String>>();
        for (var step : steps) {
            if (byId.put(step.id(), step) != null) throw new IllegalArgumentException("Duplicate graph node");
            remaining.put(step.id(), step.dependsOn().size()); children.put(step.id(), new ArrayList<>());
        }
        for (var step : steps) for (var parent : step.dependsOn()) {
            if (!children.containsKey(parent)) throw new IllegalArgumentException("Unknown graph dependency");
            children.get(parent).add(step.id());
        }
        // Node id only breaks ties among currently independent nodes, never supplies a graph edge.
        var ready = new PriorityQueue<String>();
        remaining.forEach((id, count) -> { if (count == 0) ready.add(id); });
        var ordered = new ArrayList<StepDefinition>();
        while (!ready.isEmpty()) {
            String id = ready.remove(); ordered.add(byId.get(id));
            for (var child : children.get(id)) {
                if (remaining.compute(child, (key, count) -> count - 1) == 0) ready.add(child);
            }
        }
        if (ordered.size() != steps.size()) throw new IllegalArgumentException("Graph contains a cycle");
        return List.copyOf(ordered);
    }
}
