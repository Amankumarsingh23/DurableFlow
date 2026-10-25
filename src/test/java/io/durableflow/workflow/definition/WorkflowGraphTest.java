package io.durableflow.workflow.definition;

import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class WorkflowGraphTest {
    private StepDefinition step(String id, String... parents) { return new StepDefinition(id, "demo", List.of(parents)); }

    @Test
    void dependenciesOverrideArrayOrderAndIndependentNodesHaveStableIdTieBreak() {
        var graph = List.of(step("join", "B", "C"), step("C", "A"), step("B", "A"), step("A"));
        assertThat(WorkflowGraph.topologicalOrder(graph)).extracting(StepDefinition::id).containsExactly("A", "B", "C", "join");
        var shuffled = new ArrayList<>(graph); Collections.reverse(shuffled);
        assertThat(WorkflowGraph.topologicalOrder(shuffled)).isEqualTo(WorkflowGraph.topologicalOrder(graph));
    }

    @Test
    void pathsThroughNonCompensatableGatesStillConstrainOrder() {
        var graph = List.of(step("A", "wait"), new StepDefinition("wait", null, List.of("Z"), NodeType.SIGNAL, null), step("Z"));
        assertThat(WorkflowGraph.topologicalOrder(graph)).extracting(StepDefinition::id).containsExactly("Z", "wait", "A");
        assertThatThrownBy(() -> WorkflowGraph.topologicalOrder(List.of(step("A", "B"), step("B", "A"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cycle");
        assertThatThrownBy(() -> WorkflowGraph.topologicalOrder(List.of(step("A", "missing"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unknown");
    }
}
