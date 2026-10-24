package io.durableflow.workflow.timer;

import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/** The two supported node completion rules, shared by dependency and whole-run evaluation. */
public class NodeSatisfaction {
    private final Map<String, StepDefinition> nodes;
    private final Map<String, ActivityExecutionEntity> activities;
    private final Map<String, WorkflowTimer> timers;
    public NodeSatisfaction(List<StepDefinition> steps, Map<String, ActivityExecutionEntity> activities,
                            Map<String, WorkflowTimer> timers) {
        nodes = steps.stream().collect(Collectors.toMap(StepDefinition::id, Function.identity()));
        this.activities = activities; this.timers = timers;
    }
    public boolean satisfied(String id) {
        return switch (nodes.get(id).type()) {
            case ACTIVITY -> activities.containsKey(id) && activities.get(id).getStatus() == ActivityExecutionStatus.COMPLETED;
            case TIMER -> timers.containsKey(id) && timers.get(id).status() == WorkflowTimer.Status.FIRED;
        };
    }
    public boolean dependenciesSatisfied(StepDefinition step) { return step.dependsOn().stream().allMatch(this::satisfied); }
    public boolean allSatisfied() { return nodes.keySet().stream().allMatch(this::satisfied); }
}
