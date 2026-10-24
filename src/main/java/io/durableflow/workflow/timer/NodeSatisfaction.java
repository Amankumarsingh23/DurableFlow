package io.durableflow.workflow.timer;

import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.signal.SignalWait;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Supported node completion rules, shared by dependency and whole-run evaluation. */
public class NodeSatisfaction {
    private final Map<String, StepDefinition> nodes;
    private final Map<String, ActivityExecutionEntity> activities;
    private final Map<String, WorkflowTimer> timers;
    private final Map<String, SignalWait> signals;
    public NodeSatisfaction(List<StepDefinition> steps, Map<String, ActivityExecutionEntity> activities,
                            Map<String, WorkflowTimer> timers, Map<String, SignalWait> signals) {
        nodes = steps.stream().collect(Collectors.toMap(StepDefinition::id, Function.identity()));
        this.activities = activities; this.timers = timers;
        this.signals = signals;
    }
    public boolean satisfied(String id) {
        return switch (nodes.get(id).type()) {
            case ACTIVITY -> activities.containsKey(id) && activities.get(id).getStatus() == ActivityExecutionStatus.COMPLETED;
            case TIMER -> timers.containsKey(id) && timers.get(id).status() == WorkflowTimer.Status.FIRED;
            case SIGNAL -> signals.containsKey(id) && signals.get(id).status() == SignalWait.Status.RECEIVED;
        };
    }
    public boolean dependenciesSatisfied(StepDefinition step) { return step.dependsOn().stream().allMatch(this::satisfied); }
    public boolean allSatisfied() { return nodes.keySet().stream().allMatch(this::satisfied); }
}
