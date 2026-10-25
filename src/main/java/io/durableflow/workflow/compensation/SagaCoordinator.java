package io.durableflow.workflow.compensation;

import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.task.*;
import io.durableflow.workflow.history.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import java.time.Clock;
import java.util.*;
import java.util.stream.Collectors;

/** Caller holds the workflow run lock. Never locks existing task rows under that lock. */
@Service
@Transactional(propagation = Propagation.MANDATORY)
public class SagaCoordinator {
    private final ActivityExecutionRepository activities;
    private final CompensationExecutionRepository compensations;
    private final WorkflowDefinitionRepository definitions;
    private final WorkflowDefinitionValidator validator;
    private final ActivityTaskRepository tasks;
    private final WorkflowHistoryWriter history;
    private final Clock clock;

    public SagaCoordinator(ActivityExecutionRepository activities, CompensationExecutionRepository compensations,
            WorkflowDefinitionRepository definitions, WorkflowDefinitionValidator validator,
            ActivityTaskRepository tasks, WorkflowHistoryWriter history, Clock clock) {
        this.activities = activities; this.compensations = compensations; this.definitions = definitions;
        this.validator = validator; this.tasks = tasks; this.history = history; this.clock = clock;
    }

    public void forwardFailed(WorkflowRunEntity run, String code) {
        if (run.getStatus() != WorkflowRunStatus.RUNNING) return;
        if (plan(run).isEmpty()) {
            run.markFailed(clock.instant());
            history.append(run.getId(), null, WorkflowEventType.WORKFLOW_FAILED, Map.of("code", code));
            return;
        }
        run.startCompensating(clock.instant());
        history.append(run.getId(), null, WorkflowEventType.WORKFLOW_COMPENSATING, Map.of("code", code));
        advance(run);
    }

    public void advance(WorkflowRunEntity run) {
        if (run.getStatus() != WorkflowRunStatus.COMPENSATING) return;
        for (var item : plan(run)) {
            var existing = compensations.findByOriginalActivityExecutionId(item.original().getId());
            if (existing.isPresent()) {
                if (existing.get().getStatus() == ActivityExecutionStatus.COMPLETED) continue;
                if (existing.get().getStatus() == ActivityExecutionStatus.FAILED) {
                    throw new IllegalStateException("Terminal compensation must terminate the run atomically");
                }
                return; // Exactly one in-flight logical compensation, including delayed retries.
            }
            var execution = compensations.saveAndFlush(CompensationExecutionEntity.pending(
                    item.original(), item.handler(), clock.instant()));
            var task = tasks.save(ActivityTaskEntity.compensationReady(execution.getId(), clock.instant()));
            history.append(run.getId(), item.original().getId(), WorkflowEventType.COMPENSATION_SCHEDULED,
                    Map.of("compensationExecutionId", execution.getId().toString(), "handler", item.handler(),
                            "taskId", task.getId().toString()));
            return;
        }
        run.finishCompensation(true, clock.instant());
        history.append(run.getId(), null, WorkflowEventType.WORKFLOW_COMPENSATED, Map.of());
    }

    public void compensationFailed(WorkflowRunEntity run, CompensationExecutionEntity execution, String code) {
        run.finishCompensation(false, clock.instant());
        history.append(run.getId(), execution.getOriginalActivityExecutionId(), WorkflowEventType.COMPENSATION_FAILED,
                Map.of("compensationExecutionId", execution.getId().toString(), "handler", execution.getHandler(),
                        "attemptCount", execution.getAttemptCount(), "code", code));
        history.append(run.getId(), null, WorkflowEventType.WORKFLOW_COMPENSATION_FAILED, Map.of("code", code));
    }

    private List<Item> plan(WorkflowRunEntity run) {
        var definition = definitions.findById(run.getWorkflowDefinitionId()).orElseThrow();
        var steps = validator.normalizeAndValidate(new RegisterWorkflowDefinitionRequest(definition.getName(),
                definition.getVersion(), WorkflowDefinitionDocument.steps(definition.getDefinitionJson()))).steps();
        if (steps.stream().noneMatch(s -> s.compensation() != null)) return List.of();
        // Graph validation guarantees one connected acyclic chain, including timer/signal gates.
        var children = new HashMap<String, StepDefinition>();
        for (var step : steps) for (var dependency : step.dependsOn()) children.put(dependency, step);
        var rows = activities.findByWorkflowRunIdOrderByActivityIdAsc(run.getId()).stream()
                .collect(Collectors.toMap(ActivityExecutionEntity::getActivityId, a -> a));
        var reverse = new ArrayList<Item>();
        var step = steps.stream().filter(s -> s.dependsOn().isEmpty()).findFirst().orElseThrow();
        while (step != null) {
            var original = rows.get(step.id());
            if (step.compensation() != null && original != null && original.getStatus() == ActivityExecutionStatus.COMPLETED) {
                reverse.add(new Item(original, step.compensation()));
            }
            step = children.get(step.id());
        }
        Collections.reverse(reverse);
        return reverse;
    }
    private record Item(ActivityExecutionEntity original, String handler) {}
}
