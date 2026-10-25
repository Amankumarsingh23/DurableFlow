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
        if (run.getStatus() == WorkflowRunStatus.COMPENSATING) { advance(run); return; }
        if (run.getStatus() != WorkflowRunStatus.RUNNING) return;
        if (plan(run).isEmpty() && !(hasRunningForward(run) && steps(run).stream().anyMatch(s -> s.compensation() != null))) {
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
        // Freeze the compensation set only after every already-started forward execution settles.
        // No new PENDING forward execution may start; bounded retry/reclaim may resolve RUNNING work.
        if (hasRunningForward(run)) return;
        var plan = plan(run);
        if (plan.isEmpty()) {
            run.finishWithoutCompensation(clock.instant());
            history.append(run.getId(), null, WorkflowEventType.WORKFLOW_FAILED, Map.of("code", "NO_COMPLETED_COMPENSATABLE_EFFECTS"));
            return;
        }
        for (var item : plan) {
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

    private boolean hasRunningForward(WorkflowRunEntity run) {
        return activities.findByWorkflowRunIdOrderByActivityIdAsc(run.getId()).stream()
                .anyMatch(a -> a.getStatus() == ActivityExecutionStatus.RUNNING);
    }

    private List<StepDefinition> steps(WorkflowRunEntity run) {
        var definition = definitions.findById(run.getWorkflowDefinitionId()).orElseThrow();
        return validator.normalizeAndValidate(new RegisterWorkflowDefinitionRequest(definition.getName(),
                definition.getVersion(), WorkflowDefinitionDocument.steps(definition.getDefinitionJson()))).steps();
    }

    private List<Item> plan(WorkflowRunEntity run) {
        // Sort ALL graph nodes before filtering: dependencies through non-compensatable nodes,
        // TIMERs and SIGNALs must still constrain ancestor/descendant compensation order.
        var ordered = new ArrayList<>(WorkflowGraph.topologicalOrder(steps(run)));
        Collections.reverse(ordered);
        var rows = activities.findByWorkflowRunIdOrderByActivityIdAsc(run.getId()).stream()
                .collect(Collectors.toMap(ActivityExecutionEntity::getActivityId, a -> a));
        var result = new ArrayList<Item>();
        for (var step : ordered) {
            var original = rows.get(step.id());
            if (step.compensation() != null && original != null && original.getStatus() == ActivityExecutionStatus.COMPLETED) {
                result.add(new Item(original, step.compensation()));
            }
        }
        return result;
    }
    private record Item(ActivityExecutionEntity original, String handler) {}
}
