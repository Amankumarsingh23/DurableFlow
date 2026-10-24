package io.durableflow.workflow.engine;

import io.durableflow.api.ApiException;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.run.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.time.Clock;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Separate bean so checkpoints commit before handler code runs. No method here invokes handlers. */
@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class WorkflowEngineState {
    private final WorkflowRunRepository runs;
    private final WorkflowDefinitionRepository definitions;
    private final ActivityExecutionRepository activities;
    private final WorkflowDefinitionValidator validator;
    private final Clock clock;

    public WorkflowEngineState(WorkflowRunRepository runs, WorkflowDefinitionRepository definitions,
                               ActivityExecutionRepository activities, WorkflowDefinitionValidator validator, Clock clock) {
        this.runs = runs;
        this.definitions = definitions;
        this.activities = activities;
        this.validator = validator;
        this.clock = clock;
    }

    public record Work(UUID activityExecutionId, String activityType, Map<String, Object> input) {}
    public record Decision(Work work, String error) {}

    /** Reload durable state; allowStart=false checks/finalizes the run without claiming another activity. */
    public Decision next(UUID runId, boolean allowStart) {
        var run = running(runId);
        var rows = activities.findByWorkflowRunIdOrderByActivityIdAsc(runId);
        var uncertain = rows.stream().filter(row -> row.getStatus() == ActivityExecutionStatus.RUNNING)
                .map(ActivityExecutionEntity::getActivityId).toList();
        if (!uncertain.isEmpty()) {
            // Uncertainty takes priority, even over other corrupt state. Never invent a terminal outcome.
            // No entity has been changed; rollback preserves every existing checkpoint.
            throw new OrphanedActivityException(runId, uncertain);
        }
        var definition = definitions.findById(run.getWorkflowDefinitionId()).orElseThrow();
        List<StepDefinition> steps;
        try {
            var document = WorkflowDefinitionDocument.steps(definition.getDefinitionJson());
            steps = validator.normalizeAndValidate(new RegisterWorkflowDefinitionRequest(
                    definition.getName(), definition.getVersion(), document)).steps();
        } catch (RuntimeException invalidDefinition) {
            return noProgress(run, "Stored workflow definition is invalid");
        }
        var byId = rows.stream().collect(Collectors.toMap(ActivityExecutionEntity::getActivityId, Function.identity()));
        Set<String> expectedIds = steps.stream().map(StepDefinition::id).collect(Collectors.toSet());
        if (!byId.keySet().equals(expectedIds)) {
            return noProgress(run, "Activity rows do not match the workflow definition");
        }
        if (rows.stream().allMatch(row -> row.getStatus() == ActivityExecutionStatus.COMPLETED)) {
            run.markCompleted(clock.instant());
            return new Decision(null, null);
        }
        if (rows.stream().anyMatch(row -> row.getStatus() == ActivityExecutionStatus.FAILED)) {
            return noProgress(run, "Run contains a failed activity; automatic retry is disabled");
        }
        // Temporary dev/test budget: stop only at a committed checkpoint, before claiming more work.
        if (!allowStart) return new Decision(null, null);
        for (var step : steps) {
            var row = byId.get(step.id());
            boolean eligible = row.getStatus() == ActivityExecutionStatus.PENDING
                    && step.dependsOn().stream()
                    .allMatch(id -> byId.get(id).getStatus() == ActivityExecutionStatus.COMPLETED);
            if (eligible) {
                row.markRunning(run.getInputJson(), clock.instant());
                return new Decision(new Work(row.getId(), step.activity(), row.getInputJson()), null);
            }
        }
        return noProgress(run, "No eligible activity exists and the workflow is incomplete");
    }

    public void complete(UUID runId, UUID activityId, Map<String, Object> output) {
        running(runId);
        activity(runId, activityId).markCompleted(output, clock.instant());
    }

    /** Activity and workflow failure commit atomically. */
    public void fail(UUID runId, UUID activityId, String message) {
        var run = running(runId);
        var now = clock.instant();
        activity(runId, activityId).markFailed(message, now);
        run.markFailed(now);
    }

    private Decision noProgress(WorkflowRunEntity run, String message) {
        run.markFailed(clock.instant());
        // Return rather than throw so the FAILED checkpoint commits before HTTP reports the error.
        return new Decision(null, message);
    }

    private WorkflowRunEntity running(UUID runId) {
        var run = runs.findById(runId)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Workflow run not found"));
        if (run.getStatus() != WorkflowRunStatus.RUNNING) {
            throw new ApiException(HttpStatus.CONFLICT, "Only RUNNING workflow runs can execute");
        }
        return run;
    }

    private ActivityExecutionEntity activity(UUID runId, UUID activityId) {
        var activity = activities.findById(activityId).orElseThrow();
        if (!runId.equals(activity.getWorkflowRunId())) throw new IllegalStateException("Activity belongs to another run");
        return activity;
    }
}
