package io.durableflow.worker;

import io.durableflow.api.ApiException;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.task.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import java.time.Clock;
import java.util.*;

/** Queue writers always lock task -> run -> read/change activity; handlers never run here. */
@Service
@Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
public class TaskTransactions {
    private final ActivityTaskRepository tasks;
    private final ActivityExecutionRepository activities;
    private final WorkflowRunRepository runs;
    private final WorkflowDefinitionRepository definitions;
    private final Clock clock;

    public TaskTransactions(ActivityTaskRepository tasks, ActivityExecutionRepository activities,
                            WorkflowRunRepository runs, WorkflowDefinitionRepository definitions, Clock clock) {
        this.tasks = tasks; this.activities = activities; this.runs = runs;
        this.definitions = definitions; this.clock = clock;
    }

    public Optional<ClaimedTask> claim(String workerId) {
        if (workerId == null || workerId.isBlank() || workerId.length() > 200) {
            throw new IllegalArgumentException("Worker ID must contain 1-200 characters");
        }
        var selected = tasks.lockNextReady(clock.instant());
        if (selected.isEmpty()) return Optional.empty();
        var task = selected.get();
        // The first read is only a reference lookup; reload state after acquiring the run lock.
        UUID runId = activities.findWorkflowRunId(task.getActivityExecutionId()).orElseThrow();
        var run = runs.findByIdForUpdate(runId).orElseThrow();
        var activity = activities.findById(task.getActivityExecutionId()).orElseThrow();
        if (run.getStatus() != WorkflowRunStatus.RUNNING || activity.getStatus() != ActivityExecutionStatus.PENDING) {
            return Optional.empty();
        }
        var definition = definitions.findById(run.getWorkflowDefinitionId()).orElseThrow();
        String type = WorkflowDefinitionDocument.steps(definition.getDefinitionJson()).stream()
                .filter(step -> step.id().equals(activity.getActivityId())).findFirst().orElseThrow().activity();
        var now = clock.instant();
        task.claim(workerId, now);
        activity.markRunning(run.getInputJson(), now);
        return Optional.of(new ClaimedTask(task.getId(), runId, activity.getId(), type, activity.getInputJson(), workerId));
    }

    public void complete(ClaimedTask claim, Map<String, Object> output) {
        var task = owned(claim);
        var run = runs.findByIdForUpdate(claim.runId()).orElseThrow();
        allowOutcome(run);
        var activity = runningActivity(claim);
        activity.markCompleted(output, clock.instant());
        task.complete();
    }

    public void fail(ClaimedTask claim, String message) {
        var task = owned(claim);
        var run = runs.findByIdForUpdate(claim.runId()).orElseThrow();
        allowOutcome(run);
        var now = clock.instant();
        runningActivity(claim).markFailed(message, now);
        task.fail();
        if (run.getStatus() == WorkflowRunStatus.RUNNING) run.markFailed(now);
    }

    private ActivityTaskEntity owned(ClaimedTask claim) {
        var task = tasks.findByIdForUpdate(claim.taskId()).orElseThrow();
        if (task.getStatus() != ActivityTaskStatus.CLAIMED || !Objects.equals(task.getWorkerId(), claim.workerId())
                || !task.getActivityExecutionId().equals(claim.activityExecutionId())) {
            throw new ApiException(HttpStatus.CONFLICT, "Task is not claimed by this worker");
        }
        return task;
    }

    private ActivityExecutionEntity runningActivity(ClaimedTask claim) {
        var activity = activities.findById(claim.activityExecutionId()).orElseThrow();
        if (!activity.getWorkflowRunId().equals(claim.runId()) || activity.getStatus() != ActivityExecutionStatus.RUNNING) {
            throw new ApiException(HttpStatus.CONFLICT, "Activity does not match the claimed task");
        }
        return activity;
    }

    private void allowOutcome(WorkflowRunEntity run) {
        // A different already-claimed root can fail while this handler is executing. Record its outcome
        // without reopening the FAILED workflow; new READY tasks are excluded by the claim query.
        if (run.getStatus() != WorkflowRunStatus.RUNNING && run.getStatus() != WorkflowRunStatus.FAILED) {
            throw new ApiException(HttpStatus.CONFLICT, "Workflow no longer accepts task outcomes");
        }
    }
}
