package io.durableflow.worker;

import io.durableflow.api.ApiException;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.task.*;
import io.durableflow.worker.retry.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;

/** Queue writers always lock task -> run -> read/change activity; handlers never run here. */
@Service
@Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
public class TaskTransactions {
    private final ActivityTaskRepository tasks;
    private final ActivityExecutionRepository activities;
    private final WorkflowRunRepository runs;
    private final WorkflowDefinitionRepository definitions;
    private final JdbcTemplate jdbc;
    private final WorkerLeaseSettings leases;
    private final RetryPolicy retryPolicy;
    private final RetryDelayCalculator retryDelays;
    private final FailureClassifier classifier;

    public TaskTransactions(ActivityTaskRepository tasks, ActivityExecutionRepository activities,
                            WorkflowRunRepository runs, WorkflowDefinitionRepository definitions,
                            JdbcTemplate jdbc, WorkerLeaseSettings leases, RetryPolicy retryPolicy,
                            RetryDelayCalculator retryDelays, FailureClassifier classifier) {
        this.tasks = tasks; this.activities = activities; this.runs = runs;
        this.definitions = definitions; this.jdbc = jdbc; this.leases = leases;
        this.retryPolicy = retryPolicy; this.retryDelays = retryDelays; this.classifier = classifier;
    }

    public Optional<ClaimedTask> claim(String workerId) {
        if (workerId == null || workerId.isBlank() || workerId.length() > 200) {
            throw new IllegalArgumentException("Worker ID must contain 1-200 characters");
        }
        var selected = tasks.lockNextClaimable();
        if (selected.isEmpty()) return Optional.empty();
        var task = selected.get();
        // The first read is only a reference lookup; reload state after acquiring the run lock.
        UUID runId = activities.findWorkflowRunId(task.getActivityExecutionId()).orElseThrow();
        var run = runs.findByIdForUpdate(runId).orElseThrow();
        var activity = activities.findById(task.getActivityExecutionId()).orElseThrow();
        if (run.getStatus() != WorkflowRunStatus.RUNNING) return Optional.empty();
        Instant now = databaseNow(); // After lock waits, never a worker clock or transaction-start NOW().
        boolean reclaim = task.getStatus() == ActivityTaskStatus.CLAIMED;
        if (reclaim) {
            if (task.getLeaseExpiresAt().isAfter(now) || activity.getStatus() != ActivityExecutionStatus.RUNNING) {
                return Optional.empty();
            }
        } else if ((activity.getStatus() != ActivityExecutionStatus.PENDING
                && !(activity.getStatus() == ActivityExecutionStatus.RUNNING && task.getLeaseGeneration() > 0))
                || task.getAvailableAt().isAfter(now)) {
            return Optional.empty();
        }
        if (activity.getAttemptCount() >= retryPolicy.maxAttempts()) {
            // Recover a crashed final attempt without starting an extra handler invocation.
            task.claimLease(workerId, now, now.plusMillis(leases.leaseMillis()));
            activity.markFailed("Maximum attempts exhausted before another execution could start", now);
            run.markFailed(now);
            task.fail();
            return Optional.empty();
        }
        var definition = definitions.findById(run.getWorkflowDefinitionId()).orElseThrow();
        String type = WorkflowDefinitionDocument.steps(definition.getDefinitionJson()).stream()
                .filter(step -> step.id().equals(activity.getActivityId())).findFirst().orElseThrow().activity();
        task.claimLease(workerId, now, now.plusMillis(leases.leaseMillis()));
        if (activity.getStatus() == ActivityExecutionStatus.RUNNING) activity.startNextAttempt(now);
        else activity.markRunning(run.getInputJson(), now);
        return Optional.of(new ClaimedTask(task.getId(), runId, activity.getId(), type, activity.getInputJson(),
                workerId, task.getLeaseGeneration(), task.getLeaseExpiresAt()));
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED, timeout = 5)
    public boolean heartbeat(ClaimedTask claim) {
        return tasks.renewOwned(claim.taskId(), claim.workerId(), claim.leaseGeneration(), leases.leaseMillis()) == 1;
    }

    public void complete(ClaimedTask claim, Map<String, Object> output) {
        owned(claim);
        var run = runs.findByIdForUpdate(claim.runId()).orElseThrow();
        allowOutcome(run);
        var activity = runningActivity(claim);
        activity.markCompleted(output, databaseNow());
        finish(claim, ActivityTaskStatus.COMPLETED);
    }

    public void fail(ClaimedTask claim, String message) {
        owned(claim);
        var run = runs.findByIdForUpdate(claim.runId()).orElseThrow();
        allowOutcome(run);
        var now = databaseNow();
        runningActivity(claim).markFailed(message, now);
        if (run.getStatus() == WorkflowRunStatus.RUNNING) run.markFailed(now);
        finish(claim, ActivityTaskStatus.FAILED);
    }

    public void handleFailure(ClaimedTask claim, Exception failure) {
        owned(claim);
        var run = runs.findByIdForUpdate(claim.runId()).orElseThrow();
        allowOutcome(run);
        var activity = runningActivity(claim);
        String message = failure.getClass().getSimpleName() + ": "
                + Objects.toString(failure.getMessage(), "Activity execution failed");
        message = message.substring(0, Math.min(message.length(), 2000));
        if (run.getStatus() == WorkflowRunStatus.RUNNING
                && classifier.classify(failure) == FailureClassifier.Classification.RETRYABLE
                && activity.getAttemptCount() < retryPolicy.maxAttempts()) {
            long delay = retryDelays.delayMillis(retryPolicy, activity.getAttemptCount());
            activity.recordRetryFailure(message, databaseNow());
            if (tasks.rescheduleOwned(claim.taskId(), claim.workerId(), claim.leaseGeneration(), delay) != 1) {
                throw new OwnershipLostException(claim); // Rolls back the failure metadata too.
            }
        } else {
            activity.markFailed(message, databaseNow());
            if (run.getStatus() == WorkflowRunStatus.RUNNING) run.markFailed(databaseNow());
            finish(claim, ActivityTaskStatus.FAILED);
        }
    }

    private ActivityTaskEntity owned(ClaimedTask claim) {
        var task = tasks.findByIdForUpdate(claim.taskId()).orElseThrow(() -> new OwnershipLostException(claim));
        if (task.getStatus() != ActivityTaskStatus.CLAIMED || !Objects.equals(task.getWorkerId(), claim.workerId())
                || task.getLeaseGeneration() != claim.leaseGeneration()
                || !task.getActivityExecutionId().equals(claim.activityExecutionId())) {
            throw new OwnershipLostException(claim);
        }
        return task;
    }

    private void finish(ClaimedTask claim, ActivityTaskStatus outcome) {
        // Flush logical changes, then test ownership at the actual final write. Zero rows throws
        // and rolls the ENTIRE transaction back, including already flushed activity/run changes.
        // The locked task entity itself is not mutated, so it cannot overwrite a renewed deadline.
        if (tasks.finishOwned(claim.taskId(), claim.workerId(), claim.leaseGeneration(), outcome.name()) != 1) {
            throw new OwnershipLostException(claim);
        }
    }

    private Instant databaseNow() {
        return Objects.requireNonNull(jdbc.queryForObject("SELECT clock_timestamp()", OffsetDateTime.class)).toInstant();
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
