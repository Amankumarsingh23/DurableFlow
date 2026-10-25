package io.durableflow.worker;

import io.durableflow.api.ApiException;
import io.durableflow.workflow.compensation.*;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.task.*;
import io.durableflow.worker.retry.*;
import io.durableflow.workflow.history.*;
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
    private final WorkflowHistoryWriter history;
    private final CompensationExecutionRepository compensations;
    private final SagaCoordinator sagas;
    private final ActivityInputPreparation inputPreparation;
    private final List<ActivityCompletionParticipant> completionParticipants;

    public TaskTransactions(ActivityTaskRepository tasks, ActivityExecutionRepository activities,
                            WorkflowRunRepository runs, WorkflowDefinitionRepository definitions,
                            JdbcTemplate jdbc, WorkerLeaseSettings leases, RetryPolicy retryPolicy,
                            RetryDelayCalculator retryDelays, FailureClassifier classifier, WorkflowHistoryWriter history,
                            CompensationExecutionRepository compensations, SagaCoordinator sagas, ActivityInputPreparation inputPreparation, List<ActivityCompletionParticipant> completionParticipants) {
        this.tasks = tasks; this.activities = activities; this.runs = runs;
        this.definitions = definitions; this.jdbc = jdbc; this.leases = leases;
        this.retryPolicy = retryPolicy; this.retryDelays = retryDelays; this.classifier = classifier;
        this.history = history; this.compensations = compensations; this.sagas = sagas; this.inputPreparation = inputPreparation; this.completionParticipants = List.copyOf(completionParticipants);
    }

    public Optional<ClaimedTask> claim(String workerId) {
        if (workerId == null || workerId.isBlank() || workerId.length() > 200) {
            throw new IllegalArgumentException("Worker ID must contain 1-200 characters");
        }
        var selected = tasks.lockNextClaimable();
        if (selected.isEmpty()) return Optional.empty();
        var task = selected.get();
        // The first read is only a reference lookup; reload state after acquiring the run lock.
        UUID runId = task.isCompensation()
                ? compensations.findWorkflowRunId(task.getCompensationExecutionId()).orElseThrow()
                : activities.findWorkflowRunId(task.getActivityExecutionId()).orElseThrow();
        var run = runs.findByIdForUpdate(runId).orElseThrow();
        DurableExecution activity = task.isCompensation()
                ? compensations.findById(task.getCompensationExecutionId()).orElseThrow()
                : activities.findById(task.getActivityExecutionId()).orElseThrow();
        boolean settlingForward = !task.isCompensation() && run.getStatus() == WorkflowRunStatus.COMPENSATING
                && activity.getStatus() == ActivityExecutionStatus.RUNNING && task.getLeaseGeneration() > 0;
        if (!settlingForward && run.getStatus() != (task.isCompensation() ? WorkflowRunStatus.COMPENSATING : WorkflowRunStatus.RUNNING)) {
            return Optional.empty();
        }
        Instant now = databaseNow(); // After lock waits, never a worker clock or transaction-start NOW().
        boolean reclaim = task.getStatus() == ActivityTaskStatus.CLAIMED;
        String previousWorker = task.getWorkerId();
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
            task.fail();
            claimEvent(task, runId, activity, reclaim, previousWorker, false);
            terminalEvents(run, activity, "ATTEMPTS_EXHAUSTED");
            return Optional.empty();
        }
        String type;
        StepDefinition forwardStep = null;
        UUID originalId;
        if (activity instanceof CompensationExecutionEntity compensation) {
            type = compensation.getHandler(); originalId = compensation.getOriginalActivityExecutionId();
        } else {
            var forward = (ActivityExecutionEntity) activity;
            var definition = definitions.findById(run.getWorkflowDefinitionId()).orElseThrow();
            forwardStep = WorkflowDefinitionDocument.steps(definition.getDefinitionJson()).stream()
                    .filter(step -> step.id().equals(forward.getActivityId())).findFirst().orElseThrow();
            type = forwardStep.activity();
            originalId = forward.getId();
        }
        task.claimLease(workerId, now, now.plusMillis(leases.leaseMillis()));
        if (activity.getStatus() == ActivityExecutionStatus.RUNNING) activity.startNextAttempt(now);
        else activity.markRunning(task.isCompensation() ? activity.getInputJson()
                : inputPreparation.prepare(runId, forwardStep, run.getInputJson()), now);
        claimEvent(task, runId, activity, reclaim, previousWorker, true);
        return Optional.of(new ClaimedTask(task.getId(), runId, originalId, type, activity.getInputJson(),
                workerId, task.getLeaseGeneration(), task.getLeaseExpiresAt(), task.getCompensationExecutionId()));
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED, timeout = 5)
    public boolean heartbeat(ClaimedTask claim) {
        return tasks.renewOwned(claim.taskId(), claim.workerId(), claim.leaseGeneration(), leases.leaseMillis()) == 1;
    }

    public void complete(ClaimedTask claim, Map<String, Object> output) {
        owned(claim);
        var run = runs.findByIdForUpdate(claim.runId()).orElseThrow();
        allowOutcome(run, claim);
        var activity = runningExecution(claim);
        activity.markCompleted(output, databaseNow());
        finish(claim, ActivityTaskStatus.COMPLETED);
        if (activity instanceof ActivityExecutionEntity forward) {
            for (var participant : completionParticipants) participant.completed(forward, output);
        }
        history.append(claim.runId(), claim.activityExecutionId(),
                claim.isCompensation() ? WorkflowEventType.COMPENSATION_COMPLETED : WorkflowEventType.ACTIVITY_COMPLETED,
                executionData(claim, Map.of("generation", claim.leaseGeneration(), "attemptCount", activity.getAttemptCount())));
        if (run.getStatus() == WorkflowRunStatus.COMPENSATING) sagas.advance(run);
    }

    public void fail(ClaimedTask claim, String message) {
        owned(claim);
        var run = runs.findByIdForUpdate(claim.runId()).orElseThrow();
        allowOutcome(run, claim);
        var now = databaseNow();
        var execution = runningExecution(claim);
        execution.markFailed(message, now);
        finish(claim, ActivityTaskStatus.FAILED);
        terminalEvents(run, execution, "ACTIVITY_FAILURE");
    }

    public void handleFailure(ClaimedTask claim, Exception failure) {
        owned(claim);
        var run = runs.findByIdForUpdate(claim.runId()).orElseThrow();
        allowOutcome(run, claim);
        var activity = runningExecution(claim);
        String message = failure.getClass().getSimpleName() + ": "
                + Objects.toString(failure.getMessage(), "Activity execution failed");
        message = message.substring(0, Math.min(message.length(), 2000));
        if ((run.getStatus() == WorkflowRunStatus.RUNNING || run.getStatus() == WorkflowRunStatus.COMPENSATING)
                && classifier.classify(failure) == FailureClassifier.Classification.RETRYABLE
                && activity.getAttemptCount() < retryPolicy.maxAttempts()) {
            long delay = retryDelays.delayMillis(retryPolicy, activity.getAttemptCount());
            activity.recordRetryFailure(message, databaseNow());
            if (tasks.rescheduleOwned(claim.taskId(), claim.workerId(), claim.leaseGeneration(), delay) != 1) {
                throw new OwnershipLostException(claim); // Rolls back the failure metadata too.
            }
            timeoutEvent(claim, failure, activity.getAttemptCount());
            String available = Objects.requireNonNull(jdbc.queryForObject(
                    "SELECT available_at FROM activity_task WHERE id = ?", OffsetDateTime.class, claim.taskId())).toInstant().toString();
            history.append(claim.runId(), claim.activityExecutionId(), WorkflowEventType.RETRY_SCHEDULED,
                    executionData(claim, Map.of("attemptCount", activity.getAttemptCount(), "generation", claim.leaseGeneration(),
                            "delayMillis", delay, "availableAt", available, "classification", "RETRYABLE",
                            "code", failure instanceof ActivityTimeoutException ? "ACTIVITY_TIMEOUT" : "TRANSIENT_FAILURE")));
        } else {
            activity.markFailed(message, databaseNow());
            finish(claim, ActivityTaskStatus.FAILED);
            timeoutEvent(claim, failure, activity.getAttemptCount());
            terminalEvents(run, activity,
                    classifier.classify(failure) == FailureClassifier.Classification.RETRYABLE
                            ? (activity.getAttemptCount() >= retryPolicy.maxAttempts() ? "RETRY_BUDGET_EXHAUSTED" : "WORKFLOW_ALREADY_FAILED")
                            : "NON_RETRYABLE_FAILURE");
        }
    }

    private void claimEvent(ActivityTaskEntity task, UUID runId, DurableExecution activity,
                            boolean reclaimed, String previousWorker, boolean executionStarted) {
        var data = new HashMap<String, Object>();
        data.put("taskId", task.getId().toString()); data.put("workerId", task.getWorkerId());
        data.put("generation", task.getLeaseGeneration()); data.put("attemptCount", activity.getAttemptCount());
        data.put("executionStarted", executionStarted);
        if (reclaimed) data.put("previousWorkerId", previousWorker);
        UUID originalId = activity instanceof CompensationExecutionEntity c ? c.getOriginalActivityExecutionId() : activity.getId();
        if (task.isCompensation()) { data.put("taskKind", "COMPENSATION"); data.put("compensationExecutionId", activity.getId().toString()); }
        history.append(runId, originalId, reclaimed ? WorkflowEventType.TASK_RECLAIMED : WorkflowEventType.TASK_CLAIMED, data);
    }

    private void timeoutEvent(ClaimedTask claim, Exception failure, int attempts) {
        if (failure instanceof ActivityTimeoutException timeout) {
            history.append(claim.runId(), claim.activityExecutionId(), WorkflowEventType.ACTIVITY_TIMED_OUT,
                    executionData(claim, Map.of("timeoutMillis", timeout.timeoutMillis(), "attemptCount", attempts, "generation", claim.leaseGeneration())));
        }
    }

    private Map<String, Object> executionData(ClaimedTask claim, Map<String, Object> data) {
        if (!claim.isCompensation()) return data;
        var result = new HashMap<String, Object>(data);
        result.put("taskKind", "COMPENSATION"); result.put("compensationExecutionId", claim.compensationExecutionId().toString());
        return result;
    }

    private void terminalEvents(WorkflowRunEntity run, DurableExecution execution, String code) {
        if (execution instanceof CompensationExecutionEntity compensation) {
            sagas.compensationFailed(run, compensation, code);
        } else {
            history.append(run.getId(), execution.getId(), WorkflowEventType.ACTIVITY_FAILED, Map.of("code", code));
            sagas.forwardFailed(run, code);
        }
    }

    private ActivityTaskEntity owned(ClaimedTask claim) {
        var task = tasks.findByIdForUpdate(claim.taskId()).orElseThrow(() -> new OwnershipLostException(claim));
        if (task.getStatus() != ActivityTaskStatus.CLAIMED || !Objects.equals(task.getWorkerId(), claim.workerId())
                || task.getLeaseGeneration() != claim.leaseGeneration()
                || task.isCompensation() != claim.isCompensation()
                || (claim.isCompensation() ? !Objects.equals(task.getCompensationExecutionId(), claim.compensationExecutionId())
                    : !Objects.equals(task.getActivityExecutionId(), claim.activityExecutionId()))) {
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

    private DurableExecution runningExecution(ClaimedTask claim) {
        DurableExecution activity = claim.isCompensation()
                ? compensations.findById(claim.compensationExecutionId()).orElseThrow()
                : activities.findById(claim.activityExecutionId()).orElseThrow();
        if (activity instanceof CompensationExecutionEntity c
                && !c.getOriginalActivityExecutionId().equals(claim.activityExecutionId())) throw new OwnershipLostException(claim);
        if (!activity.getWorkflowRunId().equals(claim.runId()) || activity.getStatus() != ActivityExecutionStatus.RUNNING) {
            throw new ApiException(HttpStatus.CONFLICT, "Activity does not match the claimed task");
        }
        return activity;
    }

    private void allowOutcome(WorkflowRunEntity run, ClaimedTask claim) {
        if (claim.isCompensation()) {
            if (run.getStatus() != WorkflowRunStatus.COMPENSATING) throw new OwnershipLostException(claim);
            return;
        }
        // Accept a fenced late sibling outcome while the Saga waits for started forward work.
        // PENDING forward tasks remain excluded; FAILED non-Saga runs retain their prior behavior.
        if (run.getStatus() != WorkflowRunStatus.RUNNING && run.getStatus() != WorkflowRunStatus.FAILED
                && run.getStatus() != WorkflowRunStatus.COMPENSATING) {
            throw new ApiException(HttpStatus.CONFLICT, "Workflow no longer accepts task outcomes");
        }
    }
}
