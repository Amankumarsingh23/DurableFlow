package io.durableflow.workflow.task;

import io.durableflow.api.ApiException;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.engine.OrphanedActivityException;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.history.*;
import io.durableflow.workflow.timer.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import java.time.Clock;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Authorizes durable work only. No handler invocation or task-row write locks under the run lock. */
@Service
public class WorkflowTaskOrchestrator {
    private final WorkflowRunRepository runs;
    private final ActivityExecutionRepository activities;
    private final WorkflowDefinitionRepository definitions;
    private final WorkflowDefinitionValidator validator;
    private final ActivityTaskRepository tasks;
    private final WorkflowRunService runService;
    private final Clock clock;
    private final WorkflowHistoryWriter history;
    private final WorkflowTimerRepository timers;

    public WorkflowTaskOrchestrator(WorkflowRunRepository runs, ActivityExecutionRepository activities,
            WorkflowDefinitionRepository definitions, WorkflowDefinitionValidator validator,
            ActivityTaskRepository tasks, WorkflowRunService runService, Clock clock, WorkflowHistoryWriter history,
            WorkflowTimerRepository timers) {
        this.runs = runs; this.activities = activities; this.definitions = definitions;
        this.validator = validator; this.tasks = tasks; this.runService = runService; this.clock = clock;
        this.history = history;
        this.timers = timers;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public WorkflowRunResponse advanceRun(UUID runId) {
        return advanceInTransaction(runId);
    }

    /** Timer firing and downstream materialization share one commit; no crash gap after FIRED. */
    @Transactional(propagation = Propagation.MANDATORY)
    public WorkflowRunResponse advanceInTransaction(UUID runId) {
        var run = runs.findByIdForUpdate(runId)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Workflow run not found"));
        if (run.getStatus() != WorkflowRunStatus.RUNNING) return runService.get(runId);
        var rows = activities.findByWorkflowRunIdOrderByActivityIdAsc(runId);
        var queued = tasks.findByRunId(runId).stream()
                .collect(Collectors.toMap(ActivityTaskEntity::getActivityExecutionId, Function.identity()));
        var uncertain = rows.stream().filter(a -> a.getStatus() == ActivityExecutionStatus.RUNNING)
                .filter(a -> !queued.containsKey(a.getId())
                        || (queued.get(a.getId()).getStatus() != ActivityTaskStatus.CLAIMED
                            && !(queued.get(a.getId()).getStatus() == ActivityTaskStatus.READY
                                && queued.get(a.getId()).getLeaseGeneration() > 0)))
                .map(ActivityExecutionEntity::getActivityId).toList();
        if (!uncertain.isEmpty()) throw new OrphanedActivityException(runId, uncertain);
        var definition = definitions.findById(run.getWorkflowDefinitionId()).orElseThrow();
        var steps = validator.normalizeAndValidate(new RegisterWorkflowDefinitionRequest(definition.getName(),
                definition.getVersion(), WorkflowDefinitionDocument.steps(definition.getDefinitionJson()))).steps();
        var byId = rows.stream().collect(Collectors.toMap(ActivityExecutionEntity::getActivityId, Function.identity()));
        if (!byId.keySet().equals(steps.stream().filter(step -> step.type() == NodeType.ACTIVITY)
                .map(StepDefinition::id).collect(Collectors.toSet()))) {
            throw new ApiException(HttpStatus.CONFLICT, "Activity rows do not match the workflow definition");
        }
        var timerRows = timers.findByRun(runId).stream().collect(Collectors.toMap(WorkflowTimer::timerId, Function.identity()));
        var satisfaction = new NodeSatisfaction(steps, byId, timerRows);
        if (rows.stream().anyMatch(a -> a.getStatus() == ActivityExecutionStatus.FAILED)) {
            run.markFailed(clock.instant());
            history.append(runId, null, WorkflowEventType.WORKFLOW_FAILED, Map.of("code", "ACTIVITY_FAILURE"));
        } else if (satisfaction.allSatisfied()) {
            run.markCompleted(clock.instant());
            history.append(runId, null, WorkflowEventType.WORKFLOW_COMPLETED, Map.of());
        } else {
            for (var step : steps) {
                if (step.type() == NodeType.TIMER) {
                    if (!timerRows.containsKey(step.id()) && satisfaction.dependenciesSatisfied(step)) {
                        var timer = timers.schedule(runId, step.id(), step.delayMillis());
                        timerRows.put(step.id(), timer);
                        history.append(runId, null, WorkflowEventType.TIMER_SCHEDULED,
                                Map.of("timerId", timer.timerId(), "delayMillis", step.delayMillis(), "wakeAt", timer.wakeAt().toString()));
                    }
                    continue;
                }
                var row = byId.get(step.id());
                if (row.getStatus() == ActivityExecutionStatus.PENDING && satisfaction.dependenciesSatisfied(step)) {
                    var existing = queued.get(row.getId());
                    if (existing == null) {
                        var task = tasks.save(ActivityTaskEntity.ready(row.getId(), clock.instant()));
                        history.append(runId, row.getId(), WorkflowEventType.ACTIVITY_SCHEDULED, Map.of("taskId", task.getId().toString()));
                    }
                    else if (existing.getStatus() != ActivityTaskStatus.READY) {
                        throw new ApiException(HttpStatus.CONFLICT, "Queued task does not match activity state");
                    }
                }
            }
        }
        return runService.get(runId);
    }
}
