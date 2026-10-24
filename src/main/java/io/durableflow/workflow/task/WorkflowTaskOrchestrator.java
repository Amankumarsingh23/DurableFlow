package io.durableflow.workflow.task;

import io.durableflow.api.ApiException;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.engine.OrphanedActivityException;
import io.durableflow.workflow.run.*;
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

    public WorkflowTaskOrchestrator(WorkflowRunRepository runs, ActivityExecutionRepository activities,
            WorkflowDefinitionRepository definitions, WorkflowDefinitionValidator validator,
            ActivityTaskRepository tasks, WorkflowRunService runService, Clock clock) {
        this.runs = runs; this.activities = activities; this.definitions = definitions;
        this.validator = validator; this.tasks = tasks; this.runService = runService; this.clock = clock;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public WorkflowRunResponse advanceRun(UUID runId) {
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
        if (!byId.keySet().equals(steps.stream().map(StepDefinition::id).collect(Collectors.toSet()))) {
            throw new ApiException(HttpStatus.CONFLICT, "Activity rows do not match the workflow definition");
        }
        if (rows.stream().anyMatch(a -> a.getStatus() == ActivityExecutionStatus.FAILED)) {
            run.markFailed(clock.instant());
        } else if (rows.stream().allMatch(a -> a.getStatus() == ActivityExecutionStatus.COMPLETED)) {
            run.markCompleted(clock.instant());
        } else {
            for (var step : steps) {
                var row = byId.get(step.id());
                if (row.getStatus() == ActivityExecutionStatus.PENDING && step.dependsOn().stream()
                        .allMatch(id -> byId.get(id).getStatus() == ActivityExecutionStatus.COMPLETED)) {
                    var existing = queued.get(row.getId());
                    if (existing == null) tasks.save(ActivityTaskEntity.ready(row.getId(), clock.instant()));
                    else if (existing.getStatus() != ActivityTaskStatus.READY) {
                        throw new ApiException(HttpStatus.CONFLICT, "Queued task does not match activity state");
                    }
                }
            }
        }
        return runService.get(runId);
    }
}
