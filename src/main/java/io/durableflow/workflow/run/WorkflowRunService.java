package io.durableflow.workflow.run;

import io.durableflow.api.ApiException;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.history.*;
import io.durableflow.workflow.timer.WorkflowTimerRepository;
import io.durableflow.workflow.signal.SignalRepository;
import jakarta.validation.Validator;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.time.Clock;
import java.util.UUID;

@Service
public class WorkflowRunService {
    private final WorkflowDefinitionService definitions;
    private final WorkflowDefinitionRepository definitionRepository;
    private final WorkflowRunRepository runs;
    private final ActivityExecutionRepository activities;
    private final Validator validator;
    private final Clock clock;
    private final WorkflowHistoryWriter history;
    private final WorkflowTimerRepository timers;
    private final SignalRepository signals;

    public WorkflowRunService(WorkflowDefinitionService definitions, WorkflowDefinitionRepository definitionRepository,
                              WorkflowRunRepository runs, ActivityExecutionRepository activities, Validator validator, Clock clock,
                              WorkflowHistoryWriter history, WorkflowTimerRepository timers, SignalRepository signals) {
        this.definitions = definitions;
        this.definitionRepository = definitionRepository;
        this.runs = runs;
        this.activities = activities;
        this.validator = validator;
        this.clock = clock;
        this.history = history;
        this.timers = timers;
        this.signals = signals;
    }

    @Transactional
    public WorkflowRunResponse start(StartWorkflowRunRequest request) {
        if (request == null || !validator.validate(request).isEmpty()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "Workflow name, positive version and input object are required");
        }
        var definition = definitions.get(request.workflowName().strip(), request.version());
        Instant now = clock.instant();
        var run = new WorkflowRunEntity();
        run.setWorkflowDefinitionId(definition.id());
        run.setStatus(WorkflowRunStatus.RUNNING);
        run.setInputJson(request.input());
        run.setCreatedAt(now);
        run.setUpdatedAt(now);
        run.setStartedAt(now);
        runs.saveAndFlush(run);

        var executions = definition.steps().stream().filter(step -> step.type() == NodeType.ACTIVITY).map(step -> {
            var execution = new ActivityExecutionEntity();
            execution.setWorkflowRunId(run.getId());
            execution.setActivityId(step.id());
            execution.setStatus(ActivityExecutionStatus.PENDING);
            execution.setAttemptCount(0);
            execution.setCreatedAt(now);
            execution.setUpdatedAt(now);
            return execution;
        }).toList();
        // Every insert, including the run above, belongs to this transaction.
        activities.saveAllAndFlush(executions);
        history.append(run.getId(), null, WorkflowEventType.WORKFLOW_STARTED, java.util.Map.of());
        return new WorkflowRunResponse(run.getId(), definition.name(), definition.version(), run.getStatus(),
                run.getInputJson(), executions.stream()
                        .map(e -> new WorkflowRunResponse.ActivityState(e.getActivityId(), e.getStatus(),
                                e.getOutputJson(), e.getErrorMessage())).toList());
    }

    @Transactional(readOnly = true)
    public WorkflowRunResponse get(UUID runId) {
        var run = runs.findById(runId)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Workflow run not found"));
        var definition = definitionRepository.findById(run.getWorkflowDefinitionId()).orElseThrow();
        var states = activities.findByWorkflowRunIdOrderByActivityIdAsc(runId).stream()
                .map(e -> new WorkflowRunResponse.ActivityState(e.getActivityId(), e.getStatus(),
                        e.getOutputJson(), e.getErrorMessage())).toList();
        return new WorkflowRunResponse(run.getId(), definition.getName(), definition.getVersion(),
                run.getStatus(), run.getInputJson(), states, timers.findByRun(runId), signals.waits(runId));
    }
}
