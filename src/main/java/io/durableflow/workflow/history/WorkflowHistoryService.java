package io.durableflow.workflow.history;

import io.durableflow.api.ApiException;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.run.WorkflowRunRepository;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class WorkflowHistoryService {
    private final WorkflowRunRepository runs;
    private final ActivityExecutionRepository activities;
    private final WorkflowEventRepository events;
    public WorkflowHistoryService(WorkflowRunRepository runs, ActivityExecutionRepository activities, WorkflowEventRepository events) {
        this.runs = runs; this.activities = activities; this.events = events;
    }
    @Transactional(readOnly = true)
    public List<WorkflowEventResponse> read(UUID runId) {
        if (!runs.existsById(runId)) throw new ApiException(HttpStatus.NOT_FOUND, "Workflow run not found");
        var ids = activities.findByWorkflowRunIdOrderByActivityIdAsc(runId).stream()
                .collect(Collectors.toMap(ActivityExecutionEntity::getId, ActivityExecutionEntity::getActivityId));
        return events.findByWorkflowRunIdOrderBySequenceNumberAsc(runId).stream().map(event -> new WorkflowEventResponse(
                event.getId(), event.getSequenceNumber(), event.getEventType(), event.getCreatedAt(),
                event.getActivityExecutionId(), event.getActivityExecutionId() == null ? null : ids.get(event.getActivityExecutionId()),
                event.getEventData())).toList();
    }
}
