package io.durableflow.workflow.run;

import io.durableflow.workflow.activity.ActivityExecutionStatus;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import io.durableflow.workflow.timer.WorkflowTimer;

public record WorkflowRunResponse(
        UUID runId, String workflowName, int version, WorkflowRunStatus status,
        Map<String, Object> input, List<ActivityState> activities, List<WorkflowTimer> timers) {
    public WorkflowRunResponse(UUID runId, String workflowName, int version, WorkflowRunStatus status,
                               Map<String, Object> input, List<ActivityState> activities) {
        this(runId, workflowName, version, status, input, activities, List.of());
    }
    public record ActivityState(
            String activityId, ActivityExecutionStatus status, Map<String, Object> output, String errorMessage) {}
}
