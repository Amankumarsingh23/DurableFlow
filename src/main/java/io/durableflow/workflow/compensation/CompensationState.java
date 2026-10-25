package io.durableflow.workflow.compensation;

import io.durableflow.workflow.activity.ActivityExecutionStatus;
import java.util.*;

/** Current compensation state; original input/output snapshots remain internal to handlers. */
public record CompensationState(UUID id, UUID originalActivityExecutionId, String handler,
        ActivityExecutionStatus status, int attemptCount, Map<String, Object> output, String errorMessage) {
    public static CompensationState from(CompensationExecutionEntity execution) {
        return new CompensationState(execution.getId(), execution.getOriginalActivityExecutionId(), execution.getHandler(),
                execution.getStatus(), execution.getAttemptCount(), execution.getOutputJson(), execution.getErrorMessage());
    }
}
