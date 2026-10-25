package io.durableflow.worker;

import java.util.Map;
import java.util.UUID;
import java.time.Instant;
import io.durableflow.workflow.activity.ActivityExecutionContext;

/** Detached claim; activityExecutionId always identifies the original forward execution. */
public record ClaimedTask(UUID taskId, UUID runId, UUID activityExecutionId, String activityType,
        Map<String, Object> input, String workerId, long leaseGeneration, Instant leaseExpiresAt,
        UUID compensationExecutionId) {
    public ClaimedTask(UUID taskId, UUID runId, UUID activityExecutionId, String activityType,
            Map<String, Object> input, String workerId, long leaseGeneration, Instant leaseExpiresAt) {
        this(taskId, runId, activityExecutionId, activityType, input, workerId, leaseGeneration, leaseExpiresAt, null);
    }
    public boolean isCompensation() { return compensationExecutionId != null; }
    @SuppressWarnings("unchecked")
    public ActivityExecutionContext context() {
        return isCompensation() ? new ActivityExecutionContext(runId, activityExecutionId, true,
                (Map<String, Object>) input.get("originalInput"), (Map<String, Object>) input.get("originalOutput"))
                : new ActivityExecutionContext(runId, activityExecutionId);
    }
}
