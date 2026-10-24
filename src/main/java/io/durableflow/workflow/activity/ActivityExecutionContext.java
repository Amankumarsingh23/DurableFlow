package io.durableflow.workflow.activity;

import java.util.Objects;
import java.util.UUID;

/** Logical operation identity only; lease ownership remains a worker-infrastructure concern. */
public record ActivityExecutionContext(UUID workflowRunId, UUID activityExecutionId) {
    public ActivityExecutionContext {
        Objects.requireNonNull(workflowRunId, "workflowRunId");
        Objects.requireNonNull(activityExecutionId, "activityExecutionId");
    }

    public String idempotencyKey() { return "df:v1:" + activityExecutionId; }
}
