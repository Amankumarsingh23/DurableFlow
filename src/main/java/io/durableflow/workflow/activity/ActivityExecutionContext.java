package io.durableflow.workflow.activity;

import java.util.*;

/** Compensation identity refers to the ORIGINAL execution, never a lease attempt. */
public record ActivityExecutionContext(UUID workflowRunId, UUID activityExecutionId,
        boolean compensation, Map<String, Object> originalInput, Map<String, Object> originalOutput) {
    public ActivityExecutionContext {
        Objects.requireNonNull(workflowRunId, "workflowRunId");
        Objects.requireNonNull(activityExecutionId, "activityExecutionId");
        originalInput = Collections.unmodifiableMap(new HashMap<>(originalInput));
        originalOutput = Collections.unmodifiableMap(new HashMap<>(originalOutput));
    }
    public ActivityExecutionContext(UUID workflowRunId, UUID activityExecutionId) {
        this(workflowRunId, activityExecutionId, false, Map.of(), Map.of());
    }
    public String idempotencyKey() {
        return "df:v1:" + (compensation ? "compensate:" : "") + activityExecutionId;
    }
}
