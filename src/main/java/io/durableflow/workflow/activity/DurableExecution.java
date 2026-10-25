package io.durableflow.workflow.activity;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Logical execution lifecycle shared by forward work and compensation, independent of queue ownership. */
public interface DurableExecution {
    UUID getId();
    UUID getWorkflowRunId();
    ActivityExecutionStatus getStatus();
    int getAttemptCount();
    Map<String, Object> getInputJson();
    void markRunning(Map<String, Object> input, Instant now);
    void startNextAttempt(Instant now);
    void markCompleted(Map<String, Object> output, Instant now);
    void markFailed(String message, Instant now);
    void recordRetryFailure(String message, Instant now);
}
