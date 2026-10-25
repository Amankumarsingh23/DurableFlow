package io.durableflow.workflow.activity;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "activity_execution")
public class ActivityExecutionEntity implements DurableExecution {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "workflow_run_id", nullable = false)
    private UUID workflowRunId;

    @Column(name = "activity_id", nullable = false, length = 200)
    private String activityId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 50)
    private ActivityExecutionStatus status;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "input_json", nullable = true, columnDefinition = "jsonb")
    private Map<String, Object> inputJson;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "output_json", nullable = true, columnDefinition = "jsonb")
    private Map<String, Object> outputJson;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "started_at", nullable = true)
    private Instant startedAt;

    @Column(name = "completed_at", nullable = true)
    private Instant completedAt;

    @Column(name = "error_message", columnDefinition = "text")
    private String errorMessage;

    public ActivityExecutionEntity() {}

    public String getErrorMessage() { return errorMessage; }

    public void markRunning(Map<String, Object> input, Instant now) {
        requireStatus(ActivityExecutionStatus.PENDING);
        status = ActivityExecutionStatus.RUNNING;
        inputJson = input;
        startedAt = now;
        updatedAt = now;
        attemptCount++;
    }

    public void markCompleted(Map<String, Object> output, Instant now) {
        requireStatus(ActivityExecutionStatus.RUNNING);
        outputJson = java.util.Objects.requireNonNull(output, "Activity output must be a JSON object");
        status = ActivityExecutionStatus.COMPLETED;
        updatedAt = now;
        completedAt = now;
    }

    /** A reclaimed or scheduled execution is not a RUNNING -> PENDING lifecycle reset. */
    public void startNextAttempt(Instant databaseNow) {
        requireStatus(ActivityExecutionStatus.RUNNING);
        attemptCount = Math.incrementExact(attemptCount);
        updatedAt = databaseNow;
    }

    public void markFailed(String message, Instant now) {
        requireStatus(ActivityExecutionStatus.RUNNING);
        errorMessage = java.util.Objects.requireNonNull(message);
        status = ActivityExecutionStatus.FAILED;
        updatedAt = now;
        completedAt = now;
    }

    public void recordRetryFailure(String message, Instant databaseNow) {
        requireStatus(ActivityExecutionStatus.RUNNING);
        errorMessage = java.util.Objects.requireNonNull(message);
        updatedAt = databaseNow;
    }

    private void requireStatus(ActivityExecutionStatus expected) {
        if (status != expected) throw new IllegalStateException("Activity must be " + expected + ", was " + status);
    }

    public UUID getId() { return id; }
    public UUID getWorkflowRunId() { return workflowRunId; }
    public void setWorkflowRunId(UUID workflowRunId) { this.workflowRunId = workflowRunId; }
    public String getActivityId() { return activityId; }
    public void setActivityId(String activityId) { this.activityId = activityId; }
    public ActivityExecutionStatus getStatus() { return status; }
    public void setStatus(ActivityExecutionStatus status) { this.status = status; }
    public Map<String, Object> getInputJson() { return inputJson; }
    public void setInputJson(Map<String, Object> inputJson) { this.inputJson = inputJson; }
    public Map<String, Object> getOutputJson() { return outputJson; }
    public void setOutputJson(Map<String, Object> outputJson) { this.outputJson = outputJson; }
    public int getAttemptCount() { return attemptCount; }
    public void setAttemptCount(int attemptCount) { this.attemptCount = attemptCount; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }
    public Instant getCompletedAt() { return completedAt; }
    public void setCompletedAt(Instant completedAt) { this.completedAt = completedAt; }
}
