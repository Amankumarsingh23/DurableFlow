package io.durableflow.workflow.activity;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "activity_execution")
public class ActivityExecutionEntity {
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

    public ActivityExecutionEntity() {}

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
