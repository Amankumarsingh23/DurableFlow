package io.durableflow.workflow.run;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "workflow_run")
public class WorkflowRunEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "workflow_definition_id", nullable = false)
    private UUID workflowDefinitionId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 50)
    private WorkflowRunStatus status;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "input_json", nullable = false, columnDefinition = "jsonb")
    private Map<String, Object> inputJson;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "started_at", nullable = true)
    private Instant startedAt;

    @Column(name = "completed_at", nullable = true)
    private Instant completedAt;

    @Column(name = "child_depth", nullable = false)
    private int childDepth;
    public int getChildDepth() { return childDepth; }
    public void setChildDepth(int depth) { childDepth = depth; }

    public WorkflowRunEntity() {}

    public void markCompleted(Instant now) {
        requireRunning();
        status = WorkflowRunStatus.COMPLETED;
        updatedAt = now;
        completedAt = now;
    }

    public void markFailed(Instant now) {
        requireRunning();
        status = WorkflowRunStatus.FAILED;
        updatedAt = now;
        completedAt = now;
    }

    public void startCompensating(Instant now) {
        requireRunning(); status = WorkflowRunStatus.COMPENSATING; updatedAt = now;
    }

    public void finishCompensation(boolean success, Instant now) {
        if (status != WorkflowRunStatus.COMPENSATING) throw new IllegalStateException("Workflow must be COMPENSATING");
        status = success ? WorkflowRunStatus.COMPENSATED : WorkflowRunStatus.COMPENSATION_FAILED;
        updatedAt = now; completedAt = now;
    }

    public void finishWithoutCompensation(Instant now) {
        if (status != WorkflowRunStatus.COMPENSATING) throw new IllegalStateException("Workflow must be COMPENSATING");
        status = WorkflowRunStatus.FAILED; updatedAt = now; completedAt = now;
    }

    private void requireRunning() {
        if (status != WorkflowRunStatus.RUNNING) {
            throw new IllegalStateException("Workflow run must be RUNNING, was " + status);
        }
    }

    public UUID getId() { return id; }
    public UUID getWorkflowDefinitionId() { return workflowDefinitionId; }
    public void setWorkflowDefinitionId(UUID workflowDefinitionId) { this.workflowDefinitionId = workflowDefinitionId; }
    public WorkflowRunStatus getStatus() { return status; }
    public void setStatus(WorkflowRunStatus status) { this.status = status; }
    public Map<String, Object> getInputJson() { return inputJson; }
    public void setInputJson(Map<String, Object> inputJson) { this.inputJson = inputJson; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }
    public Instant getCompletedAt() { return completedAt; }
    public void setCompletedAt(Instant completedAt) { this.completedAt = completedAt; }
}
