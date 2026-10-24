package io.durableflow.workflow.history;

import jakarta.persistence.*;
import org.hibernate.annotations.Immutable;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.Instant;
import java.util.*;

/** Immutable audit rows; no setters or domain update/delete API. */
@Entity @Immutable
@Table(name = "workflow_event")
public class WorkflowEventEntity {
    @Id private UUID id;
    @Column(name = "workflow_run_id", nullable = false) private UUID workflowRunId;
    @Column(name = "activity_execution_id") private UUID activityExecutionId;
    @Column(name = "sequence_number", nullable = false) private long sequenceNumber;
    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false, length = 60) private WorkflowEventType eventType;
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "event_data", nullable = false, columnDefinition = "jsonb") private Map<String, Object> eventData;
    @Column(name = "created_at", nullable = false) private Instant createdAt;

    protected WorkflowEventEntity() {}
    WorkflowEventEntity(UUID run, UUID activity, long sequence, WorkflowEventType type, Map<String, Object> data, Instant now) {
        id = UUID.randomUUID(); workflowRunId = run; activityExecutionId = activity;
        sequenceNumber = sequence; eventType = type; eventData = Map.copyOf(data); createdAt = now;
    }
    public UUID getId() { return id; }
    public UUID getActivityExecutionId() { return activityExecutionId; }
    public long getSequenceNumber() { return sequenceNumber; }
    public WorkflowEventType getEventType() { return eventType; }
    public Map<String, Object> getEventData() { return Map.copyOf(eventData); }
    public Instant getCreatedAt() { return createdAt; }
}
