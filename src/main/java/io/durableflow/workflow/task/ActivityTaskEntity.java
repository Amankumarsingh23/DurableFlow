package io.durableflow.workflow.task;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "activity_task")
public class ActivityTaskEntity {
    @Id
    private UUID id;
    @Column(name = "activity_execution_id", nullable = false)
    private UUID activityExecutionId;
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private ActivityTaskStatus status;
    @Column(name = "available_at", nullable = false)
    private Instant availableAt;
    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
    @Column(name = "claimed_at")
    private Instant claimedAt;
    @Column(name = "worker_id", length = 200)
    private String workerId;
    @Column(name = "lease_expires_at")
    private Instant leaseExpiresAt;
    @Column(name = "lease_generation", nullable = false)
    private long leaseGeneration;

    protected ActivityTaskEntity() {}

    public static ActivityTaskEntity ready(UUID activityExecutionId, Instant now) {
        var task = new ActivityTaskEntity();
        task.id = UUID.randomUUID();
        task.activityExecutionId = activityExecutionId;
        task.status = ActivityTaskStatus.READY;
        task.availableAt = now;
        task.createdAt = now;
        return task;
    }

    public void claimLease(String workerId, Instant databaseNow, Instant expiresAt) {
        if (status != ActivityTaskStatus.READY && (status != ActivityTaskStatus.CLAIMED
                || leaseExpiresAt == null || leaseExpiresAt.isAfter(databaseNow))) {
            throw new IllegalStateException("Task is not READY or expired CLAIMED work");
        }
        if (workerId == null || workerId.isBlank() || workerId.length() > 200) {
            throw new IllegalArgumentException("Worker ID must contain 1-200 characters");
        }
        if (!expiresAt.isAfter(databaseNow)) throw new IllegalArgumentException("Lease duration must be positive");
        this.workerId = workerId;
        claimedAt = databaseNow; // Current attempt start; heartbeats never change it.
        leaseExpiresAt = expiresAt;
        leaseGeneration = Math.incrementExact(leaseGeneration);
        status = ActivityTaskStatus.CLAIMED;
    }

    public void complete() { require(ActivityTaskStatus.CLAIMED); status = ActivityTaskStatus.COMPLETED; }
    public void fail() { require(ActivityTaskStatus.CLAIMED); status = ActivityTaskStatus.FAILED; }

    private void require(ActivityTaskStatus expected) {
        if (status != expected) throw new IllegalStateException("Task must be " + expected + ", was " + status);
    }

    public UUID getId() { return id; }
    public UUID getActivityExecutionId() { return activityExecutionId; }
    public ActivityTaskStatus getStatus() { return status; }
    public Instant getAvailableAt() { return availableAt; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getClaimedAt() { return claimedAt; }
    public String getWorkerId() { return workerId; }
    public Instant getLeaseExpiresAt() { return leaseExpiresAt; }
    public long getLeaseGeneration() { return leaseGeneration; }
}
