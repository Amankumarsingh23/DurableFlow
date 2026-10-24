package io.durableflow.workflow.signal;

import java.time.Instant;
import java.util.UUID;

/** Lightweight current-state DTO. Stored payload is deliberately not echoed in routine run inspection. */
public record SignalWait(UUID id, UUID workflowRunId, String signalNodeId, Status status,
                         Instant createdAt, Instant receivedAt) {
    public enum Status { WAITING, RECEIVED }
}
