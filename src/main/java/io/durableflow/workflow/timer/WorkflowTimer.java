package io.durableflow.workflow.timer;

import java.time.Instant;
import java.util.UUID;

public record WorkflowTimer(UUID id, UUID workflowRunId, String timerId, Status status,
                            Instant wakeAt, Instant createdAt, Instant firedAt) {
    public enum Status { WAITING, FIRED }
}
