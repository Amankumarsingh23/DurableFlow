package io.durableflow.workflow.signal;

import java.time.Instant;
import java.util.UUID;

public record SignalReceipt(UUID id, UUID workflowRunId, String signalId, String signalNodeId,
                            Instant receivedAt, Instant consumedAt, Status status) {
    public enum Status { BUFFERED, CONSUMED }
    public SignalReceipt(UUID id, UUID run, String signalId, String node, Instant received, Instant consumed) {
        this(id, run, signalId, node, received, consumed, consumed == null ? Status.BUFFERED : Status.CONSUMED);
    }
}
