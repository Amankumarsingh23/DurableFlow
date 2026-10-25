package io.durableflow.workflow.child;

import io.durableflow.workflow.run.WorkflowRunStatus;
import java.time.Instant;
import java.util.*;

public record ChildWorkflow(UUID id, UUID parentRunId, String childNodeId, UUID childRunId,
        UUID childDefinitionId, WorkflowRunStatus status, WorkflowRunStatus terminalStatus, Instant createdAt) {
    public boolean terminal() { return Set.of(WorkflowRunStatus.COMPLETED, WorkflowRunStatus.FAILED,
            WorkflowRunStatus.COMPENSATED, WorkflowRunStatus.COMPENSATION_FAILED, WorkflowRunStatus.CANCELLED).contains(status); }
    public boolean failed() { return terminal() && status != WorkflowRunStatus.COMPLETED; }
}
