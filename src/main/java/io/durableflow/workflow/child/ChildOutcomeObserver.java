package io.durableflow.workflow.child;

import io.durableflow.workflow.history.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import java.util.*;

/** Caller holds parent run lock, including terminal parents; observing a child never reopens one. */
@Service
@Transactional(propagation = Propagation.MANDATORY)
public class ChildOutcomeObserver {
    private final ChildWorkflowRepository children;
    private final WorkflowHistoryWriter history;
    public ChildOutcomeObserver(ChildWorkflowRepository children, WorkflowHistoryWriter history) { this.children = children; this.history = history; }
    public List<ChildWorkflow> refresh(UUID parent) {
        var result = children.findByParent(parent);
        for (var child : result) if (child.terminal() && child.terminalStatus() == null) {
            children.observe(child);
            history.append(parent, null, child.failed() ? WorkflowEventType.CHILD_WORKFLOW_FAILED : WorkflowEventType.CHILD_WORKFLOW_COMPLETED,
                    Map.of("childNodeId", child.childNodeId(), "childRunId", child.childRunId().toString(), "status", child.status().name()));
        }
        return result;
    }
}
