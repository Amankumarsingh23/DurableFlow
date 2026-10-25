package io.durableflow.workflow.child;

import io.durableflow.workflow.definition.*;
import io.durableflow.api.ApiException;
import org.springframework.http.HttpStatus;
import io.durableflow.workflow.compensation.SagaCoordinator;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.history.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import java.util.*;

/** Parent run lock is held; no child run lock is acquired under it. */
@Service
@Transactional(propagation = Propagation.MANDATORY)
public class ChildCoordinator {
    private final ChildWorkflowRepository children;
    private final WorkflowRunService runs;
    private final WorkflowDefinitionRepository definitions;
    private final WorkflowHistoryWriter history;
    private final ChildSettings settings;
    private final SagaCoordinator sagas;
    public ChildCoordinator(ChildWorkflowRepository children, WorkflowRunService runs,
            WorkflowDefinitionRepository definitions, WorkflowHistoryWriter history, ChildSettings settings,
            SagaCoordinator sagas) {
        this.children = children; this.runs = runs; this.definitions = definitions; this.history = history; this.settings = settings; this.sagas = sagas;
    }
    public boolean materialize(WorkflowRunEntity parent, StepDefinition step) {
        if (parent.getChildDepth() >= settings.maxDepth()) {
            history.append(parent.getId(), null, WorkflowEventType.CHILD_WORKFLOW_FAILED,
                    Map.of("childNodeId", step.id(), "code", "CHILD_DEPTH_EXCEEDED"));
            sagas.forwardFailed(parent, "CHILD_DEPTH_EXCEEDED");
            return false;
        }
        var definition = definitions.findByNameAndVersion(step.childWorkflow(), step.childVersion())
                .orElseThrow(() -> new ApiException(HttpStatus.CONFLICT,
                        "Pinned child workflow definition is not registered"));
        var input = step.childInput() == null ? parent.getInputJson() : step.childInput();
        var child = runs.startAtDepth(new StartWorkflowRunRequest(definition.getName(), definition.getVersion(), input), parent.getChildDepth() + 1);
        children.insert(parent.getId(), step.id(), child.runId(), definition.getId());
        history.append(parent.getId(), null, WorkflowEventType.CHILD_WORKFLOW_STARTED,
                Map.of("childNodeId", step.id(), "childRunId", child.runId().toString(), "childDefinitionId", definition.getId().toString(),
                        "childVersion", definition.getVersion()));
        return true;
    }
}
