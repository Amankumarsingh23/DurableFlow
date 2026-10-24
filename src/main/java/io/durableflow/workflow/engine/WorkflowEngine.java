package io.durableflow.workflow.engine;

import io.durableflow.workflow.run.WorkflowRunResponse;
import io.durableflow.workflow.task.WorkflowTaskOrchestrator;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import java.util.UUID;

/** Normal orchestration materializes tasks; handler execution belongs to TaskWorker. */
@Service
@Profile("!worker")
public class WorkflowEngine {
    private final WorkflowTaskOrchestrator orchestrator;
    private final WorkflowLocalExecutor developmentExecutor;

    public WorkflowEngine(WorkflowTaskOrchestrator orchestrator, WorkflowLocalExecutor developmentExecutor) {
        this.orchestrator = orchestrator;
        this.developmentExecutor = developmentExecutor;
    }

    public WorkflowRunResponse advanceRun(UUID runId) { return orchestrator.advanceRun(runId); }

    /** Compatibility scaffolding for Sessions 7-9; rejects any run with queued tasks. */
    public WorkflowRunResponse executeRun(UUID runId) { return developmentExecutor.executeRun(runId); }
    public WorkflowRunResponse executeRun(UUID runId, int maxActivities) {
        return developmentExecutor.executeRun(runId, maxActivities);
    }
}
