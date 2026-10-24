package io.durableflow.workflow.engine;

import io.durableflow.api.ApiException;
import io.durableflow.workflow.activity.ActivityRegistry;
import io.durableflow.workflow.run.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class WorkflowEngine {
    private final WorkflowEngineState state;
    private final ActivityRegistry registry;
    private final WorkflowRunService runs;
    // Guard the temporary synchronous endpoint in this JVM only. This is not a distributed claim.
    private final Set<UUID> executing = ConcurrentHashMap.newKeySet();

    public WorkflowEngine(WorkflowEngineState state, ActivityRegistry registry, WorkflowRunService runs) {
        this.state = state;
        this.registry = registry;
        this.runs = runs;
    }

    @Transactional(propagation = Propagation.NEVER)
    public WorkflowRunResponse executeRun(UUID runId) {
        if (!executing.add(runId)) throw new ApiException(HttpStatus.CONFLICT, "Run is already executing in this process");
        try {
            while (true) {
                var decision = state.next(runId);
                if (decision.error() != null) throw new ApiException(HttpStatus.CONFLICT, decision.error());
                var work = decision.work();
                if (work == null) return runs.get(runId);
                Map<String, Object> output;
                try {
                    // RUNNING has committed; arbitrary handler code executes without a DB transaction.
                    output = Objects.requireNonNull(registry.require(work.activityType()).execute(work.input()),
                            "Activity output must be a JSON object");
                } catch (Exception failure) {
                    if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
                    String detail = failure.getClass().getSimpleName() + ": "
                            + Objects.toString(failure.getMessage(), "Activity execution failed");
                    state.fail(runId, work.activityExecutionId(), detail.substring(0, Math.min(detail.length(), 2000)));
                    return runs.get(runId);
                }
                // DB failures propagate; never retry side effects or misclassify a DB failure as a handler failure.
                state.complete(runId, work.activityExecutionId(), output);
                // Each successful iteration commits another COMPLETED activity, or next() terminates.
            }
        } finally {
            executing.remove(runId);
        }
    }
}
