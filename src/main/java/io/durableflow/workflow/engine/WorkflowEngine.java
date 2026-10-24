package io.durableflow.workflow.engine;

import io.durableflow.api.ApiException;
import io.durableflow.workflow.activity.ActivityRegistry;
import io.durableflow.workflow.run.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class WorkflowEngine {
    private static final Logger LOG = LoggerFactory.getLogger(WorkflowEngine.class);
    private final WorkflowEngineState state;
    private final ActivityRegistry registry;
    private final WorkflowRunService runs;
    // Fast rejection for this object only. Database coordination is in WorkflowEngineState, not this set.
    private final Set<UUID> executing = ConcurrentHashMap.newKeySet();

    public WorkflowEngine(WorkflowEngineState state, ActivityRegistry registry, WorkflowRunService runs) {
        this.state = state;
        this.registry = registry;
        this.runs = runs;
    }

    @Transactional(propagation = Propagation.NEVER)
    public WorkflowRunResponse executeRun(UUID runId) {
        return executeRun(runId, Integer.MAX_VALUE);
    }

    /** Temporary dev/test facility. The budget counts successful activities in this invocation only. */
    @Transactional(propagation = Propagation.NEVER)
    public WorkflowRunResponse executeRun(UUID runId, int maxActivities) {
        if (maxActivities < 1) throw new ApiException(HttpStatus.BAD_REQUEST, "maxActivities must be positive");
        if (!executing.add(runId)) throw new ApiException(HttpStatus.CONFLICT, "Run is already executing in this process");
        try {
            int completed = 0;
            while (true) {
                var decision = state.next(runId, completed < maxActivities);
                if (decision.error() != null) throw new ApiException(HttpStatus.CONFLICT, decision.error());
                var work = decision.work();
                if (work == null) return runs.get(runId);
                Map<String, Object> output;
                try {
                    // RUNNING has committed; arbitrary handler code executes without a DB transaction.
                    LOG.info("Executing activity {} for workflow run {}", work.activityType(), runId);
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
                completed++;
                // Each successful iteration commits another COMPLETED activity, or next() terminates.
            }
        } finally {
            executing.remove(runId);
        }
    }
}
