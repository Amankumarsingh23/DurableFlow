package io.durableflow.worker;

import io.durableflow.workflow.activity.ActivityRegistry;
import io.durableflow.workflow.task.WorkflowTaskOrchestrator;
import org.slf4j.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import java.util.*;

@Service
public class TaskWorker {
    private static final Logger LOG = LoggerFactory.getLogger(TaskWorker.class);
    private final TaskTransactions transactions;
    private final ActivityRegistry registry;
    private final WorkflowTaskOrchestrator orchestrator;
    private final WorkerIdentity identity;

    public TaskWorker(TaskTransactions transactions, ActivityRegistry registry,
                      WorkflowTaskOrchestrator orchestrator, WorkerIdentity identity) {
        this.transactions = transactions; this.registry = registry;
        this.orchestrator = orchestrator; this.identity = identity;
    }

    @Transactional(propagation = Propagation.NEVER)
    public boolean runOne() {
        var selected = transactions.claim(identity.id());
        if (selected.isEmpty()) return false;
        var claim = selected.get();
        LOG.info("Worker {} executing task {} ({}) for run {}", identity.id(), claim.taskId(), claim.activityType(), claim.runId());
        Map<String, Object> output;
        try {
            output = Objects.requireNonNull(registry.require(claim.activityType()).execute(claim.input()),
                    "Activity output must be a JSON object");
        } catch (Exception failure) {
            String message = failure.getClass().getSimpleName() + ": "
                    + Objects.toString(failure.getMessage(), "Activity execution failed");
            transactions.fail(claim, message.substring(0, Math.min(message.length(), 2000)));
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            return true;
        }
        // A database error here is not a handler failure and never triggers a duplicate invocation.
        transactions.complete(claim, output);
        // Deliberately simple follow-up, not an event bus/outbox. If this fails after completion,
        // the task stays COMPLETED and explicit /advance can resume pending work.
        orchestrator.advanceRun(claim.runId());
        return true;
    }
}
