package io.durableflow.worker;

import io.durableflow.workflow.activity.ActivityRegistry;
import io.durableflow.workflow.task.WorkflowTaskOrchestrator;
import org.slf4j.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import jakarta.annotation.PreDestroy;
import java.util.*;

@Service
public class TaskWorker {
    private static final Logger LOG = LoggerFactory.getLogger(TaskWorker.class);
    private final TaskTransactions transactions;
    private final ActivityRegistry registry;
    private final WorkflowTaskOrchestrator orchestrator;
    private final WorkerIdentity identity;
    private final WorkerHeartbeats heartbeats;
    private final Object lifecycle = new Object();
    private volatile boolean stopping;

    public TaskWorker(TaskTransactions transactions, ActivityRegistry registry,
                      WorkflowTaskOrchestrator orchestrator, WorkerIdentity identity, WorkerHeartbeats heartbeats) {
        this.transactions = transactions; this.registry = registry;
        this.orchestrator = orchestrator; this.identity = identity;
        this.heartbeats = heartbeats;
    }

    @Transactional(propagation = Propagation.NEVER)
    public boolean runOne() {
        ClaimedTask claim;
        WorkerHeartbeats.Session session;
        synchronized (lifecycle) {
            if (stopping) return false;
            var selected = transactions.claim(identity.id());
            if (selected.isEmpty()) return false;
            claim = selected.get();
            session = heartbeats.start(claim);
        }
        LOG.info("Worker {} executing task {} ({}) for run {}", identity.id(), claim.taskId(), claim.activityType(), claim.runId());
        Map<String, Object> output = null;
        Exception handlerFailure = null;
        try {
            try {
                session.ensureOwned();
                output = Objects.requireNonNull(registry.require(claim.activityType()).execute(claim.input()),
                        "Activity output must be a JSON object");
            } catch (Exception failure) {
                handlerFailure = failure;
            } finally {
                session.close(); // No renewal after handler termination; final write still verifies DB ownership.
            }
            session.ensureOwned();
            if (stopping) throw new OwnershipLostException(claim);
            if (handlerFailure != null) {
                String message = handlerFailure.getClass().getSimpleName() + ": "
                        + Objects.toString(handlerFailure.getMessage(), "Activity execution failed");
                transactions.fail(claim, message.substring(0, Math.min(message.length(), 2000)));
                return true;
            }
            // SQL failure is not a handler failure. Expired/stale ownership cannot alter logical state.
            transactions.complete(claim, output);
            orchestrator.advanceRun(claim.runId());
            return true;
        } catch (OwnershipLostException lost) {
            LOG.warn("{}; handler result discarded, task left for its valid owner/reclamation", lost.getMessage());
            throw lost;
        } finally {
            session.close();
            if (handlerFailure instanceof InterruptedException) Thread.currentThread().interrupt();
        }
    }

    @EventListener(ContextClosedEvent.class)
    public void contextClosed() { stop(); }

    @PreDestroy
    public void stop() {
        synchronized (lifecycle) { stopping = true; }
        heartbeats.close();
    }
}
