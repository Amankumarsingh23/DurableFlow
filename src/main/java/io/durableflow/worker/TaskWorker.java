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
import java.util.concurrent.TimeUnit;

@Service
public class TaskWorker {
    private static final Logger LOG = LoggerFactory.getLogger(TaskWorker.class);
    private final TaskTransactions transactions;
    private final ActivityRegistry registry;
    private final WorkflowTaskOrchestrator orchestrator;
    private final WorkerIdentity identity;
    private final WorkerHeartbeats heartbeats;
    private final HandlerExecutions executions;
    private final WorkerExecutionSettings settings;
    private final io.durableflow.observability.SafeTelemetry telemetry;
    private final Object lifecycle = new Object();
    private boolean draining;
    private int active;
    private volatile boolean abandoning;

    public TaskWorker(TaskTransactions transactions, ActivityRegistry registry,
                      WorkflowTaskOrchestrator orchestrator, WorkerIdentity identity, WorkerHeartbeats heartbeats,
                      HandlerExecutions executions, WorkerExecutionSettings settings) {
        this(transactions,registry,orchestrator,identity,heartbeats,executions,settings,io.durableflow.observability.SafeTelemetry.disabled());
    }
    @org.springframework.beans.factory.annotation.Autowired
    public TaskWorker(TaskTransactions transactions,ActivityRegistry registry,WorkflowTaskOrchestrator orchestrator,
            WorkerIdentity identity,WorkerHeartbeats heartbeats,HandlerExecutions executions,WorkerExecutionSettings settings,
            io.durableflow.observability.SafeTelemetry telemetry) {
        this.telemetry=telemetry;
        this.transactions = transactions; this.registry = registry;
        this.orchestrator = orchestrator; this.identity = identity;
        this.heartbeats = heartbeats;
        this.executions = executions; this.settings = settings;
    }

    @Transactional(propagation = Propagation.NEVER)
    public boolean runOne() {
        ClaimedTask claim;
        WorkerHeartbeats.Session session;
        HandlerExecutions.Ticket ticket;
        synchronized (lifecycle) {
            if (draining) return false;
            var reserved = executions.reserve();
            if (reserved.isEmpty()) return false;
            ticket = reserved.get();
            try {
                var selected = transactions.claim(identity.id());
                if (selected.isEmpty()) { ticket.close(); return false; }
                claim = selected.get();
                session = heartbeats.start(claim);
                active++;
            } catch (RuntimeException failed) { ticket.close(); throw failed; }
        }
        LOG.info("Worker {} executing task {} ({}) for run {}", identity.id(), claim.taskId(), claim.activityType(), claim.runId());
        Map<String, Object> output = null;
        Exception handlerFailure = null;
        try {
            try {
                session.ensureOwned();
                output = ticket.execute(() -> {
                    long started=System.nanoTime();String status="FAILURE";
                    try {
                        var result=Objects.requireNonNull(registry.require(claim.activityType()).execute(claim.context(),claim.input()),"Activity output must be a JSON object");
                        status="SUCCESS";return result;
                    } finally {
                        var tags=Map.of("activity",claim.activityType(),"status",status);
                        telemetry.duration(io.durableflow.observability.Metric.ACTIVITY_ATTEMPT_DURATION,System.nanoTime()-started,tags);
                    }
                });
            } catch (HandlerExecutions.Abandoned cancelled) {
                throw new OwnershipLostException(claim);
            } catch (Exception failure) {
                handlerFailure = failure;
            } finally {
                session.close(); // Stop renewal after result/timeout/abandonment, even if external work continues.
            }
            session.ensureOwned();
            if (abandoning) throw new OwnershipLostException(claim);
            if (handlerFailure != null) {
                transactions.handleFailure(claim, handlerFailure);
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
            ticket.close();
            synchronized (lifecycle) { active--; lifecycle.notifyAll(); }
        }
    }

    @EventListener(ContextClosedEvent.class)
    public void contextClosed() { stop(); }

    @PreDestroy
    public void stop() {
        synchronized (lifecycle) {
            if (draining) return;
            draining = true; // Claim gate closes before any drain or executor teardown.
            long remaining = TimeUnit.MILLISECONDS.toNanos(settings.shutdownGraceMillis());
            long last = System.nanoTime();
            while (active > 0 && remaining > 0) {
                try { TimeUnit.NANOSECONDS.timedWait(lifecycle, remaining); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); break; }
                long now = System.nanoTime();
                remaining -= now - last;
                last = now;
            }
            abandoning = true;
        }
        executions.close();
        heartbeats.close();
    }

    // Package-local lifecycle observations for deterministic shutdown tests.
    boolean isDraining() { synchronized (lifecycle) { return draining; } }
}
