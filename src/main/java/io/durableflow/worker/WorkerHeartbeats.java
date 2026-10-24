package io.durableflow.worker;

import jakarta.annotation.PreDestroy;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** One scheduler per application, cancellable session per active claim; no thread per heartbeat. */
@Component
public class WorkerHeartbeats implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerHeartbeats.class);
    private final TaskTransactions transactions;
    private final WorkerLeaseSettings settings;
    private final ScheduledExecutorService scheduler;
    private final Set<Session> active = ConcurrentHashMap.newKeySet();
    private boolean stopping;

    @Autowired
    public WorkerHeartbeats(TaskTransactions transactions, WorkerLeaseSettings settings) {
        this(transactions, settings, executor());
    }

    // Package-local constructor permits deterministic scheduler tests without sleeps.
    WorkerHeartbeats(TaskTransactions transactions, WorkerLeaseSettings settings, ScheduledExecutorService scheduler) {
        this.transactions = transactions; this.settings = settings; this.scheduler = scheduler;
    }

    private static ScheduledExecutorService executor() {
        var executor = new ScheduledThreadPoolExecutor(1, runnable -> {
            var thread = new Thread(runnable, "durableflow-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    public synchronized Session start(ClaimedTask claim) {
        if (stopping) throw new OwnershipLostException(claim);
        var session = new Session(claim);
        active.add(session);
        try {
            session.future = scheduler.scheduleWithFixedDelay(session::renew,
                    settings.heartbeatMillis(), settings.heartbeatMillis(), TimeUnit.MILLISECONDS);
            if (session.closed.get()) session.future.cancel(false);
        } catch (RuntimeException failure) {
            active.remove(session);
            throw failure;
        }
        return session;
    }

    public final class Session implements AutoCloseable {
        private final ClaimedTask claim;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicBoolean lost = new AtomicBoolean();
        private volatile ScheduledFuture<?> future;
        private Session(ClaimedTask claim) { this.claim = claim; }

        private synchronized void renew() {
            if (closed.get()) return;
            try {
                if (!transactions.heartbeat(claim)) abandon("Lease renewal rejected", null);
            } catch (Exception failure) {
                // Strong conservative policy: any unconfirmed renewal abandons this local attempt.
                abandon("Lease renewal unavailable; ownership is unconfirmed", failure);
            }
        }

        private void abandon(String reason, Exception failure) {
            if (!closed.get()) {
                lost.set(true);
                LOG.warn("{}: task {}, generation {}", reason, claim.taskId(), claim.leaseGeneration(), failure);
                close();
            }
        }

        public void ensureOwned() {
            if (lost.get()) throw new OwnershipLostException(claim);
        }

        @Override
        public synchronized void close() {
            closed.set(true);
            var scheduled = future;
            if (scheduled != null) scheduled.cancel(false);
            active.remove(this);
        }
    }

    @Override
    @PreDestroy
    public synchronized void close() {
        if (stopping) return;
        stopping = true;
        var sessions = List.copyOf(active);
        sessions.forEach(session -> session.lost.set(true));
        scheduler.shutdownNow();
        sessions.forEach(Session::close);
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) LOG.warn("Heartbeat scheduler did not terminate within shutdown timeout");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
