package io.durableflow.worker;

import io.durableflow.worker.retry.ActivityTimeoutException;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** One bounded daemon execution slot. A non-cooperative zombie retains its slot until it really exits. */
@Component
public class HandlerExecutions implements AutoCloseable {
    @FunctionalInterface
    interface Awaiter {
        Map<String, Object> await(Future<Map<String, Object>> future, long millis)
                throws InterruptedException, ExecutionException, TimeoutException;
    }
    public static class Abandoned extends RuntimeException {}
    private final ExecutorService executor;
    private final WorkerExecutionSettings settings;
    private final Awaiter awaiter;
    private Ticket active;
    private boolean closed;

    @Autowired
    public HandlerExecutions(WorkerExecutionSettings settings) {
        this(settings, new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1), runnable -> {
                    var thread = new Thread(runnable, "durableflow-handler");
                    thread.setDaemon(true);
                    return thread;
                }), (future, millis) -> future.get(millis, TimeUnit.MILLISECONDS));
    }

    HandlerExecutions(WorkerExecutionSettings settings, ExecutorService executor, Awaiter awaiter) {
        this.settings = settings; this.executor = executor; this.awaiter = awaiter;
    }

    /** Reserve before claiming DB work; never claim attempts that cannot be dispatched locally. */
    public synchronized Optional<Ticket> reserve() {
        if (closed || active != null) return Optional.empty();
        active = new Ticket();
        return Optional.of(active);
    }

    public final class Ticket implements AutoCloseable {
        private final AtomicBoolean released = new AtomicBoolean();
        private FutureTask<Map<String, Object>> future;

        public Map<String, Object> execute(Callable<Map<String, Object>> handler) throws Exception {
            long dispatchedAt;
            synchronized (HandlerExecutions.this) {
                if (closed || released.get() || future != null) throw new Abandoned();
                future = new FutureTask<>(() -> {
                    try { return handler.call(); }
                    finally { release(); } // Before successful Future.get returns: next sequential claim can reserve.
                }) {
                    @Override public void run() {
                        try { super.run(); }
                        finally { release(); } // Also releases a task cancelled before callable entry.
                    }
                };
                dispatchedAt = System.nanoTime();
                try { executor.execute(future); }
                catch (RejectedExecutionException rejected) { release(); throw new Abandoned(); }
            }
            try {
                long remaining = TimeUnit.MILLISECONDS.toNanos(settings.timeoutMillis()) - (System.nanoTime() - dispatchedAt);
                if (remaining <= 0) throw new TimeoutException();
                return awaiter.await(future, Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
            } catch (TimeoutException timedOut) {
                future.cancel(true);
                throw new ActivityTimeoutException(settings.timeoutMillis());
            } catch (CancellationException cancelled) {
                throw new Abandoned();
            } catch (InterruptedException interrupted) {
                future.cancel(true);
                Thread.currentThread().interrupt();
                throw new Abandoned(); // Supervisor shutdown/interruption is not a business handler failure.
            } catch (ExecutionException failed) {
                if (failed.getCause() instanceof Exception exception) throw exception;
                if (failed.getCause() instanceof Error error) throw error;
                throw new IllegalStateException(failed.getCause());
            }
        }

        private void release() {
            if (released.compareAndSet(false, true)) {
                synchronized (HandlerExecutions.this) {
                    if (active == this) active = null;
                }
            }
        }

        @Override public void close() {
            synchronized (HandlerExecutions.this) {
                if (future == null) release();
                else future.cancel(true); // Cancellation never releases a still-running zombie's capacity.
            }
        }
    }

    @Override @PreDestroy
    public synchronized void close() {
        if (closed) return;
        closed = true;
        if (active != null) active.close();
        executor.shutdownNow(); // Never wait indefinitely for a handler ignoring interruption.
    }
}
