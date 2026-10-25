package io.durableflow.observability;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** Bounded and lossy, with no backpressure into correctness. No work on commit callbacks. */
@Component
public class TelemetryDispatcher {
    private final ThreadPoolExecutor executor;
    private final AtomicLong dropped=new AtomicLong(),failed=new AtomicLong();
    public TelemetryDispatcher() {this(1024);}
    public TelemetryDispatcher(int capacity) {
        executor=new ThreadPoolExecutor(1,1,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(capacity),r -> {
            var thread=new Thread(r,"durableflow-telemetry");thread.setDaemon(true);return thread;
        },new ThreadPoolExecutor.AbortPolicy());
    }
    public void afterCommit(Runnable work) {
        try {
            if(TransactionSynchronizationManager.isActualTransactionActive()) {
                if(!TransactionSynchronizationManager.isSynchronizationActive()) {dropped.incrementAndGet();return;}
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override public void afterCommit() {submit(work);}
                });
            } else submit(work);
        } catch(RuntimeException ignored) {failed.incrementAndGet();}
    }
    private void submit(Runnable work) {
        try {executor.execute(() -> {try {work.run();}catch(RuntimeException ignored) {failed.incrementAndGet();}});}
        catch(RejectedExecutionException ignored) {dropped.incrementAndGet();}
    }
    public long dropped() {return dropped.get();}
    public long failed() {return failed.get();}
    @PreDestroy public void close() {executor.shutdownNow();}
}
