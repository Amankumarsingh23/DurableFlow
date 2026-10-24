package io.durableflow.worker;

import io.durableflow.worker.retry.*;
import org.junit.jupiter.api.Test;
import java.util.Map;
import java.util.concurrent.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class HandlerExecutionsTest {
    @Test
    void fastResultAndOriginalHandlerFailureReachSupervisor() throws Exception {
        try (var executions = new HandlerExecutions(new WorkerExecutionSettings(60000, 1000))) {
            try (var ticket = executions.reserve().orElseThrow()) {
                assertThat(ticket.execute(() -> Map.of("ok", true))).containsEntry("ok", true);
            }
            var failure = new IllegalArgumentException("bad payload");
            try (var ticket = executions.reserve().orElseThrow()) {
                assertThatThrownBy(() -> ticket.execute(() -> { throw failure; })).isSameAs(failure);
            }
        }
    }

    @Test
    void actualFutureDeadlineInterruptsCooperativeHandler() throws Exception {
        var interrupted = new CountDownLatch(1);
        var entered = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor();
        try (var executions = new HandlerExecutions(new WorkerExecutionSettings(60000, 0), executor, (future, millis) -> {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            return future.get(100, TimeUnit.MILLISECONDS); // Actual timed wait, started after deterministic entry.
        });
             var ticket = executions.reserve().orElseThrow()) {
            assertThatThrownBy(() -> ticket.execute(() -> {
                entered.countDown();
                try { new CountDownLatch(1).await(); }
                catch (InterruptedException failure) { interrupted.countDown(); throw failure; }
                return Map.of();
            })).isInstanceOf(ActivityTimeoutException.class);
            assertThat(interrupted.await(5, TimeUnit.SECONDS)).isTrue();
        } finally { executor.shutdownNow(); }
    }

    @Test
    void ignoredInterruptionRetainsCapacityUntilPhysicalExitAndLateResultIsDiscarded() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor(runnable -> {
            var thread = new Thread(runnable); thread.setDaemon(true); return thread;
        });
        try (var executions = new HandlerExecutions(new WorkerExecutionSettings(1000, 0), executor, (future, millis) -> {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            throw new TimeoutException(); // Deterministic deadline signal after handler entry.
        }); var ticket = executions.reserve().orElseThrow()) {
            assertThatThrownBy(() -> ticket.execute(() -> {
                entered.countDown();
                while (release.getCount() != 0) {
                    try { release.await(); } catch (InterruptedException ignored) { /* Deliberately non-cooperative fixture. */ }
                }
                return Map.of("late", true);
            })).isInstanceOf(ActivityTimeoutException.class);
            assertThat(executions.reserve()).isEmpty();
            release.countDown();
            await().atMost(5, TimeUnit.SECONDS).until(() -> executions.reserve().map(next -> {
                next.close(); return true;
            }).orElse(false));
            executions.close();
            assertThat(executions.reserve()).isEmpty();
        } finally { release.countDown(); executor.shutdownNow(); }
    }

    @Test
    void positiveExecutionTimeoutAndRetryableClassificationAreExplicit() {
        assertThatThrownBy(() -> new WorkerExecutionSettings(0, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WorkerExecutionSettings(1, -1)).isInstanceOf(IllegalArgumentException.class);
        assertThat(new FailureClassifier().classify(new ActivityTimeoutException(100)))
                .isEqualTo(FailureClassifier.Classification.RETRYABLE);
    }
}
