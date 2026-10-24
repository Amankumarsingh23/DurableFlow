package io.durableflow.worker;

import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.task.WorkflowTaskOrchestrator;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class WorkerHeartbeatsTest {
    private final TaskTransactions transactions = mock(TaskTransactions.class);
    private final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    private final ScheduledFuture<?> future = mock(ScheduledFuture.class);
    private final ArgumentCaptor<Runnable> tick = ArgumentCaptor.forClass(Runnable.class);
    private final ClaimedTask claim = new ClaimedTask(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
            "demo", Map.of(), "A", 1, Instant.parse("2030-01-01T00:00:00Z"));

    private WorkerHeartbeats heartbeats() throws InterruptedException {
        doReturn(future).when(scheduler).scheduleWithFixedDelay(tick.capture(), eq(10000L), eq(10000L), eq(TimeUnit.MILLISECONDS));
        when(scheduler.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);
        return new WorkerHeartbeats(transactions, new WorkerLeaseSettings(30000, 10000), scheduler);
    }

    @Test
    void failedRenewalLosesOwnershipAndCancelsFurtherTicks() throws Exception {
        when(transactions.heartbeat(claim)).thenReturn(true, false);
        try (var heartbeats = heartbeats()) {
            var session = heartbeats.start(claim);
            tick.getValue().run();
            session.ensureOwned();
            tick.getValue().run();
            assertThatThrownBy(session::ensureOwned).isInstanceOf(OwnershipLostException.class);
            tick.getValue().run();
            verify(transactions, times(2)).heartbeat(claim);
            verify(future).cancel(false);
        }
    }

    @Test
    void databaseFailureAlsoAbandonsUnconfirmedOwnership() throws Exception {
        when(transactions.heartbeat(claim)).thenThrow(new IllegalStateException("database unavailable"));
        try (var heartbeats = heartbeats()) {
            var session = heartbeats.start(claim);
            tick.getValue().run();
            assertThatThrownBy(session::ensureOwned).isInstanceOf(OwnershipLostException.class);
            verify(future).cancel(false);
        }
    }

    @Test
    void handlerEndCancelsHeartbeatAndShutdownRejectsNewSessions() throws Exception {
        var heartbeats = heartbeats();
        var session = heartbeats.start(claim);
        session.close();
        tick.getValue().run();
        verifyNoInteractions(transactions);
        heartbeats.close();
        heartbeats.close();
        verify(scheduler, times(1)).shutdownNow();
        assertThatThrownBy(() -> heartbeats.start(claim)).isInstanceOf(OwnershipLostException.class);
    }

    @Test
    void shutdownInvalidatesActiveSessionAndStopsScheduler() throws Exception {
        var heartbeats = heartbeats();
        var session = heartbeats.start(claim);
        heartbeats.close();
        assertThatThrownBy(session::ensureOwned).isInstanceOf(OwnershipLostException.class);
        tick.getValue().run();
        verifyNoInteractions(transactions);
        verify(future).cancel(false);
        verify(scheduler).shutdownNow();
    }

    @Test
    void workerDiscardsHandlerSuccessWhenHeartbeatLosesOwnership() throws Exception {
        when(transactions.claim("A")).thenReturn(Optional.of(claim));
        when(transactions.heartbeat(claim)).thenReturn(false);
        var orchestrator = mock(WorkflowTaskOrchestrator.class);
        var registry = new ActivityRegistry(List.of(new ActivityHandler() {
            public String type() { return "demo"; }
            public Map<String, Object> execute(Map<String, Object> input) {
                tick.getValue().run(); // Deterministic ownership loss during handler work.
                return Map.of("result", "late");
            }
        }));
        try (var heartbeats = heartbeats()) {
            var worker = new TaskWorker(transactions, registry, orchestrator, new WorkerIdentity("A"), heartbeats);
            assertThatThrownBy(worker::runOne).isInstanceOf(OwnershipLostException.class);
            verify(transactions, never()).complete(any(), any());
            verify(transactions, never()).fail(any(), any());
            verifyNoInteractions(orchestrator);
            worker.stop();
            assertThat(worker.runOne()).isFalse();
            verify(transactions, times(1)).claim("A");
        }
    }

    @Test
    void heartbeatMustBePositiveAndAtMostOneThirdOfLease() {
        assertThat(new WorkerLeaseSettings(30000, 10000).leaseMillis()).isEqualTo(30000);
        assertThatThrownBy(() -> new WorkerLeaseSettings(0, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WorkerLeaseSettings(30000, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WorkerLeaseSettings(30000, 10001)).isInstanceOf(IllegalArgumentException.class);
    }
}
