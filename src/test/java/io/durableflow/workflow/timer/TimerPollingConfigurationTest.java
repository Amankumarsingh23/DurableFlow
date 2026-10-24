package io.durableflow.workflow.timer;

import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class TimerPollingConfigurationTest {
    @Test
    void shutdownStopsNewPollsButDoesNotInterruptInProgressDispatch() throws Exception {
        var transactions = mock(TimerTransactions.class);
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        when(transactions.fireOne()).thenAnswer(invocation -> {
            entered.countDown(); assertThat(release.await(10, TimeUnit.SECONDS)).isTrue(); return true;
        });
        var poller = new TimerPollingConfiguration.TimerPoller(transactions);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var active = executor.submit(poller::poll);
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            poller.contextClosed(); poller.poll();
            assertThat(active.isDone()).isFalse();
            release.countDown(); active.get(10, TimeUnit.SECONDS);
            poller.poll(); verify(transactions, times(1)).fireOne();
        } finally { release.countDown(); executor.shutdownNow(); poller.close(); }
    }

    @Test
    void dispatcherFailureDoesNotStopFuturePollingAndDelayMustBePositive() {
        var transactions = mock(TimerTransactions.class);
        when(transactions.fireOne()).thenThrow(new IllegalStateException("rollback")).thenReturn(false);
        var configuration = new TimerPollingConfiguration();
        assertThatThrownBy(() -> configuration.timerPoller(transactions, 0)).isInstanceOf(IllegalArgumentException.class);
        try (var poller = configuration.timerPoller(transactions, 500)) {
            assertThatCode(poller::poll).doesNotThrowAnyException();
            poller.poll(); verify(transactions, times(2)).fireOne();
        }
    }
}
