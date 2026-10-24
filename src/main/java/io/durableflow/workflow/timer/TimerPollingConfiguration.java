package io.durableflow.workflow.timer;

import org.slf4j.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.*;
import org.springframework.context.event.*;
import org.springframework.scheduling.annotation.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Configuration(proxyBeanMethods = false) @Profile("!worker") @EnableScheduling
public class TimerPollingConfiguration {
    @Bean
    @ConditionalOnProperty(name = "durableflow.timers.polling-enabled", havingValue = "true", matchIfMissing = true)
    TimerPoller timerPoller(TimerTransactions transactions, @Value("${durableflow.timers.poll-delay-ms:500}") long delay) {
        if (delay < 1) throw new IllegalArgumentException("Timer poll delay must be positive");
        return new TimerPoller(transactions);
    }
    public static class TimerPoller implements AutoCloseable {
        private static final Logger LOG = LoggerFactory.getLogger(TimerPoller.class);
        private final TimerTransactions transactions;
        private final AtomicBoolean stopping = new AtomicBoolean();
        public TimerPoller(TimerTransactions transactions) { this.transactions = transactions; }
        @Scheduled(fixedDelayString = "${durableflow.timers.poll-delay-ms:500}")
        public void poll() {
            if (stopping.get()) return;
            try { transactions.fireOne(); }
            catch (Exception failure) { LOG.error("Timer dispatch failed; transaction rollback preserves durable timer", failure); }
        }
        @EventListener(ContextClosedEvent.class) public void contextClosed() { close(); }
        @Override public void close() { stopping.set(true); }
    }
}
