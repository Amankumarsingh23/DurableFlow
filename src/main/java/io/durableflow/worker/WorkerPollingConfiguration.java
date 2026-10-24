package io.durableflow.worker;

import org.slf4j.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.*;
import org.springframework.scheduling.annotation.*;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import java.util.concurrent.atomic.AtomicBoolean;

@Configuration(proxyBeanMethods = false)
@Profile("worker")
@EnableScheduling
public class WorkerPollingConfiguration {
    @Bean
    @ConditionalOnProperty(name = "durableflow.worker.polling-enabled", havingValue = "true", matchIfMissing = true)
    Poller taskPoller(TaskWorker worker, @Value("${durableflow.worker.poll-delay-ms:500}") long delay) {
        if (delay < 1) throw new IllegalArgumentException("Worker polling delay must be positive");
        return new Poller(worker);
    }

    public static class Poller implements AutoCloseable {
        private static final Logger LOG = LoggerFactory.getLogger(Poller.class);
        private final TaskWorker worker;
        private final AtomicBoolean stopping = new AtomicBoolean();
        Poller(TaskWorker worker) { this.worker = worker; }

        @Scheduled(fixedDelayString = "${durableflow.worker.poll-delay-ms:500}")
        public void poll() {
            if (stopping.get()) return;
            try { worker.runOne(); }
            catch (OwnershipLostException lost) { LOG.warn("{}", lost.getMessage()); }
            catch (Exception failure) { LOG.error("Worker poll failed; durable task state determines subsequent recovery", failure); }
        }

        @EventListener(ContextClosedEvent.class)
        public void contextClosed() { close(); }

        @Override
        public void close() { stopping.set(true); }
    }
}
