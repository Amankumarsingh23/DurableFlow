package io.durableflow.worker;

import org.slf4j.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.*;
import org.springframework.scheduling.annotation.*;

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

    public static class Poller {
        private static final Logger LOG = LoggerFactory.getLogger(Poller.class);
        private final TaskWorker worker;
        Poller(TaskWorker worker) { this.worker = worker; }

        @Scheduled(fixedDelayString = "${durableflow.worker.poll-delay-ms:500}")
        public void poll() {
            try { worker.runOne(); }
            catch (Exception failure) { LOG.error("Worker poll failed; no task retry or reclamation is performed", failure); }
        }
    }
}
