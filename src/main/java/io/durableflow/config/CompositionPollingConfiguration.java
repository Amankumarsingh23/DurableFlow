package io.durableflow.config;

import io.durableflow.workflow.child.ChildReconciler;
import io.durableflow.workflow.schedule.ScheduleDispatcher;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.*;
import org.springframework.context.event.*;
import org.springframework.scheduling.annotation.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Configuration(proxyBeanMethods = false) @Profile("!worker") @EnableScheduling
public class CompositionPollingConfiguration {
    @Bean @ConditionalOnProperty(name="durableflow.children.polling-enabled", havingValue="true", matchIfMissing=true)
    ChildPoller childPoller(ChildReconciler reconciler, @Value("${durableflow.children.poll-delay-ms:500}") long delay) {
        if (delay < 1) throw new IllegalArgumentException("Child poll delay must be positive");
        return new ChildPoller(reconciler);
    }
    @Bean @ConditionalOnProperty(name="durableflow.schedules.polling-enabled", havingValue="true", matchIfMissing=true)
    SchedulePoller schedulePoller(ScheduleDispatcher dispatcher, @Value("${durableflow.schedules.poll-delay-ms:500}") long delay) {
        if (delay < 1) throw new IllegalArgumentException("Schedule poll delay must be positive");
        return new SchedulePoller(dispatcher);
    }
    public static class ChildPoller {
        private static final Logger LOG = LoggerFactory.getLogger(ChildPoller.class);
        private final ChildReconciler reconciler;
        private final AtomicBoolean stopping = new AtomicBoolean();
        ChildPoller(ChildReconciler reconciler) { this.reconciler = reconciler; }
        @Scheduled(fixedDelayString="${durableflow.children.poll-delay-ms:500}")
        public void poll() {
            for (int i=0; i<10 && !stopping.get(); i++) {
                try { if (!reconciler.reconcileOne()) break; }
                catch (Exception failure) { LOG.error("Child reconciliation failed; durable state retained", failure); break; }
            }
        }
        @EventListener(ContextClosedEvent.class) public void close() { stopping.set(true); }
    }
    public static class SchedulePoller {
        private static final Logger LOG = LoggerFactory.getLogger(SchedulePoller.class);
        private final ScheduleDispatcher dispatcher;
        private final AtomicBoolean stopping = new AtomicBoolean();
        SchedulePoller(ScheduleDispatcher dispatcher) { this.dispatcher = dispatcher; }
        @Scheduled(fixedDelayString="${durableflow.schedules.poll-delay-ms:500}")
        public void poll() {
            if (stopping.get()) return;
            try { dispatcher.pollBatch(); } catch (Exception failure) { LOG.error("Schedule materialization failed; transaction rolled back", failure); }
        }
        @EventListener(ContextClosedEvent.class) public void close() { stopping.set(true); }
    }
}
