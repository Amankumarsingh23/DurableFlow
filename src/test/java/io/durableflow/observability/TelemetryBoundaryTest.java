package io.durableflow.observability;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.assertj.core.api.Assertions.*;

class TelemetryBoundaryTest {
    @Test void arbitraryRunIdsAndUnknownNamesCannotGrowMeterCardinality() {
        try(var registry=new SimpleMeterRegistry()) {
            var dispatcher=new TelemetryDispatcher();
            try {
                var adapter=new MicrometerTelemetry(registry,dispatcher);
                for(int n=0;n<1000;n++)adapter.measure(Metric.MODEL_ATTEMPTS,DurableFlowTelemetry.Kind.COUNTER,1,
                    Map.of("workflowRunId",UUID.randomUUID().toString(),"activityExecutionId",UUID.randomUUID().toString(),"userId","user-"+n,"toolRequestId",UUID.randomUUID().toString(),"provider","untrusted-"+n,"model","prompt-"+n));
                assertThat(registry.find("durableflow.model.attempts").counters()).hasSize(1);
                assertThat(registry.find("durableflow.model.attempts").counter().getId().getTags()).allMatch(tag -> Set.of("provider","model").contains(tag.getKey()) && tag.getValue().equals("other"));
                assertThat(registry.find("durableflow.model.attempts").counter().count()).isEqualTo(1000);
            } finally {dispatcher.close();}
        }
    }
    @Test void sanitizerDropsContentNestedArgumentsSecretsAndUntrustedClassifications() {
        String id=UUID.randomUUID().toString();var dirty=new LinkedHashMap<String,Object>();
        dirty.put("workflowRunId",id);dirty.put("inputTokens",42);dirty.put("prompt","SECRET");dirty.put("text","hostile chunk");
        dirty.put("arguments",Map.of("amount",900));dirty.put("privateReasoning","hidden");dirty.put("provider","SECRET-provider");dirty.put("fingerprint","a".repeat(64));dirty.put("providerRequestId","user-secret");
        var safe=SafeMetadata.sanitize(dirty);
        assertThat(safe).containsEntry("workflowRunId",id).containsEntry("inputTokens",42L).containsEntry("provider","other").containsEntry("fingerprint","a".repeat(64));
        assertThat(safe.keySet()).containsExactlyInAnyOrder("workflowRunId","inputTokens","provider","fingerprint");
    }
    @Test void callbacksOnlySubmitAfterCommitAndNeverExecuteOnTransactionThread() throws Exception {
        var dispatcher=new TelemetryDispatcher();var called=new CountDownLatch(1);var active=new AtomicBoolean(true);
        try {
            TransactionSynchronizationManager.initSynchronization();TransactionSynchronizationManager.setActualTransactionActive(true);
            dispatcher.afterCommit(() -> {active.set(TransactionSynchronizationManager.isActualTransactionActive());called.countDown();});
            assertThat(called.getCount()).isEqualTo(1);
            TransactionSynchronizationUtils.triggerAfterCommit();
            assertThat(called.await(5,TimeUnit.SECONDS)).isTrue();assertThat(active).isFalse();
        } finally {TransactionSynchronizationManager.clear();dispatcher.close();}
    }
    @Test void rollbackDropsObservationAndSinkFailuresDoNotEscape() throws Exception {
        var dispatcher=new TelemetryDispatcher();var delivered=new AtomicInteger();var barrier=new CountDownLatch(1);
        try {
            TransactionSynchronizationManager.initSynchronization();TransactionSynchronizationManager.setActualTransactionActive(true);
            dispatcher.afterCommit(delivered::incrementAndGet);
            TransactionSynchronizationUtils.triggerAfterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK);
            TransactionSynchronizationManager.clear();
            dispatcher.afterCommit(() -> {throw new IllegalStateException("unavailable sink");});
            dispatcher.afterCommit(barrier::countDown);
            assertThat(barrier.await(5,TimeUnit.SECONDS)).isTrue();assertThat(delivered).hasValue(0);assertThat(dispatcher.failed()).isEqualTo(1);
        } finally {TransactionSynchronizationManager.clear();dispatcher.close();}
    }
    @Test void saturatedQueueDropsWithoutBlockingTheCaller() throws Exception {
        var dispatcher=new TelemetryDispatcher(1);var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try {
            dispatcher.afterCommit(() -> {entered.countDown();try {release.await(5,TimeUnit.SECONDS);}catch(InterruptedException e) {Thread.currentThread().interrupt();}});
            assertThat(entered.await(5,TimeUnit.SECONDS)).isTrue();dispatcher.afterCommit(() -> {});
            dispatcher.afterCommit(() -> fail("Full queue must drop this observation"));assertThat(dispatcher.dropped()).isEqualTo(1);
        } finally {release.countDown();dispatcher.close();}
    }
    @Test void traceLinksCarryCorrelationWithoutUsingMeterTags() {
        UUID run=UUID.randomUUID();var c=new TelemetryCorrelation(run,null,null,null,null,null,null);
        var root=TraceContext.root(c);var activity=root.child(TraceContext.Boundary.ACTIVITY,c);
        assertThat(activity.parentSpanId()).isEqualTo(root.spanId());assertThat(c.metadata()).containsEntry("workflowRunId",run.toString());
        assertThat(MetricLabels.bounded(Map.of("workflowRunId",run.toString()))).isEmpty();
    }
}
