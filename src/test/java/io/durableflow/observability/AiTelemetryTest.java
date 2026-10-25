package io.durableflow.observability;
import io.durableflow.ai.model.*;
import io.durableflow.ai.provider.FakeModelProvider;
import io.durableflow.ai.retrieval.*;
import io.durableflow.workflow.activity.ActivityExecutionContext;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import static org.assertj.core.api.Assertions.*;

class AiTelemetryTest {
    private void flush(SafeTelemetry telemetry) throws Exception {var done=new CountDownLatch(1);telemetry.deferred(done::countDown);assertThat(done.await(5,TimeUnit.SECONDS)).isTrue();}
    @Test void retrievalMeasuresScopedCandidatesSelectionContextAndFailuresWithoutRawText() throws Exception {
        var dispatcher=new TelemetryDispatcher();
        var registry=new SimpleMeterRegistry();
        try {
            var safe=new SafeTelemetry(dispatcher,new MicrometerTelemetry(registry,dispatcher));
            var input=Map.<String,Object>of("retrievalRequest",Map.of("requestVersion",1,"retriever","fake","corpusVersion","policies-v1","rankingVersion","local-v1","query","ACCT-1042","strategy","SPARSE","topK",1,"maxContextChars",3000,"filters",Map.of("tenantId","tenant-a","documentType","note")));
            var context=new ActivityExecutionContext(UUID.randomUUID(),UUID.randomUUID());
            var good=new RetrieveContextActivity(new RetrievalGateway(List.of(new FakeRetriever())),new ContextBuilder(),safe);
            assertThat(good.execute(context,input)).containsKey("selectedChunks");
            var failed=new RetrieveContextActivity(new RetrievalGateway(List.of(new FakeRetriever(FakeCorpus.policiesV1(),List.of(FakeRetriever.Outcome.UNAVAILABLE)))),new ContextBuilder(),safe);
            assertThatThrownBy(() -> failed.execute(context,input)).isInstanceOf(io.durableflow.worker.retry.RetryableActivityException.class);
            flush(safe);
            assertThat(registry.find("durableflow.retrieval.calls").tags("status","SUCCESS").counter().count()).isEqualTo(1);
            assertThat(registry.find("durableflow.retrieval.calls").tags("status","FAILURE").counter().count()).isEqualTo(1);
            assertThat(registry.find("durableflow.retrieval.candidates").summary().totalAmount()).isEqualTo(1);
            assertThat(registry.find("durableflow.retrieval.budget.utilization").summary().max()).isBetween(0.0,1.0);
            assertThat(registry.getMeters()).allMatch(m -> m.getId().getTags().stream().allMatch(t -> Set.of("strategy","status").contains(t.getKey())));
        } finally {dispatcher.close();registry.close();}
    }
    @Test void providerAttemptsHaveUsageCostLatencyAndSeparateSuccessFailure() throws Exception {
        var dispatcher=new TelemetryDispatcher();
        var registry=new SimpleMeterRegistry();
        try {
            var safe=new SafeTelemetry(dispatcher,new MicrometerTelemetry(registry,dispatcher));
            var provider=new FakeModelProvider(List.of(FakeModelProvider.Outcome.UNAVAILABLE,FakeModelProvider.Outcome.SUCCESS));
            var gateway=new ModelGateway(List.of(provider),null,safe);var request=new ModelRequest(1,"fake","fake-recommendation-v1","Synthetic system","Synthetic user",0.0,128,true);
            assertThatThrownBy(() -> gateway.call(request,"same-key")).isInstanceOf(io.durableflow.worker.retry.RetryableActivityException.class);
            var result=gateway.call(request,"same-key");flush(safe);
            assertThat(registry.find("durableflow.model.attempts").counter().count()).isEqualTo(2);
            assertThat(registry.find("durableflow.model.attempt.duration").tags("status","FAILURE").timer().count()).isEqualTo(1);
            assertThat(registry.find("durableflow.model.attempt.duration").tags("status","SUCCESS").timer().count()).isEqualTo(1);
            assertThat(registry.find("durableflow.model.input.tokens").summary().totalAmount()).isEqualTo(result.usage().inputTokens());
            assertThat(registry.find("durableflow.model.cost.micros").summary().totalAmount()).isEqualTo(result.usage().inputTokens()+2.0*result.usage().outputTokens());
        } finally {dispatcher.close();registry.close();}
    }
}
