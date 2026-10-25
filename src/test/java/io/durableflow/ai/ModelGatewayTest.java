package io.durableflow.ai;

import io.durableflow.ai.activity.*;
import io.durableflow.ai.demo.FakeApprovedActionProvider;
import io.durableflow.ai.model.*;
import io.durableflow.ai.provider.*;
import io.durableflow.worker.retry.*;
import io.durableflow.workflow.activity.ActivityExecutionContext;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class ModelGatewayTest {
    private ModelRequest request(boolean structured) {
        return new ModelRequest(1, "fake", "fake-recommendation-v1", "Frozen system instructions", "Frozen user content", 0.0, 256, structured);
    }
    @Test
    void providerNeutralGatewayMapsStructuredResultAndUsageAndCorrelatesLogicalOperation() throws Exception {
        var provider = new FakeModelProvider(); var gateway = new ModelGateway(List.of(provider));
        var response = gateway.call(request(true), "logical-operation");
        assertThat(response.provider()).isEqualTo("fake"); assertThat(response.model()).isEqualTo("fake-recommendation-v1");
        assertThat(response.structuredJson()).containsKeys("recommendation", "inputFingerprint"); assertThat(response.content()).isNull();
        assertThat(response.usage().inputTokens()).isPositive(); assertThat(response.usage().outputTokens()).isPositive();
        assertThat(response.latencyMillis()).isEqualTo(7); assertThat(response.providerRequestId()).startsWith("fake-");
        assertThat(provider.invocations()).hasSize(1).allMatch(i -> i.request().equals(request(true)) && i.operationKey().equals("logical-operation"));
    }
    @Test
    void textResponseIsSupportedWithoutStructuredIntentAndFakeGenerationIsNotDeduplicated() throws Exception {
        var provider = new FakeModelProvider(); var gateway = new ModelGateway(List.of(provider));
        var first = gateway.call(request(false), "key"); var second = gateway.call(request(false), "key");
        assertThat(first.content()).isEqualTo(second.content()); assertThat(first.structuredJson()).isNull();
        assertThat(second.providerRequestId()).isNotEqualTo(first.providerRequestId()); assertThat(provider.callCount()).isEqualTo(2);
    }
    @Test
    void typedProviderFailuresMapToExistingRetryClassificationWithoutMessageMatching() {
        var classifier = new FailureClassifier();
        for (var kind : ModelProviderException.Kind.values()) {
            ModelProvider provider = new ModelProvider() {
                public String id() { return "fake"; }
                public ModelResponse generate(ModelRequest request, String key) { throw new ModelProviderException(kind); }
            };
            var gateway = new ModelGateway(List.of(provider));
            try { gateway.call(request(true), "key"); fail("Expected typed provider failure"); }
            catch (Exception failure) {
                boolean retryable = Set.of(ModelProviderException.Kind.RATE_LIMITED, ModelProviderException.Kind.UNAVAILABLE, ModelProviderException.Kind.TIMEOUT).contains(kind);
                assertThat(classifier.classify(failure)).isEqualTo(retryable ? FailureClassifier.Classification.RETRYABLE : FailureClassifier.Classification.NON_RETRYABLE);
                assertThat(failure.getMessage()).isEqualTo("MODEL_"+kind);
            }
        }
    }
    @Test
    void unknownProgrammingFailureRemainsTerminalEvenIfMessageContainsRateLimit() {
        ModelProvider provider = new ModelProvider() {
            public String id() { return "fake"; }
            public ModelResponse generate(ModelRequest request, String key) { throw new IllegalStateException("rate limit timeout temporary unavailable"); }
        };
        assertThatThrownBy(() -> new ModelGateway(List.of(provider)).call(request(true), "key"))
                .isInstanceOf(IllegalStateException.class).isNotInstanceOf(RetryableActivityException.class);
    }
    @Test
    void invalidProviderModelConfigurationAndMalformedStructuredOutputAreTerminal() {
        var gateway = new ModelGateway(List.of(new FakeModelProvider()));
        assertThatThrownBy(() -> gateway.call(new ModelRequest(1,"missing","model","sys","user",null,256,true),"key"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("INVALID_PROVIDER");
        assertThatThrownBy(() -> gateway.call(new ModelRequest(1,"fake","missing-model","sys","user",null,256,true),"key"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("INVALID_MODEL");
        var malformed = new ModelGateway(List.of(new FakeModelProvider(List.of(FakeModelProvider.Outcome.MALFORMED_STRUCTURED))));
        assertThatThrownBy(() -> malformed.call(request(true),"key")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("MALFORMED_RESPONSE");
        assertThatThrownBy(() -> new ModelGateway(List.of(new FakeModelProvider(), new FakeModelProvider()))).isInstanceOf(IllegalArgumentException.class);
    }
    @Test
    void modelActivityReturnsJsonCompatibleResponseMetadata() throws Exception {
        var handler = new ModelCallActivity(new ModelGateway(List.of(new FakeModelProvider())));
        var output = handler.execute(new ActivityExecutionContext(UUID.randomUUID(), UUID.randomUUID()), input());
        assertThat(output).containsKeys("outputVersion","structuredJson","provider","model","usage","latencyMillis","providerRequestId");
        assertThat(output).doesNotContainKeys("systemInstructions","userContent");
    }
    @Test
    void versionedRequestRejectsImplicitDefaultsUnknownFieldsAndInvalidSettings() {
        var input = input(); var request = new HashMap<String,Object>((Map<String,Object>) input.get("modelRequest"));
        assertThat(ModelRequest.fromInput(input)).isEqualTo(request(true));
        for (String required : List.of("requestVersion","provider","model","systemInstructions","userContent","maxOutputTokens")) {
            var missing = new HashMap<>(request); missing.remove(required);
            assertThatThrownBy(() -> ModelRequest.fromInput(Map.of("modelRequest",missing))).isInstanceOf(IllegalArgumentException.class);
        }
        for (var invalid : List.of(Map.entry("requestVersion",2), Map.entry("temperature",Double.NaN),
                Map.entry("maxOutputTokens",-1), Map.entry("unknownSetting",1), Map.entry("structuredOutput","true"))) {
            var changed = new HashMap<>(request); changed.put(invalid.getKey(),invalid.getValue());
            assertThatThrownBy(() -> ModelRequest.fromInput(Map.of("modelRequest",changed))).isInstanceOf(IllegalArgumentException.class);
        }
    }
    @Test
    void configuredActionHasIndependentIdempotentEffectAndRejectsKeyPayloadMismatch() {
        var provider = new FakeApprovedActionProvider(); var handler = new ApplyApprovedActionActivity(provider);
        var context = new ActivityExecutionContext(UUID.randomUUID(), UUID.randomUUID());
        var input = Map.<String,Object>of("approvedAction",Map.of("name","markReviewed","target","ORDER-21"));
        var first = handler.execute(context,input); assertThat(handler.execute(context,input)).isEqualTo(first);
        assertThat(provider.effectCount()).isEqualTo(1);
        assertThatThrownBy(() -> handler.execute(context,Map.of("approvedAction",Map.of("name","markReviewed","target","OTHER"))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(provider.effectCount()).isEqualTo(1);
    }
    private Map<String,Object> input() {
        return Map.of("modelRequest",Map.of("requestVersion",1,"provider","fake","model","fake-recommendation-v1",
                "systemInstructions","Frozen system instructions","userContent","Frozen user content","temperature",0.0,"maxOutputTokens",256,"structuredOutput",true));
    }
}
