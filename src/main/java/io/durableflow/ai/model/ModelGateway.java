package io.durableflow.ai.model;

import io.durableflow.ai.provider.*;
import io.durableflow.worker.retry.RetryableActivityException;
import org.springframework.stereotype.Service;
import java.util.*;
import io.durableflow.observability.*;

@Service
public class ModelGateway {
    private final Map<String, ModelProvider> providers;
    private final SafeTelemetry telemetry;
    private final io.durableflow.ai.routing.ModelRoutingService routing;
    public ModelGateway(List<ModelProvider> providers) {this(providers,null);}
    public ModelGateway(List<ModelProvider> providers,io.durableflow.ai.routing.ModelRoutingService routing) {this(providers,routing,SafeTelemetry.disabled());}
    @org.springframework.beans.factory.annotation.Autowired
    public ModelGateway(List<ModelProvider> providers,io.durableflow.ai.routing.ModelRoutingService routing,SafeTelemetry telemetry) {
        this.telemetry=telemetry;
        this.routing=routing;
        var indexed = new HashMap<String, ModelProvider>();
        for (var provider : providers) {
            if (provider.id() == null || provider.id().isBlank() || indexed.putIfAbsent(provider.id(), provider) != null) {
                throw new IllegalArgumentException("Provider ids must be unique and nonblank");
            }
        }
        this.providers = Map.copyOf(indexed);
    }
    public ModelResponse call(ModelRequest request,String operationKey) throws Exception {return call(request,operationKey,null);}
    public ModelResponse call(ModelRequest request, String operationKey,io.durableflow.workflow.activity.ActivityExecutionContext context) throws Exception {
        Objects.requireNonNull(request);
        if (operationKey == null || operationKey.isBlank()) throw new IllegalArgumentException("Model operation key required");
        if (request.provider().equals("routed") || routing!=null && request.provider().equals("fake") && request.model().startsWith("fake-agent-")) {
            if(routing==null)throw new IllegalArgumentException("MODEL_ROUTING_NOT_CONFIGURED");
            return routing.call(request,operationKey,context==null?null:context.ownership());
        }
        if(Set.of("fake-a","fake-b").contains(request.provider()))throw new IllegalArgumentException("MODEL_ROUTED_PROVIDER_REQUIRES_ROUTER");
        var provider = providers.get(request.provider());
        if (provider == null) throw new IllegalArgumentException("MODEL_INVALID_PROVIDER_CONFIGURATION");
        long started=System.nanoTime();String status="FAILURE";
        var labels=Map.of("provider",request.provider(),"model","legacy");
        telemetry.count(Metric.MODEL_ATTEMPTS,labels);
        try {
            var response = Objects.requireNonNull(provider.generate(request, operationKey), "Model response required");
            if (!provider.id().equals(response.provider()) || (request.structuredOutput() && response.structuredJson() == null)) {
                throw new ModelProviderException(ModelProviderException.Kind.MALFORMED_RESPONSE);
            }
            telemetry.sample(Metric.MODEL_INPUT_TOKENS,response.usage().inputTokens(),labels);
            telemetry.sample(Metric.MODEL_OUTPUT_TOKENS,response.usage().outputTokens(),labels);
            telemetry.sample(Metric.MODEL_COST_MICROS,response.usage().inputTokens()+2.0*response.usage().outputTokens(),labels);
            status="SUCCESS";return response;
        } catch (ModelProviderException failure) {
            switch (failure.kind()) {
                case RATE_LIMITED, UNAVAILABLE, TIMEOUT -> throw new RetryableActivityException("MODEL_" + failure.kind());
                default -> throw new IllegalArgumentException("MODEL_" + failure.kind());
            }
        } finally {
            telemetry.duration(Metric.MODEL_ATTEMPT_DURATION,System.nanoTime()-started,Map.of("provider",request.provider(),"model","legacy","status",status));
        }
    }
}
