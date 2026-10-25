package io.durableflow.ai.model;

import io.durableflow.ai.provider.*;
import io.durableflow.worker.retry.RetryableActivityException;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
public class ModelGateway {
    private final Map<String, ModelProvider> providers;
    public ModelGateway(List<ModelProvider> providers) {
        var indexed = new HashMap<String, ModelProvider>();
        for (var provider : providers) {
            if (provider.id() == null || provider.id().isBlank() || indexed.putIfAbsent(provider.id(), provider) != null) {
                throw new IllegalArgumentException("Provider ids must be unique and nonblank");
            }
        }
        this.providers = Map.copyOf(indexed);
    }
    public ModelResponse call(ModelRequest request, String operationKey) throws Exception {
        Objects.requireNonNull(request);
        if (operationKey == null || operationKey.isBlank()) throw new IllegalArgumentException("Model operation key required");
        var provider = providers.get(request.provider());
        if (provider == null) throw new IllegalArgumentException("MODEL_INVALID_PROVIDER_CONFIGURATION");
        try {
            var response = Objects.requireNonNull(provider.generate(request, operationKey), "Model response required");
            if (!provider.id().equals(response.provider()) || (request.structuredOutput() && response.structuredJson() == null)) {
                throw new ModelProviderException(ModelProviderException.Kind.MALFORMED_RESPONSE);
            }
            return response;
        } catch (ModelProviderException failure) {
            switch (failure.kind()) {
                case RATE_LIMITED, UNAVAILABLE, TIMEOUT -> throw new RetryableActivityException("MODEL_" + failure.kind());
                default -> throw new IllegalArgumentException("MODEL_" + failure.kind());
            }
        }
    }
}
