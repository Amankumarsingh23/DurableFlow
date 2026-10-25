package io.durableflow.ai.provider;

import io.durableflow.ai.model.*;

/** Provider-neutral adapter. operationKey is correlation, not a promise of provider idempotency. */
public interface ModelProvider {
    String id();
    ModelResponse generate(ModelRequest request, String operationKey) throws Exception;
}
