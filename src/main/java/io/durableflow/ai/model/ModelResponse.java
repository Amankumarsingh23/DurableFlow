package io.durableflow.ai.model;

import java.util.*;

/** Exactly one generated representation: text content or a structured JSON object. */
public record ModelResponse(String content, Map<String, Object> structuredJson, String provider, String model,
        ModelUsage usage, long latencyMillis, String providerRequestId) {
    public ModelResponse {
        if ((content == null) == (structuredJson == null) || (content != null && content.isBlank())
                || provider == null || provider.isBlank() || model == null || model.isBlank()
                || usage == null || latencyMillis < 0) throw new IllegalArgumentException("Invalid model response");
        if (structuredJson != null) structuredJson = Collections.unmodifiableMap(new LinkedHashMap<>(structuredJson));
    }
}
