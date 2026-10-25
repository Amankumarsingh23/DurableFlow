package io.durableflow.ai.model;

import java.math.BigDecimal;
import java.util.*;

/** Version 1 is a frozen, explicit request contract stored in ActivityExecution.input_json. */
public record ModelRequest(int requestVersion, String provider, String model, String systemInstructions,
        String userContent, Double temperature, int maxOutputTokens, boolean structuredOutput) {
    private static final Set<String> FIELDS = Set.of("requestVersion", "provider", "model", "systemInstructions",
            "userContent", "temperature", "maxOutputTokens", "structuredOutput");
    public ModelRequest {
        // Version 1's optional temperature has a fixed default, never an environment/provider lookup.
        if (temperature == null) temperature = 0.0;
        if (requestVersion != 1 || provider == null || !provider.matches("[a-z][a-z0-9-]{0,63}")
                || model == null || model.isBlank() || model.length() > 200
                || systemInstructions == null || systemInstructions.length() > 64_000
                || userContent == null || userContent.isBlank() || userContent.length() > 64_000
                || maxOutputTokens < 1 || maxOutputTokens > 16_384
                || (temperature != null && (!Double.isFinite(temperature) || temperature < 0 || temperature > 2))) {
            throw new IllegalArgumentException("Invalid versioned model request");
        }
    }
    public static ModelRequest fromInput(Map<String, Object> input) {
        if (!(input.get("modelRequest") instanceof Map<?, ?> request)) throw new IllegalArgumentException("modelRequest object required");
        if (!FIELDS.containsAll(request.keySet())) throw new IllegalArgumentException("Unknown version 1 modelRequest field");
        Object structured = request.get("structuredOutput");
        if (structured != null && !(structured instanceof Boolean)) throw new IllegalArgumentException("structuredOutput must be boolean");
        Object temperature = request.get("temperature");
        if (temperature != null && !(temperature instanceof Number)) throw new IllegalArgumentException("temperature must be numeric");
        return new ModelRequest(integer(request.get("requestVersion")), text(request.get("provider")), text(request.get("model")),
                text(request.get("systemInstructions")), text(request.get("userContent")),
                temperature == null ? null : ((Number) temperature).doubleValue(), integer(request.get("maxOutputTokens")), Boolean.TRUE.equals(structured));
    }
    private static String text(Object value) {
        if (!(value instanceof String text)) throw new IllegalArgumentException("Model request text field required");
        return text;
    }
    private static int integer(Object value) {
        if (!(value instanceof Number)) throw new IllegalArgumentException("Model request integer field required");
        try { return new BigDecimal(value.toString()).intValueExact(); }
        catch (ArithmeticException | NumberFormatException invalid) { throw new IllegalArgumentException("Model request integer required"); }
    }
}
