package io.durableflow.ai.model;

public record ModelUsage(long inputTokens, long outputTokens) {
    public ModelUsage {
        if (inputTokens < 0 || outputTokens < 0) throw new IllegalArgumentException("Token counts must be nonnegative");
    }
}
