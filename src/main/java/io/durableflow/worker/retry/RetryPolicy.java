package io.durableflow.worker.retry;

/** Total attempts include the initial execution. Delays are milliseconds. */
public record RetryPolicy(int maxAttempts, long initialDelayMillis, double multiplier,
                          long maxDelayMillis, Jitter jitter) {
    public enum Jitter { FULL, NONE }

    public RetryPolicy {
        if (maxAttempts < 1 || initialDelayMillis < 0 || maxDelayMillis < initialDelayMillis
                || !Double.isFinite(multiplier) || multiplier < 1 || jitter == null) {
            throw new IllegalArgumentException("Invalid retry policy");
        }
    }
}
