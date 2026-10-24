package io.durableflow.worker.retry;

/** Local supervision deadline; does not assert that an external operation was cancelled. */
public class ActivityTimeoutException extends RetryableActivityException {
    private final long timeoutMillis;
    public ActivityTimeoutException(long millis) {
        super("Activity attempt exceeded execution timeout of " + millis + "ms");
        timeoutMillis = millis;
    }
    public long timeoutMillis() { return timeoutMillis; }
}
