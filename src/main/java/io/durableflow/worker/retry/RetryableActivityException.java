package io.durableflow.worker.retry;

/** An activity adapter explicitly identifies a transient failure; unknown exceptions are terminal. */
public class RetryableActivityException extends RuntimeException {
    public RetryableActivityException(String message) { super(message); }
}
