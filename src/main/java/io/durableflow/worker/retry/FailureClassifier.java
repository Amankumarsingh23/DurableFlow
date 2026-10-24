package io.durableflow.worker.retry;

import org.springframework.stereotype.Component;

@Component
public class FailureClassifier {
    public enum Classification { RETRYABLE, NON_RETRYABLE }
    public Classification classify(Exception failure) {
        return failure instanceof RetryableActivityException ? Classification.RETRYABLE : Classification.NON_RETRYABLE;
    }
}
