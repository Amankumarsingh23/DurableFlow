package io.durableflow.worker.retry;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class RetryPolicyTest {
    @Test
    void exponentialDelayCapsAndFullJitterIsDeterministic() {
        var policy = new RetryPolicy(5, 100, 2, 350, RetryPolicy.Jitter.NONE);
        var calculator = new RetryDelayCalculator(() -> { throw new AssertionError("No jitter requested"); });
        assertThat(calculator.delayMillis(policy, 1)).isEqualTo(100);
        assertThat(calculator.delayMillis(policy, 2)).isEqualTo(200);
        assertThat(calculator.delayMillis(policy, 3)).isEqualTo(350);
        assertThat(calculator.delayMillis(policy, Integer.MAX_VALUE)).isEqualTo(350);
        var jittered = new RetryPolicy(5, 100, 2, 350, RetryPolicy.Jitter.FULL);
        assertThat(new RetryDelayCalculator(() -> 0).delayMillis(jittered, 3)).isZero();
        assertThat(new RetryDelayCalculator(() -> 0.5).delayMillis(jittered, 3)).isEqualTo(175);
        assertThat(new RetryDelayCalculator(() -> 1).delayMillis(jittered, 3)).isEqualTo(350);
    }

    @Test
    void hugeGrowthAndZeroDelayCannotOverflow() {
        var calculator = new RetryDelayCalculator(() -> 1);
        assertThat(calculator.delayMillis(new RetryPolicy(3, 1000, Double.MAX_VALUE,
                Long.MAX_VALUE, RetryPolicy.Jitter.NONE), Integer.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
        assertThat(calculator.delayMillis(new RetryPolicy(3, 0, Double.MAX_VALUE,
                Long.MAX_VALUE, RetryPolicy.Jitter.FULL), Integer.MAX_VALUE)).isZero();
        assertThat(calculator.delayMillis(new RetryPolicy(3, 10, 1, 100,
                RetryPolicy.Jitter.NONE), Integer.MAX_VALUE)).isEqualTo(10);
    }

    @Test
    void invalidPoliciesAndRandomnessAreRejected() {
        assertThatThrownBy(() -> new RetryPolicy(0, 0, 2, 10, RetryPolicy.Jitter.FULL)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryPolicy(1, -1, 2, 10, RetryPolicy.Jitter.FULL)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryPolicy(1, 20, 2, 10, RetryPolicy.Jitter.FULL)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryPolicy(1, 1, 0.5, 10, RetryPolicy.Jitter.FULL)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryPolicy(1, 1, Double.NaN, 10, RetryPolicy.Jitter.FULL)).isInstanceOf(IllegalArgumentException.class);
        var policy = new RetryPolicy(1, 10, 2, 10, RetryPolicy.Jitter.FULL);
        assertThatThrownBy(() -> new RetryDelayCalculator(() -> -0.1).delayMillis(policy, 1)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> new RetryDelayCalculator(() -> 0.5).delayMillis(policy, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void classificationIsTypedAndConservative() {
        var classifier = new FailureClassifier();
        assertThat(classifier.classify(new RetryableActivityException("transient")))
                .isEqualTo(FailureClassifier.Classification.RETRYABLE);
        assertThat(classifier.classify(new IllegalArgumentException("Idempotency key reused with different payment payload")))
                .isEqualTo(FailureClassifier.Classification.NON_RETRYABLE);
        assertThat(classifier.classify(new IllegalStateException("HTTP 503 transient retry me")))
                .isEqualTo(FailureClassifier.Classification.NON_RETRYABLE);
    }
}
