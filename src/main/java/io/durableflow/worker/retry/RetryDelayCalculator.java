package io.durableflow.worker.retry;

import java.util.function.DoubleSupplier;

public class RetryDelayCalculator {
    private final DoubleSupplier random;
    public RetryDelayCalculator(DoubleSupplier random) { this.random = random; }

    public long delayMillis(RetryPolicy policy, int attemptNumber) {
        if (attemptNumber < 1) throw new IllegalArgumentException("Attempt number must be positive");
        // Log-space comparison prevents overflow, even for huge multipliers/attempt counts.
        long initial = policy.initialDelayMillis();
        long maximum = policy.maxDelayMillis();
        long cap;
        if (initial == 0) cap = 0;
        else if (policy.multiplier() == 1 || attemptNumber == 1) cap = initial;
        else {
            double growth = (attemptNumber - 1.0) * Math.log(policy.multiplier());
            cap = growth >= Math.log((double) maximum / initial) ? maximum
                    : Math.min(maximum, (long) (initial * Math.pow(policy.multiplier(), attemptNumber - 1.0)));
        }
        if (policy.jitter() == RetryPolicy.Jitter.NONE || cap == 0) return cap;
        double sample = random.getAsDouble();
        if (!Double.isFinite(sample) || sample < 0 || sample > 1) throw new IllegalStateException("Random sample must be in [0,1]");
        return Math.min(cap, (long) (sample * cap));
    }
}
