package io.durableflow.worker.retry;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import java.util.concurrent.ThreadLocalRandom;

@Configuration(proxyBeanMethods = false)
public class RetryConfiguration {
    @Bean
    RetryPolicy retryPolicy(@Value("${durableflow.worker.retry.max-attempts:3}") int attempts,
                            @Value("${durableflow.worker.retry.initial-delay-ms:1000}") long initial,
                            @Value("${durableflow.worker.retry.multiplier:2}") double multiplier,
                            @Value("${durableflow.worker.retry.max-delay-ms:30000}") long maximum) {
        return new RetryPolicy(attempts, initial, multiplier, maximum, RetryPolicy.Jitter.FULL);
    }

    @Bean
    RetryDelayCalculator retryDelayCalculator() {
        return new RetryDelayCalculator(() -> ThreadLocalRandom.current().nextDouble());
    }
}
