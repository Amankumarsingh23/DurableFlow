package io.durableflow.worker;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public record WorkerExecutionSettings(long timeoutMillis, long shutdownGraceMillis) {
    public WorkerExecutionSettings(
            @Value("${durableflow.worker.execution-timeout-ms:60000}") long timeoutMillis,
            @Value("${durableflow.worker.shutdown-grace-ms:10000}") long shutdownGraceMillis) {
        if (timeoutMillis <= 0 || shutdownGraceMillis < 0) {
            throw new IllegalArgumentException("Execution timeout must be positive and shutdown grace nonnegative");
        }
        this.timeoutMillis = timeoutMillis;
        this.shutdownGraceMillis = shutdownGraceMillis;
    }
}
