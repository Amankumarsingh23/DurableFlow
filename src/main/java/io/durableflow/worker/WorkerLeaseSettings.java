package io.durableflow.worker;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class WorkerLeaseSettings {
    private final long leaseMillis;
    private final long heartbeatMillis;

    public WorkerLeaseSettings(@Value("${durableflow.worker.lease-duration-ms:30000}") long leaseMillis,
                              @Value("${durableflow.worker.heartbeat-interval-ms:10000}") long heartbeatMillis) {
        if (leaseMillis < 3 || heartbeatMillis < 1 || heartbeatMillis > leaseMillis / 3) {
            throw new IllegalArgumentException("Lease must be positive and heartbeat interval at most one third of lease duration");
        }
        this.leaseMillis = leaseMillis;
        this.heartbeatMillis = heartbeatMillis;
    }

    public long leaseMillis() { return leaseMillis; }
    public long heartbeatMillis() { return heartbeatMillis; }
}
