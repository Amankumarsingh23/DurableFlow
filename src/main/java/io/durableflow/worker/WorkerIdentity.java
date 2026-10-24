package io.durableflow.worker;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.UUID;

@Component
public class WorkerIdentity {
    private final String id;

    public WorkerIdentity(@Value("${durableflow.worker.id:}") String configured) {
        id = configured.isBlank() ? "worker-" + UUID.randomUUID() : configured.strip();
        if (id.length() > 200) throw new IllegalArgumentException("Worker ID exceeds 200 characters");
    }

    public String id() { return id; }
}
