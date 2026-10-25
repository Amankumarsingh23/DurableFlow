package io.durableflow.workflow.activity.demo;

import io.durableflow.workflow.activity.*;
import org.springframework.stereotype.Component;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** Per-JVM release deduplication; the reservation demo itself has no external stock system. */
@Component
public class ReleaseInventoryActivity implements ActivityHandler {
    private record Release(Object payload, Map<String, Object> result) {}
    private final ConcurrentHashMap<String, Release> releases = new ConcurrentHashMap<>();
    public String type() { return "releaseInventory"; }
    public Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input) {
        if (!context.compensation()) throw new IllegalArgumentException("Release requires compensation context");
        var payload = FakePaymentProvider.canonical(Map.of("reservationId", context.originalOutput().get("reservationId"),
                "originalInput", context.originalInput()));
        String key = context.idempotencyKey();
        return releases.compute(key, (ignored, existing) -> {
            if (existing != null) {
                if (!existing.payload().equals(payload)) throw new IllegalArgumentException("Release key reused with different payload");
                return existing;
            }
            return new Release(payload, Map.of("releaseId", "release-demo-" +
                    UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8))));
        }).result();
    }
}
