package io.durableflow.ai.demo;

import org.springframework.stereotype.Component;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** A single named demo effect with per-JVM idempotency, not an arbitrary tool dispatcher. */
@Component
public class FakeApprovedActionProvider {
    public record Action(String name, String target) {
        public Action {
            if (!"markReviewed".equals(name) || target == null || target.isBlank() || target.length() > 200) {
                throw new IllegalArgumentException("Invalid approved demo action");
            }
        }
    }
    private record Effect(Action request, Map<String, Object> result) {}
    private final ConcurrentHashMap<String, Effect> effects = new ConcurrentHashMap<>();
    public Map<String, Object> apply(String key, Action action) {
        if (key == null || key.isBlank()) throw new IllegalArgumentException("Action idempotency key required");
        Objects.requireNonNull(action);
        return effects.compute(key, (ignored, existing) -> {
            if (existing != null) {
                if (!existing.request().equals(action)) throw new IllegalArgumentException("Action key reused with different payload");
                return existing;
            }
            return new Effect(action, Map.of("actionId", "action-demo-" + UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8)),
                    "status", "APPLIED", "target", action.target()));
        }).result();
    }
    public int effectCount() { return effects.size(); }
}
