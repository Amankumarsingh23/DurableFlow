package io.durableflow.workflow.activity.demo;

import org.springframework.stereotype.Component;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** In-memory provider simulation, shared within one application only; not a real payment service. */
@Component
public class FakePaymentProvider {
    private record Payment(Object payload, Map<String, Object> result) {}
    private final ConcurrentHashMap<String, Payment> payments = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Payment> refunds = new ConcurrentHashMap<>();

    public Map<String, Object> charge(String key, Map<String, Object> input) {
        return effect(payments, key, input, "pay-demo-", "paymentId");
    }

    public Map<String, Object> refund(String key, Map<String, Object> input) {
        if (!(input.get("paymentId") instanceof String id) || id.isBlank()) {
            throw new IllegalArgumentException("Refund requires original paymentId");
        }
        return effect(refunds, key, input, "refund-demo-", "refundId");
    }

    public int refundEffectCount() { return refunds.size(); }

    private Map<String, Object> effect(ConcurrentHashMap<String, Payment> effects, String key,
            Map<String, Object> input, String prefix, String resultField) {
        if (key == null || key.isBlank()) throw new IllegalArgumentException("Idempotency key must be nonblank");
        Objects.requireNonNull(input, "Payment payload");
        Object payload = canonical(input);
        // Atomic compare/create/result reuse: never check then perform a separate effect.
        return effects.compute(key, (ignored, existing) -> {
            if (existing != null) {
                if (!existing.payload().equals(payload)) {
                    throw new IllegalArgumentException("Idempotency key reused with different payment payload: " + key);
                }
                return existing;
            }
            String id = prefix + UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8));
            return new Payment(payload, Map.of(resultField, id));
        }).result();
    }

    /** One remembered entry represents one actual simulated charge, not the number of API invocations. */
    public int effectCount() { return payments.size(); }

    // Compare the complete JSON payload structurally; map order and equivalent numeric forms do not matter.
    // Snapshot nested values so later caller mutation cannot change the remembered request.
    static Object canonical(Object value) {
        if (value == null || value instanceof String || value instanceof Boolean) return value;
        if (value instanceof Number number) return new BigDecimal(number.toString()).stripTrailingZeros();
        if (value instanceof Map<?, ?> map) {
            var copy = new TreeMap<String, Object>();
            map.forEach((key, item) -> {
                if (!(key instanceof String text)) throw new IllegalArgumentException("Payment payload keys must be strings");
                copy.put(text, canonical(item));
            });
            return Collections.unmodifiableMap(copy);
        }
        if (value instanceof List<?> list) return list.stream().map(FakePaymentProvider::canonical).toList();
        throw new IllegalArgumentException("Payment payload must contain JSON values");
    }
}
