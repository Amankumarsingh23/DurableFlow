package io.durableflow.workflow.activity.demo;

import io.durableflow.workflow.activity.ActivityHandler;
import org.springframework.stereotype.Component;
import java.util.Map;

/** Deterministic demo only; no external services. */
@Component
public class ChargePaymentActivity implements ActivityHandler {
    public String type() { return "chargePayment"; }
    public Map<String, Object> execute(Map<String, Object> input) {
        return Map.of("paymentId", "pay-demo-123");
    }
}
