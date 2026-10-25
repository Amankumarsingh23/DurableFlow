package io.durableflow.workflow.activity.demo;

import io.durableflow.workflow.activity.*;
import org.springframework.stereotype.Component;
import java.util.Map;

@Component
public class RefundPaymentActivity implements ActivityHandler {
    private final FakePaymentProvider provider;
    public RefundPaymentActivity(FakePaymentProvider provider) { this.provider = provider; }
    public String type() { return "refundPayment"; }
    public Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input) {
        if (!context.compensation()) throw new IllegalArgumentException("Refund requires compensation context");
        return provider.refund(context.idempotencyKey(), Map.of("paymentId", context.originalOutput().get("paymentId"),
                "originalInput", context.originalInput()));
    }
}
