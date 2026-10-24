package io.durableflow.workflow.activity.demo;

import io.durableflow.workflow.activity.ActivityHandler;
import io.durableflow.workflow.activity.ActivityExecutionContext;
import org.springframework.stereotype.Component;
import java.util.Map;

/** Deterministic demo only; no external services. */
@Component
public class ChargePaymentActivity implements ActivityHandler {
    private final FakePaymentProvider provider;

    public ChargePaymentActivity(FakePaymentProvider provider) { this.provider = provider; }

    public String type() { return "chargePayment"; }
    public Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input) {
        return provider.charge(context.idempotencyKey(), input);
    }
}
