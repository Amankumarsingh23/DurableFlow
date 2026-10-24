package io.durableflow.workflow.activity.demo;

import io.durableflow.workflow.activity.ActivityHandler;
import io.durableflow.workflow.activity.ActivityExecutionContext;
import org.springframework.stereotype.Component;
import java.util.Map;

/** Deterministic demo only; no external services. */
@Component
public class CreateShipmentActivity implements ActivityHandler {
    public String type() { return "createShipment"; }
    public Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input) {
        return Map.of("shipmentId", "ship-demo-789");
    }
}
