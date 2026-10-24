package io.durableflow.workflow.activity.demo;

import io.durableflow.workflow.activity.ActivityHandler;
import io.durableflow.workflow.activity.ActivityExecutionContext;
import org.springframework.stereotype.Component;
import java.util.Map;

/** Deterministic demo only; no external services. */
@Component
public class ReserveInventoryActivity implements ActivityHandler {
    public String type() { return "reserveInventory"; }
    public Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input) {
        return Map.of("reservationId", "res-demo-456");
    }
}
