package io.durableflow.workflow.activity.demo;

import io.durableflow.workflow.activity.*;
import org.springframework.stereotype.Component;
import java.util.Map;

/** Pure static-DAG demo; no external service. */
@Component
public class FraudCheckActivity implements ActivityHandler {
    public String type() { return "checkFraud"; }
    public Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input) {
        return Map.of("fraudCheck", "PASSED");
    }
}
