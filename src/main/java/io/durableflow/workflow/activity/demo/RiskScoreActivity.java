package io.durableflow.workflow.activity.demo;

import io.durableflow.workflow.activity.*;
import org.springframework.stereotype.Component;
import java.util.Map;

/** Pure static-DAG demo; no external service. */
@Component
public class RiskScoreActivity implements ActivityHandler {
    public String type() { return "scoreRisk"; }
    public Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input) {
        if (Boolean.TRUE.equals(input.get("simulateRiskFailure"))) throw new IllegalArgumentException("Simulated permanent risk failure");
        return Map.of("riskScore", "LOW");
    }
}
