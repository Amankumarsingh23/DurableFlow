package io.durableflow.workflow.activity.demo;

import io.durableflow.workflow.activity.*;
import org.springframework.stereotype.Component;
import java.util.Map;

/** Deterministic stand-in for the human-approval walkthrough; no external effect. */
@Component
public class SubmitRequestActivity implements ActivityHandler {
    public String type() { return "submitRequest"; }
    public Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input) {
        return Map.of("requestId", context.workflowRunId().toString(), "status", "SUBMITTED");
    }
}
