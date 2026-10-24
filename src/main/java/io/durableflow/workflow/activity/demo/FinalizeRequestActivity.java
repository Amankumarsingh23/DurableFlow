package io.durableflow.workflow.activity.demo;

import io.durableflow.workflow.activity.*;
import org.springframework.stereotype.Component;
import java.util.Map;

/** Receiving a signal unlocks this demo; it does not evaluate an approval/rejection rule. */
@Component
public class FinalizeRequestActivity implements ActivityHandler {
    public String type() { return "finalizeRequest"; }
    public Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input) {
        return Map.of("requestId", context.workflowRunId().toString(), "status", "FINALIZED");
    }
}
