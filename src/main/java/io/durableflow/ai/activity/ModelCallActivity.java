package io.durableflow.ai.activity;

import io.durableflow.ai.model.*;
import io.durableflow.workflow.activity.*;
import org.springframework.stereotype.Component;
import java.util.*;

@Component
public class ModelCallActivity implements ActivityHandler {
    private final ModelGateway gateway;
    public ModelCallActivity(ModelGateway gateway) { this.gateway = gateway; }
    public String type() { return "modelCall"; }
    public Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input) throws Exception {
        var response = gateway.call(ModelRequest.fromInput(input), context.idempotencyKey());
        var output = new LinkedHashMap<String, Object>();
        output.put("outputVersion", 1); output.put("provider", response.provider()); output.put("model", response.model());
        if (response.content() != null) output.put("content", response.content());
        if (response.structuredJson() != null) output.put("structuredJson", response.structuredJson());
        output.put("usage", Map.of("inputTokens", response.usage().inputTokens(), "outputTokens", response.usage().outputTokens()));
        output.put("latencyMillis", response.latencyMillis());
        if (response.providerRequestId() != null) output.put("providerRequestId", response.providerRequestId());
        return output;
    }
}
