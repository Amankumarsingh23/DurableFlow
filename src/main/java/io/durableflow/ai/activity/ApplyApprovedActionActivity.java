package io.durableflow.ai.activity;

import io.durableflow.ai.demo.FakeApprovedActionProvider;
import io.durableflow.workflow.activity.*;
import org.springframework.stereotype.Component;
import java.util.*;

/** Approval is a workflow dependency; model-generated text never invokes an effect here. */
@Component
public class ApplyApprovedActionActivity implements ActivityHandler {
    private final FakeApprovedActionProvider provider;
    public ApplyApprovedActionActivity(FakeApprovedActionProvider provider) { this.provider = provider; }
    public String type() { return "applyApprovedAction"; }
    public Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input) {
        if (!(input.get("approvedAction") instanceof Map<?, ?> action) || !Set.of("name", "target").containsAll(action.keySet())
                || !(action.get("name") instanceof String name) || !(action.get("target") instanceof String target)) {
            throw new IllegalArgumentException("approvedAction object with name and target required");
        }
        return provider.apply(context.idempotencyKey(), new FakeApprovedActionProvider.Action(name, target));
    }
}
