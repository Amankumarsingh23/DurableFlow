package io.durableflow.workflow.activity;

import java.util.Map;

/** Input/output contain JSON-compatible values. No database transaction surrounds execute.
 * Side-effecting handlers must pass the context's stable key to a provider that honors it.
 */
public interface ActivityHandler {
    String type();
    Map<String, Object> execute(ActivityExecutionContext context, Map<String, Object> input) throws Exception;
}
