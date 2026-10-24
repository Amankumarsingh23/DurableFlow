package io.durableflow.workflow.activity;

import java.util.Map;

/** Input/output contain JSON-compatible values. No database transaction surrounds execute. */
public interface ActivityHandler {
    String type();
    Map<String, Object> execute(Map<String, Object> input) throws Exception;
}
