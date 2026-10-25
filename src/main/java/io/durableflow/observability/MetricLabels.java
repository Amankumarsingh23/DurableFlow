package io.durableflow.observability;
import java.util.*;

/** Values outside the trusted catalog collapse to other, even if shaped like a safe identifier. */
public final class MetricLabels {
    private MetricLabels() {}
    private static final Map<String,Set<String>> VALUES=Map.of(
        "provider",Set.of("fake","fake-a","fake-b","fake-mcp"),
        "model",Set.of("balanced-v1","premium-v1","economy-v1","text-v1","agent","legacy"),
        "tool",Set.of("getOrder","getRefundPolicy","refundOrder"),
        "risk",Set.of("READ_ONLY","LOW_RISK_WRITE","HIGH_RISK_WRITE"),
        "strategy",Set.of("DENSE","SPARSE","HYBRID"),
        "status",Set.of("STARTED","SUCCESS","FAILURE","COMPLETED","FAILED","COMPENSATED","COMPENSATION_FAILED","COMPENSATING","LIMIT_REACHED","RUNNING","WAITING_APPROVAL","READY","CLAIMED","CREATED","APPROVED","REJECTED","EXECUTED","RETRY","TIMEOUT","RECLAIMED","OPEN","CLOSED","HALF_OPEN","SCHEDULED"),
        "activity",Set.of("retrieveContext","modelCall","agentModelCall","executeTool","createToolRequest","other"),
        "reason",Set.of("FINAL_ANSWER","MAX_ITERATIONS","MAX_MODEL_CALLS","MAX_TOOL_CALLS","CONTEXT_LIMIT","DUPLICATE_WRITE","INVALID_MODEL_DECISION","INVALID_TOOL_PROPOSAL","MODEL_ACTIVITY_FAILED","ACTIVITY_TIMEOUT","TRANSIENT_FAILURE","RETRY_BUDGET_EXHAUSTED","NON_RETRYABLE_FAILURE","MAX_TOTAL_TOKENS","MAX_ESTIMATED_COST","TOOL_REJECTED","MODEL_ROUTE_EXHAUSTED","HEALTH_RETRIES_EXHAUSTED","CIRCUIT_OPEN","RECOVERY_ROUTE_ATTEMPTS_SPENT"));
    public static Map<String,String> forMetric(Metric metric,Map<String,String> input) {
        Set<String> keys=switch(metric) {
            case WORKFLOW_EVENTS,WORKFLOW_DURATION,ACTIVITY_EVENTS,ACTIVITY_DURATION,QUEUE_DEPTH -> Set.of("status");
            case ACTIVITY_ATTEMPT_DURATION -> Set.of("activity","status");
            case MODEL_ATTEMPTS,MODEL_ATTEMPT_DURATION,MODEL_INPUT_TOKENS,MODEL_OUTPUT_TOKENS,MODEL_COST_MICROS,MODEL_LOGICAL_CALLS -> Set.of("provider","model","status");
            case MODEL_RETRIES,MODEL_CIRCUIT_SKIPS -> Set.of("provider");
            case MODEL_FALLBACKS -> Set.of("reason");
            case CIRCUIT_TRANSITIONS -> Set.of("provider","status");
            case RETRIEVAL_CALLS,RETRIEVAL_DURATION,RETRIEVAL_CANDIDATES,RETRIEVAL_SELECTED,RETRIEVAL_CONTEXT_CHARS,RETRIEVAL_BUDGET_UTILIZATION -> Set.of("strategy","status");
            case TOOL_EVENTS,TOOL_ATTEMPTS,TOOL_DURATION,TOOL_APPROVAL_WAIT,TOOL_RETRIES,TOOL_DEDUP -> Set.of("tool","risk","status");
            case AGENT_EVENTS,AGENT_MODEL_CALLS,AGENT_MODEL_ATTEMPTS,AGENT_TOOL_CALLS,AGENT_TOKENS,AGENT_COST_MICROS,AGENT_APPROVAL_WAITS,AGENT_FALLBACKS -> Set.of("status","reason");
            default -> Set.of();
        };
        var filtered=new HashMap<String,String>();input.forEach((key,value) -> {if(keys.contains(key))filtered.put(key,value);});
        return bounded(filtered);
    }
    public static Map<String,String> bounded(Map<String,String> input) {
        var result=new TreeMap<String,String>();
        input.forEach((key,value) -> {var allowed=VALUES.get(key);if(allowed!=null)result.put(key,allowed.contains(value)?value:"other");});
        return Collections.unmodifiableMap(result);
    }
}
