package io.durableflow.observability;
import java.util.*;

/** Deny by default. Never accepts nested objects, arbitrary messages or business text. */
public final class SafeMetadata {
    private SafeMetadata() {}
    private static final Set<String> IDS=Set.of("workflowRunId","activityExecutionId","agentExecutionId","logicalModelCallId","toolRequestId");
    private static final Set<String> COUNTS=Set.of("iteration","attempts","count","inputTokens","outputTokens","costMicros","size","candidateCount","selectedCount","contextChars","budgetChars");
    private static final Set<String> CLASSIFICATIONS=Set.of("provider","model","tool","risk","status","reason","strategy","activity");
    public static Map<String,Object> sanitize(Map<String,?> input) {
        var result=new TreeMap<String,Object>();
        for(var entry:input.entrySet()) {
            String key=switch(entry.getKey()) {case "toolName" -> "tool";case "riskLevel" -> "risk";case "code" -> "reason";case "attemptCount" -> "attempts";default -> entry.getKey();};Object value=entry.getValue();
            if(value==null)continue;
            if(IDS.contains(key)) {try {result.put(key,UUID.fromString(value.toString()).toString());}catch(IllegalArgumentException ignored) {}}
            else if(COUNTS.contains(key) && value instanceof Number n && Double.isFinite(n.doubleValue()) && n.doubleValue()>=0 && n.doubleValue()<=1e15)result.put(key,n.longValue());
            else if(CLASSIFICATIONS.contains(key) && value instanceof String s)result.put(key,MetricLabels.bounded(Map.of(key,s)).get(key));
            else if(key.equals("fingerprint") && value instanceof String s && s.matches("[a-f0-9]{64}"))result.put(key,s);
            // Only the local fake provider format is trusted. Real providers need a deliberate allowlist.
            else if(key.equals("providerRequestId") && value instanceof String s && s.matches("(?:fake-|fake-[ab]-fake-)[a-f0-9-]{36}"))result.put(key,s);
        }
        return Collections.unmodifiableMap(result);
    }
}
