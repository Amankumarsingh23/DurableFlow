package io.durableflow.agent;
import java.util.*;

/** Immutable per-agent limits. Reservations cover every permitted provider attempt. */
public record AgentLimits(int maxIterations,int maxLogicalModelCalls,int maxLogicalToolCalls,long maxTotalTokens,
        long maxEstimatedCostMicros,int maxContextChars,int maxOutputTokens) {
    public AgentLimits {
        if(maxIterations<1 || maxIterations>20 || maxLogicalModelCalls<1 || maxLogicalModelCalls>20 || maxLogicalToolCalls<0 || maxLogicalToolCalls>20
                || maxTotalTokens<1 || maxTotalTokens>2000000 || maxEstimatedCostMicros<1 || maxEstimatedCostMicros>10000000
                || maxContextChars<1000 || maxContextChars>24000 || maxOutputTokens<24 || maxOutputTokens>1024)
            throw new IllegalArgumentException("AGENT_INVALID_LIMITS");
    }
    public static AgentLimits defaults() { return new AgentLimits(8,8,6,500000,1000000,12000,128); }
    public Map<String,Object> toMap() { return Map.of("maxIterations",maxIterations,"maxLogicalModelCalls",maxLogicalModelCalls,"maxLogicalToolCalls",maxLogicalToolCalls,
            "maxTotalTokens",maxTotalTokens,"maxEstimatedCostMicros",maxEstimatedCostMicros,"maxContextChars",maxContextChars,"maxOutputTokens",maxOutputTokens); }
    public static AgentLimits from(Map<String,Object> m) { return new AgentLimits(n(m,"maxIterations").intValue(),n(m,"maxLogicalModelCalls").intValue(),n(m,"maxLogicalToolCalls").intValue(),
            n(m,"maxTotalTokens").longValue(),n(m,"maxEstimatedCostMicros").longValue(),n(m,"maxContextChars").intValue(),n(m,"maxOutputTokens").intValue()); }
    private static Number n(Map<String,Object> m,String k) { return (Number)m.get(k); }
}
