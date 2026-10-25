package io.durableflow.agent;
import java.util.*;
public record AgentIteration(UUID id,UUID agentExecutionId,int number,UUID modelRunId,UUID modelActivityId,Status status,
        Map<String,Object> decision,UUID toolRunId,UUID toolRequestId,Map<String,Object> toolResult,long reservedTokens,long reservedCostMicros) {
    public enum Status { MODEL_PENDING, DECIDED, TOOL_PENDING, COMPLETED, FAILED }
}
