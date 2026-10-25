package io.durableflow.agent;
import java.util.*;
public record AgentExecution(UUID id,UUID workflowRunId,Status status,String goal,String scenario,AgentLimits limits,int maxModelAttempts,
        Map<String,Object> frozenContext,int currentIteration,int logicalModelCalls,int modelAttempts,int logicalToolCalls,
        long inputTokens,long outputTokens,long estimatedCostMicros,long reservedTokens,long reservedCostMicros,String terminationReason,Map<String,Object> finalOutput) {
    public enum Status { RUNNING, WAITING_APPROVAL, COMPLETED, LIMIT_REACHED, FAILED }
    public boolean terminal() { return status!=Status.RUNNING && status!=Status.WAITING_APPROVAL; }
}
