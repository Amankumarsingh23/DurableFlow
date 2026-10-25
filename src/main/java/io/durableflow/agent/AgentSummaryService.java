package io.durableflow.agent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import java.util.*;

@Service
public class AgentSummaryService {
    public record Summary(UUID agentExecutionId,UUID workflowRunId,String status,int iterations,int logicalModelCalls,
        long providerAttempts,int logicalToolCalls,long inputTokens,long outputTokens,long knownCostMicros,
        long reservedTokens,long reservedCostMicros,long fallbackTransitions,long approvalWaits,long pendingApprovals,String terminationReason) {}
    private final AgentRepository agents;private final JdbcTemplate jdbc;
    public AgentSummaryService(AgentRepository agents,JdbcTemplate jdbc) {this.agents=agents;this.jdbc=jdbc;}
    @Transactional(readOnly=true,isolation=Isolation.REPEATABLE_READ)
    public Summary get(UUID id) {
        var a=agents.get(id);
        var r=jdbc.queryForMap("SELECT coalesce(sum(total_attempts),0) AS attempts,coalesce(sum(active_index),0) AS fallbacks FROM model_route WHERE agent_execution_id=?",id);
        var t=jdbc.queryForMap("SELECT count(*) FILTER (WHERE t.approval_node_id IS NOT NULL) AS waits,count(*) FILTER (WHERE t.status='AWAITING_APPROVAL') AS pending FROM agent_iteration i JOIN tool_request t ON t.id=i.tool_request_id WHERE i.agent_execution_id=?",id);
        var usage=jdbc.queryForMap("SELECT coalesce(sum(p.input_tokens),0) AS input,coalesce(sum(p.output_tokens),0) AS output,coalesce(sum(p.estimated_cost_micros),0) AS cost FROM model_route_attempt p JOIN model_route r ON r.activity_execution_id=p.activity_execution_id WHERE r.agent_execution_id=?",id);
        return new Summary(id,a.workflowRunId(),a.status().name(),a.currentIteration(),a.logicalModelCalls(),((Number)r.get("attempts")).longValue(),a.logicalToolCalls(),
            ((Number)usage.get("input")).longValue(),((Number)usage.get("output")).longValue(),((Number)usage.get("cost")).longValue(),a.reservedTokens(),a.reservedCostMicros(),
            ((Number)r.get("fallbacks")).longValue(),((Number)t.get("waits")).longValue(),((Number)t.get("pending")).longValue(),a.terminationReason());
    }
}
