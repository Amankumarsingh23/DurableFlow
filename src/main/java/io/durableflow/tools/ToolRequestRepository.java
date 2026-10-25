package io.durableflow.tools;

import org.springframework.jdbc.core.*;
import org.springframework.stereotype.Repository;
import java.util.*;

@Repository
public class ToolRequestRepository {
    private final JdbcTemplate jdbc;
    public ToolRequestRepository(JdbcTemplate jdbc) { this.jdbc=jdbc; }
    private static final RowMapper<ToolRequest> ROW=(r,i) -> new ToolRequest(r.getObject("id",UUID.class),r.getObject("workflow_run_id",UUID.class),
            r.getObject("source_activity_id",UUID.class),r.getObject("proposal_activity_id",UUID.class),r.getString("execute_node_id"),r.getString("approval_node_id"),
            r.getString("tool_name"),ToolJson.decode(r.getString("arguments_json")),r.getString("fingerprint"),ToolJson.decode(r.getString("descriptor_json")),
            ToolJson.decode(r.getString("decision_json")),ToolRequest.Status.valueOf(r.getString("status")),ToolJson.decode(r.getString("result_json")));
    public Optional<ToolRequest> forRun(UUID run) { return jdbc.query("SELECT * FROM tool_request WHERE workflow_run_id=?",ROW,run).stream().findFirst(); }
    public ToolRequest require(UUID id) { return jdbc.query("SELECT * FROM tool_request WHERE id=?",ROW,id).stream().findFirst()
            .orElseThrow(() -> new IllegalArgumentException("TOOL_REQUEST_NOT_FOUND")); }
    public void insert(ToolRequest r) {
        jdbc.update("""
            INSERT INTO tool_request (id,workflow_run_id,source_activity_id,proposal_activity_id,execute_node_id,approval_node_id,
                tool_name,arguments_json,fingerprint,descriptor_json,risk_level,decision_json,status,approved_at)
            VALUES (?,?,?,?,?,?,?,CAST(? AS jsonb),?,CAST(? AS jsonb),?,CAST(? AS jsonb),?,
                CASE WHEN ?='APPROVED' THEN clock_timestamp() ELSE NULL END)
            """,r.id(),r.workflowRunId(),r.sourceActivityId(),r.proposalActivityId(),r.executeNodeId(),r.approvalNodeId(),r.toolName(),
            ToolJson.encode(r.arguments()),r.fingerprint(),ToolJson.encode(r.descriptor()),r.descriptor().get("riskLevel"),ToolJson.encode(r.decision()),r.status().name(),r.status().name());
    }
    public void approve(ToolRequest r,boolean approved) {
        if(jdbc.update("UPDATE tool_request SET status=?, approved_at=CASE WHEN ? THEN clock_timestamp() ELSE NULL END WHERE id=? AND status='AWAITING_APPROVAL'",
                approved?"APPROVED":"REJECTED",approved,r.id())!=1) throw new IllegalArgumentException("TOOL_APPROVAL_STATE");
    }
    public void complete(ToolRequest r,Map<String,Object> result) {
        if(jdbc.update("UPDATE tool_request SET status='EXECUTED',result_json=CAST(? AS jsonb),executed_at=clock_timestamp() WHERE id=? AND status='APPROVED'",
                ToolJson.encode(result),r.id())!=1) throw new IllegalArgumentException("TOOL_EXECUTION_STATE");
    }
}
