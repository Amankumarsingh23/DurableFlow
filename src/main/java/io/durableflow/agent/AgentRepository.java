package io.durableflow.agent;
import io.durableflow.tools.ToolJson;
import org.springframework.jdbc.core.*;
import org.springframework.stereotype.Repository;
import java.util.*;

@Repository
public class AgentRepository {
    private final JdbcTemplate jdbc;
    public AgentRepository(JdbcTemplate jdbc) { this.jdbc=jdbc; }
    private static final RowMapper<AgentExecution> AGENT=(r,i) -> new AgentExecution(r.getObject("id",UUID.class),r.getObject("workflow_run_id",UUID.class),
        AgentExecution.Status.valueOf(r.getString("status")),r.getString("goal"),r.getString("scenario"),AgentLimits.from(ToolJson.decode(r.getString("limits_json"))),
        r.getInt("max_model_attempts"),ToolJson.decode(r.getString("frozen_context_json")),r.getInt("current_iteration"),r.getInt("logical_model_calls"),r.getInt("model_attempts"),
        r.getInt("logical_tool_calls"),r.getLong("input_tokens"),r.getLong("output_tokens"),r.getLong("estimated_cost_micros"),r.getLong("reserved_tokens"),r.getLong("reserved_cost_micros"),
        r.getString("termination_reason"),ToolJson.decode(r.getString("final_output_json")));
    private static final RowMapper<AgentIteration> ITERATION=(r,i) -> new AgentIteration(r.getObject("id",UUID.class),r.getObject("agent_execution_id",UUID.class),
        r.getInt("iteration_number"),r.getObject("model_workflow_run_id",UUID.class),r.getObject("model_activity_execution_id",UUID.class),AgentIteration.Status.valueOf(r.getString("status")),
        ToolJson.decode(r.getString("decision_json")),r.getObject("tool_workflow_run_id",UUID.class),r.getObject("tool_request_id",UUID.class),ToolJson.decode(r.getString("tool_result_json")),
        r.getLong("reserved_tokens"),r.getLong("reserved_cost_micros"));
    public AgentExecution get(UUID id) { return require(jdbc.query("SELECT * FROM agent_execution WHERE id=?",AGENT,id)); }
    public AgentExecution lock(UUID id) { return require(jdbc.query("SELECT * FROM agent_execution WHERE id=? FOR UPDATE",AGENT,id)); }
    private AgentExecution require(List<AgentExecution> rows) { return rows.stream().findFirst().orElseThrow(() -> new IllegalArgumentException("AGENT_NOT_FOUND")); }
    public List<AgentIteration> iterations(UUID id) { return jdbc.query("SELECT * FROM agent_iteration WHERE agent_execution_id=? ORDER BY iteration_number",ITERATION,id); }
    public AgentIteration forModel(UUID model) { return jdbc.query("SELECT * FROM agent_iteration WHERE model_activity_execution_id=?",ITERATION,model).stream().findFirst()
            .orElseThrow(() -> new IllegalArgumentException("AGENT_ITERATION_REQUIRED")); }
    public void insert(UUID id,UUID run,String goal,String scenario,AgentLimits limits,int attempts,Map<String,Object> context) {
        jdbc.update("INSERT INTO agent_execution(id,workflow_run_id,status,goal,scenario,limits_json,max_model_attempts,frozen_context_json) VALUES (?,?,'RUNNING',?,?,CAST(? AS jsonb),?,CAST(? AS jsonb))",
                id,run,goal,scenario,ToolJson.encode(limits.toMap()),attempts,ToolJson.encode(context));
    }
    public void startIteration(AgentExecution a,UUID id,UUID run,UUID activity,long tokens,long cost) {
        jdbc.update("INSERT INTO agent_iteration(id,agent_execution_id,iteration_number,model_workflow_run_id,model_activity_execution_id,status,reserved_tokens,reserved_cost_micros) VALUES (?,?,?,?,?,'MODEL_PENDING',?,?)",
            id,a.id(),a.currentIteration()+1,run,activity,tokens,cost);
        jdbc.update("UPDATE agent_execution SET current_iteration=current_iteration+1,logical_model_calls=logical_model_calls+1,reserved_tokens=reserved_tokens+?,reserved_cost_micros=reserved_cost_micros+?,updated_at=clock_timestamp() WHERE id=?",tokens,cost,a.id());
    }
    public void decision(AgentIteration i,AgentDecision decision,int attempts,long input,long output,long cost) {
        jdbc.update("UPDATE agent_iteration SET status='DECIDED',decision_type=?,decision_json=CAST(? AS jsonb) WHERE id=?",decision==null?null:decision.type().name(),decision==null?"{}":ToolJson.encode(decision.payload()),i.id());
        jdbc.update("UPDATE agent_execution SET model_attempts=model_attempts+?,input_tokens=input_tokens+?,output_tokens=output_tokens+?,estimated_cost_micros=estimated_cost_micros+?,updated_at=clock_timestamp() WHERE id=?",attempts,input,output,cost,i.agentExecutionId());
    }
    public void tool(AgentIteration i,UUID run,UUID request,boolean waiting) {
        jdbc.update("UPDATE agent_iteration SET status='TOOL_PENDING',tool_workflow_run_id=?,tool_request_id=? WHERE id=?",run,request,i.id());
        jdbc.update("UPDATE agent_execution SET logical_tool_calls=logical_tool_calls+1,status=?,updated_at=clock_timestamp() WHERE id=?",waiting?"WAITING_APPROVAL":"RUNNING",i.agentExecutionId());
    }
    public void iterationCompleted(AgentIteration i,Map<String,Object> result) {
        jdbc.update("UPDATE agent_iteration SET status='COMPLETED',tool_result_json=CAST(? AS jsonb),completed_at=clock_timestamp() WHERE id=?",ToolJson.encode(result),i.id());
        jdbc.update("UPDATE agent_execution SET status='RUNNING',updated_at=clock_timestamp() WHERE id=?",i.agentExecutionId());
    }
    public void running(UUID id) { jdbc.update("UPDATE agent_execution SET status='RUNNING',updated_at=clock_timestamp() WHERE id=?",id); }
    public void terminal(AgentExecution a,AgentExecution.Status status,String reason,Map<String,Object> output) {
        jdbc.update("UPDATE agent_iteration SET status=?,completed_at=clock_timestamp() WHERE agent_execution_id=? AND status IN ('MODEL_PENDING','DECIDED','TOOL_PENDING')",
                status==AgentExecution.Status.COMPLETED?"COMPLETED":"FAILED",a.id());
        jdbc.update("UPDATE agent_execution SET status=?,termination_reason=?,final_output_json=CAST(? AS jsonb),completed_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=?",status.name(),reason,ToolJson.encode(output),a.id());
    }
}
