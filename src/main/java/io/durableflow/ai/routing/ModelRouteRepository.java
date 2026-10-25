package io.durableflow.ai.routing;
import io.durableflow.ai.model.ModelRequest;
import io.durableflow.tools.ToolJson;
import org.springframework.jdbc.core.*;
import org.springframework.stereotype.Repository;
import java.util.*;
@Repository
public class ModelRouteRepository {
    public record Route(UUID activityId,UUID agentId,String fingerprint,RouteDecision plan,int activeIndex,int totalAttempts,long baseCost,long extraCost,String status,String code,String transition,long reservedTokens,long reservedCost) {}
    public record Summary(int attempts,long inputTokens,long outputTokens,long knownCostMicros,long extraReservedCost,String terminalCode) {}
    private final JdbcTemplate jdbc;
    public ModelRouteRepository(JdbcTemplate jdbc) {this.jdbc=jdbc;}
    private static final RowMapper<Route> ROW=(r,i) -> new Route(r.getObject("activity_execution_id",UUID.class),r.getObject("agent_execution_id",UUID.class),r.getString("request_fingerprint"),
        RouteDecision.from(ToolJson.decode(r.getString("plan_json"))),r.getInt("active_index"),r.getInt("total_attempts"),r.getLong("base_attempt_cost_micros"),r.getLong("extra_reserved_cost_micros"),r.getString("status"),r.getString("terminal_code"),r.getString("transition_reason"),r.getLong("reserved_tokens"),r.getLong("reserved_cost_micros"));
    public Optional<Route> find(UUID activity) {return jdbc.query("SELECT * FROM model_route WHERE activity_execution_id=?",ROW,activity).stream().findFirst();}
    public Route lock(UUID activity) {return jdbc.query("SELECT * FROM model_route WHERE activity_execution_id=? FOR UPDATE",ROW,activity).getFirst();}
    public void insert(UUID activity,UUID agent,ModelRequest request,RouteDecision plan) {
        jdbc.update("INSERT INTO model_route(activity_execution_id,agent_execution_id,request_fingerprint,plan_json,base_attempt_cost_micros) VALUES (?,?,?,CAST(? AS jsonb),?)",
            activity,agent,fingerprint(request),ToolJson.encode(plan.toMap()),plan.primary().estimatedCost(request.systemInstructions().length()+request.userContent().length(),request.maxOutputTokens()));
    }
    public static String fingerprint(ModelRequest r) {try {return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(ToolJson.encode(ToolJson.freeze(Map.of("requestVersion",r.requestVersion(),"provider",r.provider(),"model",r.model(),
        "systemInstructions",r.systemInstructions(),"userContent",r.userContent(),"temperature",r.temperature(),"maxOutputTokens",r.maxOutputTokens(),"structuredOutput",r.structuredOutput()))).getBytes(java.nio.charset.StandardCharsets.UTF_8)));}catch(java.security.NoSuchAlgorithmException impossible){throw new IllegalStateException(impossible);}}
    public int routeAttempts(UUID id,int index) {return Objects.requireNonNull(jdbc.queryForObject("SELECT count(*) FROM model_route_attempt WHERE activity_execution_id=? AND route_index=?",Integer.class,id,index));}
    public Optional<Summary> summary(UUID id) {
        var route=find(id);if(route.isEmpty())return Optional.empty();var r=route.get();
        return Optional.of(Objects.requireNonNull(jdbc.queryForObject("SELECT COALESCE(sum(input_tokens),0) AS i,COALESCE(sum(output_tokens),0) AS o,COALESCE(sum(estimated_cost_micros),0) AS c FROM model_route_attempt WHERE activity_execution_id=?",
            (row,index) -> new Summary(r.totalAttempts(),row.getLong("i"),row.getLong("o"),row.getLong("c"),r.extraCost(),r.code()),id)));
    }
    public List<Map<String,Object>> attempts(UUID id) {return jdbc.queryForList("SELECT attempt_number,route_index,route_attempt_number,provider,model,status,failure_code,input_tokens,output_tokens,estimated_cost_micros FROM model_route_attempt WHERE activity_execution_id=? ORDER BY attempt_number",id);}
}
