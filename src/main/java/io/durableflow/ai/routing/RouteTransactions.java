package io.durableflow.ai.routing;
import io.durableflow.ai.model.*;
import io.durableflow.ai.provider.ModelProviderException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import java.util.*;

/** Bounded database transitions only; never invokes a provider. */
@Service
public class RouteTransactions {
    public record Ticket(UUID id,UUID activityId,ModelDescriptor descriptor,int routeIndex,int attemptNumber,String code) {}
    private final ModelRouteRepository routes;private final JdbcTemplate jdbc;private final ProviderCircuitBreaker breaker;
    public RouteTransactions(ModelRouteRepository routes,JdbcTemplate jdbc,ProviderCircuitBreaker breaker) {this.routes=routes;this.jdbc=jdbc;this.breaker=breaker;}
    @Transactional(propagation=Propagation.MANDATORY)
    public void prepare(UUID activity,UUID agent,ModelRequest request,RouteDecision decision) {
        var old=routes.find(activity);if(old.isEmpty())routes.insert(activity,agent,request,decision);
        else if(!old.get().fingerprint().equals(ModelRouteRepository.fingerprint(request)))throw new IllegalArgumentException("MODEL_LOGICAL_REQUEST_CHANGED");
    }
    @Transactional(propagation=Propagation.REQUIRES_NEW)
    public void prepareStandalone(UUID activity,ModelRequest request,RouteDecision decision,io.durableflow.workflow.activity.ActivityExecutionContext.TaskOwnership owner) {
        if(!owned(activity,owner,true))throw new IllegalStateException("MODEL_ROUTE_OWNERSHIP_LOST");
        UUID run=jdbc.queryForObject("SELECT workflow_run_id FROM activity_execution WHERE id=?",UUID.class,activity);
        jdbc.queryForObject("SELECT id FROM workflow_run WHERE id=? FOR UPDATE",UUID.class,run);
        if(routes.find(activity).isEmpty())routes.insert(activity,null,request,decision);
        if(!owned(activity,owner,false))throw new IllegalStateException("MODEL_ROUTE_OWNERSHIP_LOST");
    }
    @Transactional(propagation=Propagation.REQUIRES_NEW)
    public Ticket reserve(UUID activity,ModelRequest request,io.durableflow.workflow.activity.ActivityExecutionContext.TaskOwnership owner) {
        if(!owned(activity,owner,true))throw new IllegalStateException("MODEL_ROUTE_OWNERSHIP_LOST");
        var reference=routes.find(activity).orElseThrow();Map<String,Object> agent=null;
        if(reference.agentId()!=null)agent=jdbc.queryForMap("SELECT * FROM agent_execution WHERE id=? FOR UPDATE",reference.agentId());
        var r=routes.lock(activity);
        if(!r.fingerprint().equals(ModelRouteRepository.fingerprint(request)))return ticket(r,"MODEL_LOGICAL_REQUEST_CHANGED");
        if(!r.status().equals("READY"))return ticket(r,r.code());
        if(!"RUNNING".equals(jdbc.queryForObject("SELECT status FROM activity_execution WHERE id=?",String.class,activity)))return ticket(r,"MODEL_ACTIVITY_NOT_RUNNING");
        if(r.totalAttempts()>=r.plan().maxProviderAttempts() || r.activeIndex()>=r.plan().candidates().size())return blocked(r,"MODEL_ROUTE_EXHAUSTED","EXHAUSTED");
        int count=routes.routeAttempts(activity,r.activeIndex());
        if(count>=r.plan().attemptsPerRoute())return next(r,"RECOVERY_ROUTE_ATTEMPTS_SPENT");
        var descriptor=r.plan().candidates().get(r.activeIndex());
        long cost=descriptor.estimatedCost(request.systemInstructions().length()+request.userContent().length(),request.maxOutputTokens());
        long tokens=(long)request.systemInstructions().length()+request.userContent().length()+request.maxOutputTokens();
        long tokenCap=((Number)r.plan().constraints().get("remainingTokens")).longValue(),costCap=((Number)r.plan().constraints().get("remainingCostMicros")).longValue();
        if(tokens>tokenCap-r.reservedTokens())return blocked(r,"MAX_TOTAL_TOKENS","BUDGET_BLOCKED");
        if(cost>costCap-r.reservedCost())return blocked(r,"MAX_ESTIMATED_COST","BUDGET_BLOCKED");
        long extra=agent==null?0:Math.max(0,cost-r.baseCost());
        if(agent!=null) {
            if(!"RUNNING".equals(agent.get("status")))return ticket(r,"AGENT_NOT_RUNNING");
            long cap=((Number)jdbc.queryForObject("SELECT (limits_json->>'maxEstimatedCostMicros')::bigint FROM agent_execution WHERE id=?",Long.class,r.agentId())).longValue();
            if(extra>cap-((Number)agent.get("reserved_cost_micros")).longValue())return blocked(r,"MAX_ESTIMATED_COST","BUDGET_BLOCKED");
            // Total provider attempts cannot exceed the token slots already reserved by Session 24.
            int tokenSlots=((Number)agent.get("max_model_attempts")).intValue();
            if(r.totalAttempts()>=tokenSlots)return blocked(r,"MAX_TOTAL_TOKENS","BUDGET_BLOCKED");
        }
        if(!breaker.acquire(descriptor.provider()))return next(r,"CIRCUIT_OPEN");
        UUID id=UUID.randomUUID();
        jdbc.update("INSERT INTO model_route_attempt(id,activity_execution_id,attempt_number,route_index,route_attempt_number,provider,model,status) VALUES (?,?,?,?,?,?,?,'STARTED')",
            id,activity,r.totalAttempts()+1,r.activeIndex(),count+1,descriptor.provider(),descriptor.model());
        jdbc.update("UPDATE model_route SET total_attempts=total_attempts+1,extra_reserved_cost_micros=extra_reserved_cost_micros+?,reserved_tokens=reserved_tokens+?,reserved_cost_micros=reserved_cost_micros+?,updated_at=clock_timestamp() WHERE activity_execution_id=?",extra,tokens,cost,activity);
        if(extra>0) {
            jdbc.update("UPDATE agent_execution SET reserved_cost_micros=reserved_cost_micros+?,updated_at=clock_timestamp() WHERE id=?",extra,r.agentId());
            jdbc.update("UPDATE agent_iteration SET routing_extra_reserved_cost_micros=routing_extra_reserved_cost_micros+? WHERE model_activity_execution_id=?",extra,activity);
        }
        if(!owned(activity,owner,false))throw new IllegalStateException("MODEL_ROUTE_OWNERSHIP_LOST");
        return new Ticket(id,activity,descriptor,r.activeIndex(),r.totalAttempts()+1,null);
    }
    private boolean owned(UUID activity,io.durableflow.workflow.activity.ActivityExecutionContext.TaskOwnership owner,boolean lock) {
        if(owner==null)return false;
        return !jdbc.queryForList("SELECT id FROM activity_task WHERE id=? AND activity_execution_id=? AND status='CLAIMED' AND worker_id=? AND lease_generation=? AND lease_expires_at>clock_timestamp()"+(lock?" FOR UPDATE":""),
            UUID.class,owner.taskId(),activity,owner.workerId(),owner.generation()).isEmpty();
    }
    private Ticket ticket(ModelRouteRepository.Route r,String code) {return new Ticket(null,r.activityId(),null,r.activeIndex(),r.totalAttempts(),code);}
    private Ticket blocked(ModelRouteRepository.Route r,String code,String status) {
        jdbc.update("UPDATE model_route SET status=?,terminal_code=?,updated_at=clock_timestamp() WHERE activity_execution_id=?",status,code,r.activityId());return ticket(r,code);
    }
    private Ticket next(ModelRouteRepository.Route r,String reason) {
        if(r.activeIndex()+1>=r.plan().candidates().size() || r.totalAttempts()>=r.plan().maxProviderAttempts())return blocked(r,"MODEL_ROUTE_EXHAUSTED","EXHAUSTED");
        jdbc.update("UPDATE model_route SET active_index=active_index+1,transition_reason=?,updated_at=clock_timestamp() WHERE activity_execution_id=?",reason,r.activityId());
        return ticket(r,"MODEL_ROUTE_ADVANCED");
    }
    @Transactional(propagation=Propagation.REQUIRES_NEW)
    public void success(Ticket ticket,ModelResponse response) {
        var d=ticket.descriptor();long cost=Math.addExact(Math.multiplyExact(response.usage().inputTokens(),d.inputCostMicros()),Math.multiplyExact(response.usage().outputTokens(),d.outputCostMicros()));
        jdbc.update("UPDATE model_route_attempt SET status='SUCCESS',input_tokens=?,output_tokens=?,estimated_cost_micros=?,provider_request_id=?,completed_at=clock_timestamp() WHERE id=? AND status='STARTED'",
            response.usage().inputTokens(),response.usage().outputTokens(),cost,response.providerRequestId(),ticket.id());
    }
    @Transactional(propagation=Propagation.REQUIRES_NEW)
    public String failure(Ticket ticket,ModelProviderException.Kind kind,boolean health,io.durableflow.workflow.activity.ActivityExecutionContext.TaskOwnership owner) {
        boolean valid=owned(ticket.activityId(),owner,true);
        var r=routes.lock(ticket.activityId());
        jdbc.update("UPDATE model_route_attempt SET status=?,failure_code=?,completed_at=clock_timestamp() WHERE id=? AND status='STARTED'",health?"HEALTH_FAILURE":"REQUEST_FAILURE","MODEL_"+kind,ticket.id());
        if(!valid)return "MODEL_ROUTE_OWNERSHIP_LOST";
        if(ticket.attemptNumber()!=r.totalAttempts() || r.activeIndex()!=ticket.routeIndex() || !r.status().equals("READY"))return r.code()==null?"MODEL_RETRY_SAME_ROUTE":r.code();
        String outcome;
        if(!health)outcome=blocked(r,"MODEL_"+kind,"TERMINAL").code();
        else if(routes.routeAttempts(r.activityId(),r.activeIndex())>=r.plan().attemptsPerRoute() || r.totalAttempts()>=r.plan().maxProviderAttempts())outcome=next(r,"HEALTH_RETRIES_EXHAUSTED").code();
        else outcome="MODEL_RETRY_SAME_ROUTE";
        if(!owned(ticket.activityId(),owner,false))throw new IllegalStateException("MODEL_ROUTE_OWNERSHIP_LOST");
        return outcome;
    }
}
