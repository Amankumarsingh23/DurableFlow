package io.durableflow.ai.routing;
import io.durableflow.ai.model.*;
import io.durableflow.ai.provider.*;
import io.durableflow.worker.retry.RetryableActivityException;
import org.springframework.stereotype.Service;
import java.util.*;
import io.durableflow.observability.*;

/** One provider invocation per worker delivery; every retry/fallback goes through the existing queue. */
@Service
public class ModelRoutingService {
    private final ModelRouter router;private final RoutingPolicy policy;private final RouteTransactions transactions;
    private final ModelRouteRepository routes;private final ProviderCircuitBreaker breaker;private final Map<String,ModelProvider> providers;
    private final SafeTelemetry telemetry;
    public ModelRoutingService(ModelRouter router,RoutingPolicy policy,RouteTransactions transactions,ModelRouteRepository routes,ProviderCircuitBreaker breaker,List<ModelProvider> providers) {
        this(router,policy,transactions,routes,breaker,providers,SafeTelemetry.disabled());
    }
    @org.springframework.beans.factory.annotation.Autowired
    public ModelRoutingService(ModelRouter router,RoutingPolicy policy,RouteTransactions transactions,ModelRouteRepository routes,ProviderCircuitBreaker breaker,List<ModelProvider> providers,SafeTelemetry telemetry) {
        this.telemetry=telemetry;
        this.router=router;this.policy=policy;this.transactions=transactions;this.routes=routes;this.breaker=breaker;
        var indexed=new HashMap<String,ModelProvider>();for(var p:providers)if(indexed.put(p.id(),p)!=null)throw new IllegalArgumentException("Duplicate model provider");this.providers=Map.copyOf(indexed);
    }
    public RouteDecision plan(ModelRequest request,boolean tools,long tokens,long cost,int cap) {return router.select(policy.request(request,tools,tokens,cost,cap));}
    public void prepare(UUID activity,UUID agent,ModelRequest request,RouteDecision plan) {transactions.prepare(activity,agent,request,plan);}
    public ModelResponse call(ModelRequest request,String logicalKey,io.durableflow.workflow.activity.ActivityExecutionContext.TaskOwnership owner) throws Exception {
        if(owner==null)throw new IllegalArgumentException("MODEL_ROUTING_REQUIRES_WORKER");
        if(!logicalKey.startsWith("df:v1:") || logicalKey.startsWith("df:v1:compensate:"))throw new IllegalArgumentException("MODEL_ROUTING_REQUIRES_FORWARD_ACTIVITY");
        UUID activity=UUID.fromString(logicalKey.substring("df:v1:".length()));
        if(routes.find(activity).isEmpty()) {
            if(request.model().startsWith("fake-agent-"))throw new IllegalArgumentException("MODEL_AGENT_ROUTE_REQUIRED");
            transactions.prepareStandalone(activity,request,plan(request,false,2000000,10000000,10),owner);
        }
        var ticket=transactions.reserve(activity,request,owner);
        if(ticket.code()!=null)throwCode(ticket.code());
        var descriptor=ticket.descriptor();ModelResponse response;
        long started=System.nanoTime();
        var labels=Map.of("provider",descriptor.provider(),"model",descriptor.model());
        telemetry.count(Metric.MODEL_ATTEMPTS,labels);
        try {
            var provider=providers.get(descriptor.provider());if(provider==null)throw new ModelProviderException(ModelProviderException.Kind.INVALID_MODEL);
            var routed=new ModelRequest(request.requestVersion(),descriptor.provider(),descriptor.model(),request.systemInstructions(),request.userContent(),request.temperature(),request.maxOutputTokens(),request.structuredOutput());
            response=provider.generate(routed,logicalKey+":route:"+ticket.routeIndex());
            if(response==null || !descriptor.provider().equals(response.provider()) || !descriptor.model().equals(response.model()) || request.structuredOutput() && response.structuredJson()==null)
                throw new ModelProviderException(ModelProviderException.Kind.MALFORMED_RESPONSE);
        } catch(Exception failure) {
            var kind=failure instanceof ModelProviderException typed?typed.kind():ModelProviderException.Kind.UNCLASSIFIED;
            telemetry.duration(Metric.MODEL_ATTEMPT_DURATION,System.nanoTime()-started,Map.of("provider",descriptor.provider(),"model",descriptor.model(),"status","FAILURE"));
            telemetry.event("PROVIDER_ATTEMPT_FAILED",Map.of("logicalModelCallId",activity.toString(),"activityExecutionId",activity.toString(),"provider",descriptor.provider(),"model",descriptor.model(),"status","FAILURE"));
            boolean health=Set.of(ModelProviderException.Kind.UNAVAILABLE,ModelProviderException.Kind.RATE_LIMITED,ModelProviderException.Kind.TIMEOUT).contains(kind);
            if(health)breaker.healthFailure(descriptor.provider());else breaker.neutral(descriptor.provider());
            throwCode(transactions.failure(ticket,kind,health,owner));throw new IllegalStateException("Unreachable");
        }
        telemetry.duration(Metric.MODEL_ATTEMPT_DURATION,System.nanoTime()-started,Map.of("provider",descriptor.provider(),"model",descriptor.model(),"status","SUCCESS"));
        telemetry.sample(Metric.MODEL_INPUT_TOKENS,response.usage().inputTokens(),labels);
        telemetry.sample(Metric.MODEL_OUTPUT_TOKENS,response.usage().outputTokens(),labels);
        telemetry.sample(Metric.MODEL_COST_MICROS,(double)response.usage().inputTokens()*descriptor.inputCostMicros()+(double)response.usage().outputTokens()*descriptor.outputCostMicros(),labels);
        telemetry.event("PROVIDER_ATTEMPT_COMPLETED",Map.of("logicalModelCallId",activity.toString(),"provider",descriptor.provider(),"model",descriptor.model(),"providerRequestId",Objects.toString(response.providerRequestId(),""),"status","SUCCESS"));
        breaker.success(descriptor.provider());transactions.success(ticket,response);
        var state=routes.find(activity).orElseThrow();var summary=routes.summary(activity).orElseThrow();
        var metadata=new LinkedHashMap<String,Object>();metadata.put("logicalModelCallId",activity.toString());metadata.put("primaryProvider",state.plan().primary().provider());metadata.put("primaryModel",state.plan().primary().model());
        metadata.put("finalProvider",descriptor.provider());metadata.put("finalModel",descriptor.model());metadata.put("fallbackUsed",ticket.routeIndex()>0);metadata.put("routeIndex",ticket.routeIndex());metadata.put("routeCount",state.plan().candidates().size());
        metadata.put("routingReason",state.plan().reason());metadata.put("transitionReason",state.transition());metadata.put("policyVersion",state.plan().policyVersion());metadata.put("providerAttempts",summary.attempts());
        metadata.put("knownInputTokens",summary.inputTokens());metadata.put("knownOutputTokens",summary.outputTokens());metadata.put("knownCostMicros",summary.knownCostMicros());metadata.put("authorizedCostMicros",state.reservedCost());metadata.put("authorizedTokens",state.reservedTokens());
        return new ModelResponse(response.content(),response.structuredJson(),response.provider(),response.model(),response.usage(),response.latencyMillis(),response.providerRequestId(),metadata);
    }
    private void throwCode(String code) {
        if(Set.of("MODEL_RETRY_SAME_ROUTE","MODEL_ROUTE_ADVANCED").contains(code))throw new RetryableActivityException(code);
        throw new IllegalArgumentException(code);
    }
}
