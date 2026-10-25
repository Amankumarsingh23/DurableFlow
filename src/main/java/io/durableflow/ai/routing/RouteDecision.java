package io.durableflow.ai.routing;
import java.util.*;
public record RouteDecision(String policyVersion,String reason,List<ModelDescriptor> candidates,int attemptsPerRoute,int maxProviderAttempts,Map<String,Object> constraints) {
    public RouteDecision {candidates=List.copyOf(candidates);constraints=io.durableflow.tools.ToolJson.freeze(constraints);if(candidates.isEmpty() || candidates.size()>3)throw new IllegalArgumentException("Invalid route decision");}
    public ModelDescriptor primary() {return candidates.getFirst();}
    public Map<String,Object> toMap() {return Map.of("policyVersion",policyVersion,"reason",reason,"candidates",candidates.stream().map(ModelDescriptor::toMap).toList(),"attemptsPerRoute",attemptsPerRoute,"maxProviderAttempts",maxProviderAttempts,"constraints",constraints);}
    public static RouteDecision from(Map<String,Object> m) {return new RouteDecision((String)m.get("policyVersion"),(String)m.get("reason"),((List<?>)m.get("candidates")).stream()
        .map(x -> ModelDescriptor.from(io.durableflow.tools.ToolJson.object(x))).toList(),((Number)m.get("attemptsPerRoute")).intValue(),((Number)m.get("maxProviderAttempts")).intValue(),io.durableflow.tools.ToolJson.object(m.get("constraints")));}
}
