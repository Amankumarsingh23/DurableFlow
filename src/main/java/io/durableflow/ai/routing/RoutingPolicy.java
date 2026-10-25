package io.durableflow.ai.routing;
import io.durableflow.ai.model.ModelRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.*;
@Component
public class RoutingPolicy {
    private final Set<String> allowed,forbidden,tags;private final int quality,routes,attempts,total;private final boolean downgrade;
    public RoutingPolicy(@Value("${durableflow.routing.allowed-providers:fake-a,fake-b}") String allowed,@Value("${durableflow.routing.forbidden-providers:}") String forbidden,
        @Value("${durableflow.routing.required-tags:local}") String tags,@Value("${durableflow.routing.required-quality:2}") int quality,
        @Value("${durableflow.routing.allow-quality-downgrade:false}") boolean downgrade,@Value("${durableflow.routing.max-routes:2}") int routes,
        @Value("${durableflow.routing.attempts-per-route:2}") int attempts,@Value("${durableflow.routing.max-provider-attempts:3}") int total) {
        this.allowed=split(allowed);this.forbidden=split(forbidden);this.tags=split(tags);this.quality=quality;this.downgrade=downgrade;this.routes=routes;this.attempts=attempts;this.total=total;
        request(new ModelRequest(1,"routed","demo","","demo",0.0,24,true),false,1000000,1000000,total);
    }
    private static Set<String> split(String value) {return value.isBlank()?Set.of():Set.copyOf(Arrays.stream(value.split(",")).map(String::strip).toList());}
    public ModelRouteRequest request(ModelRequest task,boolean tools,long tokens,long cost,int cap) {return new ModelRouteRequest(task,task.structuredOutput(),tools,quality,downgrade,tokens,cost,allowed,forbidden,tags,routes,attempts,Math.min(total,cap));}
}
