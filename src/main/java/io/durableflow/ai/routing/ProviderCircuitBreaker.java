package io.durableflow.ai.routing;
import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.Component;
import java.time.*;
import java.util.*;
import io.durableflow.observability.*;

/** Per-instance heuristic only, never the durable route or agent budget authority. */
@Component
public class ProviderCircuitBreaker {
    public enum State { CLOSED, OPEN, HALF_OPEN }
    private static class Circuit {State state=State.CLOSED;int failures;Instant until=Instant.MIN;boolean probe;}
    private final Map<String,Circuit> circuits=new HashMap<>();private final Clock clock;private final int threshold;private final long cooldownMillis;
    private final SafeTelemetry telemetry;
    public ProviderCircuitBreaker(Clock clock,int threshold,long cooldown) {this(clock,threshold,cooldown,SafeTelemetry.disabled());}
    @Autowired public ProviderCircuitBreaker(Clock clock,@Value("${durableflow.routing.breaker-threshold:2}") int threshold,@Value("${durableflow.routing.breaker-cooldown-ms:30000}") long cooldown,SafeTelemetry telemetry) {
        this.telemetry=telemetry;
        if(threshold<1 || cooldown<1)throw new IllegalArgumentException("Invalid breaker settings");this.clock=clock;this.threshold=threshold;this.cooldownMillis=cooldown;
    }
    private Circuit get(String provider) {return circuits.computeIfAbsent(provider,k -> new Circuit());}
    public synchronized State state(String provider) {var c=get(provider);if(c.state==State.OPEN && !clock.instant().isBefore(c.until)){c.state=State.HALF_OPEN;c.probe=false;transition(provider,c.state);}return c.state;}
    public synchronized boolean available(String provider) {var state=state(provider);var c=get(provider);if(state==State.HALF_OPEN && c.probe && !clock.instant().isBefore(c.until))c.probe=false;return state==State.CLOSED || state==State.HALF_OPEN && !get(provider).probe;}
    public synchronized boolean acquire(String provider) {if(!available(provider))return false;var c=get(provider);if(c.state==State.HALF_OPEN){c.probe=true;c.until=clock.instant().plusMillis(cooldownMillis);}return true;}
    public synchronized void success(String provider) {var c=get(provider);if(c.state!=State.CLOSED)transition(provider,State.CLOSED);c.state=State.CLOSED;c.failures=0;c.probe=false;}
    public synchronized void healthFailure(String provider) {var c=get(provider);if(c.state==State.HALF_OPEN || ++c.failures>=threshold){if(c.state!=State.OPEN)transition(provider,State.OPEN);c.state=State.OPEN;c.until=clock.instant().plusMillis(cooldownMillis);}c.probe=false;}
    private void transition(String provider,State state) {
        telemetry.count(Metric.CIRCUIT_TRANSITIONS,Map.of("provider",provider,"status",state.name()));
        telemetry.event("CIRCUIT_TRANSITION",Map.of("provider",provider,"status",state.name()));
    }
    public synchronized void neutral(String provider) {get(provider).probe=false;}
}
