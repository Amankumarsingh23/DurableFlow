package io.durableflow.observability;
import io.micrometer.core.instrument.*;
import org.slf4j.*;
import org.springframework.stereotype.Component;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class MicrometerTelemetry implements DurableFlowTelemetry {
    private static final Logger LOG=LoggerFactory.getLogger(MicrometerTelemetry.class);
    private final MeterRegistry registry;
    private final Map<String,AtomicReference<Double>> gauges=new ConcurrentHashMap<>();
    public MicrometerTelemetry(MeterRegistry registry,TelemetryDispatcher dispatcher) {
        this.registry=registry;
        Gauge.builder("durableflow.telemetry.dropped",dispatcher,TelemetryDispatcher::dropped).register(registry);
        Gauge.builder("durableflow.telemetry.failed",dispatcher,TelemetryDispatcher::failed).register(registry);
    }
    public void measure(Metric metric,Kind kind,double value,Map<String,String> labels) {
        if(!Double.isFinite(value) || value<0)return;
        String name="durableflow."+metric.name().toLowerCase(Locale.ROOT).replace('_','.');
        var bounded=MetricLabels.forMetric(metric,labels);
        var tags=Tags.of(bounded.entrySet().stream().map(e -> Tag.of(e.getKey(),e.getValue())).toList());
        switch(kind) {
            case COUNTER -> registry.counter(name,tags).increment(value);
            case SAMPLE -> registry.summary(name,tags).record(value);
            case MILLISECONDS -> registry.timer(name,tags).record((long)(value*1000000),TimeUnit.NANOSECONDS);
            case GAUGE -> gauges.computeIfAbsent(name+bounded,k -> {
                var cell=new AtomicReference<>(0.0);Gauge.builder(name,cell,c -> c.get()).tags(tags).register(registry);return cell;
            }).set(value);
        }
    }
    public void event(String event,Map<String,Object> metadata) {
        // event names are internal constants; metadata is sanitized again at the adapter boundary.
        LOG.info("durableflow event={} metadata={}",event,SafeMetadata.sanitize(metadata));
    }
}
