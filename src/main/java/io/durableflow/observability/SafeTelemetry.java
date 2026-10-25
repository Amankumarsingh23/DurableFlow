package io.durableflow.observability;
import org.springframework.stereotype.Component;
import java.util.*;

/** Catch at the call boundary AND in the async dispatcher. Legacy test fixtures can opt out. */
@Component
public class SafeTelemetry {
    private final TelemetryDispatcher dispatcher;private final DurableFlowTelemetry sink;
    public SafeTelemetry(TelemetryDispatcher dispatcher,DurableFlowTelemetry sink) {this.dispatcher=dispatcher;this.sink=sink;}
    public static SafeTelemetry disabled() {return new SafeTelemetry(null,null);}
    public void deferred(Runnable work) {try {if(dispatcher!=null)dispatcher.afterCommit(work);}catch(RuntimeException ignored) {}}
    public void measure(Metric metric,DurableFlowTelemetry.Kind kind,double value,Map<String,String> labels) {
        try {var bounded=MetricLabels.forMetric(metric,labels);deferred(() -> sink.measure(metric,kind,value,bounded));}catch(RuntimeException ignored) {}
    }
    public void count(Metric metric,Map<String,String> labels) {measure(metric,DurableFlowTelemetry.Kind.COUNTER,1,labels);}
    public void sample(Metric metric,double value,Map<String,String> labels) {measure(metric,DurableFlowTelemetry.Kind.SAMPLE,value,labels);}
    public void duration(Metric metric,long nanos,Map<String,String> labels) {measure(metric,DurableFlowTelemetry.Kind.MILLISECONDS,Math.max(0,nanos)/1000000.0,labels);}
    public void event(String event,Map<String,?> metadata) {
        try {var safe=SafeMetadata.sanitize(metadata);deferred(() -> sink.event(event,safe));}catch(RuntimeException ignored) {}
    }
}
