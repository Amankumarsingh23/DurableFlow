package io.durableflow.observability;
import java.util.Map;

/** Adapter boundary. Never invoked on a workflow transaction thread by SafeTelemetry. */
public interface DurableFlowTelemetry {
    enum Kind { COUNTER, SAMPLE, MILLISECONDS, GAUGE }
    void measure(Metric metric, Kind kind, double value, Map<String,String> labels);
    void event(String event, Map<String,Object> safeMetadata);
}
