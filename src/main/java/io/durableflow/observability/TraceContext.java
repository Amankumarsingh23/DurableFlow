package io.durableflow.observability;
import java.util.*;

/** Internal span-link contract for a future tracing adapter; UUID links never enter metric labels.
 * workflow -> activity -> retrieval / logical-model -> provider-attempt / tool-execution.
 * No ThreadLocal: asynchronous workers propagate this immutable context explicitly. */
public record TraceContext(UUID spanId, UUID parentSpanId, Boundary boundary, TelemetryCorrelation correlation) {
    public enum Boundary { WORKFLOW, ACTIVITY, RETRIEVAL, LOGICAL_MODEL, PROVIDER_ATTEMPT, TOOL_EXECUTION }
    public static TraceContext root(TelemetryCorrelation correlation) {return new TraceContext(UUID.randomUUID(),null,Boundary.WORKFLOW,correlation);}
    public TraceContext child(Boundary boundary,TelemetryCorrelation correlation) {return new TraceContext(UUID.randomUUID(),spanId,boundary,correlation);}
}
