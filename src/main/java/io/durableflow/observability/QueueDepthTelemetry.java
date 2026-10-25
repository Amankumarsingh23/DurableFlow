package io.durableflow.observability;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.*;

/** Scrapes never query the database. Failed polls retain the previous sample and increase its age. */
@Component
public class QueueDepthTelemetry {
    private final JdbcTemplate jdbc;private final SafeTelemetry telemetry;private long lastSuccess=System.nanoTime();
    public QueueDepthTelemetry(JdbcTemplate jdbc,SafeTelemetry telemetry) {this.jdbc=jdbc;this.telemetry=telemetry;}
    @Scheduled(fixedDelayString="${durableflow.telemetry.queue-poll-ms:5000}")
    public void sample() {
        try {
            var counts=jdbc.queryForList("SELECT status,count(*) AS depth FROM activity_task WHERE status IN ('READY','CLAIMED') GROUP BY status");
            for(String status:List.of("READY","CLAIMED")) {
                long depth=counts.stream().filter(r -> status.equals(r.get("status"))).mapToLong(r -> ((Number)r.get("depth")).longValue()).sum();
                telemetry.measure(Metric.QUEUE_DEPTH,DurableFlowTelemetry.Kind.GAUGE,depth,Map.of("status",status));
            }
            lastSuccess=System.nanoTime();
        } catch(RuntimeException ignored) { /* Source of truth remains the task table. */ }
        telemetry.measure(Metric.QUEUE_SAMPLE_AGE,DurableFlowTelemetry.Kind.GAUGE,(System.nanoTime()-lastSuccess)/1e9,Map.of());
    }
}
