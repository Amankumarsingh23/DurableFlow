package io.durableflow.observability;
import io.durableflow.workflow.history.WorkflowEventType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import java.util.*;

/** Post-commit projection. Queries execute only on the dispatcher thread, never a transition transaction.
 * Loss is expected: history/database records, rather than these counters, are authoritative. */
@Component
public class HistoryTelemetry {
    private final SafeTelemetry telemetry;private final JdbcTemplate jdbc;
    public HistoryTelemetry(SafeTelemetry telemetry,JdbcTemplate jdbc) {this.telemetry=telemetry;this.jdbc=jdbc;}
    public void committed(UUID run,UUID activity,WorkflowEventType type,Map<String,Object> data,java.time.Instant occurredAt) {
        try {
            var safe=new LinkedHashMap<String,Object>(SafeMetadata.sanitize(data));safe.put("workflowRunId",run.toString());
            if(activity!=null)safe.put("activityExecutionId",activity.toString());
            telemetry.event(type.name(),safe);
            boolean first=Boolean.TRUE.equals(data.get("executionStarted")) && data.get("attemptCount") instanceof Number n && n.intValue()==1 && !"COMPENSATION".equals(data.get("taskKind"));
            telemetry.deferred(() -> project(run,activity,type,safe,occurredAt,first));
        } catch(RuntimeException ignored) { /* Telemetry cannot fail an append. */ }
    }
    private void project(UUID run,UUID activity,WorkflowEventType type,Map<String,Object> safe,java.time.Instant occurredAt,boolean first) {
        String event=type.name();
        if(event.startsWith("WORKFLOW_")) {
            telemetry.count(Metric.WORKFLOW_EVENTS,Map.of("status",event.substring(9)));
            if(Set.of("WORKFLOW_COMPLETED","WORKFLOW_FAILED","WORKFLOW_COMPENSATED","WORKFLOW_COMPENSATION_FAILED").contains(event))
                duration(Metric.WORKFLOW_DURATION,"workflow_run",run,Map.of("status",event.substring(9)));
        }
        String activityStatus=switch(type) {
            case ACTIVITY_STARTED, TASK_CLAIMED -> "STARTED";case ACTIVITY_COMPLETED -> "COMPLETED";case ACTIVITY_FAILED -> "FAILED";
            case RETRY_SCHEDULED -> "RETRY";case ACTIVITY_TIMED_OUT -> "TIMEOUT";case TASK_RECLAIMED -> "RECLAIMED";default -> null;
        };
        if(activity!=null && (first || type==WorkflowEventType.ACTIVITY_STARTED)) {
            var legacy=jdbc.queryForList("SELECT a.id FROM activity_execution a JOIN workflow_run r ON r.id=a.workflow_run_id JOIN workflow_definition d ON d.id=r.workflow_definition_id WHERE a.id=? AND EXISTS (SELECT 1 FROM jsonb_array_elements(d.definition_json->'steps') s WHERE s->>'id'=a.activity_id AND s->>'activity'='modelCall') AND a.input_json->'modelRequest'->>'provider'='fake' AND a.input_json->'modelRequest'->>'model' NOT LIKE 'fake-agent-%'",activity);
            if(!legacy.isEmpty())telemetry.count(Metric.MODEL_LOGICAL_CALLS,Map.of("provider","fake","model","legacy"));
        }
        if(activityStatus!=null)telemetry.count(Metric.ACTIVITY_EVENTS,Map.of("status",activityStatus));
        if(type==WorkflowEventType.RETRY_SCHEDULED && activity!=null) {
            // Requests are linked by run; do not read/log tool arguments.
            if(!jdbc.queryForList("SELECT t.id FROM tool_request t JOIN activity_execution a ON a.workflow_run_id=t.workflow_run_id AND a.activity_id=t.execute_node_id WHERE a.id=?",activity).isEmpty())telemetry.count(Metric.TOOL_RETRIES,Map.of());
        }
        if(activity!=null && Set.of(WorkflowEventType.ACTIVITY_COMPLETED,WorkflowEventType.ACTIVITY_FAILED).contains(type))
            duration(Metric.ACTIVITY_DURATION,"activity_execution",activity,Map.of("status",activityStatus));
        if(event.startsWith("TOOL_")) {
            var rows=jdbc.queryForList("SELECT tool_name,risk_level,extract(epoch from (coalesce(approved_at,CAST(? AS timestamptz))-created_at))*1000 AS wait_ms FROM tool_request WHERE workflow_run_id=?",java.time.OffsetDateTime.ofInstant(occurredAt,java.time.ZoneOffset.UTC),run);
            if(!rows.isEmpty()) {
                var r=rows.getFirst();var tags=Map.of("tool",r.get("tool_name").toString(),"risk",r.get("risk_level").toString(),"status",event.substring(5).replace("REQUEST_CREATED","CREATED"));
                telemetry.count(Metric.TOOL_EVENTS,tags);
                if(type==WorkflowEventType.TOOL_REQUEST_CREATED && "APPROVED".equals(safe.get("status"))) {
                    var automatic=new HashMap<>(tags);automatic.put("status","APPROVED");telemetry.count(Metric.TOOL_EVENTS,automatic);
                }
                if(type==WorkflowEventType.TOOL_APPROVED || type==WorkflowEventType.TOOL_REJECTED)
                    telemetry.measure(Metric.TOOL_APPROVAL_WAIT,DurableFlowTelemetry.Kind.MILLISECONDS,((Number)r.get("wait_ms")).doubleValue(),tags);
            }
        }
        if(event.startsWith("AGENT_")) {
            var rows=jdbc.queryForList("SELECT id,status,termination_reason,logical_model_calls,model_attempts,logical_tool_calls,input_tokens,output_tokens,estimated_cost_micros FROM agent_execution WHERE workflow_run_id=?",run);if(rows.isEmpty())return;
            var a=rows.getFirst();String status=type==WorkflowEventType.AGENT_STARTED?"STARTED":Objects.toString(safe.get("status"),a.get("status").toString());var tags=Map.of("status",status,"reason",Objects.toString(a.get("termination_reason"),"other"));
            if(type!=WorkflowEventType.AGENT_ITERATION_COMPLETED)telemetry.count(Metric.AGENT_EVENTS,tags);
            if(type==WorkflowEventType.AGENT_ITERATION_COMPLETED)telemetry.count(Metric.AGENT_ITERATIONS,Map.of());
            if(type==WorkflowEventType.AGENT_COMPLETED || type==WorkflowEventType.AGENT_LIMIT_REACHED) {
                telemetry.sample(Metric.AGENT_MODEL_CALLS,number(a,"logical_model_calls"),tags);
                telemetry.sample(Metric.AGENT_MODEL_ATTEMPTS,number(a,"model_attempts"),tags);
                telemetry.sample(Metric.AGENT_TOOL_CALLS,number(a,"logical_tool_calls"),tags);
                telemetry.sample(Metric.AGENT_TOKENS,number(a,"input_tokens")+number(a,"output_tokens"),tags);
                telemetry.sample(Metric.AGENT_COST_MICROS,number(a,"estimated_cost_micros"),tags);
                telemetry.sample(Metric.AGENT_APPROVAL_WAITS,Objects.requireNonNull(jdbc.queryForObject("SELECT count(*) FROM agent_iteration i JOIN tool_request t ON t.id=i.tool_request_id WHERE i.agent_execution_id=? AND t.approval_node_id IS NOT NULL",Long.class,a.get("id"))),tags);
                telemetry.sample(Metric.AGENT_FALLBACKS,Objects.requireNonNull(jdbc.queryForObject("SELECT coalesce(sum(active_index),0) FROM model_route WHERE agent_execution_id=?",Long.class,a.get("id"))),tags);
            }
        }
    }
    private double number(Map<String,Object> data,String key) {return ((Number)data.get(key)).doubleValue();}
    private void duration(Metric metric,String table,UUID id,Map<String,String> tags) {
        Double millis=jdbc.queryForObject("SELECT extract(epoch from (completed_at-started_at))*1000 FROM "+table+" WHERE id=?",Double.class,id);
        if(millis!=null)telemetry.measure(metric,DurableFlowTelemetry.Kind.MILLISECONDS,millis,tags);
    }
}
