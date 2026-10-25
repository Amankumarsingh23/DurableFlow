package io.durableflow.workflow.activity;
import java.util.Map;
/** Local database work only, inside fenced task completion; never call external providers here. */
public interface ActivityCompletionParticipant {
    void completed(ActivityExecutionEntity activity,Map<String,Object> output);
}
