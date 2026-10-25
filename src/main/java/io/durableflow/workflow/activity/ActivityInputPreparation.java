package io.durableflow.workflow.activity;

import io.durableflow.workflow.definition.StepDefinition;
import org.springframework.stereotype.Component;
import java.util.*;

@Component
public class ActivityInputPreparation {
    private final Map<String,ActivityInputPreparer> preparers;
    public ActivityInputPreparation(List<ActivityInputPreparer> preparers) {
        var map=new HashMap<String,ActivityInputPreparer>();
        for (var preparer:preparers) if (preparer.activityType()==null || preparer.activityType().isBlank()
                || map.putIfAbsent(preparer.activityType(),preparer)!=null) throw new IllegalArgumentException("Unique input preparer types required");
        this.preparers=Map.copyOf(map);
    }
    public Map<String,Object> prepare(UUID runId,StepDefinition step,Map<String,Object> workflowInput) {
        var preparer=preparers.get(step.activity());
        return preparer==null ? workflowInput : Objects.requireNonNull(preparer.prepare(runId,step,workflowInput));
    }
}
