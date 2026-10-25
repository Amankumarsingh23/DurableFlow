package io.durableflow.workflow.activity;

import io.durableflow.workflow.definition.StepDefinition;
import java.util.*;

/** Optional first-claim input snapshot adapter. Only bounded local/DB reads; never invoke external handlers here. */
public interface ActivityInputPreparer {
    String activityType();
    Map<String,Object> prepare(UUID runId,StepDefinition step,Map<String,Object> workflowInput);
}
