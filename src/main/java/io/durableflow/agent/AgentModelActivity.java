package io.durableflow.agent;
import io.durableflow.ai.activity.ModelCallActivity;
import io.durableflow.workflow.activity.*;
import io.durableflow.tools.*;
import org.springframework.stereotype.Component;
import java.util.*;

/** Existing model handler/gateway and worker delivery; only bounded/sanitized agent output is checkpointed. */
@Component
public class AgentModelActivity implements ActivityHandler {
    private final ModelCallActivity model;
    private final AgentRepository agents;
    private final ActivityExecutionRepository activities;
    private final ToolRegistry registry;
    public AgentModelActivity(ModelCallActivity model,AgentRepository agents,ActivityExecutionRepository activities,ToolRegistry registry) {this.model=model;this.agents=agents;this.activities=activities;this.registry=registry;}
    public String type() {return "agentModelCall";}
    public boolean requiresDurableWorker() {return true;}
    public Map<String,Object> execute(ActivityExecutionContext context,Map<String,Object> input) throws Exception {
        var iteration=agents.forModel(context.activityExecutionId());var agent=agents.get(iteration.agentExecutionId());
        if(context.compensation() || agent.terminal() || !iteration.modelRunId().equals(context.workflowRunId())
                || activities.findById(context.activityExecutionId()).orElseThrow().getAttemptCount()>agent.maxModelAttempts())
            throw new IllegalArgumentException("AGENT_MODEL_ATTEMPT_LIMIT");
        var raw=model.execute(context,input);var safe=new LinkedHashMap<String,Object>();
        for(String key:List.of("outputVersion","provider","model","usage","latencyMillis","providerRequestId","routing","estimatedCostMicros"))if(raw.containsKey(key))safe.put(key,raw.get(key));
        try {
            var decision=AgentDecision.parse(raw.get("structuredJson"));
            if(decision.type()==AgentDecision.Type.TOOL_CALL) {
                String name=(String)decision.payload().get("toolName");
                var args=registry.require(name).validate(ToolJson.object(decision.payload().get("arguments")));
                decision=AgentDecision.parse(Map.of("type","TOOL_CALL","toolName",name,"arguments",args));
            }
            safe.put("structuredJson",decision.payload());safe.put("decisionSchemaValid",true);
        }
        catch(IllegalArgumentException invalid) {safe.put("decisionSchemaValid",false);}
        return safe;
    }
}
