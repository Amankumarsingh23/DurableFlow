package io.durableflow.agent;
import io.durableflow.ai.activity.ModelCallActivity;
import io.durableflow.tools.*;
import io.durableflow.workflow.activity.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class AgentModelActivityTest {
    private Map<String,Object> execute(Map<String,Object> decision) throws Exception {
        UUID run=UUID.randomUUID(),modelId=UUID.randomUUID(),agentId=UUID.randomUUID();
        var agent=new AgentExecution(agentId,UUID.randomUUID(),AgentExecution.Status.RUNNING,"Goal","refund",AgentLimits.defaults(),3,Map.of(),1,1,0,0,0,0,0,1000,1000,null,Map.of());
        var iteration=new AgentIteration(UUID.randomUUID(),agentId,1,run,modelId,AgentIteration.Status.MODEL_PENDING,Map.of(),null,null,Map.of(),1000,1000);
        var repository=mock(AgentRepository.class);when(repository.forModel(modelId)).thenReturn(iteration);when(repository.get(agentId)).thenReturn(agent);
        var activities=mock(ActivityExecutionRepository.class);var execution=new ActivityExecutionEntity();execution.setAttemptCount(1);when(activities.findById(modelId)).thenReturn(Optional.of(execution));
        var delegate=mock(ModelCallActivity.class);var context=new ActivityExecutionContext(run,modelId);Map<String,Object> input=Map.of();
        when(delegate.execute(context,input)).thenReturn(Map.of("outputVersion",1,"provider","fake","model","fake-agent-refund-v1","usage",Map.of("inputTokens",10,"outputTokens",4),
                "structuredJson",decision,"privateReasoning","raw-private-field-must-not-persist"));
        return new AgentModelActivity(delegate,repository,activities,new ToolRegistry(List.of())).execute(context,input);
    }
    @Test void stripsPrivateResponseFieldsButPreservesSafeDecisionAndKnownUsage() throws Exception {
        var output=execute(Map.of("type","TOOL_CALL","toolName","getOrder","arguments",Map.of("orderId","O-42")));
        assertThat(output).containsEntry("decisionSchemaValid",true).containsKeys("structuredJson","usage");
        assertThat(ToolJson.encode(output)).doesNotContain("privateReasoning","raw-private-field-must-not-persist");
    }
    @Test void rejectsUnsafeExtraToolArgumentsBeforeActivityOutputCheckpoint() throws Exception {
        var output=execute(Map.of("type","TOOL_CALL","toolName","getOrder","arguments",Map.of("orderId","O-42","reasoning","nested-private-field-must-not-persist")));
        assertThat(output).containsEntry("decisionSchemaValid",false).containsKey("usage").doesNotContainKey("structuredJson");
        assertThat(ToolJson.encode(output)).doesNotContain("reasoning","nested-private-field-must-not-persist");
    }
}
