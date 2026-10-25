package io.durableflow.agent;
import io.durableflow.api.ApiException;
import io.durableflow.workflow.definition.WorkflowDefinitionRepository;
import io.durableflow.workflow.run.WorkflowRunRepository;
import io.durableflow.workflow.signal.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import java.util.UUID;

/** Host completion belongs to the durable driver, never a caller-supplied SIGNAL. */
@Component
public class AgentHostSignalGuard implements SignalAcceptanceGuard {
    private final WorkflowRunRepository runs;
    private final WorkflowDefinitionRepository definitions;
    public AgentHostSignalGuard(WorkflowRunRepository runs,WorkflowDefinitionRepository definitions) {this.runs=runs;this.definitions=definitions;}
    public void beforeAccept(UUID run,ReceiveSignalRequest signal) {
        var definition=definitions.findById(runs.findById(run).orElseThrow().getWorkflowDefinitionId()).orElseThrow();
        if(AgentWorkflows.HOST.equals(definition.getName())) throw new ApiException(HttpStatus.CONFLICT,"AGENT_HOST_SIGNAL_IS_INTERNAL");
    }
}
