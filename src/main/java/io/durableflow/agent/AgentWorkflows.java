package io.durableflow.agent;
import io.durableflow.workflow.definition.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import java.util.*;

/** Four fixed code-owned templates, never arbitrary model-generated DAGs. */
@Service
public class AgentWorkflows {
    public static final String HOST="DurableFlowAgentHostV1", MODEL="DurableFlowAgentModelV1", READ="DurableFlowAgentReadV1", WRITE="DurableFlowAgentWriteV1";
    private final WorkflowDefinitionService definitions;
    private final WorkflowDefinitionRepository repository;
    private final JdbcTemplate jdbc;
    public AgentWorkflows(WorkflowDefinitionService definitions,WorkflowDefinitionRepository repository,JdbcTemplate jdbc) {this.definitions=definitions;this.repository=repository;this.jdbc=jdbc;}
    @org.springframework.transaction.annotation.Transactional(propagation=org.springframework.transaction.annotation.Propagation.MANDATORY)
    public void ensure() {
        jdbc.execute("SELECT pg_advisory_xact_lock(240024)");
        ensure(HOST,List.of(new StepDefinition("agentFinished",null,List.of(),NodeType.SIGNAL,null)));
        ensure(MODEL,List.of(new StepDefinition("decision","agentModelCall",List.of())));
        ensure(READ,List.of(new StepDefinition("execute","executeTool",List.of())));
        ensure(WRITE,List.of(new StepDefinition("approval",null,List.of(),NodeType.SIGNAL,null),new StepDefinition("execute","executeTool",List.of("approval"))));
    }
    private void ensure(String name,List<StepDefinition> steps) {
        if(repository.findByNameAndVersion(name,1).isEmpty()) definitions.register(new RegisterWorkflowDefinitionRequest(name,1,steps));
        if(!definitions.get(name,1).steps().equals(steps))throw new IllegalArgumentException("AGENT_INTERNAL_TEMPLATE_MISMATCH");
    }
}
