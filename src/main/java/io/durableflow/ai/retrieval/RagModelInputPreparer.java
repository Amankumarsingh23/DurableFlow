package io.durableflow.ai.retrieval;

import io.durableflow.ai.model.ModelRequest;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.run.WorkflowRunRepository;
import org.springframework.stereotype.Component;
import java.math.BigDecimal;
import java.util.*;

/** Runs under the first-claim transaction/run lock; never calls a retriever or model provider. */
@Component
public class RagModelInputPreparer implements ActivityInputPreparer {
    private final ActivityExecutionRepository activities;
    private final WorkflowRunRepository runs;
    private final WorkflowDefinitionRepository definitions;
    private final GroundedPromptBuilder builder;
    public RagModelInputPreparer(ActivityExecutionRepository activities,WorkflowRunRepository runs,
            WorkflowDefinitionRepository definitions,GroundedPromptBuilder builder) {
        this.activities=activities; this.runs=runs; this.definitions=definitions; this.builder=builder;
    }
    public String activityType() { return "modelCall"; }
    public Map<String,Object> prepare(UUID runId,StepDefinition step,Map<String,Object> workflowInput) {
        if (workflowInput.get("grounding")==null) return workflowInput;
        var prepared=new LinkedHashMap<String,Object>(workflowInput);
        try {
            if (!(workflowInput.get("grounding") instanceof Map<?,?> config) || !Set.of("groundingVersion","sourceNodeId").containsAll(config.keySet())
                    || !(config.get("groundingVersion") instanceof Number number) || new BigDecimal(number.toString()).compareTo(BigDecimal.ONE)!=0
                    || !(config.get("sourceNodeId") instanceof String source) || !step.dependsOn().contains(source)) {
                throw new IllegalArgumentException("Direct retrieval dependency and groundingVersion 1 required");
            }
            var run=runs.findById(runId).orElseThrow(); var definition=definitions.findById(run.getWorkflowDefinitionId()).orElseThrow();
            var sourceStep=WorkflowDefinitionDocument.steps(definition.getDefinitionJson()).stream().filter(s -> s.id().equals(source)).findFirst().orElseThrow();
            if (sourceStep.type()!=NodeType.ACTIVITY || !"retrieveContext".equals(sourceStep.activity())) throw new IllegalArgumentException("Retrieval activity source required");
            var execution=activities.findByWorkflowRunIdOrderByActivityIdAsc(runId).stream().filter(a -> a.getActivityId().equals(source)).findFirst().orElseThrow();
            if (execution.getStatus()!=ActivityExecutionStatus.COMPLETED) throw new IllegalArgumentException("Completed retrieval checkpoint required");
            var retrieval=RetrievalRequest.fromInput(execution.getInputJson());
            if (!retrieval.filters().equals(RetrievalRequest.fromInput(workflowInput).filters())) throw new IllegalArgumentException("Grounding scope mismatch");
            var grounded=builder.build(ModelRequest.fromInput(workflowInput),retrieval,execution.getOutputJson(),execution.getId(),source);
            prepared.put("modelRequest",grounded.modelRequest()); prepared.put("groundingPreparation",grounded.provenance());
        } catch (IllegalArgumentException | NoSuchElementException invalid) {
            // Freeze an invalid preparation too; handler reports it through ordinary terminal failure,
            // rather than repeatedly rolling back a READY task's claim.
            prepared.put("groundingPreparation",Map.of("status","INVALID","preparationVersion",1));
        }
        return prepared;
    }
}
