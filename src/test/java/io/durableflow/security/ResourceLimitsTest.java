package io.durableflow.security;
import io.durableflow.api.ApiException;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.signal.*;
import io.durableflow.workflow.schedule.*;
import io.durableflow.tools.*;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ResourceLimitsTest {
    private ResourceLimits small() {return new ResourceLimits(256,2000,2,1,64,32,32,48,4,20);}
    private void oversized(Runnable action,String code) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(ApiException.class,e -> {assertThat(e.status().value()).isEqualTo(413);assertThat(e.getMessage()).isEqualTo(code);});
    }
    @Test void byteLimitsCountUtf8InsteadOfCharactersAndAcceptExactBoundary() {
        // {"x":"..."} has eight UTF-8 overhead bytes.
        var limits=small();limits.checkJson(ResourceLimits.Payload.SIGNAL,Map.of("x","a".repeat(24)));
        oversized(() -> limits.checkJson(ResourceLimits.Payload.SIGNAL,Map.of("x","a".repeat(25))),"SIGNAL_BYTES_LIMIT");
        oversized(() -> limits.checkJson(ResourceLimits.Payload.SIGNAL,Map.of("x","\u754c".repeat(9))),"SIGNAL_BYTES_LIMIT");
    }
    @Test void deepWideAndCyclicInputsFailBeforeSerializationCanExhaustTheStack() {
        Object nested=Map.of("x",1);for(int i=0;i<5;i++)nested=Map.of("x",nested);Object tooDeep=nested;
        oversized(() -> small().checkJson(ResourceLimits.Payload.WORKFLOW_INPUT,tooDeep),"JSON_DEPTH_LIMIT");
        oversized(() -> small().checkJson(ResourceLimits.Payload.WORKFLOW_INPUT,Collections.nCopies(21,1)),"JSON_ELEMENTS_LIMIT");
        assertThatThrownBy(() -> small().checkJson(ResourceLimits.Payload.SIGNAL,Map.of("x",Double.NaN))).isInstanceOf(ApiException.class).hasMessage("JSON_VALUES_REQUIRED");
        var cycle=new HashMap<String,Object>();cycle.put("self",cycle);
        oversized(() -> small().checkJson(ResourceLimits.Payload.WORKFLOW_INPUT,cycle),"JSON_DEPTH_LIMIT");
        assertThatThrownBy(() -> small().checkJson(ResourceLimits.Payload.SIGNAL,Map.of("x",new Object()))).isInstanceOf(ApiException.class).hasMessage("JSON_VALUES_REQUIRED");
    }
    @Test void definitionNodeAndRawDependencyBoundsRunBeforeGraphTraversalOrBeanValidation() {
        var validator=mock(Validator.class);var definitions=new WorkflowDefinitionValidator(validator,small());
        var step=new StepDefinition("a","payment",List.of());
        oversized(() -> definitions.normalizeNewDefinition(new RegisterWorkflowDefinitionRequest("large",1,List.of(step,step,step))),"DEFINITION_NODES_LIMIT");
        oversized(() -> definitions.normalizeNewDefinition(new RegisterWorkflowDefinitionRequest("wide",1,List.of(new StepDefinition("a","payment",List.of("b","b"))))),"NODE_DEPENDENCIES_LIMIT");
        verifyNoInteractions(validator);
        try(var factory=jakarta.validation.Validation.buildDefaultValidatorFactory()) {
            var guarded=new WorkflowDefinitionValidator(factory.getValidator(),small());
            var old=new RegisterWorkflowDefinitionRequest("existing",1,List.of(new StepDefinition("a","payment",List.of()),new StepDefinition("b","payment",List.of()),new StepDefinition("c","payment",List.of())));
            // Recovery validation is separate from registration admission after limits are lowered.
            assertThat(guarded.normalizeAndValidate(old).steps()).hasSize(3);
            oversized(() -> guarded.normalizeNewDefinition(old),"DEFINITION_NODES_LIMIT");
        }
    }
    @Test void serviceEntryPointsEnforceLimitsBeforeRepositoryWork() {
        var large=Map.<String,Object>of("x","s".repeat(80));var limits=small();
        var runs=new WorkflowRunService(null,null,null,null,null,null,null,null,null,null,null,null,limits);
        oversized(() -> runs.start(new StartWorkflowRunRequest("none",1,large)),"WORKFLOW_INPUT_BYTES_LIMIT");
        var signals=new SignalReceiver(null,null,null,null,null,null,List.of(),limits);
        oversized(() -> signals.receive(UUID.randomUUID(),new ReceiveSignalRequest("s","approval",large)),"SIGNAL_BYTES_LIMIT");
        var scheduleRepository=mock(ScheduleRepository.class);var definitions=mock(WorkflowDefinitionRepository.class);var validator=mock(Validator.class);
        var schedules=new ScheduleService(scheduleRepository,definitions,validator,limits);
        oversized(() -> schedules.create(new CreateScheduleRequest("s","none",1,large,ScheduleType.START_AT,java.time.Instant.now(),null)),"SCHEDULE_INPUT_BYTES_LIMIT");
        verifyNoInteractions(scheduleRepository,definitions,validator);
        var tools=new ToolRequestFactory(null,null,null,null,null,"demo-agent","tenant-a",limits);
        oversized(() -> tools.create(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"execute",null,"getOrder",large),"TOOL_ARGUMENTS_BYTES_LIMIT");
    }
    @Test void definitionSerializationAndChildInputsAreAlsoBounded() {
        var limits=new ResourceLimits(256,64,2,1,32,32,32,32,4,20);
        oversized(() -> limits.definition(new RegisterWorkflowDefinitionRequest("n",1,List.of(new StepDefinition("a","payment",List.of())))),"DEFINITION_BYTES_LIMIT");
        var child=new StepDefinition("child",null,List.of(),NodeType.CHILD_WORKFLOW,null,null,"child",1,Map.of("x","s".repeat(40)));
        oversized(() -> limits.definition(new RegisterWorkflowDefinitionRequest("n",1,List.of(child))),"WORKFLOW_INPUT_BYTES_LIMIT");
    }
    @Test void invalidConfigurationCannotDisableLimitsOrEnableUnboundedAllocations() {
        assertThatThrownBy(() -> new ResourceLimits(0,1,1,1,1,1,1,1,1,1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ResourceLimits(9000000,1,1,1,1,1,1,1,1,1)).isInstanceOf(IllegalArgumentException.class);
    }
}
