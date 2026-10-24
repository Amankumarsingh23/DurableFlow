package io.durableflow.workflow.definition;

import io.durableflow.api.ApiException;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;

class WorkflowDefinitionValidatorTest {
    static ValidatorFactory factory;
    static WorkflowDefinitionValidator validator;

    @BeforeAll
    static void setUp() {
        factory = Validation.buildDefaultValidatorFactory();
        validator = new WorkflowDefinitionValidator(factory.getValidator());
    }

    @AfterAll
    static void close() { factory.close(); }

    static StepDefinition step(String id, String... dependencies) {
        return new StepDefinition(id, "activity-" + id, List.of(dependencies));
    }

    static RegisterWorkflowDefinitionRequest request(List<StepDefinition> steps) {
        return new RegisterWorkflowDefinitionRequest("OrderFulfillment", 1, steps);
    }

    @Test
    void acceptsSequentialWorkflow() {
        assertThat(validator.normalizeAndValidate(request(List.of(
                step("payment"), step("inventory", "payment"), step("shipment", "inventory")))).steps())
                .hasSize(3);
    }

    @Test
    void acceptsBranchingDiamondAndDisconnectedRootsRegardlessOfListOrder() {
        assertThatCode(() -> validator.normalizeAndValidate(request(List.of(
                step("D", "B", "C"), step("C", "A"), step("B", "A"), step("A"), step("E")))))
                .doesNotThrowAnyException();
    }

    @Test
    void normalizesWhitespaceAndOmittedOrRepeatedDependencies() {
        var result = validator.normalizeAndValidate(new RegisterWorkflowDefinitionRequest(" Flow ", 1, List.of(
                new StepDefinition(" A ", " first ", null),
                new StepDefinition(" B ", " second ", List.of(" A ", "A")))));
        assertThat(result.name()).isEqualTo("Flow");
        assertThat(result.steps().getFirst()).isEqualTo(new StepDefinition("A", "first", List.of()));
        assertThat(result.steps().get(1).dependsOn()).containsExactly("A");
    }

    static Stream<List<StepDefinition>> invalidGraphs() {
        return Stream.of(
                List.of(step("A"), step("A")),
                List.of(step(" A "), step("A")),
                List.of(step("A", "missing")),
                List.of(step("A", "A")),
                List.of(step("A", "B"), step("B", "C"), step("C", "A")),
                List.of(step("root"), step("A", "B"), step("B", "A")));
    }

    @ParameterizedTest
    @MethodSource("invalidGraphs")
    void rejectsInvalidGraph(List<StepDefinition> steps) {
        assertThatThrownBy(() -> validator.normalizeAndValidate(request(steps)))
                .isInstanceOf(ApiException.class);
    }

    static Stream<RegisterWorkflowDefinitionRequest> invalidFields() {
        return Stream.of(
                new RegisterWorkflowDefinitionRequest(" ", 1, List.of(step("A"))),
                new RegisterWorkflowDefinitionRequest("Flow", 0, List.of(step("A"))),
                request(List.of()),
                request(List.of(new StepDefinition(" ", "activity", List.of()))),
                request(List.of(new StepDefinition("A", " ", List.of()))),
                request(List.of(new StepDefinition("A", "activity", List.of(" ")))),
                request(java.util.Arrays.asList((StepDefinition) null)),
                request(List.of(new StepDefinition("A", "activity", java.util.Arrays.asList((String) null)))),
                new RegisterWorkflowDefinitionRequest("x".repeat(201), 1, List.of(step("A"))),
                request(List.of(step("x".repeat(201)))));
    }

    @ParameterizedTest
    @MethodSource("invalidFields")
    void rejectsInvalidFields(RegisterWorkflowDefinitionRequest request) {
        assertThatThrownBy(() -> validator.normalizeAndValidate(request)).isInstanceOf(ApiException.class);
    }
}
