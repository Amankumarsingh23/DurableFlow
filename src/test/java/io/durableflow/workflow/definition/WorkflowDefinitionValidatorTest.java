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
    void childNodesRequirePinnedVersionAndBoundedLiteralInputAndRoundTrip() {
        var child = new StepDefinition("shipping", null, List.of(), NodeType.CHILD_WORKFLOW, null, null, " Shipping ", 2, java.util.Map.of("region", "IN"));
        var normalized = validator.normalizeAndValidate(request(List.of(child, step("finish", "shipping"))));
        assertThat(normalized.steps().getFirst().childWorkflow()).isEqualTo("Shipping");
        assertThat(WorkflowDefinitionDocument.steps(WorkflowDefinitionDocument.encode(normalized))).isEqualTo(normalized.steps());
        for (var invalid : List.of(
                new StepDefinition("x", null, List.of(), NodeType.CHILD_WORKFLOW, null, null, "Shipping", 0, null),
                new StepDefinition("x", null, List.of(), NodeType.CHILD_WORKFLOW, null, null, null, 1, null),
                new StepDefinition("x", "handler", List.of(), NodeType.CHILD_WORKFLOW, null, null, "Shipping", 1, null),
                new StepDefinition("x", null, List.of(), NodeType.CHILD_WORKFLOW, null, "undo", "Shipping", 1, null),
                new StepDefinition("x", "handler", List.of(), NodeType.ACTIVITY, null, null, "Shipping", 1, null))) {
            assertThatThrownBy(() -> validator.normalizeAndValidate(request(List.of(invalid)))).isInstanceOf(ApiException.class);
        }
    }

    @Test
    void compensationRoundTripsAndAcceptsStaticDags() {
        var payment = new StepDefinition("payment", "chargePayment", List.of(), NodeType.ACTIVITY, null, " refundPayment ");
        var valid = validator.normalizeAndValidate(request(List.of(step("shipment", "payment"), payment)));
        assertThat(valid.steps().get(1).compensation()).isEqualTo("refundPayment");
        assertThat(WorkflowDefinitionDocument.steps(WorkflowDefinitionDocument.encode(valid))).isEqualTo(valid.steps());
        assertThatCode(() -> validator.normalizeAndValidate(request(List.of(step("A"), step("B"))))).doesNotThrowAnyException();
        for (var invalid : List.of(
                new StepDefinition("x", null, List.of(), NodeType.TIMER, 1L, "refundPayment"),
                new StepDefinition("x", null, List.of(), NodeType.SIGNAL, null, "refundPayment"),
                new StepDefinition("x", "chargePayment", List.of(), NodeType.ACTIVITY, null, " "),
                new StepDefinition("x", "chargePayment", List.of(), NodeType.ACTIVITY, null, "bad handler"))) {
            assertThatThrownBy(() -> validator.normalizeAndValidate(request(List.of(invalid)))).isInstanceOf(ApiException.class);
        }
        assertThatCode(() -> validator.normalizeAndValidate(request(List.of(payment, step("other"))))).doesNotThrowAnyException();
        assertThatCode(() -> validator.normalizeAndValidate(request(List.of(payment, step("B", "payment"), step("C", "payment")))))
                .doesNotThrowAnyException();
        assertThatCode(() -> validator.normalizeAndValidate(request(List.of(payment, step("B", "payment"), step("C", "payment", "B")))))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> validator.normalizeAndValidate(request(List.of(payment, step("B", "missing")))))
                .isInstanceOf(ApiException.class).hasMessageContaining("Unknown");
        var cyclicPayment = new StepDefinition("payment", "chargePayment", List.of("B"), NodeType.ACTIVITY, null, "refundPayment");
        assertThatThrownBy(() -> validator.normalizeAndValidate(request(List.of(cyclicPayment, step("B", "payment")))))
                .isInstanceOf(ApiException.class).hasMessageContaining("cycle");
    }

    @Test
    void supportsExactSignalNodesInMixedGraphAndDocumentRoundTrip() {
        var signal = new StepDefinition("approval", null, List.of("wait"), NodeType.SIGNAL, null);
        var mixed = validator.normalizeAndValidate(request(List.of(step("submit"),
                new StepDefinition("wait", null, List.of("submit"), NodeType.TIMER, 1000L), signal,
                step("finish", "approval"))));
        assertThat(WorkflowDefinitionDocument.steps(WorkflowDefinitionDocument.encode(mixed))).isEqualTo(mixed.steps());
    }

    @Test
    void rejectsAmbiguousSignalFieldsDuplicateIdsAndMixedCycles() {
        assertThatThrownBy(() -> validator.normalizeAndValidate(request(List.of(
                new StepDefinition("approval", "submitRequest", List.of(), NodeType.SIGNAL, null)))))
                .isInstanceOf(ApiException.class);
        assertThatThrownBy(() -> validator.normalizeAndValidate(request(List.of(
                new StepDefinition("approval", null, List.of(), NodeType.SIGNAL, 1000L)))))
                .isInstanceOf(ApiException.class);
        assertThatThrownBy(() -> validator.normalizeAndValidate(request(List.of(step("same"),
                new StepDefinition("same", null, List.of(), NodeType.SIGNAL, null)))))
                .isInstanceOf(ApiException.class);
        assertThatThrownBy(() -> validator.normalizeAndValidate(request(List.of(step("A", "approval"),
                new StepDefinition("wait", null, List.of("A"), NodeType.TIMER, 1000L),
                new StepDefinition("approval", null, List.of("wait"), NodeType.SIGNAL, null)))))
                .isInstanceOf(ApiException.class).hasMessageContaining("cycle");
    }

    @Test
    void acceptsMixedNodesAndRoundTripsBothFormats() {
        var timer = new StepDefinition("wait", null, List.of("payment"), NodeType.TIMER, 86400000L);
        var normalized = validator.normalizeAndValidate(request(List.of(step("payment"), timer, step("shipment", "wait"))));
        assertThat(WorkflowDefinitionDocument.steps(WorkflowDefinitionDocument.encode(normalized))).isEqualTo(normalized.steps());
        var old = java.util.Map.<String, Object>of("steps", List.of(java.util.Map.of(
                "id", "payment", "activity", "chargePayment", "dependsOn", List.of())));
        assertThat(WorkflowDefinitionDocument.steps(old).getFirst().type()).isEqualTo(NodeType.ACTIVITY);
    }

    @Test
    void rejectsInvalidTimerDurationsAmbiguousNodesAndMixedCycles() {
        for (Long delay : java.util.Arrays.asList(null, 0L, -1L, Long.MAX_VALUE)) {
            assertThatThrownBy(() -> validator.normalizeAndValidate(request(List.of(
                    new StepDefinition("wait", null, List.of(), NodeType.TIMER, delay)))))
                    .isInstanceOf(ApiException.class);
        }
        assertThatThrownBy(() -> validator.normalizeAndValidate(request(List.of(
                new StepDefinition("wait", "chargePayment", List.of(), NodeType.TIMER, 1000L)))))
                .isInstanceOf(ApiException.class);
        assertThatThrownBy(() -> validator.normalizeAndValidate(request(List.of(
                new StepDefinition("A", "chargePayment", List.of(), NodeType.ACTIVITY, 1000L)))))
                .isInstanceOf(ApiException.class);
        assertThatThrownBy(() -> validator.normalizeAndValidate(request(List.of(
                step("A", "wait"), new StepDefinition("wait", null, List.of("A"), NodeType.TIMER, 1000L)))))
                .isInstanceOf(ApiException.class).hasMessageContaining("cycle");
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
