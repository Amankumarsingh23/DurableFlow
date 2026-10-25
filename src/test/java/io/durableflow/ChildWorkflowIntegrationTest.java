package io.durableflow;

import io.durableflow.api.ApiException;
import io.durableflow.worker.*;
import io.durableflow.workflow.activity.*;
import io.durableflow.workflow.child.*;
import io.durableflow.workflow.compensation.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.history.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.task.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.util.*;
import java.util.concurrent.*;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
@SpringBootTest(properties={"durableflow.timers.polling-enabled=false", "durableflow.children.polling-enabled=false",
        "durableflow.schedules.polling-enabled=false", "durableflow.children.max-depth=2"})
class ChildWorkflowIntegrationTest {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.6");
    @DynamicPropertySource static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl); registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
    @Autowired WorkflowDefinitionService definitions;
    @Autowired WorkflowRunService runs;
    @Autowired WorkflowTaskOrchestrator orchestrator;
    @Autowired ChildWorkflowRepository children;
    @Autowired ChildReconciler reconciler;
    @Autowired TaskTransactions transactions;
    @Autowired ActivityExecutionRepository activities;
    @Autowired ActivityTaskRepository tasks;
    @Autowired CompensationExecutionRepository compensations;
    @Autowired WorkflowHistoryService history;
    @Autowired JdbcTemplate jdbc;
    @Autowired io.durableflow.workflow.signal.SignalReceiver receiver;
    @BeforeEach void clean() { jdbc.execute("TRUNCATE agent_iteration, agent_execution, fake_tool_effect, tool_request, workflow_schedule_occurrence, workflow_schedule, child_workflow_execution, compensation_execution, workflow_signal_delivery, workflow_signal_wait, workflow_timer, workflow_event, activity_task, activity_execution, workflow_run, workflow_definition"); }

    @Test
    void createsOnePinnedChildWithInheritedInputAndWaitsWithoutWorkerTask() {
        String leaf = name(); register(leaf, 1, List.of(activity("old"))); register(leaf, 2, List.of(activity("new")));
        UUID parent = start(List.of(child("shipping", leaf, 1, null), activity("finish", "shipping")));
        orchestrator.advanceRun(parent); orchestrator.advanceRun(parent);
        var relationship = children.findByParent(parent).getFirst();
        assertThat(children.findByParent(parent)).hasSize(1); assertThat(tasks.findByRunId(parent)).isEmpty();
        assertThat(runs.get(parent).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        assertThat(runs.get(parent).children()).hasSize(1);
        assertThat(runs.get(relationship.childRunId()).version()).isEqualTo(1);
        assertThat(runs.get(relationship.childRunId()).input()).containsEntry("orderId", "child-demo");
        assertThat(runs.get(relationship.childRunId()).activities()).extracting(WorkflowRunResponse.ActivityState::activityId).containsExactly("old");
        register(leaf, 3, List.of(activity("latest")));
        assertThat(reconciler.reconcileOne()).isTrue();
        assertThat(transactions.claim("worker").orElseThrow().runId()).isEqualTo(relationship.childRunId());
        assertThat(count(parent, WorkflowEventType.CHILD_WORKFLOW_STARTED)).isEqualTo(1);
        assertThat(count(relationship.childRunId(), WorkflowEventType.WORKFLOW_STARTED)).isEqualTo(1);
    }

    @Test
    void literalChildInputDoesNotMergeParentInput() {
        String leaf = leaf(); UUID parent = start(List.of(child("child", leaf, 1, Map.of("region", "IN"))));
        orchestrator.advanceRun(parent);
        assertThat(runs.get(children.findByParent(parent).getFirst().childRunId()).input()).containsExactlyEntriesOf(Map.of("region", "IN"));
    }

    @Test
    void concurrentAdvancementCreatesExactlyOneChildAndOneParentEvent() throws Exception {
        String leaf = leaf(); UUID parent = start(List.of(child("child", leaf, 1, null)));
        race(List.of(() -> { orchestrator.advanceRun(parent); return true; }, () -> { orchestrator.advanceRun(parent); return true; },
                () -> { orchestrator.advanceRun(parent); return true; }));
        assertThat(children.findByParent(parent)).hasSize(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_run", Long.class)).isEqualTo(2L);
        assertThat(count(parent, WorkflowEventType.CHILD_WORKFLOW_STARTED)).isEqualTo(1);
    }

    @Test
    void terminalChildPollAdvancesParentAndSeparatesHistories() {
        String leaf = leaf(); UUID parent = start(List.of(child("child", leaf, 1, null), activity("finish", "child")));
        orchestrator.advanceRun(parent); var child = children.findByParent(parent).getFirst();
        finishChild(child.childRunId()); // Parent has not yet observed the committed child outcome.
        assertThat(count(parent, WorkflowEventType.CHILD_WORKFLOW_COMPLETED)).isZero();
        assertThat(reconciler.reconcileOne()).isTrue();
        assertThat(tasks.findByRunId(parent)).hasSize(1);
        orchestrator.advanceRun(parent);
        assertThat(count(parent, WorkflowEventType.CHILD_WORKFLOW_COMPLETED)).isEqualTo(1);
        assertThat(history.read(parent)).filteredOn(e -> e.eventType() == WorkflowEventType.ACTIVITY_COMPLETED).isEmpty();
        assertThat(history.read(child.childRunId())).filteredOn(e -> e.eventType() == WorkflowEventType.CHILD_WORKFLOW_STARTED).isEmpty();
        var finish = transactions.claim("parent-finish").orElseThrow(); assertThat(finish.runId()).isEqualTo(parent);
        transactions.complete(finish, Map.of("done", true)); orchestrator.advanceRun(parent);
        assertThat(runs.get(parent).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
    }

    @Test
    void freshContextRecoversChildCompletionBeforeParentNotification() {
        String leaf = leaf(); UUID parent = start(List.of(child("child", leaf, 1, null), activity("finish", "child")));
        orchestrator.advanceRun(parent); var child = children.findByParent(parent).getFirst(); finishChild(child.childRunId());
        try (var fresh = freshContext()) {
            var advance = fresh.getBean(WorkflowTaskOrchestrator.class);
            advance.advanceRun(parent); advance.advanceRun(parent);
            assertThat(fresh.getBean(ChildWorkflowRepository.class).findByParent(parent)).hasSize(1);
        }
        assertThat(tasks.findByRunId(parent)).hasSize(1);
        assertThat(count(parent, WorkflowEventType.CHILD_WORKFLOW_COMPLETED)).isEqualTo(1);
    }

    @Test
    void failedChildFailsParentAndCannotSatisfyJoin() {
        String leaf = leaf(); UUID parent = start(List.of(child("child", leaf, 1, null), activity("finish", "child")));
        orchestrator.advanceRun(parent); var child = children.findByParent(parent).getFirst();
        orchestrator.advanceRun(child.childRunId()); transactions.fail(transactions.claim("failure").orElseThrow(), "permanent");
        orchestrator.advanceRun(parent);
        assertThat(runs.get(parent).status()).isEqualTo(WorkflowRunStatus.FAILED);
        assertThat(tasks.findByRunId(parent)).isEmpty();
        assertThat(history.read(parent)).filteredOn(e -> e.eventType() == WorkflowEventType.CHILD_WORKFLOW_FAILED)
                .allMatch(e -> e.metadata().get("status").equals("FAILED")).hasSize(1);
    }

    @Test
    void childCompensatesOwnInternalsAndParentCompensatesOnlyItsOwnActivities() {
        var fixture = sagaChild(); UUID parent = fixture.parent(); UUID child = fixture.child();
        orchestrator.advanceRun(child);
        var a = transactions.claim("child-A").orElseThrow(); transactions.complete(a, Map.of("paymentId", "child-payment")); orchestrator.advanceRun(child);
        transactions.fail(transactions.claim("child-B").orElseThrow(), "failed");
        orchestrator.advanceRun(parent);
        assertThat(runs.get(parent).status()).isEqualTo(WorkflowRunStatus.RUNNING); // Child is still COMPENSATING.
        var undo = transactions.claim("child-undo").orElseThrow(); transactions.complete(undo, Map.of("undone", true));
        assertThat(runs.get(child).status()).isEqualTo(WorkflowRunStatus.COMPENSATED);
        orchestrator.advanceRun(parent);
        assertThat(runs.get(parent).status()).isEqualTo(WorkflowRunStatus.COMPENSATING);
        var parentUndo = transactions.claim("parent-undo").orElseThrow();
        assertThat(parentUndo.runId()).isEqualTo(parent); assertThat(parentUndo.activityExecutionId()).isEqualTo(fixture.parentActivity());
        assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(parent)).hasSize(1)
                .allMatch(e -> e.getOriginalActivityExecutionId().equals(fixture.parentActivity()));
        transactions.complete(parentUndo, Map.of("ok", true));
        assertThat(runs.get(parent).status()).isEqualTo(WorkflowRunStatus.COMPENSATED);
        assertThat(count(parent, WorkflowEventType.CHILD_WORKFLOW_FAILED)).isEqualTo(1);
        assertThat(count(parent, WorkflowEventType.CHILD_WORKFLOW_COMPLETED)).isZero();
    }

    @Test
    void childCompensationFailureIsExplicitInParentHistory() {
        var fixture = sagaChild(); UUID child = fixture.child();
        orchestrator.advanceRun(child); transactions.complete(transactions.claim("A").orElseThrow(), Map.of("paymentId", "child")); orchestrator.advanceRun(child);
        transactions.fail(transactions.claim("B").orElseThrow(), "fail");
        transactions.fail(transactions.claim("undo").orElseThrow(), "refund unavailable permanently");
        orchestrator.advanceRun(fixture.parent());
        assertThat(runs.get(child).status()).isEqualTo(WorkflowRunStatus.COMPENSATION_FAILED);
        assertThat(history.read(fixture.parent())).filteredOn(e -> e.eventType() == WorkflowEventType.CHILD_WORKFLOW_FAILED)
                .hasSize(1).allMatch(e -> e.metadata().get("status").equals("COMPENSATION_FAILED"));
    }

    @Test
    void childFanOutAndAndJoinWaitForEveryChild() {
        String leaf = leaf(); UUID parent = start(List.of(activity("finish", "left", "right"), child("left", leaf, 1, null), child("right", leaf, 1, null)));
        orchestrator.advanceRun(parent);
        var children = this.children.findByParent(parent); assertThat(children).hasSize(2);
        finishChild(children.getFirst().childRunId()); orchestrator.advanceRun(parent);
        assertThat(tasks.findByRunId(parent)).isEmpty(); assertThat(runs.get(parent).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        finishChild(children.getLast().childRunId()); orchestrator.advanceRun(parent); orchestrator.advanceRun(parent);
        assertThat(tasks.findByRunId(parent)).hasSize(1); assertThat(count(parent, WorkflowEventType.CHILD_WORKFLOW_COMPLETED)).isEqualTo(2);
    }

    @Test
    void parentSagaWaitsForActiveChildAndDoesNotUndoSuccessfulChildInternals() {
        String leaf = name(); register(leaf, 1, List.of(undoable("child-payment")));
        UUID parent = start(List.of(undoable("root"),
                new StepDefinition("child", null, List.of("root"), NodeType.CHILD_WORKFLOW, null, null, leaf, 1, null), activity("fail", "root")));
        orchestrator.advanceRun(parent);
        transactions.complete(transactions.claim("root").orElseThrow(), Map.of("paymentId", "parent")); orchestrator.advanceRun(parent);
        var child = children.findByParent(parent).getFirst();
        transactions.fail(transactions.claim("sibling").orElseThrow(), "failed");
        assertThat(runs.get(parent).status()).isEqualTo(WorkflowRunStatus.COMPENSATING);
        assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(parent)).isEmpty();
        finishChild(child.childRunId()); orchestrator.advanceRun(parent);
        assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(parent)).hasSize(1);
        assertThat(compensations.findByWorkflowRunIdOrderByCreatedAtAsc(child.childRunId())).isEmpty();
        assertThat(runs.get(child.childRunId()).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
    }

    @Test
    void directAndReachableRecursiveDefinitionsAreRejected() {
        String direct = name(); assertThatThrownBy(() -> register(direct, 1, List.of(child("self", direct, 1, null))))
                .isInstanceOf(ApiException.class).hasMessageContaining("Recursive");
        String a = name(), b = name(); register(a, 1, List.of(child("B", b, 1, null)));
        assertThatThrownBy(() -> register(b, 1, List.of(child("A", a, 1, null))))
                .isInstanceOf(ApiException.class).hasMessageContaining("Recursive");
    }

    @Test
    void racingForwardReferenceRegistrationsCannotCommitACompositionCycle() throws Exception {
        String a = name(), b = name();
        var results = race(List.of(() -> attemptRegister(a, b), () -> attemptRegister(b, a)));
        assertThat(results).containsExactlyInAnyOrder(true, false);
    }

    @Test
    void runtimeDepthLimitTerminatesWithoutCreatingAnUnboundedChain() {
        String a = name(), b = name(), c = name(), d = leaf();
        register(c, 1, List.of(child("D", d, 1, null))); register(b, 1, List.of(child("C", c, 1, null))); register(a, 1, List.of(child("B", b, 1, null)));
        UUID root = runs.start(new StartWorkflowRunRequest(a, 1, Map.of())).runId(); orchestrator.advanceRun(root);
        for (int i=0; i<12 && runs.get(root).status() == WorkflowRunStatus.RUNNING; i++) reconciler.reconcileOne();
        assertThat(runs.get(root).status()).isEqualTo(WorkflowRunStatus.FAILED);
        assertThat(jdbc.queryForObject("SELECT max(child_depth) FROM workflow_run", Integer.class)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_run", Long.class)).isEqualTo(3L);
    }

    @Test
    void parentHistoryFailureRollsBackChildRunRelationAndChildHistory() {
        String leaf = leaf(); UUID parent = start(List.of(child("child", leaf, 1, null)));
        jdbc.execute("CREATE FUNCTION reject_child_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_type='CHILD_WORKFLOW_STARTED' THEN RAISE EXCEPTION 'injected'; END IF; RETURN NEW; END $$");
        jdbc.execute("CREATE TRIGGER reject_child_event BEFORE INSERT ON workflow_event FOR EACH ROW EXECUTE FUNCTION reject_child_event()");
        try {
            assertThatThrownBy(() -> orchestrator.advanceRun(parent)).isInstanceOf(RuntimeException.class);
            assertThat(children.findByParent(parent)).isEmpty(); assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_run", Long.class)).isEqualTo(1L);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_event", Long.class)).isEqualTo(1L);
        } finally { jdbc.execute("DROP TRIGGER reject_child_event ON workflow_event"); jdbc.execute("DROP FUNCTION reject_child_event()"); }
        orchestrator.advanceRun(parent); assertThat(children.findByParent(parent)).hasSize(1);
    }

    @Test
    void unresolvedPinnedForwardReferenceCreatesNothingAndRecoversAfterRegistration() {
        String future = name(); UUID parent = start(List.of(child("child", future, 1, null)));
        assertThatThrownBy(() -> orchestrator.advanceRun(parent)).isInstanceOf(ApiException.class).hasMessageContaining("not registered");
        assertThat(runs.get(parent).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        assertThat(children.findByParent(parent)).isEmpty(); assertThat(count(parent, WorkflowEventType.CHILD_WORKFLOW_STARTED)).isZero();
        register(future, 1, List.of(activity("work"))); orchestrator.advanceRun(parent);
        assertThat(children.findByParent(parent)).hasSize(1);
    }

    @Test
    void bufferedSignalFixedPointDoesNotMaterializeRootChildTwice() {
        String leaf = leaf(); UUID parent = start(List.of(child("child", leaf, 1, null),
                new StepDefinition("gate", null, List.of(), NodeType.SIGNAL, null), activity("finish", "child", "gate")));
        // Receipt performs advancement with a newly satisfied signal, forcing a second fixed-point pass.
        receiver.receive(parent, new io.durableflow.workflow.signal.ReceiveSignalRequest("early", "gate", Map.of("ok", true)));
        assertThat(children.findByParent(parent)).hasSize(1);
        assertThat(count(parent, WorkflowEventType.CHILD_WORKFLOW_STARTED)).isEqualTo(1);
        assertThat(tasks.findByRunId(parent)).isEmpty();
    }

    private record SagaFixture(UUID parent, UUID child, UUID parentActivity) {}
    private SagaFixture sagaChild() {
        String leaf = name(); register(leaf, 1, List.of(undoable("A"), activity("B", "A")));
        UUID parent = start(List.of(undoable("parent-payment"), new StepDefinition("child", null, List.of("parent-payment"), NodeType.CHILD_WORKFLOW, null, null, leaf, 1, null)));
        orchestrator.advanceRun(parent); var root = transactions.claim("root").orElseThrow();
        transactions.complete(root, Map.of("paymentId", "parent-payment")); orchestrator.advanceRun(parent);
        return new SagaFixture(parent, children.findByParent(parent).getFirst().childRunId(), root.activityExecutionId());
    }
    private void finishChild(UUID child) {
        orchestrator.advanceRun(child); var claim = transactions.claim("child-worker").orElseThrow(); assertThat(claim.runId()).isEqualTo(child);
        transactions.complete(claim, Map.of("done", true)); orchestrator.advanceRun(child);
    }
    private boolean attemptRegister(String from, String to) {
        try { register(from, 1, List.of(child("next", to, 1, null))); return true; } catch (ApiException rejected) { return false; }
    }
    private String leaf() { String name = name(); register(name, 1, List.of(activity("work"))); return name; }
    private String name() { return "Child-" + UUID.randomUUID(); }
    private void register(String name, int version, List<StepDefinition> steps) { definitions.register(new RegisterWorkflowDefinitionRequest(name, version, steps)); }
    private UUID start(List<StepDefinition> steps) { String name = name(); register(name, 1, steps); return runs.start(new StartWorkflowRunRequest(name, 1, Map.of("orderId", "child-demo"))).runId(); }
    private StepDefinition activity(String id, String... parents) { return new StepDefinition(id, "submitRequest", List.of(parents)); }
    private StepDefinition undoable(String id) { return new StepDefinition(id, "chargePayment", List.of(), NodeType.ACTIVITY, null, "refundPayment"); }
    private StepDefinition child(String id, String name, int version, Map<String, Object> input) { return new StepDefinition(id, null, List.of(), NodeType.CHILD_WORKFLOW, null, null, name, version, input); }
    private long count(UUID run, WorkflowEventType type) { return history.read(run).stream().filter(e -> e.eventType() == type).count(); }
    private List<Boolean> race(List<Callable<Boolean>> actions) throws Exception {
        var pool = Executors.newFixedThreadPool(actions.size()); var gate = new CountDownLatch(1);
        try {
            var futures = new ArrayList<Future<Boolean>>();
            for (var action : actions) futures.add(pool.submit(() -> { assertThat(gate.await(10, TimeUnit.SECONDS)).isTrue(); return action.call(); }));
            gate.countDown(); var results = new ArrayList<Boolean>(); for (var future : futures) results.add(future.get(10, TimeUnit.SECONDS)); return results;
        } finally { gate.countDown(); pool.shutdownNow(); }
    }
    private ConfigurableApplicationContext freshContext() {
        return new SpringApplicationBuilder(DurableFlowApplication.class).web(WebApplicationType.NONE).run(
                "--spring.datasource.url="+POSTGRES.getJdbcUrl(), "--spring.datasource.username="+POSTGRES.getUsername(), "--spring.datasource.password="+POSTGRES.getPassword(),
                "--durableflow.timers.polling-enabled=false", "--durableflow.children.polling-enabled=false", "--durableflow.schedules.polling-enabled=false", "--durableflow.children.max-depth=2");
    }
}
