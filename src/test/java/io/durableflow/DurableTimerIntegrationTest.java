package io.durableflow;

import io.durableflow.api.ApiException;
import io.durableflow.worker.*;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.engine.WorkflowEngine;
import io.durableflow.workflow.history.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.task.*;
import io.durableflow.workflow.timer.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "durableflow.timers.polling-enabled=false")
class DurableTimerIntegrationTest {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.6");
    @DynamicPropertySource static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
    @Autowired WorkflowDefinitionService definitions;
    @Autowired WorkflowRunService runs;
    @Autowired WorkflowTaskOrchestrator orchestrator;
    @Autowired WorkflowTimerRepository timers;
    @Autowired TimerTransactions dispatcher;
    @Autowired WorkflowHistoryService history;
    @Autowired ActivityTaskRepository tasks;
    @Autowired TaskWorker worker;
    @Autowired WorkflowEngine legacy;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager transactionManager;
    @LocalServerPort int port;
    @BeforeEach void clean() { jdbc.execute("TRUNCATE compensation_execution, workflow_signal_delivery, workflow_signal_wait, workflow_timer, workflow_event, activity_task, activity_execution, workflow_run, workflow_definition"); }

    @Test
    void mixedWorkflowWaitsWithoutWorkerAndResumesToCompletionWithHistoryAndApiVisibility() throws Exception {
        UUID run = start(List.of(activity("payment", "chargePayment"), timer("wait", "payment"), activity("shipment", "createShipment", "wait")));
        orchestrator.advanceRun(run);
        assertThat(timers.findByRun(run)).isEmpty();
        Instant before = now();
        assertThat(worker.runOne()).isTrue();
        var waiting = timers.findByRun(run).getFirst();
        assertThat(waiting.wakeAt()).isBetween(before.plusSeconds(3600), now().plusSeconds(3600));
        assertThat(waiting.status()).isEqualTo(WorkflowTimer.Status.WAITING);
        assertThat(waiting.firedAt()).isNull();
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        assertThat(runs.get(run).activities()).hasSize(2); // TIMER has no ActivityExecution or worker claim.
        assertThat(tasks.findByRunId(run)).hasSize(1);
        assertThat(worker.runOne()).isFalse();
        assertThat(dispatcher.fireOne()).isFalse();
        orchestrator.advanceRun(run);
        assertThat(timers.findByRun(run)).containsExactly(waiting);
        assertThat(runs.get(run).timers()).containsExactly(waiting);
        var response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(
                "http://localhost:" + port + "/api/workflow-runs/" + run)).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.body()).contains("\"timerId\":\"wait\"", "WAITING", "wakeAt", "firedAt");
        assertThat(history.read(run)).filteredOn(e -> e.eventType() == WorkflowEventType.TIMER_SCHEDULED).hasSize(1);
        due(waiting.id());
        Instant firingBefore = now();
        assertThat(dispatcher.fireOne()).isTrue();
        var fired = timers.findById(waiting.id());
        assertThat(fired.firedAt()).isBetween(firingBefore, now());
        assertThat(fired.firedAt()).isAfterOrEqualTo(fired.wakeAt());
        assertThat(tasks.findByRunId(run)).hasSize(2); // No second advance call needed after firing.
        assertThat(worker.runOne()).isTrue();
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        assertThat(history.read(run).stream().map(WorkflowEventResponse::eventType).toList())
                .containsSubsequence(WorkflowEventType.ACTIVITY_COMPLETED, WorkflowEventType.TIMER_SCHEDULED,
                        WorkflowEventType.TIMER_FIRED, WorkflowEventType.ACTIVITY_SCHEDULED, WorkflowEventType.WORKFLOW_COMPLETED);
    }

    @Test
    void concurrentAdvancementSchedulesOneTimerAndOneEvent() throws Exception {
        UUID run = start(List.of(timer("wait")));
        race(() -> { orchestrator.advanceRun(run); return true; }, () -> { orchestrator.advanceRun(run); return true; });
        assertThat(timers.findByRun(run)).hasSize(1);
        assertThat(history.read(run)).filteredOn(e -> e.eventType() == WorkflowEventType.TIMER_SCHEDULED).hasSize(1);
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.RUNNING);
    }

    @Test
    void twoPollersFireOneDueTimerExactlyOnce() throws Exception {
        UUID run = timerRun(); due(timers.findByRun(run).getFirst().id());
        assertThat(race(dispatcher::fireOne, dispatcher::fireOne)).containsExactlyInAnyOrder(true, false);
        assertThat(history.read(run)).filteredOn(e -> e.eventType() == WorkflowEventType.TIMER_FIRED).hasSize(1);
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
    }

    @Test
    void lockedDueHeadDoesNotBlockAnIndependentDueRun() throws Exception {
        UUID first = timerRun(); UUID second = timerRun();
        var head = timers.findByRun(first).getFirst(); var other = timers.findByRun(second).getFirst();
        jdbc.update("UPDATE workflow_timer SET wake_at=clock_timestamp()-INTERVAL '2 seconds' WHERE id=?", head.id());
        due(other.id());
        var locked = new CountDownLatch(1); var release = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var holder = executor.submit(() -> new TransactionTemplate(transactionManager).executeWithoutResult(tx -> {
                jdbc.queryForObject("SELECT id FROM workflow_timer WHERE id=? FOR UPDATE", UUID.class, head.id());
                locked.countDown(); await(release);
            }));
            await(locked);
            assertThat(executor.submit(dispatcher::fireOne).get(10, TimeUnit.SECONDS)).isTrue();
            assertThat(holder.isDone()).isFalse();
            assertThat(timers.findById(head.id()).status()).isEqualTo(WorkflowTimer.Status.WAITING);
            assertThat(timers.findById(other.id()).status()).isEqualTo(WorkflowTimer.Status.FIRED);
            release.countDown(); holder.get(10, TimeUnit.SECONDS);
        } finally { release.countDown(); executor.shutdownNow(); }
    }

    @Test
    void freshApplicationContextKeepsExactDeadlineAndCannotFireEarly() {
        UUID run = timerRun(); var scheduled = timers.findByRun(run).getFirst();
        try (var fresh = freshContext()) {
            var recovered = fresh.getBean(WorkflowTimerRepository.class);
            fresh.getBean(WorkflowTaskOrchestrator.class).advanceRun(run);
            assertThat(recovered.findByRun(run)).containsExactly(scheduled);
            assertThat(fresh.getBean(TimerTransactions.class).fireOne()).isFalse();
            assertThat(recovered.findByRun(run).getFirst().wakeAt()).isEqualTo(scheduled.wakeAt());
        }
    }

    @Test
    void overdueTimerFiresInFreshContextAfterSimulatedDowntime() {
        UUID run = timerRun(); var scheduled = timers.findByRun(run).getFirst(); due(scheduled.id());
        Instant overdueDeadline = timers.findById(scheduled.id()).wakeAt();
        try (var fresh = freshContext()) {
            assertThat(fresh.getBean(TimerTransactions.class).fireOne()).isTrue();
            var recovered = fresh.getBean(WorkflowTimerRepository.class).findById(scheduled.id());
            assertThat(recovered.wakeAt()).isEqualTo(overdueDeadline);
            assertThat(recovered.status()).isEqualTo(WorkflowTimer.Status.FIRED);
            assertThat(fresh.getBean(WorkflowRunService.class).get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
        }
    }

    @Test
    void timerEventFailureRollsBackScheduleOrFire() {
        UUID run = start(List.of(timer("wait")));
        reject(WorkflowEventType.TIMER_SCHEDULED);
        try {
            assertThatThrownBy(() -> orchestrator.advanceRun(run)).isInstanceOf(RuntimeException.class);
            assertThat(timers.findByRun(run)).isEmpty();
        } finally { dropRejection(); }
        orchestrator.advanceRun(run);
        var scheduled = timers.findByRun(run).getFirst(); due(scheduled.id());
        var before = history.read(run);
        reject(WorkflowEventType.TIMER_FIRED);
        try {
            assertThatThrownBy(dispatcher::fireOne).isInstanceOf(RuntimeException.class);
            assertThat(timers.findById(scheduled.id()).status()).isEqualTo(WorkflowTimer.Status.WAITING);
            assertThat(timers.findById(scheduled.id()).firedAt()).isNull();
            assertThat(history.read(run)).isEqualTo(before);
        } finally { dropRejection(); }
        assertThat(dispatcher.fireOne()).isTrue();
    }

    @Test
    void downstreamMaterializationFailureRollsBackFiringSoRecoveryHasNoGap() {
        UUID run = start(List.of(timer("wait"), activity("shipment", "createShipment", "wait")));
        orchestrator.advanceRun(run); var scheduled = timers.findByRun(run).getFirst(); due(scheduled.id());
        var before = history.read(run);
        reject(WorkflowEventType.ACTIVITY_SCHEDULED);
        try {
            assertThatThrownBy(dispatcher::fireOne).isInstanceOf(RuntimeException.class);
            assertThat(timers.findById(scheduled.id()).status()).isEqualTo(WorkflowTimer.Status.WAITING);
            assertThat(tasks.findByRunId(run)).isEmpty();
            assertThat(history.read(run)).isEqualTo(before);
        } finally { dropRejection(); }
        assertThat(dispatcher.fireOne()).isTrue();
        assertThat(tasks.findByRunId(run)).hasSize(1);
    }

    @Test
    void timerChainRequiresEveryTimerAndLegacyExecutionRejectsWithoutFailingRun() {
        UUID run = start(List.of(timer("first"), timer("second", "first")));
        assertThatThrownBy(() -> legacy.executeRun(run)).isInstanceOf(ApiException.class);
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        orchestrator.advanceRun(run); var first = timers.findByRun(run).getFirst(); due(first.id());
        assertThat(dispatcher.fireOne()).isTrue();
        assertThat(timers.findByRun(run)).hasSize(2);
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        var second = timers.findByRun(run).stream().filter(t -> t.timerId().equals("second")).findFirst().orElseThrow();
        assertThat(second.status()).isEqualTo(WorkflowTimer.Status.WAITING);
        due(second.id()); assertThat(dispatcher.fireOne()).isTrue();
        assertThat(runs.get(run).status()).isEqualTo(WorkflowRunStatus.COMPLETED);
    }

    private UUID timerRun() { UUID run = start(List.of(timer("wait"))); orchestrator.advanceRun(run); return run; }
    private UUID start(List<StepDefinition> steps) {
        String name = "Timer-" + UUID.randomUUID(); definitions.register(new RegisterWorkflowDefinitionRequest(name, 1, steps));
        return runs.start(new StartWorkflowRunRequest(name, 1, Map.of("orderId", "TIMERS"))).runId();
    }
    private StepDefinition activity(String id, String type, String... dependencies) { return new StepDefinition(id, type, List.of(dependencies)); }
    private StepDefinition timer(String id, String... dependencies) { return new StepDefinition(id, null, List.of(dependencies), NodeType.TIMER, 3600000L); }
    private Instant now() { return Objects.requireNonNull(jdbc.queryForObject("SELECT clock_timestamp()", OffsetDateTime.class)).toInstant(); }
    private void due(UUID id) { jdbc.update("UPDATE workflow_timer SET wake_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?", id); }
    private ConfigurableApplicationContext freshContext() {
        return new SpringApplicationBuilder(DurableFlowApplication.class).web(WebApplicationType.NONE).run(
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(), "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(), "--durableflow.timers.polling-enabled=false");
    }
    private List<Boolean> race(Callable<Boolean> a, Callable<Boolean> b) throws Exception {
        var barrier = new CyclicBarrier(2); var executor = Executors.newFixedThreadPool(2);
        try {
            var one = executor.submit(() -> { barrier.await(10, TimeUnit.SECONDS); return a.call(); });
            var two = executor.submit(() -> { barrier.await(10, TimeUnit.SECONDS); return b.call(); });
            return List.of(one.get(10, TimeUnit.SECONDS), two.get(10, TimeUnit.SECONDS));
        } finally { executor.shutdownNow(); }
    }
    private static void await(CountDownLatch latch) {
        try { assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue(); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
    }
    private void reject(WorkflowEventType type) {
        jdbc.execute("CREATE FUNCTION reject_timer_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "IF NEW.event_type='" + type.name() + "' THEN RAISE EXCEPTION 'injected event failure'; END IF; RETURN NEW; END $$");
        jdbc.execute("CREATE TRIGGER reject_timer_event BEFORE INSERT ON workflow_event FOR EACH ROW EXECUTE FUNCTION reject_timer_event()");
    }
    private void dropRejection() { jdbc.execute("DROP TRIGGER reject_timer_event ON workflow_event"); jdbc.execute("DROP FUNCTION reject_timer_event()"); }
}
