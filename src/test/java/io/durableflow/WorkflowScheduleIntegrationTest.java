package io.durableflow;

import io.durableflow.api.ApiException;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.history.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.schedule.*;
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
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
@SpringBootTest(properties={"durableflow.timers.polling-enabled=false", "durableflow.children.polling-enabled=false",
        "durableflow.schedules.polling-enabled=false", "durableflow.schedules.max-catch-up-occurrences=2"})
class WorkflowScheduleIntegrationTest {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.6");
    @DynamicPropertySource static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl); registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
    @Autowired WorkflowDefinitionService definitions;
    @Autowired WorkflowRunService runs;
    @Autowired WorkflowHistoryService history;
    @Autowired ScheduleService service;
    @Autowired ScheduleRepository schedules;
    @Autowired ScheduleTransactions transactions;
    @Autowired ScheduleDispatcher dispatcher;
    @Autowired ActivityTaskRepository tasks;
    @Autowired JdbcTemplate jdbc;
    @BeforeEach void clean() { jdbc.execute("TRUNCATE fake_tool_effect, tool_request, workflow_schedule_occurrence, workflow_schedule, child_workflow_execution, compensation_execution, workflow_signal_delivery, workflow_signal_wait, workflow_timer, workflow_event, activity_task, activity_execution, workflow_run, workflow_definition"); }

    @Test
    void futureScheduleDoesNotFireEarlyAndDueOneTimeCreatesOneNormallyInitializedRun() {
        var schedule = create(ScheduleType.START_AT, now().plusSeconds(3600), null);
        assertThat(transactions.fireOne()).isFalse(); assertThat(service.occurrences(schedule.id())).isEmpty();
        jdbc.update("UPDATE workflow_schedule SET next_fire_at=clock_timestamp()-INTERVAL '1 second' WHERE id=?", schedule.id());
        var due = service.get(schedule.id());
        assertThat(transactions.fireOne()).isTrue(); assertThat(transactions.fireOne()).isFalse();
        var occurrence = service.occurrences(schedule.id()).getFirst();
        assertThat(occurrence.scheduledFor()).isEqualTo(due.nextFireAt());
        assertThat(service.get(schedule.id()).enabled()).isFalse();
        assertThat(service.occurrences(schedule.id())).hasSize(1);
        assertThat(runs.get(occurrence.workflowRunId()).status()).isEqualTo(WorkflowRunStatus.RUNNING);
        assertThat(tasks.findByRunId(occurrence.workflowRunId())).hasSize(1);
        assertThat(history.read(occurrence.workflowRunId())).extracting(WorkflowEventResponse::eventType)
                .containsExactly(WorkflowEventType.WORKFLOW_STARTED, WorkflowEventType.ACTIVITY_SCHEDULED);
    }

    @Test
    void separateServerContextAndConcurrentPollersCannotDuplicateOccurrence() throws Exception {
        var schedule = create(ScheduleType.START_AT, now().minusSeconds(1), null);
        try (var fresh = freshContext()) {
            var other = fresh.getBean(ScheduleTransactions.class); var pool = Executors.newFixedThreadPool(2); var gate = new CountDownLatch(1);
            try {
                var a = pool.submit(() -> { gate.await(); return transactions.fireOne(); });
                var b = pool.submit(() -> { gate.await(); return other.fireOne(); }); gate.countDown();
                assertThat(List.of(a.get(10, TimeUnit.SECONDS), b.get(10, TimeUnit.SECONDS))).containsExactlyInAnyOrder(true, false);
            } finally { gate.countDown(); pool.shutdownNow(); }
        }
        assertThat(service.occurrences(schedule.id())).hasSize(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_run", Long.class)).isEqualTo(1L);
    }

    @Test
    void crashBeforeDeadlineUpdateRollsBackOccurrenceRunHistoryAndTasksThenRetryCreatesOne() {
        var schedule = create(ScheduleType.START_AT, now().minusSeconds(1), null);
        jdbc.execute("CREATE FUNCTION reject_schedule_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'crash before schedule advance'; END $$");
        jdbc.execute("CREATE TRIGGER reject_schedule_update BEFORE UPDATE ON workflow_schedule FOR EACH ROW EXECUTE FUNCTION reject_schedule_update()");
        try {
            assertThatThrownBy(() -> transactions.fireOne()).isInstanceOf(RuntimeException.class);
            assertThat(service.occurrences(schedule.id())).isEmpty();
            for (var table : List.of("workflow_run", "workflow_event", "activity_execution", "activity_task")) {
                assertThat(jdbc.queryForObject("SELECT count(*) FROM " + table, Long.class)).isZero();
            }
            assertThat(service.get(schedule.id()).nextFireAt()).isEqualTo(schedule.nextFireAt()); assertThat(service.get(schedule.id()).enabled()).isTrue();
        } finally { jdbc.execute("DROP TRIGGER reject_schedule_update ON workflow_schedule"); jdbc.execute("DROP FUNCTION reject_schedule_update()"); }
        assertThat(transactions.fireOne()).isTrue(); assertThat(transactions.fireOne()).isFalse();
        assertThat(service.occurrences(schedule.id())).hasSize(1);
    }

    @Test
    void existingOccurrenceIdentitySurvivesDeadlineReplayWithoutCreatingAnotherRun() {
        var schedule = create(ScheduleType.FIXED_INTERVAL, now().minusSeconds(1), 3_600_000L);
        assertThat(transactions.fireOne()).isTrue(); var occurrence = service.occurrences(schedule.id()).getFirst();
        // Simulate an imported/legacy partially advanced schedule; normal protocol cannot commit this split.
        jdbc.update("UPDATE workflow_schedule SET next_fire_at=? WHERE id=?", occurrence.scheduledFor().atOffset(ZoneOffset.UTC), schedule.id());
        assertThat(transactions.fireOne()).isTrue();
        assertThat(service.occurrences(schedule.id())).containsExactly(occurrence);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_run", Long.class)).isEqualTo(1L);
        assertThat(service.get(schedule.id()).nextFireAt()).isEqualTo(schedule.nextFireAt().plusMillis(3_600_000L));
    }

    @Test
    void fixedIntervalUsesOriginalScheduledInstantNotPollTimeAndMissedOccurrencesAreRetained() {
        var schedule = create(ScheduleType.FIXED_INTERVAL, now().minusSeconds(5 * 3600), 3_600_000L);
        assertThat(dispatcher.pollBatch()).isEqualTo(2);
        assertThat(service.occurrences(schedule.id())).extracting(ScheduleOccurrence::scheduledFor)
                .containsExactly(schedule.nextFireAt(), schedule.nextFireAt().plusSeconds(3600));
        assertThat(service.get(schedule.id()).nextFireAt()).isEqualTo(schedule.nextFireAt().plusSeconds(7200));
        assertThat(service.get(schedule.id()).nextFireAt()).isBefore(now()); // Backlog remains durable for later batches.
        assertThat(dispatcher.pollBatch()).isEqualTo(2); assertThat(dispatcher.pollBatch()).isEqualTo(2);
        assertThat(service.occurrences(schedule.id())).hasSize(6);
        assertThat(service.get(schedule.id()).nextFireAt()).isEqualTo(schedule.nextFireAt().plusSeconds(6 * 3600));
        assertThat(dispatcher.pollBatch()).isZero(); assertThat(service.get(schedule.id()).enabled()).isTrue();
    }

    @Test
    void schedulerRestartRetainsDueOneTimeDeadlineAndRecoversMissedFire() {
        var schedule = create(ScheduleType.START_AT, now().minusSeconds(3600), null);
        try (var fresh = freshContext()) {
            assertThat(fresh.getBean(ScheduleService.class).get(schedule.id()).nextFireAt()).isEqualTo(schedule.nextFireAt());
            assertThat(fresh.getBean(ScheduleTransactions.class).fireOne()).isTrue();
        }
        try (var fresh = freshContext()) { assertThat(fresh.getBean(ScheduleTransactions.class).fireOne()).isFalse(); }
        assertThat(service.occurrences(schedule.id())).hasSize(1);
    }

    @Test
    void exactDefinitionVersionRemainsPinnedAfterNewVersionRegistration() {
        String name = name(); var v1 = register(name, 1, "old");
        var schedule = service.create(request(name, ScheduleType.START_AT, now().minusSeconds(1), null));
        register(name, 2, "new"); assertThat(transactions.fireOne()).isTrue();
        var run = runs.get(service.occurrences(schedule.id()).getFirst().workflowRunId());
        assertThat(schedule.workflowDefinitionId()).isEqualTo(v1.id()); assertThat(run.version()).isEqualTo(1);
        assertThat(run.activities()).extracting(WorkflowRunResponse.ActivityState::activityId).containsExactly("old");
        assertThat(run.input()).containsEntry("source", "schedule-demo");
    }

    @Test
    void dueDecisionUsesDatabaseDeadlineEvenWhenApplicationClockWouldDisagree() {
        var schedule = create(ScheduleType.START_AT, Instant.parse("2100-01-01T00:00:00Z"), null);
        assertThat(transactions.fireOne()).isFalse();
        jdbc.update("UPDATE workflow_schedule SET next_fire_at=statement_timestamp()-INTERVAL '1 second' WHERE id=?", schedule.id());
        assertThat(transactions.fireOne()).isTrue();
        // Dispatcher has no Clock dependency; all due checks are PostgreSQL statement time.
        assertThat(service.occurrences(schedule.id())).hasSize(1);
    }

    @Test
    void scheduleValidationRejectsInvalidIntervalAndMissingPinnedDefinition() {
        String name = name(); register(name, 1, "work");
        for (var invalid : List.of(request(name, ScheduleType.START_AT, now(), 1L),
                request(name, ScheduleType.FIXED_INTERVAL, now(), null), request(name, ScheduleType.FIXED_INTERVAL, now(), 0L))) {
            assertThatThrownBy(() -> service.create(invalid)).isInstanceOf(ApiException.class);
        }
        assertThatThrownBy(() -> service.create(request("missing", ScheduleType.START_AT, now(), null))).isInstanceOf(ApiException.class);
    }

    private WorkflowSchedule create(ScheduleType type, Instant when, Long interval) {
        String name = name(); register(name, 1, "work"); return service.create(request(name, type, when, interval));
    }
    private CreateScheduleRequest request(String name, ScheduleType type, Instant when, Long interval) {
        return new CreateScheduleRequest("Schedule-"+UUID.randomUUID(), name, 1, Map.of("source", "schedule-demo"), type, when, interval);
    }
    private String name() { return "Scheduled-"+UUID.randomUUID(); }
    private WorkflowDefinitionResponse register(String name, int version, String activity) {
        return definitions.register(new RegisterWorkflowDefinitionRequest(name, version, List.of(new StepDefinition(activity, "submitRequest", List.of()))));
    }
    private Instant now() { return Objects.requireNonNull(jdbc.queryForObject("SELECT clock_timestamp()", OffsetDateTime.class)).toInstant(); }
    private ConfigurableApplicationContext freshContext() {
        return new SpringApplicationBuilder(DurableFlowApplication.class).web(WebApplicationType.NONE).run(
                "--spring.datasource.url="+POSTGRES.getJdbcUrl(), "--spring.datasource.username="+POSTGRES.getUsername(), "--spring.datasource.password="+POSTGRES.getPassword(),
                "--durableflow.timers.polling-enabled=false", "--durableflow.children.polling-enabled=false", "--durableflow.schedules.polling-enabled=false");
    }
}
