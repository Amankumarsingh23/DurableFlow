package io.durableflow.workflow.schedule;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.*;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.*;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

@Repository
public class ScheduleRepository {
    private final JdbcTemplate jdbc;
    private final ObjectMapper json = new ObjectMapper();
    public ScheduleRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }
    private final RowMapper<WorkflowSchedule> scheduleRow = (r, n) -> new WorkflowSchedule(r.getObject("id", UUID.class), r.getString("name"),
            r.getObject("workflow_definition_id", UUID.class), decode(r.getString("input_json")), ScheduleType.valueOf(r.getString("schedule_type")),
            r.getObject("next_fire_at", OffsetDateTime.class).toInstant(), r.getObject("interval_millis", Long.class), r.getBoolean("enabled"),
            r.getObject("created_at", OffsetDateTime.class).toInstant(), r.getObject("updated_at", OffsetDateTime.class).toInstant());
    private static final RowMapper<ScheduleOccurrence> OCCURRENCE = (r, n) -> new ScheduleOccurrence(r.getObject("id", UUID.class),
            r.getObject("schedule_id", UUID.class), r.getObject("scheduled_for", OffsetDateTime.class).toInstant(),
            r.getObject("workflow_run_id", UUID.class), r.getObject("created_at", OffsetDateTime.class).toInstant());
    private String encode(Map<String, Object> input) {
        try { return json.writeValueAsString(input); } catch (Exception failure) { throw new IllegalArgumentException("Schedule input must be JSON", failure); }
    }
    private Map<String, Object> decode(String input) {
        try { return json.readValue(input, new TypeReference<Map<String, Object>>() {}); }
        catch (Exception failure) { throw new IllegalStateException("Invalid stored schedule JSON", failure); }
    }
    public Optional<WorkflowSchedule> find(UUID id) {
        return jdbc.query("SELECT * FROM workflow_schedule WHERE id=?", scheduleRow, id).stream().findFirst();
    }
    public List<ScheduleOccurrence> occurrences(UUID id) {
        return jdbc.query("SELECT * FROM workflow_schedule_occurrence WHERE schedule_id=? ORDER BY scheduled_for", OCCURRENCE, id);
    }
    @Transactional(propagation = Propagation.MANDATORY)
    public WorkflowSchedule create(CreateScheduleRequest request, UUID definition) {
        return Objects.requireNonNull(jdbc.queryForObject("""
                INSERT INTO workflow_schedule(id,name,workflow_definition_id,input_json,schedule_type,next_fire_at,interval_millis)
                VALUES (?,?,?,CAST(? AS jsonb),?,?,?) RETURNING *
                """, scheduleRow, UUID.randomUUID(), request.name().strip(), definition, encode(request.input()), request.type().name(),
                request.startAt().atOffset(ZoneOffset.UTC), request.intervalMillis()));
    }
    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<WorkflowSchedule> lockNextDue() {
        return jdbc.query("SELECT * FROM workflow_schedule WHERE enabled AND next_fire_at<=statement_timestamp() "
                + "ORDER BY next_fire_at,id LIMIT 1 FOR UPDATE SKIP LOCKED", scheduleRow).stream().findFirst();
    }
    public Optional<ScheduleOccurrence> occurrence(WorkflowSchedule schedule) {
        return jdbc.query("SELECT * FROM workflow_schedule_occurrence WHERE schedule_id=? AND scheduled_for=?", OCCURRENCE,
                schedule.id(), schedule.nextFireAt().atOffset(ZoneOffset.UTC)).stream().findFirst();
    }
    @Transactional(propagation = Propagation.MANDATORY)
    public void insertOccurrence(WorkflowSchedule schedule, UUID run) {
        jdbc.update("INSERT INTO workflow_schedule_occurrence(id,schedule_id,scheduled_for,workflow_run_id) VALUES (?,?,?,?)",
                UUID.randomUUID(), schedule.id(), schedule.nextFireAt().atOffset(ZoneOffset.UTC), run);
    }
    @Transactional(propagation = Propagation.MANDATORY)
    public void advance(WorkflowSchedule schedule) {
        if (schedule.type() == ScheduleType.START_AT) {
            jdbc.update("UPDATE workflow_schedule SET enabled=false, updated_at=clock_timestamp() WHERE id=?", schedule.id());
        } else {
            jdbc.update("UPDATE workflow_schedule SET next_fire_at=next_fire_at+CAST(interval_millis AS double precision)*INTERVAL '1 millisecond', "
                    + "updated_at=clock_timestamp() WHERE id=?", schedule.id());
        }
    }
}
