package io.durableflow.workflow.timer;

import org.springframework.jdbc.core.*;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.*;
import java.time.OffsetDateTime;
import java.util.*;

@Repository
public class WorkflowTimerRepository {
    private final JdbcTemplate jdbc;
    public WorkflowTimerRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }
    private static final RowMapper<WorkflowTimer> ROW = (row, index) -> {
        var fired = row.getObject("fired_at", OffsetDateTime.class);
        return new WorkflowTimer(row.getObject("id", UUID.class), row.getObject("workflow_run_id", UUID.class),
                row.getString("timer_id"), WorkflowTimer.Status.valueOf(row.getString("status")),
                row.getObject("wake_at", OffsetDateTime.class).toInstant(),
                row.getObject("created_at", OffsetDateTime.class).toInstant(), fired == null ? null : fired.toInstant());
    };
    public List<WorkflowTimer> findByRun(UUID run) {
        return jdbc.query("SELECT * FROM workflow_timer WHERE workflow_run_id=? ORDER BY timer_id", ROW, run);
    }
    public WorkflowTimer findById(UUID id) {
        return Objects.requireNonNull(jdbc.queryForObject("SELECT * FROM workflow_timer WHERE id=?", ROW, id));
    }
    /** Caller holds run lock; uniqueness is also enforced in PostgreSQL. Never updates an existing deadline. */
    @Transactional(propagation = Propagation.MANDATORY)
    public WorkflowTimer schedule(UUID run, String timerId, long delayMillis) {
        return Objects.requireNonNull(jdbc.queryForObject("""
                INSERT INTO workflow_timer (id, workflow_run_id, timer_id, status, wake_at)
                VALUES (?, ?, ?, 'WAITING', clock_timestamp() + CAST(? AS double precision) * INTERVAL '1 millisecond')
                RETURNING *
                """, ROW, UUID.randomUUID(), run, timerId, delayMillis));
    }
    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<WorkflowTimer> lockNextDue() {
        return jdbc.query("""
                SELECT t.* FROM workflow_timer t JOIN workflow_run r ON r.id=t.workflow_run_id
                WHERE t.status='WAITING' AND t.wake_at <= statement_timestamp() AND r.status='RUNNING'
                ORDER BY t.wake_at, t.id LIMIT 1 FOR UPDATE OF t SKIP LOCKED
                """, ROW).stream().findFirst();
    }
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean fire(UUID id) {
        return jdbc.update("UPDATE workflow_timer SET status='FIRED', fired_at=clock_timestamp() "
                + "WHERE id=? AND status='WAITING' AND wake_at <= clock_timestamp()", id) == 1;
    }
}
