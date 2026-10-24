package io.durableflow.workflow.history;

import jakarta.persistence.EntityManager;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import java.time.OffsetDateTime;
import java.util.*;

@Service
public class WorkflowHistoryWriter {
    private final EntityManager entities;
    private final JdbcTemplate jdbc;
    public WorkflowHistoryWriter(EntityManager entities, JdbcTemplate jdbc) { this.entities = entities; this.jdbc = jdbc; }

    /** Always joins the transition transaction; never REQUIRES_NEW. Caller holds the run lock. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void append(UUID run, UUID activity, WorkflowEventType type, Map<String, Object> data) {
        entities.flush(); // FK rows and state writes must be visible to JDBC in this same transaction.
        long sequence = Objects.requireNonNull(jdbc.queryForObject(
                "UPDATE workflow_run SET history_sequence = history_sequence + 1 WHERE id = ? RETURNING history_sequence",
                Long.class, run));
        var now = Objects.requireNonNull(jdbc.queryForObject("SELECT clock_timestamp()", OffsetDateTime.class)).toInstant();
        entities.persist(new WorkflowEventEntity(run, activity, sequence, type, data, now));
        entities.flush(); // Failed event insertion aborts the associated transition immediately.
    }
}
