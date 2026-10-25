package io.durableflow.workflow.child;

import io.durableflow.workflow.run.WorkflowRunStatus;
import org.springframework.jdbc.core.*;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.*;
import java.time.OffsetDateTime;
import java.util.*;

@Repository
public class ChildWorkflowRepository {
    private final JdbcTemplate jdbc;
    public ChildWorkflowRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }
    private static final RowMapper<ChildWorkflow> ROW = (r, n) -> new ChildWorkflow(r.getObject("id", UUID.class),
            r.getObject("parent_workflow_run_id", UUID.class), r.getString("child_node_id"), r.getObject("child_workflow_run_id", UUID.class),
            r.getObject("child_workflow_definition_id", UUID.class), WorkflowRunStatus.valueOf(r.getString("status")),
            r.getString("terminal_status") == null ? null : WorkflowRunStatus.valueOf(r.getString("terminal_status")),
            r.getObject("created_at", OffsetDateTime.class).toInstant());
    public List<ChildWorkflow> findByParent(UUID parent) {
        return jdbc.query("SELECT c.*, r.status FROM child_workflow_execution c JOIN workflow_run r ON r.id=c.child_workflow_run_id "
                + "WHERE c.parent_workflow_run_id=? ORDER BY c.child_node_id", ROW, parent);
    }
    public boolean hasActiveChildren(UUID parent) {
        return Boolean.TRUE.equals(jdbc.queryForObject("""
                SELECT EXISTS(SELECT 1 FROM child_workflow_execution c JOIN workflow_run r ON r.id=c.child_workflow_run_id
                  WHERE c.parent_workflow_run_id=? AND r.status IN ('PENDING','RUNNING','COMPENSATING'))
                """, Boolean.class, parent));
    }
    @Transactional(propagation = Propagation.MANDATORY)
    public void insert(UUID parent, String node, UUID child, UUID definition) {
        jdbc.update("INSERT INTO child_workflow_execution(id,parent_workflow_run_id,child_node_id,child_workflow_run_id,child_workflow_definition_id) "
                + "VALUES (?,?,?,?,?)", UUID.randomUUID(), parent, node, child, definition);
    }
    @Transactional(propagation = Propagation.MANDATORY)
    public void observe(ChildWorkflow child) {
        if (jdbc.update("UPDATE child_workflow_execution SET terminal_status=?, completed_at=clock_timestamp() WHERE id=? AND terminal_status IS NULL",
                child.status().name(), child.id()) != 1) throw new IllegalStateException("Child outcome already observed");
    }
    /** A short relationship lock only; released BEFORE taking either run lock. */
    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<ChildWorkflow> lockNextForReconciliation() {
        return jdbc.query("""
                SELECT c.*, r.status FROM child_workflow_execution c
                JOIN workflow_run r ON r.id=c.child_workflow_run_id
                WHERE c.terminal_status IS NULL
                ORDER BY c.checked_at, c.id LIMIT 1 FOR UPDATE OF c SKIP LOCKED
                """, ROW).stream().findFirst();
    }
    @Transactional(propagation = Propagation.MANDATORY)
    public void checked(UUID id) { jdbc.update("UPDATE child_workflow_execution SET checked_at=clock_timestamp() WHERE id=?", id); }
}
