package io.durableflow.workflow.task;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import java.time.Instant;
import java.util.*;

public interface ActivityTaskRepository extends JpaRepository<ActivityTaskEntity, UUID> {
    Optional<ActivityTaskEntity> findByActivityExecutionId(UUID activityExecutionId);

    @Query("select t from ActivityTaskEntity t, ActivityExecutionEntity a "
            + "where t.activityExecutionId = a.id and a.workflowRunId = :runId")
    List<ActivityTaskEntity> findByRunId(@Param("runId") UUID runId);

    /** Selection and READY -> CLAIMED must share the caller's transaction. Lock task rows only. */
    @Query(value = """
            SELECT t.* FROM activity_task t
            JOIN activity_execution a ON a.id = t.activity_execution_id
            JOIN workflow_run r ON r.id = a.workflow_run_id
            WHERE t.status = 'READY' AND t.available_at <= :now
              AND a.status = 'PENDING' AND r.status = 'RUNNING'
            ORDER BY t.available_at, t.created_at, t.id
            LIMIT 1 FOR UPDATE OF t SKIP LOCKED
            """, nativeQuery = true)
    Optional<ActivityTaskEntity> lockNextReady(@Param("now") Instant now);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select t from ActivityTaskEntity t where t.id = :taskId")
    Optional<ActivityTaskEntity> findByIdForUpdate(@Param("taskId") UUID taskId);
}
