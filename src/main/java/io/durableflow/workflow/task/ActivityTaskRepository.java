package io.durableflow.workflow.task;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import java.util.*;

public interface ActivityTaskRepository extends JpaRepository<ActivityTaskEntity, UUID> {
    Optional<ActivityTaskEntity> findByActivityExecutionId(UUID activityExecutionId);
    Optional<ActivityTaskEntity> findByCompensationExecutionId(UUID compensationExecutionId);

    @Query(value = """
            SELECT t.* FROM activity_task t
            LEFT JOIN activity_execution a ON a.id = t.activity_execution_id
            LEFT JOIN compensation_execution c ON c.id = t.compensation_execution_id
            WHERE COALESCE(a.workflow_run_id, c.workflow_run_id) = :runId
            """, nativeQuery = true)
    List<ActivityTaskEntity> findByRunId(@Param("runId") UUID runId);

    /** Claim/reclaim and ownership transfer share one transaction. All eligibility times are DB times. */
    @Query(value = """
            SELECT t.* FROM activity_task t
            LEFT JOIN activity_execution a ON a.id = t.activity_execution_id
            LEFT JOIN compensation_execution c ON c.id = t.compensation_execution_id
            JOIN workflow_run r ON r.id = COALESCE(a.workflow_run_id, c.workflow_run_id)
            WHERE ((t.task_kind = 'FORWARD' AND r.status = 'RUNNING')
                OR (t.task_kind = 'COMPENSATION' AND r.status = 'COMPENSATING')) AND (
                (t.status = 'READY' AND t.available_at <= statement_timestamp() AND
                    ((t.lease_generation = 0 AND COALESCE(c.status, a.status) = 'PENDING')
                     OR (t.lease_generation > 0 AND COALESCE(c.status, a.status) = 'RUNNING')))
                OR (t.status = 'CLAIMED' AND t.lease_expires_at <= statement_timestamp()
                    AND COALESCE(c.status, a.status) = 'RUNNING')
            )
            ORDER BY CASE WHEN t.status = 'READY' THEN t.available_at ELSE t.lease_expires_at END,
                     t.created_at, t.id
            LIMIT 1 FOR UPDATE OF t SKIP LOCKED
            """, nativeQuery = true)
    Optional<ActivityTaskEntity> lockNextClaimable();

    /** No resurrection: a heartbeat arriving after expiry is rejected even if not yet reclaimed. */
    @Modifying
    @Query(value = """
            UPDATE activity_task
            SET lease_expires_at = clock_timestamp() + CAST(:leaseMillis AS double precision) * INTERVAL '1 millisecond'
            WHERE id = :taskId AND status = 'CLAIMED' AND worker_id = :workerId
              AND lease_generation = :generation AND lease_expires_at > clock_timestamp()
            """, nativeQuery = true)
    int renewOwned(@Param("taskId") UUID taskId, @Param("workerId") String workerId,
                   @Param("generation") long generation, @Param("leaseMillis") long leaseMillis);

    /** Last ownership check occurs at the actual write, not at transaction start or before a lock wait. */
    @Modifying(flushAutomatically = true)
    @Query(value = """
            UPDATE activity_task SET status = :outcome
            WHERE id = :taskId AND status = 'CLAIMED' AND worker_id = :workerId
              AND lease_generation = :generation AND lease_expires_at > clock_timestamp()
            """, nativeQuery = true)
    int finishOwned(@Param("taskId") UUID taskId, @Param("workerId") String workerId,
                    @Param("generation") long generation, @Param("outcome") String outcome);

    @Modifying(flushAutomatically = true)
    @Query(value = """
            UPDATE activity_task SET status = 'READY', worker_id = NULL, claimed_at = NULL,
                lease_expires_at = NULL,
                available_at = clock_timestamp() + CAST(:delayMillis AS double precision) * INTERVAL '1 millisecond'
            WHERE id = :taskId AND status = 'CLAIMED' AND worker_id = :workerId
              AND lease_generation = :generation AND lease_expires_at > clock_timestamp()
            """, nativeQuery = true)
    int rescheduleOwned(@Param("taskId") UUID taskId, @Param("workerId") String workerId,
                        @Param("generation") long generation, @Param("delayMillis") long delayMillis);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select t from ActivityTaskEntity t where t.id = :taskId")
    Optional<ActivityTaskEntity> findByIdForUpdate(@Param("taskId") UUID taskId);
}
