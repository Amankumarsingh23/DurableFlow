package io.durableflow.workflow.run;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import jakarta.persistence.LockModeType;
import java.util.Optional;
import java.util.UUID;

public interface WorkflowRunRepository extends JpaRepository<WorkflowRunEntity, UUID> {
    /** All engine checkpoint writers acquire this lock before reading/changing activity state. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select run from WorkflowRunEntity run where run.id = :runId")
    Optional<WorkflowRunEntity> findByIdForUpdate(@Param("runId") UUID runId);
}
