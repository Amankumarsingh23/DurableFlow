package io.durableflow.workflow.activity;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.util.List;
import java.util.UUID;
import java.util.Optional;

public interface ActivityExecutionRepository extends JpaRepository<ActivityExecutionEntity, UUID> {
    List<ActivityExecutionEntity> findByWorkflowRunIdOrderByActivityIdAsc(UUID workflowRunId);

    // Scalar lookup: do not cache activity state before acquiring the run lock.
    @Query("select a.workflowRunId from ActivityExecutionEntity a where a.id = :id")
    Optional<UUID> findWorkflowRunId(@Param("id") UUID activityId);
}
