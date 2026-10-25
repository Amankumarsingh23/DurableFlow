package io.durableflow.workflow.compensation;

import org.springframework.data.jpa.repository.*;
import java.util.*;

public interface CompensationExecutionRepository extends JpaRepository<CompensationExecutionEntity, UUID> {
    List<CompensationExecutionEntity> findByWorkflowRunIdOrderByCreatedAtAsc(UUID runId);
    Optional<CompensationExecutionEntity> findByOriginalActivityExecutionId(UUID originalId);
    @Query("select c.workflowRunId from CompensationExecutionEntity c where c.id = :id")
    Optional<UUID> findWorkflowRunId(UUID id);
}
