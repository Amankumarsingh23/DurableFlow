package io.durableflow.workflow.definition;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface WorkflowDefinitionRepository extends JpaRepository<WorkflowDefinitionEntity, UUID> {}
