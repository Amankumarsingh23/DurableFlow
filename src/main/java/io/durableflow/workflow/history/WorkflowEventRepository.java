package io.durableflow.workflow.history;

import org.springframework.data.repository.Repository;
import java.util.*;

/** Intentionally read-only repository surface; inserts are owned by WorkflowHistoryWriter. */
public interface WorkflowEventRepository extends Repository<WorkflowEventEntity, UUID> {
    List<WorkflowEventEntity> findByWorkflowRunIdOrderBySequenceNumberAsc(UUID workflowRunId);
}
