package io.durableflow.workflow.schedule;

import java.time.Instant;
import java.util.*;
public record WorkflowSchedule(UUID id, String name, UUID workflowDefinitionId, Map<String, Object> input,
        ScheduleType type, Instant nextFireAt, Long intervalMillis, boolean enabled, Instant createdAt, Instant updatedAt) {}
