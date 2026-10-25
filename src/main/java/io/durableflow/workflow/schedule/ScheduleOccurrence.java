package io.durableflow.workflow.schedule;

import java.time.Instant;
import java.util.UUID;
public record ScheduleOccurrence(UUID id, UUID scheduleId, Instant scheduledFor, UUID workflowRunId, Instant createdAt) {}
