package io.durableflow.workflow.history;

import java.time.Instant;
import java.util.*;

public record WorkflowEventResponse(UUID id, long sequence, WorkflowEventType eventType, Instant timestamp,
                                    UUID activityExecutionId, String activityId, Map<String, Object> metadata) {}
