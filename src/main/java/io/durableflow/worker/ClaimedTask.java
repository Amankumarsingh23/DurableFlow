package io.durableflow.worker;

import java.util.Map;
import java.util.UUID;

/** Detached claim data; never carry managed entities or a transaction into handler code. */
public record ClaimedTask(UUID taskId, UUID runId, UUID activityExecutionId, String activityType,
                          Map<String, Object> input, String workerId) {}
