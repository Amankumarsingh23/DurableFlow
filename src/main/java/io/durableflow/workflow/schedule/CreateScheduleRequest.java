package io.durableflow.workflow.schedule;

import jakarta.validation.constraints.*;
import java.time.Instant;
import java.util.Map;

public record CreateScheduleRequest(@NotBlank @Size(max=200) String name,
        @NotBlank @Size(max=200) String workflowName, @Min(1) int version, @NotNull Map<String, Object> input,
        @NotNull ScheduleType type, @NotNull Instant startAt, Long intervalMillis) {}
