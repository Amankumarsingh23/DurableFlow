package io.durableflow.workflow.run;

import jakarta.validation.constraints.*;
import java.util.Map;

public record StartWorkflowRunRequest(
        @NotBlank @Size(max = 200) String workflowName,
        @Positive int version,
        @NotNull Map<String, Object> input) {}
