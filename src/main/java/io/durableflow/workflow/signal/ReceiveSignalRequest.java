package io.durableflow.workflow.signal;

import jakarta.validation.constraints.*;
import java.util.Map;

public record ReceiveSignalRequest(@NotBlank @Size(max=200) String signalId,
                                    @NotBlank @Size(max=200) String signalNodeId,
                                    @NotNull Map<String, Object> payload) {}
