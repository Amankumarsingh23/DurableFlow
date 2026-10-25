package io.durableflow.agent;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.UUID;
public record StartAgentRequest(@NotBlank @Size(max=2000) String goal,String scenario,@Valid AgentLimits limits,UUID retrievalActivityId) {}
