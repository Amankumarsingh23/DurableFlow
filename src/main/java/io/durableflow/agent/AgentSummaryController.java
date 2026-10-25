package io.durableflow.agent;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;
@RestController @Profile("!worker")
@RequestMapping("/api/agent-executions")
public class AgentSummaryController {
    private final AgentSummaryService summaries;
    public AgentSummaryController(AgentSummaryService summaries) {this.summaries=summaries;}
    @GetMapping("/{id}/summary") public AgentSummaryService.Summary get(@PathVariable UUID id) {return summaries.get(id);}
}
