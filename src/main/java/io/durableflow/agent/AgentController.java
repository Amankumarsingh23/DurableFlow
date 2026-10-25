package io.durableflow.agent;
import jakarta.validation.Valid;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;
import java.util.*;
@RestController @Profile("!worker")
@RequestMapping("/api/agent-executions")
public class AgentController {
    private final AgentStartService start;
    private final AgentLoopService loop;
    private final AgentRepository repository;
    public AgentController(AgentStartService start,AgentLoopService loop,AgentRepository repository) {this.start=start;this.loop=loop;this.repository=repository;}
    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(org.springframework.http.HttpStatus.BAD_REQUEST)
    public Map<String,Object> invalid(IllegalArgumentException failure) {return Map.of("error",Objects.toString(failure.getMessage(),"AGENT_INVALID_REQUEST"));}
    @PostMapping public AgentExecution start(@Valid @RequestBody StartAgentRequest request) {return start.start(request);}
    @PostMapping("/{id}/advance") public AgentExecution advance(@PathVariable UUID id) {return loop.advance(id);}
    @GetMapping("/{id}") public AgentExecution get(@PathVariable UUID id) {return repository.get(id);}
    @GetMapping("/{id}/iterations") public List<AgentIteration> iterations(@PathVariable UUID id) {return repository.iterations(id);}
}
