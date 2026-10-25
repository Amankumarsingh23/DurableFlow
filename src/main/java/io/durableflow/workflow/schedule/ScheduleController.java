package io.durableflow.workflow.schedule;

import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestController @Profile("!worker") @RequestMapping("/api/workflow-schedules")
public class ScheduleController {
    private final ScheduleService schedules;
    public ScheduleController(ScheduleService schedules) { this.schedules = schedules; }
    @PostMapping @ResponseStatus(HttpStatus.CREATED)
    public WorkflowSchedule create(@RequestBody CreateScheduleRequest request) { return schedules.create(request); }
    @GetMapping("/{id}") public WorkflowSchedule get(@PathVariable UUID id) { return schedules.get(id); }
    @GetMapping("/{id}/occurrences") public List<ScheduleOccurrence> occurrences(@PathVariable UUID id) { return schedules.occurrences(id); }
}
