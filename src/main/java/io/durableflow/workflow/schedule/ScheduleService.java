package io.durableflow.workflow.schedule;

import io.durableflow.api.ApiException;
import io.durableflow.workflow.definition.WorkflowDefinitionRepository;
import jakarta.validation.Validator;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.*;

@Service
public class ScheduleService {
    private final io.durableflow.security.ResourceLimits limits;
    private final ScheduleRepository schedules;
    private final WorkflowDefinitionRepository definitions;
    private final Validator validator;
    public ScheduleService(ScheduleRepository schedules, WorkflowDefinitionRepository definitions, Validator validator) {this(schedules,definitions,validator,io.durableflow.security.ResourceLimits.defaults());}
    @org.springframework.beans.factory.annotation.Autowired
    public ScheduleService(ScheduleRepository schedules, WorkflowDefinitionRepository definitions, Validator validator, io.durableflow.security.ResourceLimits limits) {
        this.limits=limits;
        this.schedules = schedules; this.definitions = definitions; this.validator = validator;
    }
    @Transactional
    public WorkflowSchedule create(CreateScheduleRequest request) {
        if(request!=null)limits.checkJson(io.durableflow.security.ResourceLimits.Payload.SCHEDULE_INPUT,request.input());
        if (request == null || !validator.validate(request).isEmpty()
                || (request.type() == ScheduleType.START_AT && request.intervalMillis() != null)
                || (request.type() == ScheduleType.FIXED_INTERVAL && (request.intervalMillis() == null
                    || request.intervalMillis() < 1 || request.intervalMillis() > 3_155_760_000_000L))) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "Valid schedule name, exact workflow version, input, startAt and interval policy required");
        }
        var definition = definitions.findByNameAndVersion(request.workflowName().strip(), request.version())
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Scheduled workflow definition not found"));
        return schedules.create(request, definition.getId());
    }
    @Transactional(readOnly = true)
    public WorkflowSchedule get(UUID id) {
        return schedules.find(id).orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Schedule not found"));
    }
    @Transactional(readOnly = true)
    public List<ScheduleOccurrence> occurrences(UUID id) { get(id); return schedules.occurrences(id); }
}
