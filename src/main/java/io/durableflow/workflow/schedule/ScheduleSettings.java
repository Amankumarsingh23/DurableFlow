package io.durableflow.workflow.schedule;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public record ScheduleSettings(int maxCatchUpOccurrences) {
    public ScheduleSettings(@Value("${durableflow.schedules.max-catch-up-occurrences:10}") int maxCatchUpOccurrences) {
        if (maxCatchUpOccurrences < 1 || maxCatchUpOccurrences > 1000) throw new IllegalArgumentException("Catch-up batch must be 1-1000");
        this.maxCatchUpOccurrences = maxCatchUpOccurrences;
    }
}
