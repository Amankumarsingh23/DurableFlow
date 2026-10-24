package io.durableflow.workflow.activity;

import org.springframework.stereotype.Component;
import java.util.*;

@Component
public class ActivityRegistry {
    private final Map<String, ActivityHandler> handlers;

    public ActivityRegistry(List<ActivityHandler> handlers) {
        Map<String, ActivityHandler> indexed = new HashMap<>();
        for (var handler : handlers) {
            String type = handler.type();
            if (type == null || type.isBlank()) throw new IllegalStateException("Activity type must be nonblank");
            if (indexed.putIfAbsent(type, handler) != null) {
                throw new IllegalStateException("Duplicate activity handler: " + type);
            }
        }
        this.handlers = Map.copyOf(indexed);
    }

    public ActivityHandler require(String type) {
        var handler = handlers.get(type);
        if (handler == null) throw new IllegalArgumentException("No activity handler registered for: " + type);
        return handler;
    }
}
