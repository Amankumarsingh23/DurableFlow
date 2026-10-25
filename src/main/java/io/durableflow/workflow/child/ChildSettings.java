package io.durableflow.workflow.child;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public record ChildSettings(int maxDepth) {
    public ChildSettings(@Value("${durableflow.children.max-depth:8}") int maxDepth) {
        if (maxDepth < 1 || maxDepth > 128) throw new IllegalArgumentException("Child depth must be 1-128");
        this.maxDepth = maxDepth;
    }
}
