package io.durableflow.tools;
import java.util.*;

/** Trusted deployment context, not model/input-supplied grants. No enterprise IAM. */
public interface ToolPermissionService {
    enum Decision { ALLOWED, DENIED }
    record Actor(String actorId,String tenantId) {}
    Decision check(Actor actor,ToolDescriptor tool,Map<String,Object> arguments);
}
