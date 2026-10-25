package io.durableflow.tools;
import org.springframework.stereotype.Component;
import java.util.*;

@Component
public class DemoToolPermissions implements ToolPermissionService {
    public Decision check(Actor actor,ToolDescriptor tool,Map<String,Object> arguments) {
        Set<String> granted=switch(actor.actorId()) { case "demo-agent" -> Set.of("ORDER_READ","ORDER_REFUND");
            case "demo-reader" -> Set.of("ORDER_READ"); default -> Set.of(); };
        // The bounded demo has one tenant-owned fixture. No arbitrary cross-tenant order lookup.
        return "tenant-a".equals(actor.tenantId()) && "O-42".equals(arguments.get("orderId"))
                && granted.containsAll(tool.requiredPermissions()) ? Decision.ALLOWED : Decision.DENIED;
    }
}
