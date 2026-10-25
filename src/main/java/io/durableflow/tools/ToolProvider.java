package io.durableflow.tools;

import java.util.*;

/** Discovery metadata is external DATA and never grants registry membership or authority. */
public interface ToolProvider {
    record DiscoveredTool(String name, String description, Map<String,String> inputSchema) {
        public DiscoveredTool {
            if (name==null || name.length()>100 || description==null || description.length()>1000 || inputSchema.size()>10)
                throw new IllegalArgumentException("Discovery bounds exceeded");
            inputSchema=Map.copyOf(inputSchema);
        }
    }
    String id();
    List<DiscoveredTool> discover();
    ToolResult execute(String tool, Map<String,Object> arguments, String fingerprint, String idempotencyKey) throws Exception;
}
