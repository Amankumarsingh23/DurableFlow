package io.durableflow.tools;

import org.springframework.stereotype.Component;
import java.util.*;

@Component
public class ToolRegistry {
    private final Map<String,ToolDescriptor> tools;
    private final Map<String,ToolProvider> providers;
    public ToolRegistry(List<ToolProvider> providers) {
        var indexed=new HashMap<String,ToolProvider>();
        for(var p:providers) if(indexed.put(p.id(),p)!=null) throw new IllegalArgumentException("Duplicate provider");
        this.providers=Map.copyOf(indexed);
        // Explicit code-owned allowlist. Never populate from discover().
        tools=Map.of("getRefundPolicy",new ToolDescriptor("getRefundPolicy","Read the demo refund policy",Map.of("orderId","order-id"),false,
                ToolRiskLevel.READ_ONLY,Set.of("ORDER_READ"),"fake-mcp","demo-v1"),"getOrder",new ToolDescriptor("getOrder","Read a demo tenant-owned order",Map.of("orderId","order-id"),false,
                ToolRiskLevel.READ_ONLY,Set.of("ORDER_READ"),"fake-mcp","demo-v1"),
            "refundOrder",new ToolDescriptor("refundOrder","Refund a demo tenant-owned order",Map.of("orderId","order-id","amount","positive-money"),true,
                ToolRiskLevel.HIGH_RISK_WRITE,Set.of("ORDER_REFUND"),"fake-mcp","demo-v1"));
    }
    public ToolDescriptor require(String name) {
        var tool=tools.get(name); if(tool==null) throw new IllegalArgumentException("TOOL_NOT_ALLOWLISTED"); return tool;
    }
    public ToolResult execute(ToolRequest request) throws Exception {
        if(request.status()!=ToolRequest.Status.APPROVED) throw new IllegalArgumentException("TOOL_NOT_APPROVED");
        var descriptor=require(request.toolName());
        if (!descriptor.snapshot().equals(request.descriptor())) throw new IllegalArgumentException("TOOL_CONFIGURATION_CHANGED");
        var args=descriptor.validate(request.arguments());
        request.requireIdentity(request.toolName(),args);
        var provider=providers.get(descriptor.provider());
        if(provider==null) throw new IllegalArgumentException("TOOL_PROVIDER_NOT_CONFIGURED");
        var result=provider.execute(descriptor.name(),args,request.fingerprint(),request.idempotencyKey());
        if(!descriptor.name().equals(result.toolName()) || ToolJson.encode(result.toMap()).length()>16000)
            throw new IllegalArgumentException("TOOL_INVALID_RESULT");
        return result;
    }
}
