package io.durableflow.tools;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import java.util.*;
import io.durableflow.observability.*;

@Service
public class FakeToolLedger {
    private final JdbcTemplate jdbc;
    private final SafeTelemetry telemetry;
    public FakeToolLedger(JdbcTemplate jdbc) {this(jdbc,SafeTelemetry.disabled());}
    @org.springframework.beans.factory.annotation.Autowired
    public FakeToolLedger(JdbcTemplate jdbc,SafeTelemetry telemetry) {this.jdbc=jdbc;this.telemetry=telemetry;}
    /** Simulates the provider's atomic side effect + durable dedupe; commits before worker completion. */
    @Transactional(propagation=Propagation.REQUIRES_NEW)
    public ToolResult apply(String tool,Map<String,Object> args,String fingerprint,String key) {
        if(!fingerprint.equals(ToolJson.fingerprint(tool,args))) throw new IllegalArgumentException("TOOL_PROVIDER_IDENTITY");
        var data=new LinkedHashMap<String,Object>(); data.put("orderId",args.get("orderId"));
        if(tool.equals("refundOrder")) { data.put("amount",args.get("amount")); data.put("status","REFUNDED"); }
        else if(tool.equals("getRefundPolicy")) { data.put("status","ELIGIBLE_FOR_REVIEW"); data.put("policyVersion","POL-RFD-042:v1"); data.put("maximumRefund",10000); }
        else if(tool.equals("getOrder")) { data.put("status","PAID"); data.put("tenantId","tenant-a"); }
        else throw new IllegalArgumentException("TOOL_PROVIDER_UNKNOWN");
        var result=new ToolResult(tool,data,"fake-"+UUID.nameUUIDFromBytes(key.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                Map.of("provider","fake-mcp","providerVersion","demo-v1","idempotencyKey",key));
        int inserted=jdbc.update("INSERT INTO fake_tool_effect(idempotency_key,fingerprint,tool_name,result_json) VALUES (?,?,?,CAST(? AS jsonb)) ON CONFLICT DO NOTHING",
                key,fingerprint,tool,ToolJson.encode(result.toMap()));
        if(inserted==0)telemetry.count(Metric.TOOL_DEDUP,Map.of("tool",tool));
        return jdbc.queryForObject("SELECT * FROM fake_tool_effect WHERE idempotency_key=?",(r,i) -> {
            if(!fingerprint.equals(r.getString("fingerprint")) || !tool.equals(r.getString("tool_name"))) throw new IllegalArgumentException("TOOL_PROVIDER_IDEMPOTENCY_MISUSE");
            var saved=ToolJson.decode(r.getString("result_json"));
            return new ToolResult((String)saved.get("toolName"),ToolJson.object(saved.get("data")),(String)saved.get("providerReference"),ToolJson.object(saved.get("executionMetadata")));
        },key);
    }
}
