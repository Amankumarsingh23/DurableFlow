package io.durableflow.tools;
import org.springframework.stereotype.Component;
import java.math.BigDecimal;
import java.util.Map;
@Component
public class DemoToolPolicy implements ToolPolicyService {
    public String version() { return "demo-policy-v1"; }
    public Decision decide(ToolDescriptor tool,Map<String,Object> arguments) {
        if(tool.riskLevel()==ToolRiskLevel.READ_ONLY) return Decision.AUTO_APPROVE;
        if(new BigDecimal(arguments.get("amount").toString()).compareTo(new BigDecimal("10000"))>0) return Decision.DENY;
        return Decision.REQUIRE_HUMAN_APPROVAL;
    }
}
