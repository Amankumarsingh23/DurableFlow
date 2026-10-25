package io.durableflow.tools;
import java.util.Map;
public interface ToolPolicyService {
    enum Decision { AUTO_APPROVE, REQUIRE_HUMAN_APPROVAL, DENY }
    String version();
    Decision decide(ToolDescriptor tool,Map<String,Object> arguments);
}
