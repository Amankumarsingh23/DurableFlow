package io.durableflow.agent;
import io.durableflow.tools.ToolJson;
import java.util.*;

/** Only actionable structured decisions are persisted, never private reasoning. */
public record AgentDecision(Type type,Map<String,Object> payload) {
    public enum Type { TOOL_CALL, FINAL }
    public AgentDecision { payload=ToolJson.freeze(payload); }
    public static AgentDecision parse(Object raw) {
        var m=ToolJson.object(raw);
        if("TOOL_CALL".equals(m.get("type")) && m.keySet().equals(Set.of("type","toolName","arguments"))
                && m.get("toolName") instanceof String name && name.length()<=100 && !name.isBlank()) {
            var args=ToolJson.object(m.get("arguments"));
            ToolJson.fingerprint(name,args);
            return new AgentDecision(Type.TOOL_CALL,Map.of("type","TOOL_CALL","toolName",name,"arguments",args));
        }
        if("FINAL".equals(m.get("type")) && m.keySet().equals(Set.of("type","answer")) && m.get("answer") instanceof String answer
                && !answer.isBlank() && answer.length()<=4000) return new AgentDecision(Type.FINAL,Map.of("type","FINAL","answer",answer));
        throw new IllegalArgumentException("AGENT_INVALID_MODEL_DECISION");
    }
}
