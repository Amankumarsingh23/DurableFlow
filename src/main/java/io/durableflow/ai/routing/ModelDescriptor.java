package io.durableflow.ai.routing;
import java.util.*;
public record ModelDescriptor(String provider,String model,int qualityTier,long inputCostMicros,long outputCostMicros,ModelCapabilities capabilities,boolean enabled,Set<String> tags) {
    public ModelDescriptor {
        if(provider==null || !provider.matches("[a-z][a-z0-9-]{0,63}") || model==null || model.isBlank() || model.length()>100 || qualityTier<1 || qualityTier>3
                || inputCostMicros<0 || inputCostMicros>100 || outputCostMicros<0 || outputCostMicros>100 || capabilities==null)throw new IllegalArgumentException("Invalid trusted model descriptor");
        tags=Set.copyOf(tags);if(tags.size()>10 || tags.stream().anyMatch(t -> t.length()>100))throw new IllegalArgumentException("Invalid model tags");
    }
    public long estimatedCost(int inputChars,int maxOutput) {return Math.addExact(Math.multiplyExact((long)inputChars,inputCostMicros),Math.multiplyExact((long)maxOutput,outputCostMicros));}
    public Map<String,Object> toMap() {return Map.of("provider",provider,"model",model,"qualityTier",qualityTier,"inputCostMicros",inputCostMicros,"outputCostMicros",outputCostMicros,
        "structuredOutput",capabilities.structuredOutput(),"toolUse",capabilities.toolUse(),"maxContextChars",capabilities.maxContextChars(),"enabled",enabled,"tags",tags.stream().sorted().toList());}
    public static ModelDescriptor from(Map<String,Object> m) {return new ModelDescriptor((String)m.get("provider"),(String)m.get("model"),((Number)m.get("qualityTier")).intValue(),
        ((Number)m.get("inputCostMicros")).longValue(),((Number)m.get("outputCostMicros")).longValue(),new ModelCapabilities((Boolean)m.get("structuredOutput"),(Boolean)m.get("toolUse"),((Number)m.get("maxContextChars")).intValue()),
        (Boolean)m.get("enabled"),new HashSet<>((List<String>)m.get("tags")));}
}
