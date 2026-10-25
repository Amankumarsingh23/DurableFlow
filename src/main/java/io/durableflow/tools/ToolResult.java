package io.durableflow.tools;
import java.util.*;

/** Result content is untrusted data if subsequently supplied to a model. */
public record ToolResult(String toolName, Map<String,Object> data, String providerReference, Map<String,Object> executionMetadata) {
    public ToolResult { data=ToolJson.freeze(data); executionMetadata=ToolJson.freeze(executionMetadata);
        Objects.requireNonNull(providerReference); }
    public Map<String,Object> toMap() { return Map.of("toolName",toolName,"data",data,"providerReference",providerReference,"executionMetadata",executionMetadata); }
}
