package io.durableflow.tools;

import java.math.BigDecimal;
import java.util.*;

/** Trusted configuration. A deliberately small schema contract, not a general JSON-schema engine. */
public record ToolDescriptor(String name, String description, Map<String,String> inputSchema,
        boolean sideEffecting, ToolRiskLevel riskLevel, Set<String> requiredPermissions, String provider, String sourceVersion) {
    public ToolDescriptor {
        Objects.requireNonNull(name); Objects.requireNonNull(description); Objects.requireNonNull(riskLevel);
        Objects.requireNonNull(provider); Objects.requireNonNull(sourceVersion);
        inputSchema=Map.copyOf(inputSchema); requiredPermissions=Set.copyOf(requiredPermissions);
        if (name.isBlank() || name.length()>100 || description.length()>1000 || requiredPermissions.isEmpty()
                || sideEffecting == (riskLevel == ToolRiskLevel.READ_ONLY)) throw new IllegalArgumentException("Invalid trusted descriptor");
    }
    public Map<String,Object> validate(Map<String,Object> arguments) {
        if (!arguments.keySet().equals(inputSchema.keySet())) throw new IllegalArgumentException("TOOL_SCHEMA_FIELDS");
        var normalized=new TreeMap<String,Object>();
        inputSchema.forEach((key,type) -> {
            var value=arguments.get(key);
            if (type.equals("order-id")) {
                if (!(value instanceof String s) || !s.matches("O-[0-9]{1,12}")) throw new IllegalArgumentException("TOOL_SCHEMA_ORDER");
                normalized.put(key,value);
            } else if (type.equals("positive-money")) {
                if (!(value instanceof Number n)) throw new IllegalArgumentException("TOOL_SCHEMA_AMOUNT");
                var amount=new BigDecimal(n.toString()).stripTrailingZeros();
                if (amount.signum()<=0 || amount.scale()>2 || amount.compareTo(new BigDecimal("1000000"))>0)
                    throw new IllegalArgumentException("TOOL_SCHEMA_AMOUNT");
                normalized.put(key,amount);
            } else throw new IllegalArgumentException("Unsupported configured schema");
        });
        return ToolJson.freeze(normalized);
    }
    public Map<String,Object> snapshot() {
        return Map.of("name",name,"description",description,"inputSchema",inputSchema,"additionalProperties",false,
                "sideEffecting",sideEffecting,"riskLevel",riskLevel.name(),"requiredPermissions",requiredPermissions.stream().sorted().toList(),
                "provider",provider,"sourceVersion",sourceVersion);
    }
}
