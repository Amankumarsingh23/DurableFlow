package io.durableflow.ai.retrieval;

import java.util.*;

/** Required tenant scope plus optional document type; not an authentication/RBAC implementation. */
public record RetrievalFilters(String tenantId, String documentType) {
    public RetrievalFilters {
        if (tenantId == null || tenantId.isBlank() || tenantId.length() > 200
                || (documentType != null && !Set.of("policy", "note").contains(documentType))) {
            throw new IllegalArgumentException("Invalid retrieval scope");
        }
    }
    public boolean allows(DocumentMetadata document) {
        return tenantId.equals(document.tenantId()) && (documentType == null || documentType.equals(document.documentType()));
    }
    public Map<String,Object> toMap() {
        var map = new LinkedHashMap<String,Object>(); map.put("tenantId",tenantId);
        if (documentType != null) map.put("documentType",documentType); return map;
    }
    public static RetrievalFilters parse(Object value) {
        if (!(value instanceof Map<?,?> map) || !Set.of("tenantId","documentType").containsAll(map.keySet())
                || !(map.get("tenantId") instanceof String tenant)
                || (map.get("documentType") != null && !(map.get("documentType") instanceof String))) {
            throw new IllegalArgumentException("Explicit retrieval filters with tenantId required");
        }
        return new RetrievalFilters(tenant,(String)map.get("documentType"));
    }
}
