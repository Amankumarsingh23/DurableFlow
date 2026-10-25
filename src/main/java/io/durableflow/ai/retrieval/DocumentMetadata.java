package io.durableflow.ai.retrieval;

import java.util.*;

public record DocumentMetadata(String documentId, String title, String source, String documentVersion,
        String tenantId, String documentType, Map<String,String> safeMetadata) {
    public DocumentMetadata {
        for (var value : List.of(documentId,title,source,documentVersion,tenantId,documentType)) {
            if (value.isBlank() || value.length()>500) throw new IllegalArgumentException("Invalid document metadata");
        }
        safeMetadata = Map.copyOf(safeMetadata);
    }
}
