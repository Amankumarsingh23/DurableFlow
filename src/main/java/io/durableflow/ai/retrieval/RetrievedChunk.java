package io.durableflow.ai.retrieval;

import java.util.*;

/** No embedding/vector field crosses this contract or enters workflow output. */
public record RetrievedChunk(String chunkId, DocumentMetadata document, int chunkIndex, String text,
        double score, RetrievalStrategy strategy, Set<RetrievalStrategy> matchedStrategies) {
    public RetrievedChunk {
        if (chunkId == null || chunkId.isBlank() || chunkId.length()>500 || document == null || chunkIndex<0
                || text == null || text.isBlank() || !Double.isFinite(score) || score<0 || strategy == null) throw new IllegalArgumentException("Invalid chunk");
        matchedStrategies = Set.copyOf(matchedStrategies);
    }
    public RetrievedChunk ranked(double score, RetrievalStrategy strategy, Set<RetrievalStrategy> matched) {
        return new RetrievedChunk(chunkId,document,chunkIndex,text,score,strategy,matched);
    }
    public Map<String,Object> sourceMap() {
        var result = new LinkedHashMap<String,Object>();
        result.put("chunkId",chunkId); result.put("documentId",document.documentId()); result.put("title",document.title());
        result.put("source",document.source()); result.put("documentVersion",document.documentVersion()); result.put("chunkIndex",chunkIndex);
        result.put("score",score); result.put("strategy",strategy.name());
        result.put("matchedStrategies",matchedStrategies.stream().map(Enum::name).sorted().toList());
        var metadata = new TreeMap<String,String>(document.safeMetadata());
        metadata.put("tenantId",document.tenantId()); metadata.put("documentType",document.documentType()); result.put("metadata",metadata);
        return result;
    }
    public Map<String,Object> toMap() { var result=sourceMap(); result.put("text",text); return result; }
}
