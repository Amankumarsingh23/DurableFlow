package io.durableflow.ai.retrieval;

import java.util.List;
public record RetrievalResponse(String retriever, String corpusVersion, String rankingVersion, List<RetrievedChunk> chunks) {
    public RetrievalResponse { chunks = List.copyOf(chunks); }
}
