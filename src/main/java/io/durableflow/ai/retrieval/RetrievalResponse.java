package io.durableflow.ai.retrieval;

import java.util.List;
public record RetrievalResponse(String retriever, String corpusVersion, String rankingVersion, List<RetrievedChunk> chunks, int candidateCount) {
    public RetrievalResponse(String retriever,String corpusVersion,String rankingVersion,List<RetrievedChunk> chunks) {this(retriever,corpusVersion,rankingVersion,chunks,chunks.size());}
    public RetrievalResponse { chunks = List.copyOf(chunks); if(candidateCount<chunks.size())throw new IllegalArgumentException("Invalid candidate count"); }
}
