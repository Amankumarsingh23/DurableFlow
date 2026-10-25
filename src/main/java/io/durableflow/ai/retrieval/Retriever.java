package io.durableflow.ai.retrieval;
public interface Retriever {
    String id();
    RetrievalResponse retrieve(RetrievalRequest request) throws Exception;
}
