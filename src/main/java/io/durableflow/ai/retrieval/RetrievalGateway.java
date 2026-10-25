package io.durableflow.ai.retrieval;

import io.durableflow.worker.retry.RetryableActivityException;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
public class RetrievalGateway {
    private final Map<String,Retriever> retrievers;
    public RetrievalGateway(List<Retriever> retrievers) {
        var map=new HashMap<String,Retriever>();
        for (var retriever:retrievers) if (retriever.id()==null || retriever.id().isBlank() || map.putIfAbsent(retriever.id(),retriever)!=null) {
            throw new IllegalArgumentException("Unique retriever ids required");
        }
        this.retrievers=Map.copyOf(map);
    }
    public RetrievalResponse retrieve(RetrievalRequest request) throws Exception {
        var retriever=retrievers.get(request.retriever());
        if (retriever==null) throw new IllegalArgumentException("RETRIEVAL_INVALID_CONFIGURATION");
        try {
            var response=Objects.requireNonNull(retriever.retrieve(request));
            if (!request.retriever().equals(response.retriever()) || !request.corpusVersion().equals(response.corpusVersion())
                    || !request.rankingVersion().equals(response.rankingVersion()) || response.chunks().size()>request.topK()
                    || response.chunks().stream().anyMatch(c -> !request.filters().allows(c.document()))) {
                throw new IllegalArgumentException("RETRIEVAL_SCOPE_OR_CONTRACT_VIOLATION");
            }
            return response;
        } catch (RetrievalException failure) {
            if (failure.kind()==RetrievalException.Kind.UNAVAILABLE || failure.kind()==RetrievalException.Kind.TIMEOUT) {
                throw new RetryableActivityException("RETRIEVAL_"+failure.kind());
            }
            throw new IllegalArgumentException("RETRIEVAL_"+failure.kind());
        }
    }
}
