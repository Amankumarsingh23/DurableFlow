package io.durableflow.ai.retrieval;

import java.math.BigDecimal;
import java.util.*;

public record RetrievalRequest(int requestVersion, String retriever, String corpusVersion, String rankingVersion,
        String query, RetrievalStrategy strategy, int topK, int maxContextChars, RetrievalFilters filters) {
    public RetrievalRequest {
        if (requestVersion != 1 || retriever == null || !retriever.matches("[a-z][a-z0-9-]{0,63}")
                || corpusVersion == null || corpusVersion.isBlank() || corpusVersion.length()>200
                || rankingVersion == null || rankingVersion.isBlank() || rankingVersion.length()>200
                || query == null || query.isBlank() || query.length()>4000 || strategy == null || topK<1 || topK>20
                || maxContextChars<1 || maxContextChars>32000 || filters == null) throw new IllegalArgumentException("Invalid retrieval request");
    }
    public static RetrievalRequest fromInput(Map<String,Object> input) {
        if (!(input.get("retrievalRequest") instanceof Map<?,?> r) || !Set.of("requestVersion","retriever","corpusVersion","rankingVersion",
                "query","strategy","topK","maxContextChars","filters").containsAll(r.keySet())) throw new IllegalArgumentException("Versioned retrievalRequest required");
        try {
            return new RetrievalRequest(integer(r.get("requestVersion")),text(r.get("retriever")),text(r.get("corpusVersion")),text(r.get("rankingVersion")),
                    text(r.get("query")),RetrievalStrategy.valueOf(text(r.get("strategy"))),integer(r.get("topK")),integer(r.get("maxContextChars")),RetrievalFilters.parse(r.get("filters")));
        } catch (RuntimeException invalid) { throw new IllegalArgumentException("Invalid retrieval request"); }
    }
    private static int integer(Object value) {
        if (!(value instanceof Number)) throw new IllegalArgumentException("Integer required");
        return new BigDecimal(value.toString()).intValueExact();
    }
    private static String text(Object value) {
        if (!(value instanceof String text)) throw new IllegalArgumentException("Text required"); return text;
    }
}
