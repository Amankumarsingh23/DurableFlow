package io.durableflow.ai.retrieval;

import io.durableflow.workflow.activity.*;
import org.springframework.stereotype.Component;
import java.util.*;

@Component
public class RetrieveContextActivity implements ActivityHandler {
    private final RetrievalGateway gateway;
    private final ContextBuilder contexts;
    public RetrieveContextActivity(RetrievalGateway gateway,ContextBuilder contexts) { this.gateway=gateway; this.contexts=contexts; }
    public String type() { return "retrieveContext"; }
    public Map<String,Object> execute(ActivityExecutionContext context,Map<String,Object> input) throws Exception {
        var request=RetrievalRequest.fromInput(input); var response=gateway.retrieve(request);
        var output=new LinkedHashMap<String,Object>();
        output.put("retrievalVersion",1); output.put("retriever",response.retriever()); output.put("corpusVersion",response.corpusVersion());
        output.put("rankingVersion",response.rankingVersion()); output.put("query",request.query()); output.put("strategy",request.strategy().name());
        output.put("filters",request.filters().toMap()); output.put("topK",request.topK());
        output.put("selectedChunks",response.chunks().stream().map(RetrievedChunk::toMap).toList());
        output.put("context",contexts.build(response.chunks(),request.maxContextChars())); return output;
    }
}
