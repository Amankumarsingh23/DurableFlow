package io.durableflow.ai.retrieval;

import io.durableflow.workflow.activity.*;
import org.springframework.stereotype.Component;
import java.util.*;
import io.durableflow.observability.*;

@Component
public class RetrieveContextActivity implements ActivityHandler {
    private final RetrievalGateway gateway;
    private final ContextBuilder contexts;
    private final SafeTelemetry telemetry;
    public RetrieveContextActivity(RetrievalGateway gateway,ContextBuilder contexts) {this(gateway,contexts,SafeTelemetry.disabled());}
    @org.springframework.beans.factory.annotation.Autowired
    public RetrieveContextActivity(RetrievalGateway gateway,ContextBuilder contexts,SafeTelemetry telemetry) {this.gateway=gateway;this.contexts=contexts;this.telemetry=telemetry;}
    public String type() { return "retrieveContext"; }
    public Map<String,Object> execute(ActivityExecutionContext context,Map<String,Object> input) throws Exception {
        long started=System.nanoTime();String status="FAILURE",strategy="other";
        try {
        var request=RetrievalRequest.fromInput(input);strategy=request.strategy().name();var response=gateway.retrieve(request);
        var output=new LinkedHashMap<String,Object>();
        output.put("retrievalVersion",1); output.put("retriever",response.retriever()); output.put("corpusVersion",response.corpusVersion());
        output.put("rankingVersion",response.rankingVersion()); output.put("query",request.query()); output.put("strategy",request.strategy().name());
        output.put("filters",request.filters().toMap()); output.put("topK",request.topK());
        output.put("selectedChunks",response.chunks().stream().map(RetrievedChunk::toMap).toList());
        var built=contexts.build(response.chunks(),request.maxContextChars());output.put("context",built);
        var tags=Map.of("strategy",strategy);
        telemetry.sample(Metric.RETRIEVAL_CANDIDATES,response.candidateCount(),tags);
        telemetry.sample(Metric.RETRIEVAL_SELECTED,response.chunks().size(),tags);
        telemetry.sample(Metric.RETRIEVAL_CONTEXT_CHARS,((Number)built.get("characterCount")).doubleValue(),tags);
        telemetry.sample(Metric.RETRIEVAL_BUDGET_UTILIZATION,((Number)built.get("characterCount")).doubleValue()/request.maxContextChars(),tags);
        status="SUCCESS";return output;
        } finally {
            var tags=Map.of("strategy",strategy,"status",status);
            telemetry.count(Metric.RETRIEVAL_CALLS,tags);telemetry.duration(Metric.RETRIEVAL_DURATION,System.nanoTime()-started,tags);
        }
    }
}
