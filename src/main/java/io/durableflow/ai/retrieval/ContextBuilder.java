package io.durableflow.ai.retrieval;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import java.util.*;

@Component
public class ContextBuilder {
    private final ObjectMapper json=new ObjectMapper();
    public Map<String,Object> build(List<RetrievedChunk> chunks,int maxChars) {
        if (maxChars<1 || maxChars>32000) throw new IllegalArgumentException("Invalid context budget");
        var unique=new LinkedHashMap<String,RetrievedChunk>(); for (var chunk:chunks) unique.putIfAbsent(chunk.chunkId(),chunk);
        var text=new StringBuilder(); var sources=new ArrayList<Map<String,Object>>();
        for (var chunk:unique.values()) {
            String label="S"+(sources.size()+1); var source=chunk.sourceMap(); source.put("label",label);
            String block;
            try { block="["+label+"] "+json.writeValueAsString(source)+"\n"+chunk.text()+"\n"; }
            catch (Exception invalid) { throw new IllegalArgumentException("Invalid source metadata",invalid); }
            if (text.length()+block.length()>maxChars) break; // Never truncate text or metadata identities; keep whole chunks only.
            text.append(block); sources.add(source);
        }
        return Map.of("text",text.toString(),"sources",sources,"characterCount",text.length(),"budgetChars",maxChars,
                "omittedChunks",unique.size()-sources.size());
    }
}
