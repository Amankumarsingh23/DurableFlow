package io.durableflow.evaluation;
import java.util.*;
public record EvalSuite(String id,String version,Map<String,String> versions,List<EvalCase> cases) {
    public EvalSuite {
        versions=Map.copyOf(versions);cases=List.copyOf(cases);
        if(cases.isEmpty() || cases.stream().map(EvalCase::id).distinct().count()!=cases.size())throw new IllegalArgumentException("Unique nonempty eval cases required");
        for(String key:List.of("dataset","modelRoutingPolicy","prompt","retrievalIndex","retrievalStrategy","toolPolicy","workflowDefinition"))
            if(!versions.containsKey(key) || versions.get(key).isBlank())throw new IllegalArgumentException("Missing eval version: "+key);
    }
}
