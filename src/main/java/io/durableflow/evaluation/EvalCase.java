package io.durableflow.evaluation;
import java.util.*;

/** Synthetic inputs and independently authored expectations. Never derive expectations from observations. */
public record EvalCase(String id,String version,String scenario,Map<String,Object> input,List<Expectation> expectations,
        List<String> relevantSourceIds,int retrievalK) {
    public enum Operator { EQUALS, AT_MOST, CONTAINS_ALL, CONTAINS_NONE, FINGERPRINT }
    public record Expectation(String category,String field,Operator operator,Object expected) {}
    public EvalCase {
        Objects.requireNonNull(id);Objects.requireNonNull(version);Objects.requireNonNull(scenario);
        input=Map.copyOf(input);expectations=List.copyOf(expectations);relevantSourceIds=List.copyOf(relevantSourceIds);
        if(expectations.isEmpty() || retrievalK<0)throw new IllegalArgumentException("Eval expectations required");
    }
}
