package io.durableflow.evaluation;
import java.util.*;

/** Evidence returned by an executor after running real fake-backed components. Missing evidence fails checks. */
public record EvalObservation(Map<String,Object> facts,Map<String,String> actualVersions) {
    public EvalObservation {facts=Map.copyOf(facts);actualVersions=Map.copyOf(actualVersions);}
}
