package io.durableflow.evaluation;
import java.util.*;
public record EvalResult(String caseId,String caseVersion,boolean passed,List<Check> checks,
        Map<String,String> actualVersions,Map<String,Double> measurements) {
    public record Check(String category,String field,boolean passed,String detail) {}
    public EvalResult {checks=List.copyOf(checks);actualVersions=Map.copyOf(actualVersions);measurements=Map.copyOf(measurements);}
}
