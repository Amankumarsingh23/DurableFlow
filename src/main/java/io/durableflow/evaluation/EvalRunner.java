package io.durableflow.evaluation;
import java.util.*;

public class EvalRunner {
    @FunctionalInterface public interface ScenarioExecutor {EvalObservation execute(EvalCase test) throws Exception;}
    public record Report(String suiteId,String suiteVersion,Map<String,String> versions,int total,int passed,int failed,
            double passRate,Map<String,Long> failuresByCategory,Map<String,Double> aggregates,List<EvalResult> results) {}
    private final Evaluator evaluator;
    public EvalRunner(Evaluator evaluator) {this.evaluator=evaluator;}
    public Report run(EvalSuite suite,ScenarioExecutor executor) {
        var results=new ArrayList<EvalResult>();var categories=new TreeMap<String,Long>();
        for(var test:suite.cases()) {
            EvalResult result;
            try {
                var observed=Objects.requireNonNull(executor.execute(test));
                var checks=new ArrayList<>(evaluator.evaluate(test,observed));
                for(var version:suite.versions().entrySet()) {
                    String actual=observed.actualVersions().get(version.getKey());
                    checks.add(new EvalResult.Check("VERSIONS",version.getKey(),version.getValue().equals(actual),actual==null?"MISSING_VERSION":"VERSION_COMPARISON"));
                }
                var measurements=new TreeMap<String,Double>();
                for(String key:List.of("costMicros","iterations","modelCalls","providerAttempts","toolCalls","fallbacks")) {
                    Object value=observed.facts().get(key);
                    if(value instanceof Number number && Double.isFinite(number.doubleValue()) && number.doubleValue()>=0)measurements.put(key,number.doubleValue());
                }
                if(!test.relevantSourceIds().isEmpty()) {
                    Object value=observed.facts().get("sourceIds");
                    if(value instanceof List<?> list && list.stream().allMatch(String.class::isInstance)) {
                        var ranked=list.stream().map(String.class::cast).toList();
                        var scores=RetrievalMetrics.score(ranked,test.relevantSourceIds(),test.retrievalK());
                        measurements.put("recallAtK",scores.recallAtK());measurements.put("reciprocalRank",scores.reciprocalRank());
                    } else checks.add(new EvalResult.Check("RETRIEVAL","sourceIds",false,"MISSING_RANKING"));
                }
                result=new EvalResult(test.id(),test.version(),checks.stream().allMatch(EvalResult.Check::passed),checks,observed.actualVersions(),measurements);
            } catch(Exception failure) {
                // Exception messages can contain prompts/arguments. Reports expose no exception message.
                result=new EvalResult(test.id(),test.version(),false,List.of(new EvalResult.Check("EXECUTION","scenario",false,"EXECUTOR_FAILED")),Map.of(),Map.of());
            }
            result.checks().stream().filter(c -> !c.passed()).map(EvalResult.Check::category).distinct().forEach(c -> categories.merge(c,1L,Long::sum));
            results.add(result);
        }
        int passed=(int)results.stream().filter(EvalResult::passed).count();var aggregates=new TreeMap<String,Double>();
        for(String key:List.of("costMicros","iterations","modelCalls","providerAttempts","toolCalls","fallbacks","recallAtK","reciprocalRank")) {
            var stats=results.stream().filter(r -> r.measurements().containsKey(key)).mapToDouble(r -> r.measurements().get(key)).summaryStatistics();
            aggregates.put(key+"MeasuredCases",(double)stats.getCount());
            if(stats.getCount()>0) {aggregates.put(key+"Average",stats.getAverage());aggregates.put(key+"Total",stats.getSum());}
        }
        return new Report(suite.id(),suite.version(),suite.versions(),results.size(),passed,results.size()-passed,(double)passed/results.size(),Collections.unmodifiableMap(categories),Collections.unmodifiableMap(aggregates),List.copyOf(results));
    }
}
