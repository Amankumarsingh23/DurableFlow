package io.durableflow.evaluation;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.durableflow.tools.ToolJson;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class EvalRunnerTest {
    private EvalSuite suite(List<EvalCase> cases) {return new EvalSuite("unit","1",Map.of("dataset","v1","modelRoutingPolicy","v1","prompt","v1","retrievalIndex","v1","retrievalStrategy","v1","toolPolicy","v1","workflowDefinition","v1"),cases);}
    private EvalCase check(String field,EvalCase.Operator op,Object expected) {return new EvalCase("unit-case","1","synthetic",Map.of(),List.of(new EvalCase.Expectation("SAFETY",field,op,expected)),List.of(),0);}
    @Test void missingEvidenceAndWrongToolExcessCostDuplicatesAndUnauthorizedSourcesFail() {
        var tests=List.of(check("tool",EvalCase.Operator.EQUALS,"refundOrder"),check("cost",EvalCase.Operator.AT_MOST,100),check("duplicates",EvalCase.Operator.EQUALS,0),check("sourceIds",EvalCase.Operator.CONTAINS_NONE,List.of("private-b")));
        var evidence=List.of(Map.<String,Object>of("tool","deleteAllOrders"),Map.<String,Object>of("cost",101),Map.<String,Object>of("duplicates",1),Map.<String,Object>of("sourceIds",List.of("private-b")));
        for(int i=0;i<tests.size();i++) {
            var evaluator=new ObjectiveEvaluator();assertThat(evaluator.evaluate(tests.get(i),new EvalObservation(evidence.get(i),Map.of()))).allMatch(c -> !c.passed());
            assertThat(evaluator.evaluate(tests.get(i),new EvalObservation(Map.of(),Map.of()))).allMatch(c -> !c.passed() && c.detail().equals("MISSING_EVIDENCE"));
        }
    }
    @Test void fingerprintComparesExactNormalizedArgumentsAndSchemaCanFailIndependently() {
        var expected=Map.<String,Object>of("toolName","refundOrder","arguments",Map.of("orderId","O-42","amount",4500));
        var test=check("fingerprint",EvalCase.Operator.FINGERPRINT,expected);var evaluator=new ObjectiveEvaluator();
        assertThat(evaluator.evaluate(test,new EvalObservation(Map.of("fingerprint",ToolJson.fingerprint("refundOrder",Map.of("orderId","O-42","amount",4500.0))),Map.of()))).allMatch(EvalResult.Check::passed);
        assertThat(evaluator.evaluate(test,new EvalObservation(Map.of("fingerprint",ToolJson.fingerprint("refundOrder",Map.of("orderId","O-42","amount",45000))),Map.of()))).noneMatch(EvalResult.Check::passed);
        assertThat(evaluator.evaluate(check("schemaValid",EvalCase.Operator.EQUALS,true),new EvalObservation(Map.of("schemaValid",false),Map.of()))).noneMatch(EvalResult.Check::passed);
    }
    @Test void retrievalScoresUseKnownRelevanceDeduplicateHitsAndRespectK() {
        var scores=RetrievalMetrics.score(List.of("noise","b","b","a"),List.of("a","b"),3);
        assertThat(scores.recallAtK()).isEqualTo(0.5);assertThat(scores.reciprocalRank()).isEqualTo(0.5);
        assertThat(RetrievalMetrics.score(List.of("noise"),List.of("a"),1).recallAtK()).isZero();
        assertThatThrownBy(() -> RetrievalMetrics.score(List.of(),List.of(),3)).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void reportCountsFailuresVersionsAndOnlyMeasuredCostsWithoutLeakingExceptionContent() throws Exception {
        var first=check("allowed",EvalCase.Operator.EQUALS,true);
        var second=new EvalCase("exception","1","broken",Map.of(),first.expectations(),List.of(),0);
        var suite=suite(List.of(first,second));
        var report=new EvalRunner(new ObjectiveEvaluator()).run(suite,c -> {if(c.id().equals("exception"))throw new IllegalStateException("SECRET-PROMPT");return new EvalObservation(Map.of("allowed",true,"costMicros",10,"iterations",2),suite.versions());});
        assertThat(report.passed()).isEqualTo(1);assertThat(report.failed()).isEqualTo(1);assertThat(report.passRate()).isEqualTo(0.5);
        assertThat(report.aggregates()).containsEntry("costMicrosTotal",10.0).containsEntry("costMicrosAverage",10.0).containsEntry("costMicrosMeasuredCases",1.0);
        String json=EvalReportRenderer.json(report);assertThat(json).doesNotContain("SECRET-PROMPT");assertThat(new ObjectMapper().readTree(json).path("failed").asInt()).isEqualTo(1);
        assertThat(EvalReportRenderer.markdown(report)).contains("PASS","FAIL","Versions");
        var mismatch=new EvalRunner(new ObjectiveEvaluator()).run(suite(List.of(first)),c -> new EvalObservation(Map.of("allowed",true),Map.of()));
        assertThat(mismatch.failuresByCategory()).containsKey("VERSIONS");
    }
    @Test void goldenDatasetHasVersionedIndependentSafetyAndDurabilityContracts() throws Exception {
        try(var in=getClass().getResourceAsStream("/eval/golden-v1.json")) {
            var suite=new ObjectMapper().readValue(in,EvalSuite.class);assertThat(suite.cases()).hasSize(12);
            assertThat(suite.cases()).anyMatch(c -> c.scenario().equals("crash")).anyMatch(c -> c.scenario().equals("hostile")).anyMatch(c -> c.scenario().equals("quality"));
            assertThat(suite.cases()).allMatch(c -> !c.expectations().isEmpty());
        }
    }
}
