package io.durableflow.evaluation;
import io.durableflow.tools.ToolJson;
import java.math.BigDecimal;
import java.util.*;

/** Deterministic contracts only. No subjective correctness or groundedness score is invented. */
public class ObjectiveEvaluator implements Evaluator {
    public List<EvalResult.Check> evaluate(EvalCase test,EvalObservation observation) {
        var result=new ArrayList<EvalResult.Check>();
        for(var check:test.expectations()) {
            Object actual=observation.facts().get(check.field());boolean passed=false;
            try {
                if(actual!=null)passed=switch(check.operator()) {
                    case EQUALS -> equal(actual,check.expected());
                    case AT_MOST -> decimal(actual).signum()>=0 && decimal(actual).compareTo(decimal(check.expected()))<=0;
                    case CONTAINS_ALL -> actual instanceof Collection<?> values && check.expected() instanceof Collection<?> expected && values.containsAll(expected);
                    case CONTAINS_NONE -> actual instanceof Collection<?> values && check.expected() instanceof Collection<?> forbidden && Collections.disjoint(values,forbidden);
                    case FINGERPRINT -> {
                        var expected=ToolJson.object(check.expected());
                        yield actual.equals(ToolJson.fingerprint((String)expected.get("toolName"),ToolJson.object(expected.get("arguments"))));
                    }
                };
            } catch(RuntimeException invalid) {passed=false;}
            result.add(new EvalResult.Check(check.category(),check.field(),passed,actual==null?"MISSING_EVIDENCE":passed?"MATCH":"CONTRACT_MISMATCH"));
        }
        return List.copyOf(result);
    }
    private boolean equal(Object a,Object b) {
        return a instanceof Number && b instanceof Number ? decimal(a).compareTo(decimal(b))==0 : Objects.equals(a,b);
    }
    private BigDecimal decimal(Object value) {if(!(value instanceof Number))throw new IllegalArgumentException();return new BigDecimal(value.toString());}
}
