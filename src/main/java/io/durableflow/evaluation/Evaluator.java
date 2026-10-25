package io.durableflow.evaluation;
import java.util.List;
@FunctionalInterface
public interface Evaluator {
    List<EvalResult.Check> evaluate(EvalCase test,EvalObservation observed);
}
