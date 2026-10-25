package io.durableflow.evaluation;

/** Future subjective evaluator seam only. No implementation/remote calls in this session.
 * Any future adapter must version judge+prompt, disclose cost, nondeterminism, prompt sensitivity,
 * self-preference and bias, and calibrate faithfulness/relevance against human review.
 * It must never approve tool execution or replace objective safety checks. */
@FunctionalInterface
public interface GenerationJudge {
    record Assessment(Double faithfulness,Double relevance,String judgeVersion,String promptVersion) {}
    Assessment assess(String syntheticQuestion,String syntheticEvidence,String syntheticAnswer);
}
