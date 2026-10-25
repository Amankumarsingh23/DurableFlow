package io.durableflow.evaluation;
import java.util.*;
public final class RetrievalMetrics {
    private RetrievalMetrics() {}
    public record Scores(double recallAtK,double reciprocalRank) {}
    public static Scores score(List<String> ranked,List<String> relevant,int k) {
        var truth=new HashSet<>(relevant);if(truth.isEmpty() || k<1)throw new IllegalArgumentException("Known relevance and positive K required");
        var top=ranked.stream().limit(k).toList();var hits=new HashSet<>(top);hits.retainAll(truth);double rr=0;
        for(int i=0;i<top.size();i++)if(truth.contains(top.get(i))) {rr=1.0/(i+1);break;}
        return new Scores((double)hits.size()/truth.size(),rr);
    }
}
