package io.durableflow.ai.retrieval;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

@Component
public class FakeRetriever implements Retriever {
    public enum Outcome { SUCCESS, UNAVAILABLE, TIMEOUT, INVALID_CONFIGURATION }
    @FunctionalInterface public interface BeforeRetrieve { void accept(RetrievalRequest request,int call) throws Exception; }
    private static final Pattern TOKEN=Pattern.compile("[a-z0-9]+(?:[-_][a-z0-9]+)*");
    private static final List<Set<String>> CONCEPTS=List.of(
            Set.of("refund","refunds","reimburse","reimbursements","reimbursement","money","back","payment","payments"),
            Set.of("fraud","suspicious","scam","risk"), Set.of("shipping","ship","delivery","parcel","dispatched"),
            Set.of("account","customer","profile"), Set.of("approval","approve","review","reviews","reviewer"));
    private static final Comparator<RetrievedChunk> ORDER=Comparator.comparingDouble(RetrievedChunk::score).reversed().thenComparing(RetrievedChunk::chunkId);
    private final List<RetrievedChunk> corpus;
    private final List<Outcome> script;
    private final BeforeRetrieve before;
    private final AtomicInteger calls=new AtomicInteger();
    @Autowired public FakeRetriever() { this(FakeCorpus.policiesV1(),List.of(Outcome.SUCCESS)); }
    public FakeRetriever(List<RetrievedChunk> corpus,List<Outcome> script) { this(corpus,script,(request,call) -> {}); }
    public FakeRetriever(List<RetrievedChunk> corpus,List<Outcome> script,BeforeRetrieve before) {
        this.corpus=List.copyOf(corpus); this.script=List.copyOf(script); this.before=Objects.requireNonNull(before);
        if (script.isEmpty() || corpus.stream().map(RetrievedChunk::chunkId).distinct().count()!=corpus.size()) {
            throw new IllegalArgumentException("Nonempty script and unique corpus chunk ids required");
        }
    }
    public String id() { return "fake"; }
    public int callCount() { return calls.get(); }
    public RetrievalResponse retrieve(RetrievalRequest request) throws Exception {
        int call=calls.incrementAndGet(); before.accept(request,call);
        if (!"policies-v1".equals(request.corpusVersion()) || !"local-v1".equals(request.rankingVersion())) {
            throw new RetrievalException(RetrievalException.Kind.INVALID_CONFIGURATION);
        }
        var outcome=script.get(Math.min(call-1,script.size()-1));
        if (outcome!=Outcome.SUCCESS) throw new RetrievalException(RetrievalException.Kind.valueOf(outcome.name()));
        // Authorization scope is applied BEFORE candidate scoring, fusion or context construction.
        var authorized=corpus.stream().filter(c -> request.filters().allows(c.document())).toList();
        var dense=rank(authorized,request.query(),RetrievalStrategy.DENSE);
        var sparse=rank(authorized,request.query(),RetrievalStrategy.SPARSE);
        var selected=switch(request.strategy()) { case DENSE -> dense; case SPARSE -> sparse; case HYBRID -> fuse(dense,sparse); };
        return new RetrievalResponse(id(),request.corpusVersion(),request.rankingVersion(),selected.stream().limit(request.topK()).toList());
    }
    private List<RetrievedChunk> rank(List<RetrievedChunk> chunks,String query,RetrievalStrategy strategy) {
        var terms=new HashSet<>(tokens(query)); var queryVector=vector(query);
        var ranked=new ArrayList<RetrievedChunk>();
        for (var chunk:chunks) {
            String searchable=chunk.document().title()+" "+chunk.text();
            double score=strategy==RetrievalStrategy.DENSE ? cosine(queryVector,vector(searchable))
                    : tokens(searchable).stream().filter(terms::contains).count();
            if (score>0) ranked.add(chunk.ranked(score,strategy,Set.of(strategy)));
        }
        ranked.sort(ORDER); return ranked;
    }
    /** Reciprocal rank fusion: sum 1/(60+rank), ranks start at 1. No raw-score scale mixing. */
    private List<RetrievedChunk> fuse(List<RetrievedChunk> dense,List<RetrievedChunk> sparse) {
        var scores=new HashMap<String,Double>(); var chunks=new HashMap<String,RetrievedChunk>();
        var sources=new HashMap<String,Set<RetrievalStrategy>>();
        for (var ranking:List.of(dense,sparse)) for (int i=0;i<ranking.size();i++) {
            var chunk=ranking.get(i); chunks.putIfAbsent(chunk.chunkId(),chunk);
            scores.merge(chunk.chunkId(),1.0/(60+i+1),Double::sum);
            sources.computeIfAbsent(chunk.chunkId(),ignored -> EnumSet.noneOf(RetrievalStrategy.class)).add(chunk.strategy());
        }
        return chunks.values().stream().map(c -> c.ranked(scores.get(c.chunkId()),RetrievalStrategy.HYBRID,sources.get(c.chunkId()))).sorted(ORDER).toList();
    }
    private static List<String> tokens(String text) {
        var matcher=TOKEN.matcher(text.toLowerCase(Locale.ROOT)); var result=new ArrayList<String>();
        while (matcher.find()) result.add(matcher.group()); return result;
    }
    private static double[] vector(String text) {
        double[] result=new double[CONCEPTS.size()];
        for (var token:tokens(text)) for (int i=0;i<CONCEPTS.size();i++) if (CONCEPTS.get(i).contains(token)) result[i]++;
        return result;
    }
    private static double cosine(double[] a,double[] b) {
        double dot=0,left=0,right=0;
        for (int i=0;i<a.length;i++) { dot+=a[i]*b[i]; left+=a[i]*a[i]; right+=b[i]*b[i]; }
        return left==0 || right==0 ? 0 : dot/(Math.sqrt(left)*Math.sqrt(right));
    }
}
