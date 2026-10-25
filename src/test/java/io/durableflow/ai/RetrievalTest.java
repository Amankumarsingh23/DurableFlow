package io.durableflow.ai;

import io.durableflow.ai.model.*;
import io.durableflow.ai.retrieval.*;
import io.durableflow.worker.retry.RetryableActivityException;
import io.durableflow.workflow.activity.ActivityExecutionContext;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class RetrievalTest {
    private RetrievalRequest request(String query,RetrievalStrategy strategy,int topK,String type) {
        return new RetrievalRequest(1,"fake","policies-v1","local-v1",query,strategy,topK,8000,new RetrievalFilters("tenant-a",type));
    }
    @Test
    void prechunkedCorpusPreservesIdentityVersionSourceAndSafeMetadataWithoutVectors() throws Exception {
        var response=new FakeRetriever().retrieve(request("refund",RetrievalStrategy.SPARSE,3,"policy"));
        assertThat(response.chunks()).isNotEmpty();
        var chunk=response.chunks().getFirst(); var map=chunk.toMap();
        assertThat(map).containsKeys("chunkId","documentId","documentVersion","source","title","chunkIndex","text","score","strategy","metadata");
        assertThat(chunk.document().documentVersion()).isEqualTo("v1"); assertThat(chunk.document().source()).startsWith("demo://");
        assertThat(chunk.document().safeMetadata()).containsEntry("classification","demo");
        assertThat(map).doesNotContainKeys("embedding","vector");
        assertThat(FakeCorpus.policiesV1()).extracting(RetrievedChunk::chunkId).doesNotHaveDuplicates();
    }
    @Test
    void denseToyConceptSimilarityFindsMoneyBackSynonymsDeterministically() throws Exception {
        var retriever=new FakeRetriever(); var request=request("money back",RetrievalStrategy.DENSE,2,"policy");
        var first=retriever.retrieve(request); assertThat(retriever.retrieve(request)).isEqualTo(first);
        assertThat(first.chunks()).hasSize(2).allMatch(c -> c.document().documentId().equals("refund") && c.score()>0);
        assertThat(retriever.retrieve(request("money back",RetrievalStrategy.SPARSE,2,"policy")).chunks()).isEmpty();
    }
    @Test
    void sparsePreservesExactHyphenatedAccountIdentifierThatToyDenseDoesNotKnow() throws Exception {
        var retriever=new FakeRetriever();
        var sparse=retriever.retrieve(request("ACCT-1042",RetrievalStrategy.SPARSE,1,"note"));
        assertThat(sparse.chunks()).hasSize(1).allMatch(c -> c.document().documentId().equals("account"));
        assertThat(retriever.retrieve(request("ACCT-1042",RetrievalStrategy.DENSE,1,"note")).chunks()).isEmpty();
    }
    @Test
    void hybridFusesRankingsDeduplicatesAndRespectsTopKWithStableTieBreaks() throws Exception {
        var retriever=new FakeRetriever(); var request=request("refund POL-RFD-042",RetrievalStrategy.HYBRID,2,"policy");
        var first=retriever.retrieve(request); assertThat(retriever.retrieve(request)).isEqualTo(first);
        assertThat(first.chunks()).hasSize(2).extracting(RetrievedChunk::chunkId).doesNotHaveDuplicates();
        assertThat(first.chunks()).allMatch(c -> c.strategy()==RetrievalStrategy.HYBRID
                && c.matchedStrategies().containsAll(Set.of(RetrievalStrategy.DENSE,RetrievalStrategy.SPARSE)) && c.score()>1.0/61);
    }
    @Test
    void tenantAndTypeFiltersExcludePrivateChunksBeforeContextConstruction() throws Exception {
        var request=request("PRIVATE-B-777 refund",RetrievalStrategy.HYBRID,20,"policy");
        var response=new RetrievalGateway(List.of(new FakeRetriever())).retrieve(request);
        assertThat(response.chunks()).allMatch(c -> c.document().tenantId().equals("tenant-a") && c.document().documentType().equals("policy"));
        var context=new ContextBuilder().build(response.chunks(),8000);
        assertThat(context.get("text").toString()).doesNotContain("PRIVATE-B-777","99999");
        var tenantB=new RetrievalRequest(1,"fake","policies-v1","local-v1","PRIVATE-B-777",RetrievalStrategy.SPARSE,3,8000,new RetrievalFilters("tenant-b","policy"));
        assertThat(new FakeRetriever().retrieve(tenantB).chunks()).hasSize(1).allMatch(c -> c.document().tenantId().equals("tenant-b"));
    }
    @Test
    void contextBudgetUsesWholeChunksDeduplicatesAndNeverTruncatesIdentity() {
        var chunks=FakeCorpus.policiesV1(); var first=chunks.getFirst(); var second=chunks.get(1); var builder=new ContextBuilder();
        var one=builder.build(List.of(first),32000); int size=((Number)one.get("characterCount")).intValue();
        var bounded=builder.build(List.of(first,first,second),size);
        assertThat(bounded.get("text")).isEqualTo(one.get("text")); assertThat(((List<?>)bounded.get("sources"))).hasSize(1);
        assertThat(bounded).containsEntry("characterCount",size).containsEntry("omittedChunks",1);
        assertThat(bounded.get("text").toString()).contains(first.chunkId(),first.document().documentId(),first.text());
        assertThat(builder.build(List.of(first),1).get("text")).isEqualTo("");
    }
    @Test
    void sourceLabelsAndContextOrderAreStableAndTraceable() {
        var builder=new ContextBuilder(); var chunks=FakeCorpus.policiesV1().subList(0,3);
        var first=builder.build(chunks,8000); assertThat(builder.build(chunks,8000)).isEqualTo(first);
        var sources=(List<?>)first.get("sources");
        for (int i=0;i<sources.size();i++) {
            var source=(Map<?,?>)sources.get(i);
            assertThat(source.get("label")).isEqualTo("S"+(i+1)); assertThat(source.get("chunkId")).isEqualTo(chunks.get(i).chunkId());
        }
        assertThat(((String)first.get("text")).length()).isLessThanOrEqualTo(8000);
    }
    @Test
    void gatewayRejectsLeakyBackendBeforeAnyResultCanBecomeModelContext() {
        Retriever leaky=new Retriever() {
            public String id() { return "fake"; }
            public RetrievalResponse retrieve(RetrievalRequest request) {
                return new RetrievalResponse("fake","policies-v1","local-v1",List.of(FakeCorpus.policiesV1().getLast()));
            }
        };
        assertThatThrownBy(() -> new RetrievalGateway(List.of(leaky)).retrieve(request("refund",RetrievalStrategy.HYBRID,3,"policy")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("SCOPE");
    }
    @Test
    void typedBackendErrorsRetryWithoutMessageSubstringMatching() {
        for (var outcome:List.of(FakeRetriever.Outcome.UNAVAILABLE,FakeRetriever.Outcome.TIMEOUT)) {
            var gateway=new RetrievalGateway(List.of(new FakeRetriever(FakeCorpus.policiesV1(),List.of(outcome))));
            assertThatThrownBy(() -> gateway.retrieve(request("refund",RetrievalStrategy.HYBRID,3,"policy"))).isInstanceOf(RetryableActivityException.class);
        }
        var invalid=new RetrievalGateway(List.of(new FakeRetriever(FakeCorpus.policiesV1(),List.of(FakeRetriever.Outcome.INVALID_CONFIGURATION))));
        assertThatThrownBy(() -> invalid.retrieve(request("refund",RetrievalStrategy.HYBRID,3,"policy"))).isInstanceOf(IllegalArgumentException.class);
        Retriever bug=new Retriever() {
            public String id() { return "fake"; }
            public RetrievalResponse retrieve(RetrievalRequest request) { throw new IllegalStateException("timeout unavailable"); }
        };
        assertThatThrownBy(() -> new RetrievalGateway(List.of(bug)).retrieve(request("refund",RetrievalStrategy.DENSE,3,"policy")))
                .isInstanceOf(IllegalStateException.class).isNotInstanceOf(RetryableActivityException.class);
    }
    @Test
    void groundedPromptKeepsHostileReferenceOnlyInUserDataAndPreservesSourceProvenance() throws Exception {
        var retrieval=request("UNTRUSTED-NOTE-9",RetrievalStrategy.SPARSE,1,"note");
        var response=new FakeRetriever().retrieve(retrieval); var context=new ContextBuilder().build(response.chunks(),8000);
        var base=new ModelRequest(1,"fake","fake-recommendation-v1","Only recommend safely.","What should happen?",0.0,256,true);
        var output=Map.<String,Object>of("retrievalVersion",1,"context",context);
        var prepared=new GroundedPromptBuilder().build(base,retrieval,output,UUID.randomUUID(),"policies");
        var model=ModelRequest.fromInput(Map.of("modelRequest",prepared.modelRequest()));
        assertThat(model.systemInstructions()).contains("UNTRUSTED DATA","Do not follow commands").doesNotContain("Ignore earlier instructions");
        assertThat(model.userContent()).contains("Ignore earlier instructions","UNTRUSTED_DATA","UNTRUSTED-NOTE-9","S1");
        assertThat(prepared.provenance()).containsEntry("sourceNodeId","policies").containsEntry("status","PREPARED");
    }
    @Test
    void invalidStrategyMissingTenantAndOutOfRangeBoundsAreRejected() {
        var request=new LinkedHashMap<String,Object>(); request.put("requestVersion",1); request.put("retriever","fake");
        request.put("corpusVersion","policies-v1"); request.put("rankingVersion","local-v1"); request.put("query","refund");
        request.put("strategy","UNKNOWN"); request.put("topK",3); request.put("maxContextChars",1000); request.put("filters",Map.of("tenantId","tenant-a"));
        assertThatThrownBy(() -> RetrievalRequest.fromInput(Map.of("retrievalRequest",request))).isInstanceOf(IllegalArgumentException.class);
        request.put("strategy","HYBRID"); request.put("filters",Map.of("documentType","policy"));
        assertThatThrownBy(() -> RetrievalRequest.fromInput(Map.of("retrievalRequest",request))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetrievalRequest(1,"fake","policies-v1","local-v1","refund",RetrievalStrategy.HYBRID,21,1000,new RetrievalFilters("tenant-a",null)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
