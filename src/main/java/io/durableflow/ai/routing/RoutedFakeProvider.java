package io.durableflow.ai.routing;
import io.durableflow.ai.model.*;
import io.durableflow.ai.provider.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Deterministic local simulation; no credentials, SDK, network or billing. */
public abstract class RoutedFakeProvider implements ModelProvider {
    public enum Outcome { SUCCESS, UNAVAILABLE, RATE_LIMITED, TIMEOUT, INVALID_REQUEST, AUTHENTICATION, UNSUPPORTED_CAPABILITY }
    public record Invocation(ModelRequest request,String key,int attempt) {}
    private final FakeModelProvider delegate;private final List<Outcome> script;
    private final Map<String,AtomicInteger> attempts=new ConcurrentHashMap<>();private final Queue<Invocation> invocations=new ConcurrentLinkedQueue<>();
    protected RoutedFakeProvider(FakeModelProvider delegate,String script) {this.delegate=delegate;this.script=Arrays.stream(script.split(",")).map(String::strip).map(Outcome::valueOf).toList();if(this.script.isEmpty())throw new IllegalArgumentException("Fake script required");}
    public List<Invocation> invocations() {return List.copyOf(invocations);}
    public ModelResponse generate(ModelRequest request,String key) throws Exception {
        int attempt=attempts.computeIfAbsent(key,k -> new AtomicInteger()).incrementAndGet();invocations.add(new Invocation(request,key,attempt));
        if(!(id().equals("fake-a")?Set.of("balanced-v1","text-v1"):Set.of("premium-v1","economy-v1")).contains(request.model()))throw new ModelProviderException(ModelProviderException.Kind.INVALID_MODEL);
        var outcome=script.get(Math.min(attempt-1,script.size()-1));if(outcome!=Outcome.SUCCESS)throw new ModelProviderException(ModelProviderException.Kind.valueOf(outcome.name()));
        if(request.structuredOutput() && request.model().equals("text-v1"))throw new ModelProviderException(ModelProviderException.Kind.UNSUPPORTED_CAPABILITY);
        String semantic="fake-recommendation-v1";
        try {var node=new ObjectMapper().readTree(request.userContent());if(node!=null && node.has("semanticModel"))semantic=node.path("semanticModel").asText();}
        catch(com.fasterxml.jackson.core.JsonProcessingException plainUserText) { /* Ordinary unstructured demo request. */ }
        var translated=new ModelRequest(request.requestVersion(),"fake",semantic,request.systemInstructions(),request.userContent(),request.temperature(),request.maxOutputTokens(),request.structuredOutput());
        var response=delegate.generate(translated,key);
        return new ModelResponse(response.content(),response.structuredJson(),id(),request.model(),response.usage(),response.latencyMillis(),id()+"-"+response.providerRequestId());
    }
}
