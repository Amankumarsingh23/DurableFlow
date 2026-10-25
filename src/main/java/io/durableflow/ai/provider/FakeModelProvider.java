package io.durableflow.ai.provider;

import io.durableflow.ai.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** No network, credentials, real inference, billing or request deduplication. */
@Component
public class FakeModelProvider implements ModelProvider {
    public enum Outcome { SUCCESS, RATE_LIMITED, UNAVAILABLE, TIMEOUT, INVALID_REQUEST, MALFORMED_STRUCTURED }
    @FunctionalInterface public interface BeforeResponse {
        void accept(ModelRequest request, String key, int attempt) throws Exception;
    }
    public record Invocation(ModelRequest request, String operationKey, int attempt) {}
    private final List<Outcome> script;
    private final BeforeResponse beforeResponse;
    private final ConcurrentHashMap<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Invocation> invocations = new ConcurrentLinkedQueue<>();
    @Autowired public FakeModelProvider() { this(List.of(Outcome.SUCCESS)); }
    public FakeModelProvider(List<Outcome> script) { this(script, (request, key, attempt) -> {}); }
    public FakeModelProvider(List<Outcome> script, BeforeResponse beforeResponse) {
        if (script == null || script.isEmpty()) throw new IllegalArgumentException("Fake response script required");
        this.script = List.copyOf(script); this.beforeResponse = Objects.requireNonNull(beforeResponse);
    }
    public String id() { return "fake"; }
    public List<Invocation> invocations() { return List.copyOf(invocations); }
    public int callCount() { return invocations.size(); }
    public ModelResponse generate(ModelRequest request, String key) throws Exception {
        int attempt = attempts.computeIfAbsent(key, ignored -> new AtomicInteger()).incrementAndGet();
        invocations.add(new Invocation(request, key, attempt));
        beforeResponse.accept(request, key, attempt);
        if (!Set.of("fake-recommendation-v1", "fake-text-v1", "fake-tool-refund-v1", "fake-tool-read-v1", "fake-agent-refund-v1", "fake-agent-runaway-v1", "fake-agent-final-v1", "fake-agent-malformed-v1", "fake-agent-repeat-write-v1").contains(request.model())) {
            throw new ModelProviderException(ModelProviderException.Kind.INVALID_MODEL);
        }
        var outcome = script.get(Math.min(attempt - 1, script.size() - 1));
        if (outcome != Outcome.SUCCESS && outcome != Outcome.MALFORMED_STRUCTURED) {
            throw new ModelProviderException(ModelProviderException.Kind.valueOf(outcome.name()));
        }
        String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(request.toString().getBytes(StandardCharsets.UTF_8))).substring(0, 16);
        String text = "Review the supplied request before applying the configured action (" + digest + ").";
        var usage = new ModelUsage((request.systemInstructions().length() + request.userContent().length() + 3L) / 4L,
                Math.min(request.maxOutputTokens(), 24));
        String requestId = "fake-" + UUID.nameUUIDFromBytes((key + ":" + attempt).getBytes(StandardCharsets.UTF_8));
        if (outcome == Outcome.MALFORMED_STRUCTURED) return new ModelResponse("not-json", null, id(), request.model(), usage, 7, requestId);
        if (request.model().startsWith("fake-agent-")) {
            if (!request.structuredOutput()) throw new ModelProviderException(ModelProviderException.Kind.INVALID_REQUEST);
            var context = new com.fasterxml.jackson.databind.ObjectMapper().readTree(request.userContent());
            int iteration = context.path("iteration").asInt();
            Map<String,Object> decision;
            if (request.model().equals("fake-agent-malformed-v1")) decision=Map.of("type","UNKNOWN","privateReasoning","must-never-be-checkpointed");
            else if (request.model().equals("fake-agent-final-v1") || (request.model().equals("fake-agent-refund-v1") && iteration>=4)) {
                String reference="";
                for (var fact : context.path("data").path("priorFacts")) {
                    if (fact.path("toolResult").path("toolName").asText().equals("refundOrder")) reference=fact.path("toolResult").path("providerReference").asText();
                }
                decision=Map.of("type","FINAL","answer",reference.isEmpty()?"Task complete.":"Refund "+reference+" issued.");
            } else {
                String tool=request.model().equals("fake-agent-runaway-v1")?"getOrder":request.model().equals("fake-agent-repeat-write-v1")?"refundOrder":
                        iteration==1?"getOrder":iteration==2?"getRefundPolicy":"refundOrder";
                var arguments=tool.equals("refundOrder")?Map.<String,Object>of("orderId","O-42","amount",4500):Map.<String,Object>of("orderId","O-42");
                decision=Map.of("type","TOOL_CALL","toolName",tool,"arguments",arguments);
            }
            return new ModelResponse(null,decision,id(),request.model(),usage,7,requestId);
        }
        if (request.model().startsWith("fake-tool-")) {
            if (!request.structuredOutput()) throw new ModelProviderException(ModelProviderException.Kind.INVALID_REQUEST);
            var proposal = request.model().equals("fake-tool-refund-v1")
                    ? Map.of("toolName", "refundOrder", "arguments", Map.of("orderId", "O-42", "amount", 4500))
                    : Map.of("toolName", "getOrder", "arguments", Map.of("orderId", "O-42"));
            return new ModelResponse(null, Map.of("toolProposal", proposal, "inputFingerprint", digest), id(), request.model(), usage, 7, requestId);
        }
        return request.structuredOutput()
                ? new ModelResponse(null, Map.of("recommendation", text, "inputFingerprint", digest), id(), request.model(), usage, 7, requestId)
                : new ModelResponse(text, null, id(), request.model(), usage, 7, requestId);
    }
}
