package io.durableflow.ai.routing;
import org.springframework.stereotype.Component;
import java.util.*;
@Component
public class ModelRouter {
    private final ModelCatalog catalog;private final ProviderCircuitBreaker breaker;
    public ModelRouter(ModelCatalog catalog,ProviderCircuitBreaker breaker) {this.catalog=catalog;this.breaker=breaker;}
    public RouteDecision select(ModelRouteRequest r) {
        var candidates=catalog.models().stream().filter(ModelDescriptor::enabled)
            .filter(m -> !r.requireStructured() || m.capabilities().structuredOutput()).filter(m -> !r.requireToolUse() || m.capabilities().toolUse())
            .filter(m -> r.inputChars()<=m.capabilities().maxContextChars()).filter(m -> r.allowedProviders().contains(m.provider()) && !r.forbiddenProviders().contains(m.provider()))
            .filter(m -> m.tags().containsAll(r.requiredTags())).filter(m -> m.qualityTier()>=r.requiredQualityTier() || r.allowQualityDowngrade())
            .filter(m -> r.attemptTokens()<=r.remainingTokens() && m.estimatedCost(r.inputChars(),r.task().maxOutputTokens())<=r.remainingCostMicros())
            .filter(m -> breaker.available(m.provider()))
            .sorted(Comparator.<ModelDescriptor>comparingInt(m -> m.qualityTier()>=r.requiredQualityTier()?0:1)
                .thenComparingLong(m -> m.estimatedCost(r.inputChars(),r.task().maxOutputTokens())).thenComparing(Comparator.comparingInt(ModelDescriptor::qualityTier).reversed())
                .thenComparing(ModelDescriptor::provider).thenComparing(ModelDescriptor::model)).limit(r.maxRoutes()).toList();
        if(candidates.isEmpty())throw new IllegalArgumentException("MODEL_NO_ELIGIBLE_ROUTE");
        return new RouteDecision("cost-quality-v1","HARD_FILTER_THEN_QUALITY_COST_STABLE_ORDER",candidates,r.attemptsPerRoute(),r.maxProviderAttempts(),Map.of("requiredQualityTier",r.requiredQualityTier(),"allowQualityDowngrade",r.allowQualityDowngrade(),
            "requireStructured",r.requireStructured(),"requireToolUse",r.requireToolUse(),"allowedProviders",r.allowedProviders().stream().sorted().toList(),"forbiddenProviders",r.forbiddenProviders().stream().sorted().toList(),
            "requiredTags",r.requiredTags().stream().sorted().toList(),"remainingTokens",r.remainingTokens(),"remainingCostMicros",r.remainingCostMicros()));
    }
}
