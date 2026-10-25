package io.durableflow.ai.routing;
import io.durableflow.ai.model.ModelRequest;
import java.util.*;
public record ModelRouteRequest(ModelRequest task,boolean requireStructured,boolean requireToolUse,int requiredQualityTier,boolean allowQualityDowngrade,
        long remainingTokens,long remainingCostMicros,Set<String> allowedProviders,Set<String> forbiddenProviders,Set<String> requiredTags,int maxRoutes,int attemptsPerRoute,int maxProviderAttempts) {
    public ModelRouteRequest {
        Objects.requireNonNull(task);requireStructured=requireStructured || task.structuredOutput();allowedProviders=Set.copyOf(allowedProviders);forbiddenProviders=Set.copyOf(forbiddenProviders);requiredTags=Set.copyOf(requiredTags);
        if(requiredQualityTier<1 || requiredQualityTier>3 || remainingTokens<0 || remainingCostMicros<0 || maxRoutes<1 || maxRoutes>3 || attemptsPerRoute<1 || attemptsPerRoute>3 || maxProviderAttempts<1 || maxProviderAttempts>10)
            throw new IllegalArgumentException("Invalid routing constraints");
    }
    public int inputChars() {return task.systemInstructions().length()+task.userContent().length();}
    public long attemptTokens() {return (long)inputChars()+task.maxOutputTokens();}
}
