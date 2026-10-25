package io.durableflow.ai.routing;
import io.durableflow.ai.model.ModelRequest;
import org.junit.jupiter.api.Test;
import java.time.*;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
class ModelRouterTest {
    private final ProviderCircuitBreaker breaker=new ProviderCircuitBreaker(Clock.fixed(Instant.EPOCH,ZoneOffset.UTC),2,1000);
    private final ModelRouter router=new ModelRouter(new ModelCatalog(),breaker);
    private ModelRouteRequest request(boolean structured,boolean tools,int quality,boolean downgrade,long tokens,long cost,Set<String> allowed,Set<String> tags,int context) {
        return new ModelRouteRequest(new ModelRequest(1,"routed","demo","system","x".repeat(context),0.0,128,structured),structured,tools,quality,downgrade,tokens,cost,allowed,Set.of(),tags,3,2,3);
    }
    private ModelRouteRequest standard() {return request(true,true,2,false,100000,1000000,Set.of("fake-a","fake-b"),Set.of("local"),100);}
    @Test void capabilityFilteringExcludesTextOnlyAndUnsupportedToolUse() {
        var plan=router.select(standard());assertThat(plan.candidates()).allMatch(m -> m.capabilities().structuredOutput() && m.capabilities().toolUse());
        assertThat(plan.candidates()).extracting(ModelDescriptor::model).doesNotContain("text-v1","economy-v1");
    }
    @Test void contextSizeIsAHardConstraint() {
        var plan=router.select(request(true,true,2,false,100000,1000000,Set.of("fake-a","fake-b"),Set.of(),25000));assertThat(plan.primary().provider()).isEqualTo("fake-b");
        assertThatThrownBy(() -> router.select(request(true,true,2,false,100000,1000000,Set.of("fake-a"),Set.of(),25000))).hasMessage("MODEL_NO_ELIGIBLE_ROUTE");
    }
    @Test void deterministicPrimaryAndCostRankingPreferBalancedOverMoreExpensivePremium() {
        var a=router.select(standard());assertThat(router.select(standard())).isEqualTo(a);assertThat(a.primary().provider()).isEqualTo("fake-a");
        assertThat(a.candidates().get(1).provider()).isEqualTo("fake-b");assertThat(a.policyVersion()).isEqualTo("cost-quality-v1");
    }
    @Test void structuredOutputRequirementCannotBeDowngradedAway() {
        var onlyText=new ModelCatalog(List.of(new ModelDescriptor("fake-a","text",3,0,0,new ModelCapabilities(false,true,12000),true,Set.of("local"))));
        assertThatThrownBy(() -> new ModelRouter(onlyText,breaker).select(standard())).hasMessage("MODEL_NO_ELIGIBLE_ROUTE");
    }
    @Test void providerRestrictionAndRequiredSecurityTagAreHardFilters() {
        assertThat(router.select(request(true,true,2,false,100000,1000000,Set.of("fake-a"),Set.of("restricted"),100)).candidates()).extracting(ModelDescriptor::provider).containsOnly("fake-a");
        breaker.healthFailure("fake-a");breaker.healthFailure("fake-a");
        assertThatThrownBy(() -> router.select(request(true,true,2,false,100000,1000000,Set.of("fake-a"),Set.of("restricted"),100))).hasMessage("MODEL_NO_ELIGIBLE_ROUTE");
    }
    @Test void forbiddenProviderCannotBeSelectedEvenWhenCheaper() {
        var r=standard();var forbidden=new ModelRouteRequest(r.task(),true,true,2,false,100000,1000000,r.allowedProviders(),Set.of("fake-a"),Set.of(),2,2,3);
        assertThat(router.select(forbidden).primary().provider()).isEqualTo("fake-b");
    }
    @Test void qualityDowngradeFalseExcludesBelowTier() {
        assertThat(router.select(request(true,false,2,false,100000,1000000,Set.of("fake-a","fake-b"),Set.of(),100)).candidates()).allMatch(m -> m.qualityTier()>=2);
    }
    @Test void qualityDowngradeTruePlacesCheaperLowTierAfterQualifiedRoutes() {
        var plan=router.select(request(true,false,2,true,100000,1000000,Set.of("fake-a","fake-b"),Set.of(),100));
        assertThat(plan.candidates()).hasSize(3);assertThat(plan.candidates().getLast().model()).isEqualTo("economy-v1");assertThat(plan.primary().qualityTier()).isGreaterThanOrEqualTo(2);
    }
    @Test void tokenOrCostImpossibilityFailsBeforeInvocation() {
        assertThatThrownBy(() -> router.select(request(true,true,2,false,1,1000000,Set.of("fake-a","fake-b"),Set.of(),100))).hasMessage("MODEL_NO_ELIGIBLE_ROUTE");
        assertThatThrownBy(() -> router.select(request(true,true,2,false,100000,1,Set.of("fake-a","fake-b"),Set.of(),100))).hasMessage("MODEL_NO_ELIGIBLE_ROUTE");
    }
    @Test void disabledModelNeverEntersRoutePlan() {
        var disabled=new ModelCatalog(List.of(new ModelDescriptor("fake-a","disabled",3,0,0,new ModelCapabilities(true,true,24000),false,Set.of("local"))));
        assertThatThrownBy(() -> new ModelRouter(disabled,breaker).select(standard())).hasMessage("MODEL_NO_ELIGIBLE_ROUTE");
    }
    @Test void routeCountIsBoundedAndSnapshotsRoundTrip() {
        var r=standard();var one=new ModelRouteRequest(r.task(),true,true,2,false,100000,1000000,r.allowedProviders(),Set.of(),Set.of(),1,2,3);
        var decision=router.select(one);assertThat(decision.candidates()).hasSize(1);assertThat(RouteDecision.from(decision.toMap())).isEqualTo(decision);
    }
    @Test void openProviderIsSkippedWithoutRelaxingConstraints() {
        breaker.healthFailure("fake-a");breaker.healthFailure("fake-a");assertThat(router.select(standard()).primary().provider()).isEqualTo("fake-b");
    }
}
