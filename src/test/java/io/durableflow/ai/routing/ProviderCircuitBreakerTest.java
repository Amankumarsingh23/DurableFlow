package io.durableflow.ai.routing;
import org.junit.jupiter.api.Test;
import java.time.*;
import static org.assertj.core.api.Assertions.*;
class ProviderCircuitBreakerTest {
    private static class MutableClock extends Clock {
        Instant now=Instant.EPOCH;public ZoneId getZone(){return ZoneOffset.UTC;}public Clock withZone(ZoneId z){return this;}public Instant instant(){return now;}
    }
    @Test void healthThresholdOpensUntilCooldownAndHalfOpenAllowsOneProbe() {
        var clock=new MutableClock();var breaker=new ProviderCircuitBreaker(clock,2,1000);
        breaker.healthFailure("fake-a");assertThat(breaker.state("fake-a")).isEqualTo(ProviderCircuitBreaker.State.CLOSED);
        breaker.healthFailure("fake-a");assertThat(breaker.state("fake-a")).isEqualTo(ProviderCircuitBreaker.State.OPEN);assertThat(breaker.acquire("fake-a")).isFalse();
        clock.now=clock.now.plusMillis(1000);assertThat(breaker.state("fake-a")).isEqualTo(ProviderCircuitBreaker.State.HALF_OPEN);
        assertThat(breaker.acquire("fake-a")).isTrue();assertThat(breaker.acquire("fake-a")).isFalse();
        breaker.success("fake-a");assertThat(breaker.state("fake-a")).isEqualTo(ProviderCircuitBreaker.State.CLOSED);assertThat(breaker.acquire("fake-a")).isTrue();
    }
    @Test void halfOpenFailureReopensAndRequestFailureDoesNotCountAsHealthFailure() {
        var clock=new MutableClock();var breaker=new ProviderCircuitBreaker(clock,1,1000);
        breaker.neutral("fake-a");assertThat(breaker.state("fake-a")).isEqualTo(ProviderCircuitBreaker.State.CLOSED);
        breaker.healthFailure("fake-a");clock.now=clock.now.plusMillis(1000);assertThat(breaker.acquire("fake-a")).isTrue();
        breaker.healthFailure("fake-a");assertThat(breaker.state("fake-a")).isEqualTo(ProviderCircuitBreaker.State.OPEN);
    }
    @Test void breakerIsPerInstanceAndProvidersAreIndependent() {
        var clock=new MutableClock();var first=new ProviderCircuitBreaker(clock,1,1000);var other=new ProviderCircuitBreaker(clock,1,1000);
        first.healthFailure("fake-a");assertThat(first.available("fake-a")).isFalse();assertThat(first.available("fake-b")).isTrue();assertThat(other.available("fake-a")).isTrue();
    }
}
