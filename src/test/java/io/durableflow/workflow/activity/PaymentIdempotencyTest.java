package io.durableflow.workflow.activity;

import io.durableflow.workflow.activity.demo.FakePaymentProvider;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;
import static org.assertj.core.api.Assertions.*;

class PaymentIdempotencyTest {
    @Test
    void keyUsesOnlyLogicalActivityIdentity() {
        UUID run = UUID.randomUUID();
        UUID activity = UUID.randomUUID();
        var first = new ActivityExecutionContext(run, activity);
        assertThat(first.idempotencyKey()).isEqualTo("df:v1:" + activity);
        assertThat(new ActivityExecutionContext(run, activity).idempotencyKey()).isEqualTo(first.idempotencyKey());
        assertThat(new ActivityExecutionContext(run, UUID.randomUUID()).idempotencyKey()).isNotEqualTo(first.idempotencyKey());
    }

    @Test
    void canonicalPayloadReusesResultAndRejectsMaterialChanges() {
        var provider = new FakePaymentProvider();
        var nested = new HashMap<String, Object>();
        nested.put("amount", 1000);
        var input = new HashMap<String, Object>();
        input.put("orderId", "O-1");
        input.put("payment", nested);
        var result = provider.charge("K", input);
        assertThat(provider.effectCount()).isEqualTo(1);
        assertThat(provider.charge("K", Map.of("payment", Map.of("amount", new BigDecimal("1000.00")), "orderId", "O-1")))
                .isEqualTo(result);
        nested.put("amount", 2000); // The stored canonical payload must be an independent snapshot.
        assertThatThrownBy(() -> provider.charge("K", input)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("different payment payload");
        assertThat(provider.effectCount()).isEqualTo(1);
        assertThat(provider.charge("K", Map.of("orderId", "O-1", "payment", Map.of("amount", 1000)))).isEqualTo(result);
        assertThat(provider.charge("other-key", input)).isNotEqualTo(result);
        assertThat(provider.effectCount()).isEqualTo(2);
    }

    @Test
    void simultaneousRequestsCreateOneEffect() throws Exception {
        var provider = new FakePaymentProvider();
        var together = new CyclicBarrier(2);
        var executor = Executors.newFixedThreadPool(2);
        try {
            Callable<Map<String, Object>> request = () -> {
                together.await(10, TimeUnit.SECONDS);
                return provider.charge("K", Map.of("amount", 1000));
            };
            var a = executor.submit(request);
            var b = executor.submit(request);
            assertThat(a.get(10, TimeUnit.SECONDS)).isEqualTo(b.get(10, TimeUnit.SECONDS));
            assertThat(provider.effectCount()).isEqualTo(1);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }
}
