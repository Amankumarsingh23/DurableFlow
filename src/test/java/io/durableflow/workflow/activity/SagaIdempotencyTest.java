package io.durableflow.workflow.activity;

import io.durableflow.workflow.activity.demo.*;
import io.durableflow.workflow.compensation.CompensationExecutionEntity;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class SagaIdempotencyTest {
    @Test
    void chargeAndRefundHaveIndependentStableKeysAndOneEffectEach() {
        var provider = new FakePaymentProvider();
        UUID run = UUID.randomUUID(), execution = UUID.randomUUID();
        var forward = new ActivityExecutionContext(run, execution);
        var input = Map.<String, Object>of("amount", 1000, "currency", "INR");
        var charge = provider.charge(forward.idempotencyKey(), input);
        assertThat(provider.charge(forward.idempotencyKey(), input)).isEqualTo(charge);
        var undo = new ActivityExecutionContext(run, execution, true, input, charge);
        assertThat(undo.idempotencyKey()).isEqualTo("df:v1:compensate:" + execution).isNotEqualTo(forward.idempotencyKey());
        var handler = new RefundPaymentActivity(provider);
        var refund = handler.execute(undo, Map.of());
        assertThat(handler.execute(undo, Map.of())).isEqualTo(refund);
        var nextAttempt = new ActivityExecutionContext(run, execution, true, input, charge);
        assertThat(handler.execute(nextAttempt, Map.of())).isEqualTo(refund);
        assertThat(provider.effectCount()).isEqualTo(1);
        assertThat(provider.refundEffectCount()).isEqualTo(1);
        var inconsistent = new ActivityExecutionContext(run, execution, true, Map.of("amount", 2000), charge);
        assertThatThrownBy(() -> handler.execute(inconsistent, Map.of())).isInstanceOf(IllegalArgumentException.class);
        assertThat(provider.refundEffectCount()).isEqualTo(1);
    }

    @Test
    void releaseDeduplicatesAndRejectsChangedReservation() {
        UUID run = UUID.randomUUID(), execution = UUID.randomUUID();
        var handler = new ReleaseInventoryActivity();
        var undo = new ActivityExecutionContext(run, execution, true, Map.of(), Map.of("reservationId", "res-1"));
        var release = handler.execute(undo, Map.of());
        assertThat(handler.execute(undo, Map.of())).isEqualTo(release);
        var changed = new ActivityExecutionContext(run, execution, true, Map.of(), Map.of("reservationId", "res-2"));
        assertThatThrownBy(() -> handler.execute(changed, Map.of())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void compensationCannotBeCreatedForPendingRunningOrFailedForwardActivity() {
        var original = new ActivityExecutionEntity();
        for (var state : List.of(ActivityExecutionStatus.PENDING, ActivityExecutionStatus.RUNNING, ActivityExecutionStatus.FAILED)) {
            original.setStatus(state);
            assertThatThrownBy(() -> CompensationExecutionEntity.pending(original, "refundPayment", Instant.EPOCH))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("completed");
        }
    }
}
