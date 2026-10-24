package io.durableflow.workflow.activity;

import io.durableflow.workflow.activity.demo.*;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

class ActivityRegistryTest {
    @Test
    void demoHandlersAreRegisteredByStableNameAndHaveDeterministicOutputs() throws Exception {
        var provider = new FakePaymentProvider();
        var context = new ActivityExecutionContext(UUID.randomUUID(), UUID.randomUUID());
        var registry = new ActivityRegistry(List.of(new ChargePaymentActivity(provider),
                new ReserveInventoryActivity(), new CreateShipmentActivity()));
        var first = registry.require("chargePayment").execute(context, Map.of());
        assertThat(first.get("paymentId")).asString().startsWith("pay-demo-");
        assertThat(registry.require("chargePayment").execute(context, Map.of())).isEqualTo(first);
        assertThat(provider.effectCount()).isEqualTo(1);
        assertThat(registry.require("reserveInventory").execute(context, Map.of())).containsEntry("reservationId", "res-demo-456");
        assertThat(registry.require("createShipment").execute(context, Map.of())).containsEntry("shipmentId", "ship-demo-789");
        assertThatThrownBy(() -> registry.require("missing")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void duplicateHandlerNamesFailConstruction() {
        var provider = new FakePaymentProvider();
        assertThatThrownBy(() -> new ActivityRegistry(List.of(new ChargePaymentActivity(provider), new ChargePaymentActivity(provider))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Duplicate activity handler");
    }
}
