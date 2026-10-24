package io.durableflow.workflow.activity;

import io.durableflow.workflow.activity.demo.*;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class ActivityRegistryTest {
    @Test
    void demoHandlersAreRegisteredByStableNameAndHaveDeterministicOutputs() throws Exception {
        var registry = new ActivityRegistry(List.of(new ChargePaymentActivity(),
                new ReserveInventoryActivity(), new CreateShipmentActivity()));
        assertThat(registry.require("chargePayment").execute(Map.of())).containsEntry("paymentId", "pay-demo-123");
        assertThat(registry.require("reserveInventory").execute(Map.of())).containsEntry("reservationId", "res-demo-456");
        assertThat(registry.require("createShipment").execute(Map.of())).containsEntry("shipmentId", "ship-demo-789");
        assertThatThrownBy(() -> registry.require("missing")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void duplicateHandlerNamesFailConstruction() {
        assertThatThrownBy(() -> new ActivityRegistry(List.of(new ChargePaymentActivity(), new ChargePaymentActivity())))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Duplicate activity handler");
    }
}
