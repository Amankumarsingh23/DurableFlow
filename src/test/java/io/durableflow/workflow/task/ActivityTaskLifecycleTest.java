package io.durableflow.workflow.task;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;

class ActivityTaskLifecycleTest {
    @Test
    void taskRequiresClaimBeforeOutcomeAndNeverReturnsToReady() {
        var now = Instant.parse("2026-10-25T10:00:00Z");
        var task = ActivityTaskEntity.ready(UUID.randomUUID(), now);
        assertThatThrownBy(task::complete).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> task.claimLease(" ", now, now.plusSeconds(30))).isInstanceOf(IllegalArgumentException.class);
        assertThat(task.getStatus()).isEqualTo(ActivityTaskStatus.READY);
        task.claimLease("worker-A", now, now.plusSeconds(30));
        assertThat(task.getWorkerId()).isEqualTo("worker-A");
        assertThat(task.getClaimedAt()).isEqualTo(now);
        task.complete();
        assertThat(task.getStatus()).isEqualTo(ActivityTaskStatus.COMPLETED);
        assertThatThrownBy(() -> task.claimLease("worker-B", now, now.plusSeconds(30))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(task::fail).isInstanceOf(IllegalStateException.class);
    }
}
