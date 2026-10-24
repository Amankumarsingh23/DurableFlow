package io.durableflow.workflow.activity;

import io.durableflow.workflow.run.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class ActivityLifecycleTest {
    private final Instant now = Instant.parse("2026-10-25T10:00:00Z");

    @Test
    void successRecordsInputOutputAttemptAndTimestamps() {
        var activity = pending();
        assertThatThrownBy(() -> activity.markCompleted(Map.of(), now)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> activity.markFailed("error", now)).isInstanceOf(IllegalStateException.class);
        activity.markRunning(Map.of("orderId", "42"), now);
        assertThat(activity.getStatus()).isEqualTo(ActivityExecutionStatus.RUNNING);
        assertThat(activity.getStartedAt()).isEqualTo(now);
        assertThat(activity.getCompletedAt()).isNull();
        assertThat(activity.getAttemptCount()).isEqualTo(1);
        assertThat(activity.getInputJson()).containsEntry("orderId", "42");
        activity.markCompleted(Map.of("result", "ok"), now.plusSeconds(1));
        assertThat(activity.getOutputJson()).containsEntry("result", "ok");
        assertThat(activity.getStatus()).isEqualTo(ActivityExecutionStatus.COMPLETED);
        assertThat(activity.getCompletedAt()).isEqualTo(now.plusSeconds(1));
        assertThat(activity.getUpdatedAt()).isEqualTo(now.plusSeconds(1));
    }

    @Test
    void failureRecordsDiagnosticAndTerminalTimestamp() {
        var activity = pending();
        activity.markRunning(Map.of(), now);
        activity.markFailed("demo failure", now.plusSeconds(1));
        assertThat(activity.getStatus()).isEqualTo(ActivityExecutionStatus.FAILED);
        assertThat(activity.getErrorMessage()).isEqualTo("demo failure");
        assertThat(activity.getCompletedAt()).isEqualTo(now.plusSeconds(1));
        assertThat(activity.getOutputJson()).isNull();
    }

    @ParameterizedTest
    @EnumSource(value = ActivityExecutionStatus.class, names = {"COMPLETED", "FAILED"})
    void terminalActivitiesCannotRestartOrTransition(ActivityExecutionStatus status) {
        var activity = pending();
        activity.setStatus(status);
        assertThatThrownBy(() -> activity.markRunning(Map.of(), now)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> activity.markCompleted(Map.of(), now)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> activity.markFailed("again", now)).isInstanceOf(IllegalStateException.class);
    }

    @ParameterizedTest
    @EnumSource(value = WorkflowRunStatus.class, names = {"PENDING", "COMPLETED", "FAILED", "CANCELLED"})
    void onlyRunningWorkflowCanCompleteOrFail(WorkflowRunStatus status) {
        var run = new WorkflowRunEntity();
        run.setStatus(status);
        assertThatThrownBy(() -> run.markCompleted(now)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> run.markFailed(now)).isInstanceOf(IllegalStateException.class);
    }

    private ActivityExecutionEntity pending() {
        var activity = new ActivityExecutionEntity();
        activity.setStatus(ActivityExecutionStatus.PENDING);
        return activity;
    }
}
