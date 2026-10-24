package io.durableflow.workflow.timer;

import io.durableflow.workflow.history.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.task.WorkflowTaskOrchestrator;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import java.util.Map;

@Service
public class TimerTransactions {
    private final WorkflowTimerRepository timers;
    private final WorkflowRunRepository runs;
    private final WorkflowHistoryWriter history;
    private final WorkflowTaskOrchestrator orchestrator;
    public TimerTransactions(WorkflowTimerRepository timers, WorkflowRunRepository runs,
                             WorkflowHistoryWriter history, WorkflowTaskOrchestrator orchestrator) {
        this.timers = timers; this.runs = runs; this.history = history; this.orchestrator = orchestrator;
    }
    /** Lock timer -> run. Orchestration never takes write locks on existing timer rows under a run lock. */
    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public boolean fireOne() {
        var selected = timers.lockNextDue();
        if (selected.isEmpty()) return false;
        var timer = selected.get();
        var run = runs.findByIdForUpdate(timer.workflowRunId()).orElseThrow();
        if (run.getStatus() != WorkflowRunStatus.RUNNING || !timers.fire(timer.id())) return false;
        var fired = timers.findById(timer.id());
        history.append(timer.workflowRunId(), null, WorkflowEventType.TIMER_FIRED,
                Map.of("timerId", timer.timerId(), "wakeAt", timer.wakeAt().toString(), "firedAt", fired.firedAt().toString()));
        orchestrator.advanceInTransaction(timer.workflowRunId());
        return true;
    }
}
