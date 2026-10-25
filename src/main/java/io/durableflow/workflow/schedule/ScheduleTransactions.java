package io.durableflow.workflow.schedule;

import io.durableflow.workflow.definition.WorkflowDefinitionRepository;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.task.WorkflowTaskOrchestrator;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;

@Service
public class ScheduleTransactions {
    private final ScheduleRepository schedules;
    private final WorkflowDefinitionRepository definitions;
    private final WorkflowRunService runs;
    private final WorkflowTaskOrchestrator orchestrator;
    public ScheduleTransactions(ScheduleRepository schedules, WorkflowDefinitionRepository definitions,
            WorkflowRunService runs, WorkflowTaskOrchestrator orchestrator) {
        this.schedules = schedules; this.definitions = definitions; this.runs = runs; this.orchestrator = orchestrator;
    }
    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public boolean fireOne() {
        var next = schedules.lockNextDue(); if (next.isEmpty()) return false;
        var schedule = next.get();
        if (schedules.occurrence(schedule).isEmpty()) {
            var definition = definitions.findById(schedule.workflowDefinitionId()).orElseThrow();
            var run = runs.start(new StartWorkflowRunRequest(definition.getName(), definition.getVersion(), schedule.input()));
            schedules.insertOccurrence(schedule, run.runId());
            orchestrator.advanceInTransaction(run.runId());
        }
        // Even a replay of an existing occurrence advances the deadline, never creates another run.
        schedules.advance(schedule);
        return true;
    }
}
