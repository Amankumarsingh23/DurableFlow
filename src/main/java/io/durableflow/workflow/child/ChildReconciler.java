package io.durableflow.workflow.child;

import io.durableflow.workflow.task.WorkflowTaskOrchestrator;
import org.springframework.stereotype.Service;

/** Durable polling is also the terminal-child notification mechanism; no callback is required. */
@Service
public class ChildReconciler {
    private final ChildReconciliationTransactions transactions;
    private final WorkflowTaskOrchestrator orchestrator;
    public ChildReconciler(ChildReconciliationTransactions transactions, WorkflowTaskOrchestrator orchestrator) {
        this.transactions = transactions; this.orchestrator = orchestrator;
    }
    public boolean reconcileOne() {
        var next = transactions.next(); if (next.isEmpty()) return false;
        var child = next.get();
        // Separate transactions and lock scopes: never hold child and parent run locks together.
        orchestrator.advanceRun(child.childRunId());
        orchestrator.advanceRun(child.parentRunId());
        return true;
    }
}
