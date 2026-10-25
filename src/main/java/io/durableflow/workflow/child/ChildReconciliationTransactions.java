package io.durableflow.workflow.child;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import java.util.Optional;

@Service
public class ChildReconciliationTransactions {
    private final ChildWorkflowRepository children;
    public ChildReconciliationTransactions(ChildWorkflowRepository children) { this.children = children; }
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Optional<ChildWorkflow> next() {
        var selected = children.lockNextForReconciliation();
        selected.ifPresent(c -> children.checked(c.id()));
        return selected;
    }
}
