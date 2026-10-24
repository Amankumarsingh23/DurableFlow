package io.durableflow.workflow.signal;

import io.durableflow.workflow.history.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import java.util.*;

@Service
public class SignalWaitCoordinator {
    private final SignalRepository signals;
    private final WorkflowHistoryWriter history;
    public SignalWaitCoordinator(SignalRepository signals, WorkflowHistoryWriter history) { this.signals = signals; this.history = history; }
    @Transactional(propagation = Propagation.MANDATORY)
    public SignalWait materializeAndConsume(UUID run, String node, SignalWait existing) {
        var wait = existing;
        if (wait == null) {
            wait = signals.createWait(run, node);
            history.append(run, null, WorkflowEventType.SIGNAL_WAIT_STARTED, Map.of("signalNodeId", node));
        }
        if (wait.status() == SignalWait.Status.WAITING) {
            var delivery = signals.forNode(run, node);
            if (delivery.isPresent() && delivery.get().consumedAt() == null) {
                wait = signals.consume(wait, delivery.get());
                history.append(run, null, WorkflowEventType.SIGNAL_RECEIVED,
                        Map.of("signalNodeId", node, "signalId", delivery.get().signalId()));
            }
        }
        return wait;
    }
}
