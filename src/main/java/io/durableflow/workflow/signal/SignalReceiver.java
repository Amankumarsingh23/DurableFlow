package io.durableflow.workflow.signal;

import io.durableflow.api.ApiException;
import io.durableflow.workflow.definition.*;
import io.durableflow.workflow.history.*;
import io.durableflow.workflow.run.*;
import io.durableflow.workflow.task.WorkflowTaskOrchestrator;
import jakarta.validation.Validator;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;
import java.util.*;

@Service
public class SignalReceiver {
    private final io.durableflow.security.ResourceLimits limits;
    private final WorkflowRunRepository runs;
    private final WorkflowDefinitionRepository definitions;
    private final SignalRepository signals;
    private final WorkflowTaskOrchestrator orchestrator;
    private final WorkflowHistoryWriter history;
    private final Validator validator;
    private final List<SignalAcceptanceGuard> guards;
    public SignalReceiver(WorkflowRunRepository runs, WorkflowDefinitionRepository definitions, SignalRepository signals,
                          WorkflowTaskOrchestrator orchestrator, WorkflowHistoryWriter history, Validator validator, List<SignalAcceptanceGuard> guards) {this(runs,definitions,signals,orchestrator,history,validator,guards,io.durableflow.security.ResourceLimits.defaults());}
    @org.springframework.beans.factory.annotation.Autowired
    public SignalReceiver(WorkflowRunRepository runs, WorkflowDefinitionRepository definitions, SignalRepository signals,
                          WorkflowTaskOrchestrator orchestrator, WorkflowHistoryWriter history, Validator validator, List<SignalAcceptanceGuard> guards, io.durableflow.security.ResourceLimits limits) {
        this.limits=limits;
        this.runs = runs; this.definitions = definitions; this.signals = signals;
        this.orchestrator = orchestrator; this.history = history; this.validator = validator; this.guards = List.copyOf(guards);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public SignalReceipt receive(UUID runId, ReceiveSignalRequest request) {
        if(request!=null)limits.checkJson(io.durableflow.security.ResourceLimits.Payload.SIGNAL,request.payload());
        if (request == null || !validator.validate(request).isEmpty()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "Nonblank signalId, signalNodeId and payload object are required");
        }
        var run = runs.findByIdForUpdate(runId).orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Workflow run not found"));
        var existing = signals.delivery(runId, request.signalId());
        if (existing.isPresent()) {
            if (!signals.matches(existing.get(), request)) throw new ApiException(HttpStatus.CONFLICT, "Signal id reused with different target or payload");
            return existing.get(); // Harmless replay even after completion; no new events or advancement.
        }
        if (run.getStatus() != WorkflowRunStatus.RUNNING) throw new ApiException(HttpStatus.CONFLICT, "Only RUNNING runs accept new signals");
        var definition = definitions.findById(run.getWorkflowDefinitionId()).orElseThrow();
        boolean known = WorkflowDefinitionDocument.steps(definition.getDefinitionJson()).stream()
                .anyMatch(step -> step.type() == NodeType.SIGNAL && step.id().equals(request.signalNodeId()));
        if (!known) throw new ApiException(HttpStatus.BAD_REQUEST, "Target must be an exact SIGNAL node id in this run's definition");
        if (signals.forNode(runId, request.signalNodeId()).isPresent()) {
            throw new ApiException(HttpStatus.CONFLICT, "Signal node already has an accepted delivery; reuse its original signalId");
        }
        for (var guard : guards) guard.beforeAccept(runId, request);
        signals.insert(runId, request);
        boolean waiting = signals.waits(runId).stream().anyMatch(wait -> wait.signalNodeId().equals(request.signalNodeId()));
        if (!waiting) history.append(runId, null, WorkflowEventType.SIGNAL_BUFFERED,
                Map.of("signalNodeId", request.signalNodeId(), "signalId", request.signalId()));
        // Delivery, possible consumption and newly eligible work share one commit, without a callback gap.
        orchestrator.advanceInTransaction(runId);
        return signals.delivery(runId, request.signalId()).orElseThrow();
    }
}
