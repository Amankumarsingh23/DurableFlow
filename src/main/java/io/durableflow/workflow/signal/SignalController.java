package io.durableflow.workflow.signal;

import jakarta.validation.Valid;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;

@RestController @Profile("!worker")
@RequestMapping("/api/workflow-runs")
public class SignalController {
    private final SignalReceiver receiver;
    public SignalController(SignalReceiver receiver) { this.receiver = receiver; }
    @PostMapping("/{runId}/signals")
    public SignalReceipt receive(@PathVariable UUID runId, @Valid @RequestBody ReceiveSignalRequest request) {
        return receiver.receive(runId, request);
    }
}
