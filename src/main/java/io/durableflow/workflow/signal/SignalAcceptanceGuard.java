package io.durableflow.workflow.signal;
import java.util.UUID;
/** Validate and persist bounded local decisions under the run lock, atomically with signal delivery. */
public interface SignalAcceptanceGuard { void beforeAccept(UUID runId,ReceiveSignalRequest request); }
