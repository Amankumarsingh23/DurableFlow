package io.durableflow.ai.retrieval;
public class RetrievalException extends RuntimeException {
    public enum Kind { UNAVAILABLE, TIMEOUT, INVALID_CONFIGURATION }
    private final Kind kind;
    public RetrievalException(Kind kind) { super("RETRIEVAL_"+java.util.Objects.requireNonNull(kind)); this.kind=kind; }
    public Kind kind() { return kind; }
}
