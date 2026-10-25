package io.durableflow.ai.provider;

/** Typed failure contract; gateway never classifies based on message text. */
public class ModelProviderException extends RuntimeException {
    public enum Kind { RATE_LIMITED, UNAVAILABLE, TIMEOUT, INVALID_REQUEST, INVALID_MODEL, MALFORMED_RESPONSE, AUTHENTICATION, UNSUPPORTED_CAPABILITY, UNCLASSIFIED }
    private final Kind kind;
    public ModelProviderException(Kind kind) { super("MODEL_" + java.util.Objects.requireNonNull(kind)); this.kind = kind; }
    public Kind kind() { return kind; }
}
