CREATE TABLE tool_request (
    id UUID PRIMARY KEY,
    workflow_run_id UUID NOT NULL UNIQUE REFERENCES workflow_run(id),
    source_activity_id UUID NOT NULL REFERENCES activity_execution(id),
    proposal_activity_id UUID NOT NULL UNIQUE REFERENCES activity_execution(id),
    execute_node_id VARCHAR(200) NOT NULL,
    approval_node_id VARCHAR(200),
    tool_name VARCHAR(100) NOT NULL,
    arguments_json JSONB NOT NULL CHECK (jsonb_typeof(arguments_json)='object'),
    fingerprint VARCHAR(64) NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
    descriptor_json JSONB NOT NULL CHECK (jsonb_typeof(descriptor_json)='object'),
    risk_level VARCHAR(30) NOT NULL CHECK (risk_level IN ('READ_ONLY','LOW_RISK_WRITE','HIGH_RISK_WRITE')),
    decision_json JSONB NOT NULL CHECK (jsonb_typeof(decision_json)='object'),
    status VARCHAR(30) NOT NULL CHECK (status IN ('AWAITING_APPROVAL','APPROVED','EXECUTED','REJECTED')),
    result_json JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(result_json)='object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    approved_at TIMESTAMPTZ,
    executed_at TIMESTAMPTZ,
    CHECK (status <> 'AWAITING_APPROVAL' OR approval_node_id IS NOT NULL),
    CHECK ((status='EXECUTED') = (executed_at IS NOT NULL)),
    CHECK (status NOT IN ('APPROVED','EXECUTED') OR approved_at IS NOT NULL),
    CHECK (status <> 'EXECUTED' OR result_json <> '{}'::jsonb)
);
-- Database enforcement supplements API/Java fingerprint checks. No payload or decision rewrites.
CREATE FUNCTION guard_tool_request_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.id,NEW.workflow_run_id,NEW.source_activity_id,NEW.proposal_activity_id,NEW.execute_node_id,
           NEW.approval_node_id,NEW.tool_name,NEW.arguments_json,NEW.fingerprint,NEW.descriptor_json,
           NEW.risk_level,NEW.decision_json,NEW.created_at)
       IS DISTINCT FROM ROW(OLD.id,OLD.workflow_run_id,OLD.source_activity_id,OLD.proposal_activity_id,OLD.execute_node_id,
           OLD.approval_node_id,OLD.tool_name,OLD.arguments_json,OLD.fingerprint,OLD.descriptor_json,
           OLD.risk_level,OLD.decision_json,OLD.created_at) THEN
        RAISE EXCEPTION 'TOOL_IMMUTABLE_REQUEST';
    END IF;
    IF NOT ((OLD.status='AWAITING_APPROVAL' AND NEW.status IN ('APPROVED','REJECTED')) OR
            (OLD.status='APPROVED' AND NEW.status='EXECUTED')) THEN
        RAISE EXCEPTION 'TOOL_INVALID_TRANSITION';
    END IF;
    IF NEW.status <> 'EXECUTED' AND (NEW.result_json IS DISTINCT FROM OLD.result_json OR NEW.executed_at IS DISTINCT FROM OLD.executed_at) THEN
        RAISE EXCEPTION 'TOOL_RESULT_BEFORE_EXECUTION';
    END IF;
    IF OLD.approved_at IS NOT NULL AND NEW.approved_at IS DISTINCT FROM OLD.approved_at THEN
        RAISE EXCEPTION 'TOOL_IMMUTABLE_APPROVAL';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER tool_request_immutable BEFORE UPDATE ON tool_request
    FOR EACH ROW EXECUTE FUNCTION guard_tool_request_update();

-- Simulated EXTERNAL provider ledger: independently committed, deliberately no run FK.
-- A real provider must own this durable idempotency/effect boundary, outside DurableFlow's DB.
CREATE TABLE fake_tool_effect (
    idempotency_key VARCHAR(100) PRIMARY KEY,
    fingerprint VARCHAR(64) NOT NULL,
    tool_name VARCHAR(100) NOT NULL,
    result_json JSONB NOT NULL CHECK (jsonb_typeof(result_json)='object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
