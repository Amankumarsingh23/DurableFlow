ALTER TABLE agent_iteration ADD COLUMN routing_extra_reserved_cost_micros BIGINT NOT NULL DEFAULT 0 CHECK (routing_extra_reserved_cost_micros>=0);
CREATE TABLE model_route (
    activity_execution_id UUID PRIMARY KEY REFERENCES activity_execution(id),
    agent_execution_id UUID REFERENCES agent_execution(id),
    request_fingerprint VARCHAR(64) NOT NULL,
    plan_json JSONB NOT NULL CHECK (jsonb_typeof(plan_json)='object'),
    active_index INTEGER NOT NULL DEFAULT 0 CHECK (active_index BETWEEN 0 AND 3),
    total_attempts INTEGER NOT NULL DEFAULT 0 CHECK (total_attempts BETWEEN 0 AND 10),
    base_attempt_cost_micros BIGINT NOT NULL CHECK (base_attempt_cost_micros>=0),
    extra_reserved_cost_micros BIGINT NOT NULL DEFAULT 0 CHECK (extra_reserved_cost_micros>=0),
    reserved_tokens BIGINT NOT NULL DEFAULT 0 CHECK (reserved_tokens>=0),
    reserved_cost_micros BIGINT NOT NULL DEFAULT 0 CHECK (reserved_cost_micros>=0),
    status VARCHAR(30) NOT NULL DEFAULT 'READY' CHECK (status IN ('READY','EXHAUSTED','BUDGET_BLOCKED','TERMINAL')),
    terminal_code VARCHAR(100),
    transition_reason VARCHAR(100) NOT NULL DEFAULT 'PRIMARY_SELECTED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE model_route_attempt (
    id UUID PRIMARY KEY,
    activity_execution_id UUID NOT NULL REFERENCES model_route(activity_execution_id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number BETWEEN 1 AND 10),
    route_index INTEGER NOT NULL CHECK (route_index BETWEEN 0 AND 2),
    route_attempt_number INTEGER NOT NULL CHECK (route_attempt_number BETWEEN 1 AND 3),
    provider VARCHAR(64) NOT NULL,
    model VARCHAR(100) NOT NULL,
    status VARCHAR(30) NOT NULL CHECK (status IN ('STARTED','SUCCESS','HEALTH_FAILURE','REQUEST_FAILURE')),
    failure_code VARCHAR(100),
    input_tokens BIGINT NOT NULL DEFAULT 0 CHECK (input_tokens>=0),
    output_tokens BIGINT NOT NULL DEFAULT 0 CHECK (output_tokens>=0),
    estimated_cost_micros BIGINT NOT NULL DEFAULT 0 CHECK (estimated_cost_micros>=0),
    provider_request_id VARCHAR(200),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    UNIQUE(activity_execution_id,attempt_number),
    UNIQUE(activity_execution_id,route_index,route_attempt_number)
);
CREATE FUNCTION guard_model_route_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.activity_execution_id,NEW.agent_execution_id,NEW.request_fingerprint,NEW.plan_json,NEW.base_attempt_cost_micros,NEW.created_at)
       IS DISTINCT FROM ROW(OLD.activity_execution_id,OLD.agent_execution_id,OLD.request_fingerprint,OLD.plan_json,OLD.base_attempt_cost_micros,OLD.created_at)
       OR NEW.active_index < OLD.active_index OR NEW.total_attempts < OLD.total_attempts
       OR NEW.extra_reserved_cost_micros < OLD.extra_reserved_cost_micros OR NEW.reserved_tokens < OLD.reserved_tokens
       OR NEW.reserved_cost_micros < OLD.reserved_cost_micros THEN RAISE EXCEPTION 'MODEL_ROUTE_IMMUTABLE_PLAN'; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER model_route_guard BEFORE UPDATE ON model_route FOR EACH ROW EXECUTE FUNCTION guard_model_route_update();
