CREATE TABLE agent_execution (
    id UUID PRIMARY KEY,
    workflow_run_id UUID NOT NULL UNIQUE REFERENCES workflow_run(id),
    status VARCHAR(30) NOT NULL CHECK (status IN ('RUNNING','WAITING_APPROVAL','COMPLETED','LIMIT_REACHED','FAILED')),
    goal VARCHAR(2000) NOT NULL CHECK (length(trim(goal))>0),
    scenario VARCHAR(30) NOT NULL CHECK (scenario IN ('refund','runaway','final','malformed','repeat-write')),
    limits_json JSONB NOT NULL CHECK (jsonb_typeof(limits_json)='object'),
    max_model_attempts INTEGER NOT NULL CHECK (max_model_attempts BETWEEN 1 AND 10),
    frozen_context_json JSONB NOT NULL CHECK (jsonb_typeof(frozen_context_json)='object'),
    current_iteration INTEGER NOT NULL DEFAULT 0 CHECK (current_iteration BETWEEN 0 AND 20),
    logical_model_calls INTEGER NOT NULL DEFAULT 0 CHECK (logical_model_calls>=0),
    model_attempts INTEGER NOT NULL DEFAULT 0 CHECK (model_attempts>=0),
    logical_tool_calls INTEGER NOT NULL DEFAULT 0 CHECK (logical_tool_calls>=0),
    input_tokens BIGINT NOT NULL DEFAULT 0 CHECK (input_tokens>=0),
    output_tokens BIGINT NOT NULL DEFAULT 0 CHECK (output_tokens>=0),
    estimated_cost_micros BIGINT NOT NULL DEFAULT 0 CHECK (estimated_cost_micros>=0),
    reserved_tokens BIGINT NOT NULL DEFAULT 0 CHECK (reserved_tokens>=0),
    reserved_cost_micros BIGINT NOT NULL DEFAULT 0 CHECK (reserved_cost_micros>=0),
    termination_reason VARCHAR(80),
    final_output_json JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(final_output_json)='object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    CHECK (current_iteration <= (limits_json->>'maxIterations')::int),
    CHECK (logical_model_calls <= (limits_json->>'maxLogicalModelCalls')::int),
    CHECK (logical_tool_calls <= (limits_json->>'maxLogicalToolCalls')::int),
    CHECK (reserved_tokens <= (limits_json->>'maxTotalTokens')::bigint),
    CHECK (reserved_cost_micros <= (limits_json->>'maxEstimatedCostMicros')::bigint),
    CHECK ((status IN ('COMPLETED','LIMIT_REACHED','FAILED')) = (completed_at IS NOT NULL)),
    CHECK ((completed_at IS NOT NULL) = (termination_reason IS NOT NULL))
);
CREATE TABLE agent_iteration (
    id UUID PRIMARY KEY,
    agent_execution_id UUID NOT NULL REFERENCES agent_execution(id),
    iteration_number INTEGER NOT NULL CHECK (iteration_number BETWEEN 1 AND 20),
    model_workflow_run_id UUID NOT NULL UNIQUE REFERENCES workflow_run(id),
    model_activity_execution_id UUID NOT NULL UNIQUE REFERENCES activity_execution(id),
    status VARCHAR(30) NOT NULL CHECK (status IN ('MODEL_PENDING','DECIDED','TOOL_PENDING','COMPLETED','FAILED')),
    decision_type VARCHAR(20) CHECK (decision_type IN ('TOOL_CALL','FINAL')),
    decision_json JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(decision_json)='object'),
    tool_workflow_run_id UUID UNIQUE REFERENCES workflow_run(id),
    tool_request_id UUID UNIQUE REFERENCES tool_request(id),
    tool_result_json JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(tool_result_json)='object'),
    reserved_tokens BIGINT NOT NULL CHECK (reserved_tokens>0),
    reserved_cost_micros BIGINT NOT NULL CHECK (reserved_cost_micros>0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    UNIQUE(agent_execution_id,iteration_number),
    CHECK ((tool_workflow_run_id IS NULL) = (tool_request_id IS NULL)),
    CHECK ((status IN ('COMPLETED','FAILED')) = (completed_at IS NOT NULL))
);
CREATE UNIQUE INDEX one_active_agent_iteration ON agent_iteration(agent_execution_id)
    WHERE status IN ('MODEL_PENDING','DECIDED','TOOL_PENDING');
CREATE FUNCTION guard_agent_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.id,NEW.workflow_run_id,NEW.goal,NEW.scenario,NEW.limits_json,NEW.max_model_attempts,NEW.frozen_context_json,NEW.created_at)
       IS DISTINCT FROM ROW(OLD.id,OLD.workflow_run_id,OLD.goal,OLD.scenario,OLD.limits_json,OLD.max_model_attempts,OLD.frozen_context_json,OLD.created_at)
       OR OLD.completed_at IS NOT NULL THEN RAISE EXCEPTION 'AGENT_IMMUTABLE_EXECUTION'; END IF;
    IF NEW.current_iteration < OLD.current_iteration OR NEW.current_iteration > OLD.current_iteration+1
       OR NEW.logical_model_calls < OLD.logical_model_calls OR NEW.logical_tool_calls < OLD.logical_tool_calls
       OR NEW.model_attempts < OLD.model_attempts OR NEW.input_tokens < OLD.input_tokens OR NEW.output_tokens < OLD.output_tokens
       OR NEW.estimated_cost_micros < OLD.estimated_cost_micros OR NEW.reserved_tokens < OLD.reserved_tokens
       OR NEW.reserved_cost_micros < OLD.reserved_cost_micros THEN RAISE EXCEPTION 'AGENT_COUNTER_REGRESSION'; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER agent_execution_guard BEFORE UPDATE ON agent_execution FOR EACH ROW EXECUTE FUNCTION guard_agent_update();
CREATE FUNCTION guard_agent_iteration_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.id,NEW.agent_execution_id,NEW.iteration_number,NEW.model_workflow_run_id,NEW.model_activity_execution_id,NEW.reserved_tokens,NEW.reserved_cost_micros,NEW.created_at)
       IS DISTINCT FROM ROW(OLD.id,OLD.agent_execution_id,OLD.iteration_number,OLD.model_workflow_run_id,OLD.model_activity_execution_id,OLD.reserved_tokens,OLD.reserved_cost_micros,OLD.created_at)
       OR OLD.completed_at IS NOT NULL OR (OLD.decision_json <> '{}'::jsonb AND ROW(NEW.decision_json,NEW.decision_type) IS DISTINCT FROM ROW(OLD.decision_json,OLD.decision_type))
       OR (OLD.tool_request_id IS NOT NULL AND ROW(NEW.tool_request_id,NEW.tool_workflow_run_id) IS DISTINCT FROM ROW(OLD.tool_request_id,OLD.tool_workflow_run_id))
       THEN RAISE EXCEPTION 'AGENT_IMMUTABLE_ITERATION'; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER agent_iteration_guard BEFORE UPDATE ON agent_iteration FOR EACH ROW EXECUTE FUNCTION guard_agent_iteration_update();
