ALTER TABLE workflow_run ADD COLUMN child_depth INTEGER NOT NULL DEFAULT 0 CHECK (child_depth >= 0);
CREATE TABLE child_workflow_execution (
    id UUID PRIMARY KEY,
    parent_workflow_run_id UUID NOT NULL REFERENCES workflow_run(id),
    child_node_id VARCHAR(200) NOT NULL CHECK (length(trim(child_node_id)) > 0),
    child_workflow_run_id UUID NOT NULL UNIQUE REFERENCES workflow_run(id),
    child_workflow_definition_id UUID NOT NULL REFERENCES workflow_definition(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    terminal_status VARCHAR(50),
    completed_at TIMESTAMPTZ,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
    UNIQUE(parent_workflow_run_id, child_node_id),
    CHECK (parent_workflow_run_id <> child_workflow_run_id),
    CHECK ((terminal_status IS NULL AND completed_at IS NULL) OR
        (terminal_status IS NOT NULL AND terminal_status IN ('COMPLETED','FAILED','COMPENSATED','COMPENSATION_FAILED','CANCELLED') AND completed_at IS NOT NULL))
);
CREATE INDEX ix_child_reconciliation ON child_workflow_execution(checked_at, id);
CREATE TABLE workflow_schedule (
    id UUID PRIMARY KEY,
    name VARCHAR(200) NOT NULL UNIQUE CHECK (length(trim(name)) > 0),
    workflow_definition_id UUID NOT NULL REFERENCES workflow_definition(id),
    input_json JSONB NOT NULL CHECK (jsonb_typeof(input_json) = 'object'),
    schedule_type VARCHAR(30) NOT NULL CHECK (schedule_type IN ('START_AT','FIXED_INTERVAL')),
    next_fire_at TIMESTAMPTZ NOT NULL,
    interval_millis BIGINT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((schedule_type = 'START_AT' AND interval_millis IS NULL) OR
        (schedule_type = 'FIXED_INTERVAL' AND interval_millis IS NOT NULL AND interval_millis > 0 AND interval_millis <= 3155760000000))
);
CREATE INDEX ix_schedule_due ON workflow_schedule(next_fire_at,id) WHERE enabled;
CREATE TABLE workflow_schedule_occurrence (
    id UUID PRIMARY KEY,
    schedule_id UUID NOT NULL REFERENCES workflow_schedule(id),
    scheduled_for TIMESTAMPTZ NOT NULL,
    workflow_run_id UUID NOT NULL UNIQUE REFERENCES workflow_run(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(schedule_id, scheduled_for)
);
