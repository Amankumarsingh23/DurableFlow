ALTER TABLE workflow_run ADD COLUMN history_sequence BIGINT NOT NULL DEFAULT 0
    CHECK (history_sequence >= 0);

CREATE TABLE workflow_event (
    id UUID PRIMARY KEY,
    workflow_run_id UUID NOT NULL REFERENCES workflow_run(id),
    activity_execution_id UUID REFERENCES activity_execution(id),
    sequence_number BIGINT NOT NULL CHECK (sequence_number > 0),
    event_type VARCHAR(60) NOT NULL,
    event_data JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(event_data) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_workflow_event_sequence UNIQUE (workflow_run_id, sequence_number)
);
-- The unique index also serves GET history WHERE workflow_run_id = ? ORDER BY sequence_number.
-- No synthetic backfill: existing runs begin their observed history after this migration.
