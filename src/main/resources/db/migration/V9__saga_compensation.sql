ALTER TABLE workflow_run DROP CONSTRAINT ck_run_status;
ALTER TABLE workflow_run ADD CONSTRAINT ck_run_status CHECK
    (status IN ('PENDING','RUNNING','COMPLETED','FAILED','CANCELLED','COMPENSATING','COMPENSATED','COMPENSATION_FAILED'));

CREATE TABLE compensation_execution (
    id UUID PRIMARY KEY,
    workflow_run_id UUID NOT NULL REFERENCES workflow_run(id),
    original_activity_execution_id UUID NOT NULL UNIQUE REFERENCES activity_execution(id),
    handler VARCHAR(200) NOT NULL CHECK (length(trim(handler)) > 0),
    status VARCHAR(50) NOT NULL CHECK (status IN ('PENDING','RUNNING','COMPLETED','FAILED')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    input_json JSONB NOT NULL,
    output_json JSONB,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);
CREATE INDEX ix_compensation_run ON compensation_execution(workflow_run_id, created_at);
-- At most one logical undo is outstanding for a run, even across delayed retries.
CREATE UNIQUE INDEX uq_compensation_active_run ON compensation_execution(workflow_run_id)
    WHERE status IN ('PENDING', 'RUNNING');

ALTER TABLE activity_task ALTER COLUMN activity_execution_id DROP NOT NULL;
ALTER TABLE activity_task ADD COLUMN task_kind VARCHAR(30) NOT NULL DEFAULT 'FORWARD';
ALTER TABLE activity_task ADD COLUMN compensation_execution_id UUID UNIQUE REFERENCES compensation_execution(id);
ALTER TABLE activity_task ADD CONSTRAINT ck_task_execution_kind CHECK (
    (task_kind = 'FORWARD' AND activity_execution_id IS NOT NULL AND compensation_execution_id IS NULL)
    OR (task_kind = 'COMPENSATION' AND activity_execution_id IS NULL AND compensation_execution_id IS NOT NULL)
);
