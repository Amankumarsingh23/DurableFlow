CREATE TABLE workflow_timer (
    id UUID PRIMARY KEY,
    workflow_run_id UUID NOT NULL REFERENCES workflow_run(id),
    timer_id VARCHAR(200) NOT NULL,
    status VARCHAR(30) NOT NULL CHECK (status IN ('WAITING', 'FIRED')),
    wake_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    fired_at TIMESTAMPTZ,
    CONSTRAINT uq_workflow_timer_node UNIQUE (workflow_run_id, timer_id),
    CONSTRAINT ck_workflow_timer_fired CHECK (
        (status = 'WAITING' AND fired_at IS NULL) OR (status = 'FIRED' AND fired_at IS NOT NULL)
    )
);
CREATE INDEX ix_workflow_timer_waiting ON workflow_timer (wake_at, id) WHERE status = 'WAITING';
