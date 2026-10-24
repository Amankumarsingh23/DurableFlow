CREATE TABLE activity_task (
    id UUID PRIMARY KEY,
    activity_execution_id UUID NOT NULL REFERENCES activity_execution(id),
    status VARCHAR(30) NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    claimed_at TIMESTAMPTZ,
    worker_id VARCHAR(200),
    CONSTRAINT uq_activity_task_execution UNIQUE (activity_execution_id),
    CONSTRAINT ck_activity_task_status CHECK (status IN ('READY', 'CLAIMED', 'COMPLETED', 'FAILED')),
    CONSTRAINT ck_activity_task_owner CHECK (
        (status = 'READY' AND claimed_at IS NULL AND worker_id IS NULL)
        OR (status <> 'READY' AND claimed_at IS NOT NULL AND worker_id IS NOT NULL AND length(trim(worker_id)) > 0)
    )
);

-- Matches the READY/available_at predicate and the complete deterministic claim order.
CREATE INDEX ix_activity_task_ready_order ON activity_task (available_at, created_at, id)
    WHERE status = 'READY';
