CREATE TABLE workflow_signal_wait (
    id UUID PRIMARY KEY,
    workflow_run_id UUID NOT NULL REFERENCES workflow_run(id),
    signal_node_id VARCHAR(200) NOT NULL,
    status VARCHAR(30) NOT NULL CHECK (status IN ('WAITING', 'RECEIVED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    received_at TIMESTAMPTZ,
    payload_json JSONB,
    CONSTRAINT uq_signal_wait_node UNIQUE (workflow_run_id, signal_node_id),
    CHECK ((status='WAITING' AND received_at IS NULL AND payload_json IS NULL)
        OR (status='RECEIVED' AND received_at IS NOT NULL AND payload_json IS NOT NULL AND jsonb_typeof(payload_json)='object'))
);

CREATE TABLE workflow_signal_delivery (
    id UUID PRIMARY KEY,
    workflow_run_id UUID NOT NULL REFERENCES workflow_run(id),
    signal_id VARCHAR(200) NOT NULL CHECK (length(trim(signal_id)) > 0),
    signal_node_id VARCHAR(200) NOT NULL,
    payload_json JSONB NOT NULL CHECK (jsonb_typeof(payload_json)='object'),
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    consumed_at TIMESTAMPTZ,
    consumed_wait_id UUID REFERENCES workflow_signal_wait(id),
    CONSTRAINT uq_signal_delivery_id UNIQUE (workflow_run_id, signal_id),
    -- V1 SIGNAL nodes are single-use: a second delivery identity cannot replace the first.
    CONSTRAINT uq_signal_delivery_node UNIQUE (workflow_run_id, signal_node_id),
    CHECK ((consumed_at IS NULL AND consumed_wait_id IS NULL)
        OR (consumed_at IS NOT NULL AND consumed_wait_id IS NOT NULL))
);
-- Unique indexes cover run inspection, exact delivery lookup and node matching; no polling index needed.
