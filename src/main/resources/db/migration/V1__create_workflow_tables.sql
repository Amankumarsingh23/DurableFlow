CREATE TABLE workflow_definition (
    id UUID PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    version INTEGER NOT NULL,
    definition_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_definition_name_version UNIQUE (name, version),
    CONSTRAINT ck_definition_version CHECK (version > 0)
);

CREATE TABLE workflow_run (
    id UUID PRIMARY KEY,
    workflow_definition_id UUID NOT NULL,
    status VARCHAR(50) NOT NULL,
    input_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    CONSTRAINT fk_run_definition FOREIGN KEY (workflow_definition_id) REFERENCES workflow_definition(id),
    CONSTRAINT ck_run_status CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED'))
);

CREATE TABLE activity_execution (
    id UUID PRIMARY KEY,
    workflow_run_id UUID NOT NULL,
    activity_id VARCHAR(200) NOT NULL,
    status VARCHAR(50) NOT NULL,
    input_json JSONB,
    output_json JSONB,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    CONSTRAINT fk_activity_run FOREIGN KEY (workflow_run_id) REFERENCES workflow_run(id),
    CONSTRAINT uq_activity_run_id UNIQUE (workflow_run_id, activity_id),
    CONSTRAINT ck_activity_attempts CHECK (attempt_count >= 0),
    CONSTRAINT ck_activity_status CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED'))
);
