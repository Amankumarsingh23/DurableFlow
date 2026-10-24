ALTER TABLE activity_task
    ADD COLUMN lease_expires_at TIMESTAMPTZ,
    ADD COLUMN lease_generation BIGINT NOT NULL DEFAULT 0;

-- Stop Session 10 workers before upgrading. Historic claims become immediately reclaimable;
-- terminal rows keep their outcome, without inventing a historical lease deadline.
UPDATE activity_task SET lease_generation = 1 WHERE status <> 'READY';
UPDATE activity_task SET lease_expires_at = clock_timestamp() WHERE status = 'CLAIMED';

ALTER TABLE activity_task ADD CONSTRAINT ck_activity_task_lease CHECK (
    (status = 'READY' AND lease_expires_at IS NULL AND lease_generation = 0)
    OR (status = 'CLAIMED' AND lease_expires_at IS NOT NULL AND lease_generation > 0)
    OR (status IN ('COMPLETED', 'FAILED') AND lease_generation > 0)
);

-- Existing READY partial index remains useful; this index filters expired ownership.
CREATE INDEX ix_activity_task_expired_claim ON activity_task (lease_expires_at, created_at, id)
    WHERE status = 'CLAIMED';
