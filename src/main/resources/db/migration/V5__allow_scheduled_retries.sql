-- READY can now represent a scheduled retry with an existing ownership history.
ALTER TABLE activity_task DROP CONSTRAINT ck_activity_task_lease;
ALTER TABLE activity_task ADD CONSTRAINT ck_activity_task_lease CHECK (
    (status = 'READY' AND lease_expires_at IS NULL AND lease_generation >= 0)
    OR (status = 'CLAIMED' AND lease_expires_at IS NOT NULL AND lease_generation > 0)
    OR (status IN ('COMPLETED', 'FAILED') AND lease_generation > 0)
);
-- V3's owner constraint already requires READY owner/claimed_at to be cleared.
-- Existing READY and CLAIMED partial indexes also cover retry scheduling/reclamation.
