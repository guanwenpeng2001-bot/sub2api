-- Append transaction-local generations, independent of the published watermark.
-- Coalesce only within the SAME source transaction / UTC bucket. Committed marker
-- identities are immutable; sequence order is deliberately not a commit order.
ALTER TABLE usage_group_rollup_state
ADD COLUMN published_generation BIGINT NOT NULL DEFAULT 0;

CREATE TABLE usage_group_rollup_dirty (
    id BIGSERIAL PRIMARY KEY,
    transaction_id BIGINT NOT NULL DEFAULT txid_current(),
    bucket_date DATE GENERATED ALWAYS AS ((affected_at AT TIME ZONE 'UTC')::date) STORED,
    affected_at TIMESTAMPTZ NOT NULL,
    UNIQUE (transaction_id, bucket_date)
);
CREATE INDEX usage_group_rollup_dirty_affected_at_idx ON usage_group_rollup_dirty (affected_at);

CREATE OR REPLACE FUNCTION invalidate_group_usage_rollup_state()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        IF OLD.group_id IS NOT NULL THEN
            INSERT INTO usage_group_rollup_dirty (affected_at) VALUES (OLD.created_at)
            ON CONFLICT (transaction_id, bucket_date) DO UPDATE
            SET affected_at = LEAST(usage_group_rollup_dirty.affected_at, EXCLUDED.affected_at);
        END IF;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        IF NEW.group_id IS NOT NULL THEN
            INSERT INTO usage_group_rollup_dirty (affected_at) VALUES (NEW.created_at)
            ON CONFLICT (transaction_id, bucket_date) DO UPDATE
            SET affected_at = LEAST(usage_group_rollup_dirty.affected_at, EXCLUDED.affected_at);
        END IF;
        RETURN NEW;
    END IF;
    RETURN OLD;
END;
$$;

-- Row triggers are cloned to partitions and also cover direct partition INSERTs.
-- They take neither the rebuild advisory lock nor the singleton state row lock.
DROP TRIGGER IF EXISTS usage_logs_group_rollup_invalidate_insert ON usage_logs;
CREATE TRIGGER usage_logs_group_rollup_invalidate_insert
AFTER INSERT ON usage_logs FOR EACH ROW
WHEN (NEW.group_id IS NOT NULL)
EXECUTE FUNCTION invalidate_group_usage_rollup_state();
DROP FUNCTION invalidate_group_usage_rollup_state_after_insert();

-- Existing UPDATE/DELETE triggers already call the replaced row function.
-- Repair potentially incomplete buckets published by the old protocol.
UPDATE usage_group_rollup_state
SET closed_before = DATE '1970-01-01', updated_at = NOW()
WHERE id = 1;
