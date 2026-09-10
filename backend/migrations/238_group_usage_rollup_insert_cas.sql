-- 热路径 usage_logs INSERT 不再对 usage_group_rollup_state 取 KEY SHARE。
-- 历史日桶重建若长时间锁同一行，KEY SHARE 会把实时用量写入堵住（#6976）。
-- 迟到回退改为无锁 CAS：只在水位已经越过受影响日期时才 UPDATE。
-- 关闭作业把聚合移出水位锁之后，今日写入的 CAS 不匹配、不会排队。

CREATE OR REPLACE FUNCTION invalidate_group_usage_rollup_state()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    affected_date DATE;
    configured_timezone TEXT := current_setting('TimeZone');
BEGIN
    IF TG_OP = 'DELETE' THEN
        affected_date := (OLD.created_at AT TIME ZONE configured_timezone)::date;
    ELSE
        IF OLD.group_id IS NULL THEN
            affected_date := (NEW.created_at AT TIME ZONE configured_timezone)::date;
        ELSIF NEW.group_id IS NULL THEN
            affected_date := (OLD.created_at AT TIME ZONE configured_timezone)::date;
        ELSE
            affected_date := LEAST(
                (OLD.created_at AT TIME ZONE configured_timezone)::date,
                (NEW.created_at AT TIME ZONE configured_timezone)::date
            );
        END IF;
    END IF;

    UPDATE usage_group_rollup_state
    SET closed_before = LEAST(closed_before, affected_date),
        updated_at = NOW()
    WHERE id = 1
      AND closed_before > affected_date;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION invalidate_group_usage_rollup_state_after_insert()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    affected_date DATE;
    configured_timezone TEXT := current_setting('TimeZone');
BEGIN
    SELECT MIN((created_at AT TIME ZONE configured_timezone)::date)
    INTO affected_date
    FROM inserted_usage_logs
    WHERE group_id IS NOT NULL;

    IF affected_date IS NULL THEN
        RETURN NULL;
    END IF;

    UPDATE usage_group_rollup_state
    SET closed_before = LEAST(closed_before, affected_date),
        updated_at = NOW()
    WHERE id = 1
      AND closed_before > affected_date;

    RETURN NULL;
END;
$$;
