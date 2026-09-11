package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/usagestats"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

func (r *usageLogRepository) getAllGroupUsageSummaryFromRollups(ctx context.Context, todayStart time.Time) (results []usagestats.GroupUsageSummary, err error) {
	todayStart = service.GroupUsageTodayStart(todayStart)
	yesterdayStart := service.GroupUsageYesterdayStart(todayStart)
	timezoneName := service.GroupUsageTimezoneName()
	todayDate := service.GroupUsageDate(todayStart)
	yesterdayDate := service.GroupUsageDate(yesterdayStart)

	const query = `
		WITH state_values AS (
			SELECT
				COUNT(*) = 1
					AND MAX(timezone_name) = $3
					AND MAX(closed_before) <= $4::date AS valid,
				LEAST(MAX(closed_before), (SELECT (MIN(affected_at) AT TIME ZONE $3::text)::date
                        FROM usage_group_rollup_dirty)) AS closed_before,
				MAX(retained_from) AS retained_from
			FROM usage_group_rollup_state
			WHERE id = 1
		),
		state AS (
			SELECT
				CASE WHEN valid THEN closed_before ELSE DATE '1970-01-01' END AS closed_before,
				CASE WHEN valid THEN retained_from ELSE TIMESTAMPTZ '1970-01-01 00:00:00+00' END AS retained_from,
				CASE
					WHEN valid THEN closed_before::timestamp AT TIME ZONE $3::text
					ELSE TIMESTAMPTZ '1970-01-01 00:00:00+00'
				END AS tail_start,
				valid
			FROM state_values
		),
		historical AS (
			SELECT
				rollup.group_id,
				COALESCE(SUM(rollup.actual_cost), 0) AS actual_cost,
				COALESCE(SUM(rollup.actual_cost) FILTER (
					WHERE rollup.bucket_date = $5::date
				), 0) AS yesterday_cost
			FROM usage_group_daily_rollups rollup
			CROSS JOIN state
			WHERE state.valid
				AND rollup.bucket_date >= (state.retained_from AT TIME ZONE $3::text)::date
				AND rollup.bucket_date < state.closed_before
			GROUP BY rollup.group_id
		),
		tail AS (
			SELECT
				ul.group_id,
				COALESCE(SUM(ul.actual_cost), 0) AS actual_cost,
				COALESCE(SUM(ul.actual_cost) FILTER (WHERE ul.created_at >= $1), 0) AS today_cost,
				COALESCE(SUM(ul.actual_cost) FILTER (
					WHERE ul.created_at >= $2
						AND ul.created_at < $1
				), 0) AS yesterday_cost
			FROM usage_logs ul
			CROSS JOIN state
			WHERE ul.created_at >= state.tail_start
			GROUP BY ul.group_id
		)
		SELECT
			g.id AS group_id,
			COALESCE(historical.actual_cost, 0) + COALESCE(tail.actual_cost, 0) AS total_cost,
			COALESCE(tail.today_cost, 0) AS today_cost,
			COALESCE(historical.yesterday_cost, 0) + COALESCE(tail.yesterday_cost, 0) AS yesterday_cost
		FROM groups g
		LEFT JOIN historical ON historical.group_id = g.id
		LEFT JOIN tail ON tail.group_id = g.id
		ORDER BY g.id
	`

	rows, err := r.sql.QueryContext(
		ctx,
		query,
		todayStart,
		yesterdayStart,
		timezoneName,
		todayDate,
		yesterdayDate,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = closeErr
			results = nil
		}
	}()

	results = make([]usagestats.GroupUsageSummary, 0)
	for rows.Next() {
		var row usagestats.GroupUsageSummary
		if err := rows.Scan(&row.GroupID, &row.TotalCost, &row.TodayCost, &row.YesterdayCost); err != nil {
			return nil, err
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

const groupUsageRollupSyncMaxAttempts = 3

type groupUsageRollupRebuildPlan struct {
	publishedGeneration int64
	todayDate           string
	retainedDate        string
	rebuildStartDate    string
	timezoneName        string
	timezoneChanged     bool
	rebuildStart        time.Time
	todayStart          time.Time
	retainedFrom        time.Time
}

// SyncGroupUsageRollups 将服务端配置时区今日以前的用量发布为分组日桶。
// 历史聚合不持有 usage_group_rollup_state 行锁，避免与 usage_logs INSERT 触发器互堵。
func (r *dashboardAggregationRepository) SyncGroupUsageRollups(ctx context.Context, todayStart time.Time) error {
	if r == nil || r.sql == nil {
		return nil
	}
	todayStart = service.GroupUsageTodayStart(todayStart)
	for attempt := 0; attempt < groupUsageRollupSyncMaxAttempts; attempt++ {
		retry, err := r.syncGroupUsageRollupsAttempt(ctx, todayStart)
		if err != nil {
			return err
		}
		if !retry {
			return nil
		}
	}
	return fmt.Errorf("分组用量在重建期间持续变化，保留原始数据回退并等待重试")
}

// All callers use one database lock. Usage writers never acquire this lock.
// READ COMMITTED lets publication validate generations committed during rebuild.
func (r *dashboardAggregationRepository) syncGroupUsageRollupsAttempt(ctx context.Context, todayStart time.Time) (bool, error) {
	db, ok := r.sql.(interface {
		BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	})
	if !ok {
		return false, fmt.Errorf("分组用量重建需要独立数据库事务")
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	txRepo := newDashboardAggregationRepositoryWithSQL(tx)
	if err := txRepo.prepareGroupUsageRollupRebuild(ctx, todayStart); err != nil {
		return false, err
	}
	plan, err := txRepo.planGroupUsageRollupRebuild(ctx, todayStart, false)
	if err != nil {
		return false, err
	}
	if plan != nil {
		if err := txRepo.rebuildGroupUsageDailyRollups(ctx, plan); err != nil {
			return false, err
		}
		retry, err := txRepo.publishGroupUsageRollupWatermarkInTx(ctx, todayStart, plan)
		if err != nil || retry {
			return retry, err
		}
	}
	return false, tx.Commit()
}

func (r *dashboardAggregationRepository) prepareGroupUsageRollupRebuild(ctx context.Context, todayStart time.Time) error {
	if _, err := r.sql.ExecContext(ctx, `SELECT pg_advisory_xact_lock(6976, 239)`); err != nil {
		return err
	}
	// Exact visible identities, never MAX(id): a smaller ID may belong to an
	// INSERT that has started but has not committed. Capture BEFORE reading raw.
	_, err := r.sql.ExecContext(ctx, `
        CREATE TEMP TABLE group_usage_rebuild_dirty ON COMMIT DROP AS
        SELECT id FROM usage_group_rollup_dirty WHERE affected_at < $1
    `, todayStart.UTC())
	return err
}

func (r *dashboardAggregationRepository) planGroupUsageRollupRebuild(ctx context.Context, todayStart time.Time, forUpdate bool) (*groupUsageRollupRebuildPlan, error) {
	var closedBefore string
	var previousRetainedFrom time.Time
	var stateTimezoneName string
	var publishedGeneration int64
	query := `
		SELECT LEAST(closed_before, (
                SELECT (MIN(affected_at) AT TIME ZONE $1::text)::date
                FROM usage_group_rollup_dirty
            ))::text, retained_from, timezone_name, published_generation
		FROM usage_group_rollup_state
		WHERE id = 1`
	if forUpdate {
		query += `
		FOR UPDATE`
	}
	if err := scanSingleRow(ctx, r.sql, query, []any{service.GroupUsageTimezoneName()}, &closedBefore, &previousRetainedFrom, &stateTimezoneName, &publishedGeneration); err != nil {
		return nil, fmt.Errorf("读取分组用量汇总水位: %w", err)
	}

	todayDate := service.GroupUsageDate(todayStart)
	timezoneName := service.GroupUsageTimezoneName()
	timezoneChanged := stateTimezoneName != timezoneName
	var closedTime time.Time
	if !timezoneChanged {
		var err error
		closedTime, err = service.ParseGroupUsageDate(closedBefore)
		if err != nil {
			return nil, fmt.Errorf("解析分组用量汇总水位 %q: %w", closedBefore, err)
		}
		todayDateTime, err := service.ParseGroupUsageDate(todayDate)
		if err != nil {
			return nil, err
		}
		if closedTime.After(todayDateTime) {
			return nil, fmt.Errorf("分组用量汇总水位位于未来: %s", closedBefore)
		}
		if closedBefore == todayDate {
			return nil, nil
		}
	}

	var earliest sql.NullTime
	if err := scanSingleRow(ctx, r.sql, "SELECT MIN(created_at) FROM usage_logs", nil, &earliest); err != nil {
		return nil, fmt.Errorf("读取最早用量记录: %w", err)
	}
	retainedFrom := todayStart
	if earliest.Valid {
		retainedFrom = earliest.Time.UTC()
	}
	retainedDate := service.GroupUsageDate(retainedFrom)
	retainedDateTime, err := service.ParseGroupUsageDate(retainedDate)
	if err != nil {
		return nil, err
	}
	rebuildStartDate := retainedDate
	if !timezoneChanged && closedTime.After(retainedDateTime) {
		rebuildStartDate = closedBefore
	}
	rebuildStart, err := service.ParseGroupUsageDate(rebuildStartDate)
	if err != nil {
		return nil, err
	}
	return &groupUsageRollupRebuildPlan{
		publishedGeneration: publishedGeneration,
		todayDate:           todayDate,
		retainedDate:        retainedDate,
		rebuildStartDate:    rebuildStartDate,
		timezoneName:        timezoneName,
		timezoneChanged:     timezoneChanged,
		rebuildStart:        rebuildStart,
		todayStart:          todayStart,
		retainedFrom:        retainedFrom,
	}, nil
}

// Private shadow version; no reader-visible bucket is touched while aggregating.
func (r *dashboardAggregationRepository) rebuildGroupUsageDailyRollups(ctx context.Context, plan *groupUsageRollupRebuildPlan) error {
	_, err := r.sql.ExecContext(ctx, `
        CREATE TEMP TABLE group_usage_rebuild_buckets ON COMMIT DROP AS
        SELECT (created_at AT TIME ZONE $3::text)::date AS bucket_date,
            group_id, COALESCE(SUM(actual_cost), 0) AS actual_cost, NOW() AS computed_at
        FROM usage_logs
        WHERE group_id IS NOT NULL AND created_at >= $1 AND created_at < $2
        GROUP BY 1, 2
    `, plan.rebuildStart.UTC(), plan.todayStart.UTC(), plan.timezoneName)
	return err
}

func (r *dashboardAggregationRepository) publishGroupUsageRollupWatermarkInTx(ctx context.Context, todayStart time.Time, plan *groupUsageRollupRebuildPlan) (bool, error) {
	current, err := r.planGroupUsageRollupRebuild(ctx, todayStart, true)
	if err != nil {
		return false, err
	}
	if current == nil || current.publishedGeneration != plan.publishedGeneration || (!plan.timezoneChanged && current.rebuildStart.Before(plan.rebuildStart)) {
		return true, nil
	}
	var changed bool
	if err := scanSingleRow(ctx, r.sql, `
        SELECT EXISTS (
            SELECT 1 FROM usage_group_rollup_dirty d WHERE affected_at < $1
            AND NOT EXISTS (SELECT 1 FROM group_usage_rebuild_dirty v WHERE v.id = d.id)
        )`, []any{plan.todayStart.UTC()}, &changed); err != nil {
		return false, err
	}
	if changed {
		return true, nil
	}
	// A writer committing after validation retains its uncaptured marker.
	// Readers see the marker and usage mutation in one snapshot and use raw.
	if plan.timezoneChanged {
		if _, err := r.sql.ExecContext(ctx, `DELETE FROM usage_group_daily_rollups`); err != nil {
			return false, err
		}
	} else if _, err := r.sql.ExecContext(ctx, `DELETE FROM usage_group_daily_rollups WHERE bucket_date >= $1::date`, plan.rebuildStartDate); err != nil {
		return false, err
	}
	if _, err := r.sql.ExecContext(ctx, `
        INSERT INTO usage_group_daily_rollups (bucket_date, group_id, actual_cost, computed_at)
        SELECT bucket_date, group_id, actual_cost, computed_at FROM group_usage_rebuild_buckets
    `); err != nil {
		return false, err
	}
	if _, err := r.sql.ExecContext(ctx, `DELETE FROM usage_group_daily_rollups WHERE bucket_date < $1::date OR bucket_date >= $2::date`, plan.retainedDate, plan.todayDate); err != nil {
		return false, err
	}
	if _, err := r.sql.ExecContext(ctx, `DELETE FROM usage_group_rollup_dirty d USING group_usage_rebuild_dirty v WHERE d.id = v.id`); err != nil {
		return false, err
	}
	_, err = r.sql.ExecContext(ctx, `
        UPDATE usage_group_rollup_state
        SET closed_before = $1::date, retained_from = $2, timezone_name = $3,
            published_generation = published_generation + 1, updated_at = NOW()
        WHERE id = 1
    `, plan.todayDate, plan.retainedFrom, plan.timezoneName)
	return false, err
}

func lockGroupUsageRollupState(ctx context.Context, tx *sql.Tx) error {
	var id int16
	if err := tx.QueryRowContext(ctx, `
		SELECT id
		FROM usage_group_rollup_state
		WHERE id = 1
		FOR UPDATE
	`).Scan(&id); err != nil {
		return fmt.Errorf("锁定分组用量汇总水位: %w", err)
	}
	return nil
}

func invalidateGroupUsageRollupsAt(ctx context.Context, tx *sql.Tx, affectedAt time.Time) error {
	timezoneName := service.GroupUsageTimezoneName()
	_, err := tx.ExecContext(ctx, `
		WITH dirty AS (
            INSERT INTO usage_group_rollup_dirty (affected_at) VALUES ($1::timestamptz)
            ON CONFLICT (transaction_id, bucket_date) DO UPDATE
            SET affected_at = LEAST(usage_group_rollup_dirty.affected_at, EXCLUDED.affected_at)
        )
        UPDATE usage_group_rollup_state
		SET closed_before = LEAST(
			closed_before,
			($1::timestamptz AT TIME ZONE $2::text)::date
		),
			updated_at = NOW()
		WHERE id = 1
	`, affectedAt.UTC(), timezoneName)
	return err
}
