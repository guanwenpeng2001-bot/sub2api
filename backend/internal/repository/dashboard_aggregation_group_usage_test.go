//go:build unit

package repository

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestGroupUsageRollupAtomicPublish(t *testing.T) {
	setGroupUsageRollupTestTimezone(t)
	for _, failure := range []string{"", "stage", "insert", "watermark", "generation", "advanced"} {
		t.Run(failure, func(t *testing.T) {
			db, mock := newSQLMock(t)
			repo := newDashboardAggregationRepositoryWithSQL(db)
			today := time.Date(2026, 8, 13, 16, 0, 0, 0, time.UTC)
			earliest := today.Add(-48 * time.Hour)
			expectGroupUsageRollupWatermarkRead(mock, false).WillReturnRows(rollupStateRows("2026-08-13", "Asia/Shanghai"))
			mock.ExpectQuery(`SELECT MIN\(created_at\)`).WillReturnRows(sqlmock.NewRows([]string{"min"}).AddRow(earliest))
			stage := mock.ExpectExec(`CREATE TEMP TABLE group_usage_rebuild_buckets`).WithArgs(today.Add(-24*time.Hour), today, "Asia/Shanghai")
			if failure == "stage" {
				stage.WillReturnError(sql.ErrConnDone)
			} else {
				stage.WillReturnResult(sqlmock.NewResult(0, 1))
				rows := rollupStateRows("2026-08-13", "Asia/Shanghai")
				if failure == "advanced" {
					rows = sqlmock.NewRows([]string{"closed_before", "retained_from", "timezone_name", "published_generation"}).AddRow("2026-08-13", earliest, "Asia/Shanghai", 1)
				}
				expectGroupUsageRollupWatermarkRead(mock, true).WillReturnRows(rows)
				mock.ExpectQuery(`SELECT MIN\(created_at\)`).WillReturnRows(sqlmock.NewRows([]string{"min"}).AddRow(earliest))
				if failure != "advanced" {
					mock.ExpectQuery(`SELECT EXISTS`).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(failure == "generation"))
					if failure != "generation" {
						mock.ExpectExec(`DELETE FROM usage_group_daily_rollups WHERE bucket_date >=`).WithArgs("2026-08-13").WillReturnResult(sqlmock.NewResult(0, 1))
						insert := mock.ExpectExec(`INSERT INTO usage_group_daily_rollups`)
						if failure == "insert" {
							insert.WillReturnError(sql.ErrConnDone)
						} else {
							insert.WillReturnResult(sqlmock.NewResult(0, 1))
							mock.ExpectExec(`DELETE FROM usage_group_daily_rollups WHERE bucket_date <`).WillReturnResult(sqlmock.NewResult(0, 0))
							mock.ExpectExec(`DELETE FROM usage_group_rollup_dirty`).WillReturnResult(sqlmock.NewResult(0, 1))
							publish := mock.ExpectExec(`UPDATE usage_group_rollup_state`).WithArgs("2026-08-14", earliest, "Asia/Shanghai")
							if failure == "watermark" {
								publish.WillReturnError(sql.ErrConnDone)
							} else {
								publish.WillReturnResult(sqlmock.NewResult(0, 1))
							}
						}
					}
				}
			}
			if failure == "" {
				mock.ExpectCommit()
			} else {
				mock.ExpectRollback()
			}
			retry, err := repo.syncGroupUsageRollupsAttempt(context.Background(), today)
			if failure == "generation" || failure == "advanced" {
				require.NoError(t, err)
				require.True(t, retry)
			} else if failure != "" {
				require.ErrorIs(t, err, sql.ErrConnDone)
			} else {
				require.NoError(t, err)
				require.False(t, retry)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestGroupUsageRollupPlanNoopAndFuture(t *testing.T) {
	setGroupUsageRollupTestTimezone(t)
	for _, date := range []string{"2026-08-14", "2026-08-15"} {
		t.Run(date, func(t *testing.T) {
			db, mock := newSQLMock(t)
			expectGroupUsageRollupWatermarkRead(mock, false).WillReturnRows(rollupStateRows(date, "Asia/Shanghai"))
			if date == "2026-08-14" {
				mock.ExpectCommit()
			} else {
				mock.ExpectRollback()
			}
			err := newDashboardAggregationRepositoryWithSQL(db).SyncGroupUsageRollups(context.Background(), time.Date(2026, 8, 13, 16, 0, 0, 0, time.UTC))
			if date == "2026-08-14" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "未来")
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestGroupUsageRollupTimezoneRebuild(t *testing.T) {
	useGroupUsageRepositoryTestTimezone(t, "America/New_York")
	db, mock := newSQLMock(t)
	today := time.Date(2026, 3, 9, 4, 0, 0, 0, time.UTC)
	earliest := time.Date(2026, 3, 1, 5, 0, 0, 0, time.UTC)
	expectGroupUsageRollupWatermarkRead(mock, false).WillReturnRows(rollupStateRows("2026-03-09", "Asia/Shanghai"))
	mock.ExpectQuery(`SELECT MIN\(created_at\)`).WillReturnRows(sqlmock.NewRows([]string{"min"}).AddRow(earliest))
	mock.ExpectExec(`CREATE TEMP TABLE group_usage_rebuild_buckets`).WithArgs(earliest, today, "America/New_York").WillReturnResult(sqlmock.NewResult(0, 1))
	expectGroupUsageRollupWatermarkRead(mock, true).WillReturnRows(rollupStateRows("2026-03-09", "Asia/Shanghai"))
	mock.ExpectQuery(`SELECT MIN\(created_at\)`).WillReturnRows(sqlmock.NewRows([]string{"min"}).AddRow(earliest))
	mock.ExpectQuery(`SELECT EXISTS`).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectExec(`DELETE FROM usage_group_daily_rollups$`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO usage_group_daily_rollups`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM usage_group_daily_rollups WHERE bucket_date <`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM usage_group_rollup_dirty`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE usage_group_rollup_state`).WithArgs("2026-03-09", earliest, "America/New_York").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	require.NoError(t, newDashboardAggregationRepositoryWithSQL(db).SyncGroupUsageRollups(context.Background(), today))
	require.NoError(t, mock.ExpectationsWereMet())
}

func rollupStateRows(date, zone string) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"closed_before", "retained_from", "timezone_name", "published_generation"}).AddRow(date, time.Unix(0, 0).UTC(), zone, 0)
}

func setGroupUsageRollupTestTimezone(t *testing.T) {
	t.Helper()
	useGroupUsageRepositoryTestTimezone(t, "Asia/Shanghai")
}

func expectGroupUsageRollupWatermarkRead(mock sqlmock.Sqlmock, forUpdate bool) *sqlmock.ExpectedQuery {
	if forUpdate {
		return mock.ExpectQuery(`(?s)SELECT LEAST\(closed_before,.*FOR UPDATE`)
	}
	mock.ExpectBegin()
	mock.ExpectExec(`SELECT pg_advisory_xact_lock`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`CREATE TEMP TABLE group_usage_rebuild_dirty`).WillReturnResult(sqlmock.NewResult(0, 1))
	return mock.ExpectQuery(`(?s)SELECT LEAST\(closed_before,.*WHERE id = 1$`)
}

func TestDashboardAggregationRepositoryRecomputeRangeInvalidatesGroupRollupsBeforeDashboardRebuild(t *testing.T) {
	setGroupUsageRollupTestTimezone(t)
	db, mock := newSQLMock(t)
	repo := newDashboardAggregationRepositoryWithSQL(db)
	start := time.Date(2026, 8, 1, 3, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM usage_group_rollup_state.*FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectExec(`(?s)WITH dirty AS.*UPDATE usage_group_rollup_state`).
		WithArgs(start, "Asia/Shanghai").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(`DELETE FROM usage_dashboard_hourly`).
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	err := repo.RecomputeRange(context.Background(), start, end)
	require.ErrorIs(t, err, sql.ErrConnDone)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDashboardAggregationRepositoryCleanupUsageLogsNonPartitionedInvalidatesEachBatchAndSyncs(t *testing.T) {
	setGroupUsageRollupTestTimezone(t)
	db, mock := newSQLMock(t)
	repo := newDashboardAggregationRepositoryWithSQL(db)
	cutoff := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	earliestDeletedAt := time.Date(2026, 5, 3, 2, 0, 0, 0, time.UTC)
	fixedNow := time.Date(2026, 8, 14, 8, 0, 0, 0, time.UTC)
	repo.clock = func() time.Time { return fixedNow }
	todayStart := service.GroupUsageTodayStart(fixedNow)

	mock.ExpectQuery(`SELECT EXISTS`).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM usage_group_rollup_state.*FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery(`(?s)DELETE FROM usage_logs.*RETURNING created_at`).
		WithArgs(cutoff, usageLogsCleanupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}).
			AddRow(earliestDeletedAt.Add(time.Hour)).
			AddRow(earliestDeletedAt))
	mock.ExpectExec(`(?s)WITH dirty AS.*UPDATE usage_group_rollup_state`).
		WithArgs(earliestDeletedAt, "Asia/Shanghai").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectGroupUsageRollupWatermarkRead(mock, false).
		WillReturnRows(sqlmock.NewRows([]string{"closed_before", "retained_from", "timezone_name", "published_generation"}).
			AddRow(service.GroupUsageDate(todayStart), time.Unix(0, 0).UTC(), "Asia/Shanghai", 0))
	mock.ExpectCommit()

	require.NoError(t, repo.CleanupUsageLogs(context.Background(), cutoff))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDashboardAggregationRepositoryCleanupUsageLogsPartitionedSortsAndInvalidatesEachDropBeforeSync(t *testing.T) {
	setGroupUsageRollupTestTimezone(t)
	db, mock := newSQLMock(t)
	repo := newDashboardAggregationRepositoryWithSQL(db)
	cutoff := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	aprilStart := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	juneStart := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	fixedNow := time.Date(2026, 8, 14, 8, 0, 0, 0, time.UTC)
	repo.clock = func() time.Time { return fixedNow }
	todayStart := service.GroupUsageTodayStart(fixedNow)

	mock.ExpectQuery(`SELECT EXISTS`).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT c.relname`).
		WillReturnRows(sqlmock.NewRows([]string{"relname"}).
			AddRow("usage_logs_202606").
			AddRow("usage_logs_invalid").
			AddRow("usage_logs_202604").
			AddRow("usage_logs_202607"))

	for _, partition := range []struct {
		name  string
		start time.Time
	}{
		{name: "usage_logs_202604", start: aprilStart},
		{name: "usage_logs_202606", start: juneStart},
	} {
		mock.ExpectBegin()
		mock.ExpectQuery(`SELECT id FROM usage_group_rollup_state.*FOR UPDATE`).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
		mock.ExpectExec(`(?s)WITH dirty AS.*UPDATE usage_group_rollup_state`).
			WithArgs(partition.start, "Asia/Shanghai").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(`DROP TABLE IF EXISTS "` + partition.name + `"`).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectCommit()
	}
	expectGroupUsageRollupWatermarkRead(mock, false).
		WillReturnRows(sqlmock.NewRows([]string{"closed_before", "retained_from", "timezone_name", "published_generation"}).
			AddRow(service.GroupUsageDate(todayStart), time.Unix(0, 0).UTC(), "Asia/Shanghai", 0))
	mock.ExpectCommit()

	require.NoError(t, repo.CleanupUsageLogs(context.Background(), cutoff))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDashboardAggregationRepositoryCleanupUsageLogsNonPartitionedFailureRollsBackWithoutSync(t *testing.T) {
	setGroupUsageRollupTestTimezone(t)
	db, mock := newSQLMock(t)
	repo := newDashboardAggregationRepositoryWithSQL(db)
	cutoff := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	deletedAt := time.Date(2026, 5, 3, 2, 0, 0, 0, time.UTC)

	mock.ExpectQuery(`SELECT EXISTS`).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM usage_group_rollup_state.*FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery(`(?s)SELECT ctid.*ORDER BY created_at ASC, id ASC.*DELETE FROM usage_logs.*RETURNING created_at`).
		WithArgs(cutoff, usageLogsCleanupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}).AddRow(deletedAt))
	mock.ExpectExec(`(?s)WITH dirty AS.*UPDATE usage_group_rollup_state`).
		WithArgs(deletedAt, "Asia/Shanghai").
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	err := repo.CleanupUsageLogs(context.Background(), cutoff)
	require.ErrorIs(t, err, sql.ErrConnDone)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDashboardAggregationRepositoryCleanupUsageLogsPartitionFailureRollsBackAndStops(t *testing.T) {
	setGroupUsageRollupTestTimezone(t)
	db, mock := newSQLMock(t)
	repo := newDashboardAggregationRepositoryWithSQL(db)
	cutoff := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	aprilStart := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	dropErr := errors.New("drop partition failed")

	mock.ExpectQuery(`SELECT EXISTS`).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT c.relname`).
		WillReturnRows(sqlmock.NewRows([]string{"relname"}).
			AddRow("usage_logs_202606").
			AddRow("usage_logs_202604"))
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM usage_group_rollup_state.*FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectExec(`(?s)WITH dirty AS.*UPDATE usage_group_rollup_state`).
		WithArgs(aprilStart, "Asia/Shanghai").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DROP TABLE IF EXISTS "usage_logs_202604"`).
		WillReturnError(dropErr)
	mock.ExpectRollback()

	err := repo.CleanupUsageLogs(context.Background(), cutoff)
	require.ErrorIs(t, err, dropErr)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDashboardAggregationRepositoryRecomputeRangeSyncsAfterDashboardCommit(t *testing.T) {
	setGroupUsageRollupTestTimezone(t)
	db, mock := newSQLMock(t)
	repo := newDashboardAggregationRepositoryWithSQL(db)
	start := time.Date(2026, 8, 1, 3, 0, 0, 0, time.UTC)
	repo.clock = func() time.Time { return time.Date(2026, 8, 14, 8, 0, 0, 0, time.UTC) }
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM usage_group_rollup_state.*FOR UPDATE`).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectExec(`(?s)WITH dirty AS.*UPDATE usage_group_rollup_state`).WithArgs(start, "Asia/Shanghai").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	for _, query := range []string{
		`DELETE FROM usage_dashboard_hourly WHERE`,
		`DELETE FROM usage_dashboard_hourly_users WHERE`,
		`DELETE FROM usage_dashboard_daily WHERE`,
		`DELETE FROM usage_dashboard_daily_users WHERE`,
		`INSERT INTO usage_dashboard_hourly_users`,
		`INSERT INTO usage_dashboard_daily_users`,
		`INSERT INTO usage_dashboard_hourly`,
		`INSERT INTO usage_dashboard_daily`,
	} {
		mock.ExpectExec(query).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectCommit()
	// The rollup starts a fresh transaction with the SAME advisory lock used by
	// startup / periodic sync. Another leader may already have rebuilt the date.
	expectGroupUsageRollupWatermarkRead(mock, false).WillReturnRows(rollupStateRows("2026-08-14", "Asia/Shanghai"))
	mock.ExpectCommit()
	require.NoError(t, repo.RecomputeRange(context.Background(), start, start.Add(time.Hour)))
	require.NoError(t, mock.ExpectationsWereMet())
}
