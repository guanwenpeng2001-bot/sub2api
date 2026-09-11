//go:build integration

package repository

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestGroupUsageRollupTriggerInvalidatesCascadedHistoricalDelete(t *testing.T) {
	useGroupUsageRepositoryTestTimezone(t, "Asia/Shanghai")
	for _, partitioned := range []bool{false, true} {
		t.Run(fmt.Sprintf("partitioned=%t", partitioned), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			schema := createGroupUsageRollupTriggerTestSchema(t, ctx, partitioned)
			reader := connectGroupUsageRollupTestSchema(t, ctx, schema)
			today := time.Date(2020, 1, 2, 16, 0, 0, 0, time.UTC)
			_, err := reader.ExecContext(ctx, `
				INSERT INTO groups VALUES (10);
				INSERT INTO users VALUES (1);
				INSERT INTO usage_logs VALUES (1, 1, 10, 1.25, '2020-01-02 08:00:00+08');
			`)
			require.NoError(t, err)
			repo := newDashboardAggregationRepositoryWithSQL(reader)
			require.NoError(t, repo.SyncGroupUsageRollups(ctx, today))
			assertGroupUsageRollupTestState(t, ctx, reader, "2020-01-03", "2020-01-03", 1, 0)
			assertGroupUsageRollupTestSummary(t, ctx, reader, today, 1.25, 0, 1.25)

			writer := beginGroupUsageRollupTriggerTestTx(t, ctx, schema)
			defer func() { _ = writer.Rollback() }()
			_, err = writer.ExecContext(ctx, "DELETE FROM users WHERE id = 1")
			require.NoError(t, err)
			// Neither the cascade nor its marker is visible until the writer commits.
			assertGroupUsageRollupTestState(t, ctx, reader, "2020-01-03", "2020-01-03", 1, 0)
			assertGroupUsageRollupTestSummary(t, ctx, reader, today, 1.25, 0, 1.25)
			require.NoError(t, writer.Commit())
			assertGroupUsageRollupTestState(t, ctx, reader, "2020-01-03", "2020-01-02", 1, 1)
			var rawCount int
			var staleCost float64
			require.NoError(t, reader.QueryRowContext(ctx, "SELECT COUNT(*) FROM usage_logs").Scan(&rawCount))
			require.Zero(t, rawCount)
			require.NoError(t, reader.QueryRowContext(ctx, "SELECT SUM(actual_cost) FROM usage_group_daily_rollups").Scan(&staleCost))
			require.Equal(t, 1.25, staleCost)
			assertGroupUsageRollupTestSummary(t, ctx, reader, today, 0, 0, 0)
			require.NoError(t, repo.SyncGroupUsageRollups(ctx, today))
			assertGroupUsageRollupTestState(t, ctx, reader, "2020-01-03", "2020-01-03", 2, 0)
			require.NoError(t, reader.QueryRowContext(ctx, "SELECT COUNT(*) FROM usage_group_daily_rollups").Scan(&rawCount))
			require.Zero(t, rawCount)
			assertGroupUsageRollupTestSummary(t, ctx, reader, today, 0, 0, 0)
		})
	}
}

func TestGroupUsageRollupTriggerPreservesLateHistoricalInsertAcrossPublish(t *testing.T) {
	useGroupUsageRepositoryTestTimezone(t, "Asia/Shanghai")
	// Both commit orders must preserve an uncaptured marker. The publication
	// has already validated and holds the state row while the INSERT executes.
	for _, writerFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("writer_commits_first=%t", writerFirst), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			schema := createGroupUsageRollupTriggerTestSchema(t, ctx, false)
			reader := connectGroupUsageRollupTestSchema(t, ctx, schema)
			today := time.Date(2020, 1, 2, 16, 0, 0, 0, time.UTC)
			_, err := reader.ExecContext(ctx, `
				INSERT INTO groups VALUES (10);
				INSERT INTO users VALUES (1);
				INSERT INTO usage_logs VALUES (1, 1, 10, 100, '2020-01-02 08:00:00+08');
			`)
			require.NoError(t, err)
			build := beginGroupUsageRollupTriggerTestTx(t, ctx, schema)
			defer func() { _ = build.Rollback() }()
			repo := newDashboardAggregationRepositoryWithSQL(build)
			require.NoError(t, repo.prepareGroupUsageRollupRebuild(ctx, today))
			plan, err := repo.planGroupUsageRollupRebuild(ctx, today, false)
			require.NoError(t, err)
			require.NotNil(t, plan)
			require.NoError(t, repo.rebuildGroupUsageDailyRollups(ctx, plan))
			retry, err := repo.publishGroupUsageRollupWatermarkInTx(ctx, today, plan)
			require.NoError(t, err)
			require.False(t, retry)

			late := beginGroupUsageRollupTriggerTestTx(t, ctx, schema)
			defer func() { _ = late.Rollback() }()
			_, err = late.ExecContext(ctx, `INSERT INTO usage_logs VALUES (2, 1, 10, 1.25, '2020-01-02 09:00:00+08')`)
			require.NoError(t, err, "late INSERT must finish while publication still holds the state row")
			assertGroupUsageRollupTestState(t, ctx, reader, "1970-01-01", "1970-01-01", 0, 1)
			assertGroupUsageRollupTestSummary(t, ctx, reader, today, 100, 0, 100)
			if writerFirst {
				require.NoError(t, late.Commit())
				assertGroupUsageRollupTestState(t, ctx, reader, "1970-01-01", "1970-01-01", 0, 2)
				assertGroupUsageRollupTestSummary(t, ctx, reader, today, 101.25, 0, 101.25)
				require.NoError(t, build.Commit())
			} else {
				require.NoError(t, build.Commit())
				assertGroupUsageRollupTestState(t, ctx, reader, "2020-01-03", "2020-01-03", 1, 0)
				assertGroupUsageRollupTestSummary(t, ctx, reader, today, 100, 0, 100)
				require.NoError(t, late.Commit())
			}
			assertGroupUsageRollupTestState(t, ctx, reader, "2020-01-03", "2020-01-02", 1, 1)
			assertGroupUsageRollupTestSummary(t, ctx, reader, today, 101.25, 0, 101.25)
			require.NoError(t, newDashboardAggregationRepositoryWithSQL(reader).SyncGroupUsageRollups(ctx, today))
			assertGroupUsageRollupTestState(t, ctx, reader, "2020-01-03", "2020-01-03", 2, 0)
			assertGroupUsageRollupTestSummary(t, ctx, reader, today, 101.25, 0, 101.25)
		})
	}
}

func TestGroupUsageRollupTriggerPreservesInsertTransactionAcrossMidnight(t *testing.T) {
	useGroupUsageRepositoryTestTimezone(t, "Asia/Shanghai")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	schema := createGroupUsageRollupTriggerTestSchema(t, ctx, false)
	reader := connectGroupUsageRollupTestSchema(t, ctx, schema)
	// Advance the supplied business clock, not wall time, across local midnight.
	today := time.Date(2026, 8, 13, 16, 0, 0, 0, time.UTC)
	tomorrow := today.AddDate(0, 0, 1)
	_, err := reader.ExecContext(ctx, `
		INSERT INTO groups VALUES (10);
		INSERT INTO users VALUES (1);
		INSERT INTO usage_logs VALUES (1, 1, 10, 2, '2026-08-13 12:00:00+08');
	`)
	require.NoError(t, err)
	repo := newDashboardAggregationRepositoryWithSQL(reader)
	require.NoError(t, repo.SyncGroupUsageRollups(ctx, today))
	writer := beginGroupUsageRollupTriggerTestTx(t, ctx, schema)
	defer func() { _ = writer.Rollback() }()
	require.NoError(t, setGroupUsageRollupTriggerTimeZone(ctx, writer, "Asia/Shanghai"))
	_, err = writer.ExecContext(ctx, `INSERT INTO usage_logs VALUES (2, 1, 10, 1.25, TIMESTAMPTZ '2026-08-14 23:59:59')`)
	require.NoError(t, err)
	assertGroupUsageRollupTestSummary(t, ctx, writer, today, 3.25, 1.25, 2)
	assertGroupUsageRollupTestSummary(t, ctx, reader, today, 2, 0, 2)
	// The rebuild cannot see the in-flight row or its generation and completes.
	require.NoError(t, repo.SyncGroupUsageRollups(ctx, tomorrow))
	assertGroupUsageRollupTestState(t, ctx, reader, "2026-08-15", "2026-08-15", 2, 0)
	assertGroupUsageRollupTestSummary(t, ctx, reader, tomorrow, 2, 0, 0)
	require.NoError(t, writer.Commit())
	assertGroupUsageRollupTestState(t, ctx, reader, "2026-08-15", "2026-08-14", 2, 1)
	assertGroupUsageRollupTestSummary(t, ctx, reader, tomorrow, 3.25, 0, 1.25)
	require.NoError(t, repo.SyncGroupUsageRollups(ctx, tomorrow))
	assertGroupUsageRollupTestState(t, ctx, reader, "2026-08-15", "2026-08-15", 3, 0)
	assertGroupUsageRollupTestSummary(t, ctx, reader, tomorrow, 3.25, 0, 1.25)
}

func TestGroupUsageRollupTriggerKeepsWatermarkForTodayInsert(t *testing.T) {
	useGroupUsageRepositoryTestTimezone(t, "Asia/Shanghai")
	ctx := context.Background()
	schema := createGroupUsageRollupTriggerTestSchema(t, ctx, false)
	tx := beginGroupUsageRollupTriggerTestTx(t, ctx, schema)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, setGroupUsageRollupTriggerTimeZone(ctx, tx, "Asia/Shanghai"))
	_, err := tx.ExecContext(ctx, `
		INSERT INTO groups VALUES (10);
		INSERT INTO users VALUES (1);
		UPDATE usage_group_rollup_state SET closed_before = '2026-08-14' WHERE id = 1;
		INSERT INTO usage_logs VALUES (1, 1, 10, 1.25, TIMESTAMPTZ '2026-08-14 12:00:00');
	`)
	require.NoError(t, err)
	// Open-day INSERTs also leave a marker for a later midnight publication.
	assertGroupUsageRollupTestState(t, ctx, tx, "2026-08-14", "2026-08-14", 0, 1)
	assertGroupUsageRollupTestSummary(t, ctx, tx, time.Date(2026, 8, 13, 16, 0, 0, 0, time.UTC), 1.25, 1.25, 0)
}

func TestGroupUsageRollupTriggerUsesSessionTimezoneAcrossDST(t *testing.T) {
	useGroupUsageRepositoryTestTimezone(t, "America/New_York")
	for _, sessionZone := range []string{"America/New_York", "UTC", "Asia/Shanghai"} {
		t.Run(sessionZone, func(t *testing.T) {
			ctx := context.Background()
			schema := createGroupUsageRollupTriggerTestSchema(t, ctx, false)
			conn := connectGroupUsageRollupTestSchema(t, ctx, schema)
			_, err := conn.ExecContext(ctx, "SET TIME ZONE "+pq.QuoteLiteral(sessionZone))
			require.NoError(t, err)
			today := time.Date(2026, 3, 9, 4, 0, 0, 0, time.UTC)
			_, err = conn.ExecContext(ctx, `
				INSERT INTO groups VALUES (10);
				INSERT INTO users VALUES (1);
				INSERT INTO usage_logs VALUES (1, 1, 10, 3, '2026-03-08 05:30:00+00');
			`)
			require.NoError(t, err)
			repo := newDashboardAggregationRepositoryWithSQL(conn)
			require.NoError(t, repo.SyncGroupUsageRollups(ctx, today))
			_, err = conn.ExecContext(ctx, `
				INSERT INTO usage_logs VALUES
				 (2, 1, 10, 1.25, '2026-03-08 04:30:00+00'),
				 (3, 1, 10, 4, '2026-03-09 03:30:00+00');
			`)
			require.NoError(t, err)
			// UTC is only the marker coalescing bucket. The effective watermark and
			// reader/rebuilder attribution use the configured zone, across the 23h day.
			var affected time.Time
			var markerBucket string
			require.NoError(t, conn.QueryRowContext(ctx, `SELECT affected_at, bucket_date::text FROM usage_group_rollup_dirty ORDER BY affected_at LIMIT 1`).Scan(&affected, &markerBucket))
			require.True(t, affected.Equal(time.Date(2026, 3, 8, 4, 30, 0, 0, time.UTC)))
			require.Equal(t, "2026-03-08", markerBucket)
			assertGroupUsageRollupTestState(t, ctx, conn, "2026-03-09", "2026-03-07", 1, 2)
			assertGroupUsageRollupTestSummary(t, ctx, conn, today, 8.25, 0, 7)
			require.NoError(t, repo.SyncGroupUsageRollups(ctx, today))
			assertGroupUsageRollupTestState(t, ctx, conn, "2026-03-09", "2026-03-09", 2, 0)
			assertGroupUsageRollupTestSummary(t, ctx, conn, today, 8.25, 0, 7)
			var previous, yesterday float64
			require.NoError(t, conn.QueryRowContext(ctx, `SELECT
				SUM(actual_cost) FILTER (WHERE bucket_date = '2026-03-07'),
				SUM(actual_cost) FILTER (WHERE bucket_date = '2026-03-08')
				FROM usage_group_daily_rollups
			`).Scan(&previous, &yesterday))
			require.Equal(t, 1.25, previous)
			require.Equal(t, 7.0, yesterday)
		})
	}
}

func TestGroupUsageSummaryIncludesYesterdayAcrossWatermark(t *testing.T) {
	ctx := context.Background()
	useGroupUsageRepositoryTestTimezone(t, "Asia/Shanghai")
	todayStart := time.Date(2026, 8, 13, 16, 0, 0, 0, time.UTC)

	tests := []struct {
		name             string
		closedBefore     string
		includeYesterday bool
		dirtyYesterday   bool
	}{
		{name: "closed_rollup", closedBefore: "2026-08-14", includeYesterday: true},
		{name: "raw_tail", closedBefore: "2026-08-13", includeYesterday: false},
		{name: "dirty_yesterday", closedBefore: "2026-08-14", includeYesterday: true, dirtyYesterday: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := createGroupUsageRollupTriggerTestSchema(t, ctx, false)
			tx := beginGroupUsageRollupTriggerTestTx(t, ctx, schema)
			defer func() { _ = tx.Rollback() }()

			_, err := tx.ExecContext(ctx, `
				INSERT INTO groups (id) VALUES (10);
				INSERT INTO users (id) VALUES (1);
				INSERT INTO usage_logs (id, user_id, group_id, actual_cost, created_at) VALUES
					(1, 1, 10, 2, TIMESTAMPTZ '2026-08-12 12:00:00+08'),
					(2, 1, 10, 3, TIMESTAMPTZ '2026-08-13 12:00:00+08'),
					(3, 1, 10, 4, TIMESTAMPTZ '2026-08-14 12:00:00+08');
				INSERT INTO usage_group_daily_rollups (bucket_date, group_id, actual_cost, computed_at)
				VALUES (DATE '2026-08-12', 10, 2, NOW());
			`)
			require.NoError(t, err)
			if tt.includeYesterday {
				_, err = tx.ExecContext(ctx, `
					INSERT INTO usage_group_daily_rollups (bucket_date, group_id, actual_cost, computed_at)
					VALUES (DATE '2026-08-13', 10, 3, NOW())
				`)
				require.NoError(t, err)
			}
			// This fixture represents a completed publication. A plain state
			// UPDATE alone cannot acknowledge the seed INSERT generations.
			_, err = tx.ExecContext(ctx, "DELETE FROM usage_group_rollup_dirty")
			require.NoError(t, err)
			_, err = tx.ExecContext(ctx, `
				UPDATE usage_group_rollup_state
				SET closed_before = $1::date,
					retained_from = TIMESTAMPTZ '2026-08-12 00:00:00+08'
				WHERE id = 1
			`, tt.closedBefore)
			require.NoError(t, err)
			assertGroupUsageRollupTestState(t, ctx, tx, tt.closedBefore, tt.closedBefore, 0, 0)
			total, yesterday := 9.0, 3.0
			if tt.dirtyYesterday {
				_, err = tx.ExecContext(ctx, "UPDATE usage_logs SET actual_cost = 4 WHERE id = 2")
				require.NoError(t, err)
				assertGroupUsageRollupTestState(t, ctx, tx, tt.closedBefore, "2026-08-13", 0, 1)
				total, yesterday = 10, 4
			}

			repo := newUsageLogRepositoryWithSQL(nil, tx)
			result, err := repo.GetAllGroupUsageSummary(ctx, todayStart)
			require.NoError(t, err)
			require.Len(t, result, 1)
			require.InDelta(t, total, result[0].TotalCost, 0.0000001)
			require.InDelta(t, 4, result[0].TodayCost, 0.0000001)
			require.InDelta(t, yesterday, result[0].YesterdayCost, 0.0000001)
		})
	}
}

func TestGroupUsageRollupSyncRebuildsAfterTimezoneChange(t *testing.T) {
	ctx := context.Background()
	useGroupUsageRepositoryTestTimezone(t, "America/New_York")
	todayStart := time.Date(2026, 3, 9, 4, 0, 0, 0, time.UTC)
	schema := createGroupUsageRollupTriggerTestSchema(t, ctx, false)
	tx := beginGroupUsageRollupTriggerTestTx(t, ctx, schema)
	defer func() { _ = tx.Rollback() }()

	_, err := tx.ExecContext(ctx, `
		SET LOCAL TIME ZONE 'UTC';
		INSERT INTO groups (id) VALUES (10);
		INSERT INTO users (id) VALUES (1);
		INSERT INTO usage_logs (id, user_id, group_id, actual_cost, created_at) VALUES
			(1, 1, 10, 3, TIMESTAMPTZ '2026-03-08 05:30:00+00'),
			(2, 1, 10, 5, TIMESTAMPTZ '2026-03-09 04:30:00+00');
		INSERT INTO usage_group_daily_rollups (bucket_date, group_id, actual_cost, computed_at)
		VALUES (DATE '2026-03-08', 10, 99, NOW());
		UPDATE usage_group_rollup_state
		SET closed_before = DATE '2026-03-09',
			retained_from = TIMESTAMPTZ '2026-03-08 05:30:00+00',
			timezone_name = 'Asia/Shanghai'
		WHERE id = 1;
	`)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())
	conn := connectGroupUsageRollupTestSchema(t, ctx, schema)
	// A timezone mismatch makes the old bucket unusable even before rebuilding.
	assertGroupUsageRollupTestSummary(t, ctx, conn, todayStart, 8, 5, 3)
	build := beginGroupUsageRollupTriggerTestTx(t, ctx, schema)
	defer func() { _ = build.Rollback() }()
	repo := newDashboardAggregationRepositoryWithSQL(build)
	require.NoError(t, repo.prepareGroupUsageRollupRebuild(ctx, todayStart))
	plan, err := repo.planGroupUsageRollupRebuild(ctx, todayStart, false)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.True(t, plan.timezoneChanged)
	require.NoError(t, repo.rebuildGroupUsageDailyRollups(ctx, plan))
	assertOldVersion := func() {
		t.Helper()
		var zone string
		var generation int64
		var cost float64
		require.NoError(t, conn.QueryRowContext(ctx, `SELECT timezone_name, published_generation,
			(SELECT SUM(actual_cost) FROM usage_group_daily_rollups)
			FROM usage_group_rollup_state WHERE id = 1
		`).Scan(&zone, &generation, &cost))
		require.Equal(t, "Asia/Shanghai", zone)
		require.Zero(t, generation)
		require.Equal(t, 99.0, cost)
		assertGroupUsageRollupTestSummary(t, ctx, conn, todayStart, 8, 5, 3)
	}
	assertOldVersion()
	retry, err := repo.publishGroupUsageRollupWatermarkInTx(ctx, todayStart, plan)
	require.NoError(t, err)
	require.False(t, retry)
	assertOldVersion() // Shadow buckets, marker deletion and state are still private.
	require.NoError(t, build.Commit())
	assertGroupUsageRollupTestState(t, ctx, conn, "2026-03-09", "2026-03-09", 1, 1)
	require.NoError(t, newDashboardAggregationRepositoryWithSQL(conn).SyncGroupUsageRollups(ctx, todayStart))
	assertGroupUsageRollupTestState(t, ctx, conn, "2026-03-09", "2026-03-09", 1, 1)

	var stateTimezone string
	var closedBefore string
	require.NoError(t, conn.QueryRowContext(ctx, `
		SELECT timezone_name, closed_before::text
		FROM usage_group_rollup_state
		WHERE id = 1
	`).Scan(&stateTimezone, &closedBefore))
	require.Equal(t, "America/New_York", stateTimezone)
	require.Equal(t, "2026-03-09", closedBefore)

	var rollupCost float64
	require.NoError(t, conn.QueryRowContext(ctx, `
		SELECT actual_cost
		FROM usage_group_daily_rollups
		WHERE bucket_date = DATE '2026-03-08' AND group_id = 10
	`).Scan(&rollupCost))
	require.InDelta(t, 3, rollupCost, 0.0000001)

	usageRepo := newUsageLogRepositoryWithSQL(nil, conn)
	result, err := usageRepo.GetAllGroupUsageSummary(ctx, todayStart)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.InDelta(t, 8, result[0].TotalCost, 0.0000001)
	require.InDelta(t, 5, result[0].TodayCost, 0.0000001)
	require.InDelta(t, 3, result[0].YesterdayCost, 0.0000001)
}

func TestGroupUsageSummaryUsesConfiguredDSTBoundaries(t *testing.T) {
	useGroupUsageRepositoryTestTimezone(t, "America/New_York")
	for _, tt := range []struct {
		name      string
		today     time.Time
		yesterday time.Time
		hours     time.Duration
	}{
		{"spring_forward", time.Date(2026, 3, 9, 4, 0, 0, 0, time.UTC), time.Date(2026, 3, 8, 5, 0, 0, 0, time.UTC), 23 * time.Hour},
		{"fall_back", time.Date(2026, 11, 2, 5, 0, 0, 0, time.UTC), time.Date(2026, 11, 1, 4, 0, 0, 0, time.UTC), 25 * time.Hour},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			schema := createGroupUsageRollupTriggerTestSchema(t, ctx, false)
			conn := connectGroupUsageRollupTestSchema(t, ctx, schema)
			_, err := conn.ExecContext(ctx, `SET TIME ZONE 'UTC'; INSERT INTO groups VALUES (10); INSERT INTO users VALUES (1)`)
			require.NoError(t, err)
			require.Equal(t, tt.hours, tt.today.Sub(tt.yesterday))
			_, err = conn.ExecContext(ctx, `INSERT INTO usage_logs VALUES
				(1, 1, 10, 100, $1), (2, 1, 10, 3, $2), (3, 1, 10, 4, $3), (4, 1, 10, 5, $4)
			`,
				tt.yesterday.Add(-time.Second), tt.yesterday, tt.today.Add(-time.Second), tt.today)
			require.NoError(t, err)
			assertGroupUsageRollupTestSummary(t, ctx, conn, tt.today, 112, 5, 7)
			repo := newDashboardAggregationRepositoryWithSQL(conn)
			// Markers coalesce within a source transaction / UTC day; the marker
			// shared by the last two rows is captured through its earlier affected_at.
			require.NoError(t, repo.SyncGroupUsageRollups(ctx, tt.today))
			assertGroupUsageRollupTestState(t, ctx, conn, service.GroupUsageDate(tt.today), service.GroupUsageDate(tt.today), 1, 0)
			assertGroupUsageRollupTestSummary(t, ctx, conn, tt.today, 112, 5, 7)
			var bucketCost float64
			require.NoError(t, conn.QueryRowContext(ctx, "SELECT actual_cost FROM usage_group_daily_rollups WHERE bucket_date = $1::date AND group_id = 10", service.GroupUsageDate(tt.yesterday)).Scan(&bucketCost))
			require.Equal(t, 7.0, bucketCost)
			_, err = conn.ExecContext(ctx, "UPDATE usage_logs SET actual_cost = 6 WHERE id = 2")
			require.NoError(t, err)
			assertGroupUsageRollupTestState(t, ctx, conn, service.GroupUsageDate(tt.today), service.GroupUsageDate(tt.yesterday), 1, 1)
			assertGroupUsageRollupTestSummary(t, ctx, conn, tt.today, 115, 5, 10)
			require.NoError(t, repo.SyncGroupUsageRollups(ctx, tt.today))
			assertGroupUsageRollupTestState(t, ctx, conn, service.GroupUsageDate(tt.today), service.GroupUsageDate(tt.today), 2, 0)
			assertGroupUsageRollupTestSummary(t, ctx, conn, tt.today, 115, 5, 10)
		})
	}
}

func createGroupUsageRollupTriggerTestSchema(t *testing.T, ctx context.Context, partitioned bool) string {
	t.Helper()

	schema := fmt.Sprintf("group_usage_rollup_trigger_%d", time.Now().UnixNano())
	quotedSchema := pq.QuoteIdentifier(schema)
	_, err := integrationDB.ExecContext(ctx, "CREATE SCHEMA "+quotedSchema)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+quotedSchema+" CASCADE")
	})

	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, setGroupUsageRollupTriggerSearchPath(ctx, tx, quotedSchema))

	usageLogsDDL := `
		CREATE TABLE usage_logs (
			id BIGINT PRIMARY KEY,
			user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			group_id BIGINT REFERENCES groups(id) ON DELETE SET NULL,
			actual_cost NUMERIC(20, 10) NOT NULL,
			created_at TIMESTAMPTZ NOT NULL
		);
	`
	if partitioned {
		usageLogsDDL = `
			CREATE TABLE usage_logs (
				id BIGINT NOT NULL,
				user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
				group_id BIGINT REFERENCES groups(id) ON DELETE SET NULL,
				actual_cost NUMERIC(20, 10) NOT NULL,
				created_at TIMESTAMPTZ NOT NULL
			) PARTITION BY RANGE (created_at);
			CREATE TABLE usage_logs_default PARTITION OF usage_logs DEFAULT;
		`
	}

	_, err = tx.ExecContext(ctx, `
		CREATE TABLE users (id BIGINT PRIMARY KEY);
		CREATE TABLE groups (id BIGINT PRIMARY KEY);
	`+usageLogsDDL)
	require.NoError(t, err)

	for _, migrationName := range []string{
		"222_group_usage_daily_rollups.sql",
		"223_group_usage_rollup_timezone.sql",
		"238_group_usage_rollup_insert_cas.sql",
	} {
		migrationSQL, readErr := migrations.FS.ReadFile(migrationName)
		require.NoError(t, readErr)
		for range 2 {
			_, err = tx.ExecContext(ctx, string(migrationSQL))
			require.NoError(t, err)
		}
	}
	// 239 replaces the invalidate trigger with the dirty-generation protocol and
	// is not idempotent (ADD COLUMN / CREATE TABLE), so it runs exactly once.
	dirtySQL, readErr := migrations.FS.ReadFile("239_group_usage_rollup_dirty_generations.sql")
	require.NoError(t, readErr)
	_, err = tx.ExecContext(ctx, string(dirtySQL))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	return schema
}

func beginGroupUsageRollupTriggerTestTx(t *testing.T, ctx context.Context, schema string) *sql.Tx {
	t.Helper()

	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, setGroupUsageRollupTriggerSearchPath(ctx, tx, pq.QuoteIdentifier(schema)))
	return tx
}

func setGroupUsageRollupTriggerSearchPath(ctx context.Context, tx *sql.Tx, quotedSchema string) error {
	_, err := tx.ExecContext(ctx, "SET LOCAL search_path TO "+quotedSchema)
	return err
}

func setGroupUsageRollupTriggerTimeZone(ctx context.Context, tx *sql.Tx, name string) error {
	_, err := tx.ExecContext(ctx, "SET LOCAL TIME ZONE "+pq.QuoteLiteral(name))
	return err
}

// Pinned connections keep schema/session settings across the independent
// transactions opened by SyncGroupUsageRollups; reset them before pool reuse.
func connectGroupUsageRollupTestSchema(t *testing.T, ctx context.Context, schema string) *sql.Conn {
	t.Helper()
	conn, err := integrationDB.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), "RESET search_path; RESET TIME ZONE")
		_ = conn.Close()
	})
	_, err = conn.ExecContext(ctx, "SET search_path TO "+pq.QuoteIdentifier(schema))
	require.NoError(t, err)
	return conn
}

func assertGroupUsageRollupTestState(t *testing.T, ctx context.Context, exec sqlExecutor, published, effective string, generation int64, pending int) {
	t.Helper()
	var gotPublished, gotEffective string
	var gotGeneration int64
	var gotPending int
	require.NoError(t, scanSingleRow(ctx, exec, `SELECT closed_before::text,
		LEAST(closed_before, (SELECT (MIN(affected_at) AT TIME ZONE $1::text)::date FROM usage_group_rollup_dirty))::text,
		published_generation, (SELECT COUNT(*) FROM usage_group_rollup_dirty)
		FROM usage_group_rollup_state WHERE id = 1
	`, []any{service.GroupUsageTimezoneName()},
		&gotPublished, &gotEffective, &gotGeneration, &gotPending))
	require.Equal(t, published, gotPublished, "published watermark")
	require.Equal(t, effective, gotEffective, "effective reader watermark")
	require.Equal(t, generation, gotGeneration, "published generation")
	require.Equal(t, pending, gotPending, "unacknowledged dirty markers")
}

func assertGroupUsageRollupTestSummary(t *testing.T, ctx context.Context, exec sqlExecutor, today time.Time, total, todayCost, yesterday float64) {
	t.Helper()
	result, err := newUsageLogRepositoryWithSQL(nil, exec).GetAllGroupUsageSummary(ctx, today)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, int64(10), result[0].GroupID)
	require.InDelta(t, total, result[0].TotalCost, 0.0000001)
	require.InDelta(t, todayCost, result[0].TodayCost, 0.0000001)
	require.InDelta(t, yesterday, result[0].YesterdayCost, 0.0000001)
}
