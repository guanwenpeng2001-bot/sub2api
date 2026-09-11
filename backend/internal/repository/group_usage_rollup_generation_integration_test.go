//go:build integration

package repository

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Two pinned PostgreSQL connections, no timing sleeps: each boundary is the
// actual prepare / aggregate / validate / publish operation used in production.
func TestGroupUsageRollupDirtyGenerationInterleavings(t *testing.T) {
	useGroupUsageRepositoryTestTimezone(t, "Asia/Shanghai")
	for _, partitioned := range []bool{false, true} {
		for _, mutation := range []string{"insert", "update", "delete", "move", "ungroup", "cascade"} {
			for _, phase := range []string{"started_before_snapshot", "before_validation", "after_validation", "after_publish"} {
				name := mutation + "/" + phase
				if partitioned {
					name = "partitioned/" + name
				}
				t.Run(name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					a, b := newGroupUsageGenerationConnections(t, ctx, partitioned)
					today := time.Date(2020, 1, 2, 16, 0, 0, 0, time.UTC)
					writer, err := b.BeginTx(ctx, nil)
					require.NoError(t, err)
					defer func() { _ = writer.Rollback() }()
					write := func() {
						table := "usage_logs"
						if partitioned {
							table = "usage_logs_default"
						} // direct leaf writes too
						statement := map[string]string{
							"insert":  "INSERT INTO " + table + " VALUES (2, 1, 10, 5, '2020-01-02 09:00:00+08')",
							"update":  "UPDATE " + table + " SET actual_cost = 125 WHERE id = 1",
							"delete":  "DELETE FROM " + table + " WHERE id = 1",
							"move":    "UPDATE " + table + " SET group_id = 20, created_at = '2020-01-01 09:00:00+08' WHERE id = 1",
							"ungroup": "UPDATE " + table + " SET group_id = NULL WHERE id = 1",
							"cascade": "DELETE FROM users WHERE id = 1",
						}[mutation]
						_, err := writer.ExecContext(ctx, statement)
						require.NoError(t, err)
					}
					if phase == "started_before_snapshot" {
						write()
						// Commit a HIGHER marker ID while the writer's lower ID
						// is invisible. MAX(id) acknowledgement would lose it.
						_, err = a.ExecContext(ctx, "INSERT INTO usage_group_rollup_dirty (affected_at) VALUES ('2020-01-02 10:00:00+08')")
						require.NoError(t, err)
					}
					build, err := a.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
					require.NoError(t, err)
					defer func() { _ = build.Rollback() }()
					repo := newDashboardAggregationRepositoryWithSQL(build)
					require.NoError(t, repo.prepareGroupUsageRollupRebuild(ctx, today))
					plan, err := repo.planGroupUsageRollupRebuild(ctx, today, false)
					require.NoError(t, err)
					require.NotNil(t, plan)
					require.NoError(t, repo.rebuildGroupUsageDailyRollups(ctx, plan))
					if phase != "started_before_snapshot" && phase != "after_validation" {
						write()
					}
					if phase == "before_validation" {
						require.NoError(t, writer.Commit())
					}
					retry, err := repo.publishGroupUsageRollupWatermarkInTx(ctx, today, plan)
					require.NoError(t, err)
					if phase == "before_validation" {
						require.True(t, retry, "committed unseen generation rejects the shadow version")
						require.NoError(t, build.Rollback())
					} else {
						require.False(t, retry)
						if phase == "after_validation" {
							// Publication holds the state row here. Usage DML
							// must still complete without waiting for that lock.
							write()
							require.NoError(t, writer.Commit())
							assertGroupUsageGenerationSummary(t, ctx, b, today)
						}
						require.NoError(t, build.Commit())
						if phase != "after_validation" {
							require.NoError(t, writer.Commit())
						}
					}
					// Before retry, uncaptured generations must already make the
					// public reader exact, including the previously open date.
					assertGroupUsageGenerationSummary(t, ctx, a, today)
					require.NoError(t, newDashboardAggregationRepositoryWithSQL(a).SyncGroupUsageRollups(ctx, today))
					assertGroupUsageGenerationSummary(t, ctx, a, today)
					var pending int
					require.NoError(t, a.QueryRowContext(ctx, "SELECT COUNT(*) FROM usage_group_rollup_dirty WHERE affected_at < $1", today).Scan(&pending))
					require.Zero(t, pending)
				})
			}
		}
	}
}

func TestGroupUsageRollupRebuildersSerializeAndRollback(t *testing.T) {
	useGroupUsageRepositoryTestTimezone(t, "Asia/Shanghai")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	a, b := newGroupUsageGenerationConnections(t, ctx, false)
	today := time.Date(2020, 1, 2, 16, 0, 0, 0, time.UTC)
	tx, err := a.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	repo := newDashboardAggregationRepositoryWithSQL(tx)
	require.NoError(t, repo.prepareGroupUsageRollupRebuild(ctx, today))
	plan, err := repo.planGroupUsageRollupRebuild(ctx, today, false)
	require.NoError(t, err)
	require.NoError(t, repo.rebuildGroupUsageDailyRollups(ctx, plan))
	var peerPID int
	require.NoError(t, b.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&peerPID))
	peerDone := make(chan error, 1)
	go func() { peerDone <- newDashboardAggregationRepositoryWithSQL(b).SyncGroupUsageRollups(ctx, today) }()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		require.NoError(t, tx.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND locktype = 'advisory' AND NOT granted)", peerPID).Scan(&waiting))
		if waiting {
			break
		}
		select {
		case err := <-peerDone:
			t.Fatalf("second rebuild did not wait for the first: %v", err)
		case <-ctx.Done():
			t.Fatal("second rebuild did not reach the advisory lock")
		case <-ticker.C:
		}
	}
	retry, err := repo.publishGroupUsageRollupWatermarkInTx(ctx, today, plan)
	require.NoError(t, err)
	require.False(t, retry)
	require.NoError(t, tx.Commit())
	// Once the first version is published, the second entry reads its current
	// watermark and no-ops. It cannot delete with the first round's old plan.
	select {
	case err := <-peerDone:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("second rebuild did not finish after the first committed")
	}
	assertGroupUsageGenerationSummary(t, ctx, b, today)

	// Force a rebuild of the already published date and fail after its DELETE.
	_, err = b.ExecContext(ctx, `
        INSERT INTO usage_group_rollup_dirty (affected_at) VALUES ('2020-01-02 09:00:00+08');
        CREATE FUNCTION reject_rollup_insert() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'injected publish failure'; END $$;
        CREATE TRIGGER reject_rollup_insert BEFORE INSERT ON usage_group_daily_rollups
        FOR EACH ROW EXECUTE FUNCTION reject_rollup_insert();
    `)
	require.NoError(t, err)
	require.ErrorContains(t, newDashboardAggregationRepositoryWithSQL(a).SyncGroupUsageRollups(ctx, today), "injected publish failure")
	var cost float64
	require.NoError(t, b.QueryRowContext(ctx, "SELECT SUM(actual_cost) FROM usage_group_daily_rollups").Scan(&cost))
	require.Equal(t, 100.0, cost, "failed replacement must roll back deletion of the published version")
	assertGroupUsageGenerationSummary(t, ctx, b, today)
	_, err = b.ExecContext(ctx, "DROP TRIGGER reject_rollup_insert ON usage_group_daily_rollups")
	require.NoError(t, err)
	require.NoError(t, newDashboardAggregationRepositoryWithSQL(a).SyncGroupUsageRollups(ctx, today))
	assertGroupUsageGenerationSummary(t, ctx, b, today)
}

func newGroupUsageGenerationConnections(t *testing.T, ctx context.Context, partitioned bool) (*sql.Conn, *sql.Conn) {
	t.Helper()
	schema := createGroupUsageRollupTriggerTestSchema(t, ctx, partitioned)
	tx := beginGroupUsageRollupTriggerTestTx(t, ctx, schema)
	defer func() { _ = tx.Rollback() }()
	_, err := tx.ExecContext(ctx, `
        INSERT INTO groups VALUES (10), (20);
        INSERT INTO users VALUES (1);
        INSERT INTO usage_logs VALUES (1, 1, 10, 100, '2020-01-02 08:00:00+08');
        DELETE FROM usage_group_rollup_dirty;
        UPDATE usage_group_rollup_state SET closed_before = '2020-01-02';
    `)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	return connectGroupUsageRollupTestSchema(t, ctx, schema), connectGroupUsageRollupTestSchema(t, ctx, schema)
}

func assertGroupUsageGenerationSummary(t *testing.T, ctx context.Context, conn *sql.Conn, today time.Time) {
	t.Helper()
	summaries, err := (&usageLogRepository{sql: conn}).getAllGroupUsageSummaryFromRollups(ctx, today)
	require.NoError(t, err)
	require.Len(t, summaries, 2)
	for _, got := range summaries {
		var total, todayCost, yesterday float64
		require.NoError(t, conn.QueryRowContext(ctx, `
            SELECT COALESCE(SUM(actual_cost), 0),
                COALESCE(SUM(actual_cost) FILTER (WHERE created_at >= $2), 0),
                COALESCE(SUM(actual_cost) FILTER (WHERE created_at >= $2::timestamptz - INTERVAL '1 day' AND created_at < $2), 0)
            FROM usage_logs WHERE group_id = $1
        `, got.GroupID, today).Scan(&total, &todayCost, &yesterday))
		require.Equal(t, total, got.TotalCost)
		require.Equal(t, todayCost, got.TodayCost)
		require.Equal(t, yesterday, got.YesterdayCost)
	}
}

func TestGroupUsageRollupInitialBackfillAndAbortedWriter(t *testing.T) {
	useGroupUsageRepositoryTestTimezone(t, "Asia/Shanghai")
	for _, commitWriter := range []bool{false, true} {
		name := "rollback"
		if commitWriter {
			name = "commit"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			a, b := newGroupUsageGenerationConnections(t, ctx, false)
			today := time.Date(2020, 1, 2, 16, 0, 0, 0, time.UTC)
			_, err := a.ExecContext(ctx, "UPDATE usage_group_rollup_state SET closed_before = '1970-01-01'")
			require.NoError(t, err)
			writer, err := b.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { _ = writer.Rollback() }()
			_, err = writer.ExecContext(ctx, "INSERT INTO usage_logs VALUES (2, 1, 10, 5, '2019-12-01 09:00:00+08')")
			require.NoError(t, err)
			// The initial backfill cannot see the pending earlier record.
			require.NoError(t, newDashboardAggregationRepositoryWithSQL(a).SyncGroupUsageRollups(ctx, today))
			assertGroupUsageGenerationSummary(t, ctx, a, today)
			if commitWriter {
				require.NoError(t, writer.Commit())
			} else {
				require.NoError(t, writer.Rollback())
			}
			assertGroupUsageGenerationSummary(t, ctx, a, today)
			require.NoError(t, newDashboardAggregationRepositoryWithSQL(a).SyncGroupUsageRollups(ctx, today))
			assertGroupUsageGenerationSummary(t, ctx, a, today)
			var pending int
			require.NoError(t, a.QueryRowContext(ctx, "SELECT COUNT(*) FROM usage_group_rollup_dirty").Scan(&pending))
			require.Zero(t, pending)
		})
	}
}
