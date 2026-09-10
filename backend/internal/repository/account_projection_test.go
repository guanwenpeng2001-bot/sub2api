package repository

import (
	"context"
	"strings"
	"testing"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/DATA-DOG/go-sqlmock"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	_ "github.com/Wei-Shaw/sub2api/ent/runtime"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestFixQDiscoverySQLProjection(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		var query string
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(captureEntQueryMatcher{actual: &query}))
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		repo := newAccountRepositoryWithSQL(nil, db, nil)
		var groupID *int64
		var groupArg any
		if grouped {
			id := int64(7)
			groupID = &id
			groupArg = id
		}
		mock.ExpectQuery("discovery projection").WithArgs(groupArg, service.PlatformOpenAI).
			WillReturnRows(sqlmock.NewRows([]string{"id", "platform", "type", "credentials", "extra"}).
				AddRow(int64(1), service.PlatformOpenAI, service.AccountTypeAPIKey, `{"model_mapping":{"alias":"upstream"}}`, `{"openai_passthrough":true}`))
		accounts, err := repo.ListModelDiscoveryAccounts(context.Background(), groupID, service.PlatformOpenAI)
		require.NoError(t, err)
		require.Len(t, accounts, 1)
		require.Equal(t, "upstream", accounts[0].GetModelMapping()["alias"])
		require.True(t, accounts[0].IsOpenAIPassthroughEnabled())
		require.Empty(t, accounts[0].GetCredential("api_key"))
		require.Empty(t, accounts[0].AccountGroups)
		require.Nil(t, accounts[0].Proxy)
		projection, _, _ := strings.Cut(query, "FROM accounts")
		require.Contains(t, projection, "a.credentials->'model_mapping'")
		require.NotContains(t, projection, "api_key")
		require.NotContains(t, projection, "access_token")
		for _, predicate := range []string{"deleted_at", "schedulable", "temp_unschedulable_until", "expires_at", "overload_until", "rate_limit_reset_at", "ag.group_id = $1"} {
			require.Contains(t, query, predicate)
		}
		require.NoError(t, mock.ExpectationsWereMet(), "no proxy or group hydration queries")
	}
}

func TestFixQModelSyncPageSQLIsBoundedAndOrdered(t *testing.T) {
	var query string
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(captureEntQueryMatcher{actual: &query}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })
	repo := newAccountRepositoryWithSQL(client, db, nil)
	mock.ExpectQuery("model sync page").WillReturnRows(sqlmock.NewRows([]string{"id"}))
	accounts, err := repo.ListActiveModelSyncPage(context.Background(), 100, 5000)
	require.NoError(t, err)
	require.Empty(t, accounts)
	normalized := normalizeSQLWhitespace(query)
	require.Contains(t, normalized, `"id" >`)
	require.Contains(t, normalized, `ORDER BY "accounts"."id" ASC LIMIT 100`)
	require.Contains(t, normalized, `"platform" IN`)
	require.Contains(t, normalized, `"type" IN`)
	require.Contains(t, normalized, `"status" =`)
	require.NoError(t, mock.ExpectationsWereMet())
}
