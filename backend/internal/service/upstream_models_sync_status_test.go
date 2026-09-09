package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
)

type upstreamSyncStateRepo struct {
	AccountRepository
	extra   map[string]any
	patches []map[string]any
	err     error
}

func (r *upstreamSyncStateRepo) UpdateExtra(ctx context.Context, _ int64, patch map[string]any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.patches = append(r.patches, patch)
	if r.err != nil {
		return r.err
	}
	for k, v := range patch {
		r.extra[k] = v
	}
	return nil
}

func TestUpstreamModelSyncStatesManualAndPeriodic(t *testing.T) {
	for _, periodic := range []bool{false, true} {
		mode := "manual"
		if periodic {
			mode = "periodic"
		}
		for _, tc := range []struct {
			name   string
			status UpstreamModelSyncStatus
		}{
			{"complete", UpstreamModelSyncSuccess},
			{"incomplete", UpstreamModelSyncPartial},
			{"timeout", UpstreamModelSyncFailed},
			{"unsupported", UpstreamModelSyncSkipped},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				old := UpstreamModelMetadataSnapshot{Source: "upstream", SyncedAt: "2025-01-01T00:00:00Z", Models: map[string]UpstreamModelMetadata{"old": {ID: "old"}}}
				extra := map[string]any{
					UpstreamModelMetadataExtraKey:    old,
					UpstreamModelSyncSuccessExtraKey: "2025-01-01T00:00:00Z",
					UpstreamModelSyncCountExtraKey:   7,
					"unrelated":                      "keep",
				}
				repo := &upstreamSyncStateRepo{extra: extra}
				account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
					Credentials: map[string]any{"api_key": "fake", "base_url": "https://provider.example/v1", "model_mapping": map[string]any{"old": "old"}},
					Extra:       map[string]any{},
				}
				for k, v := range extra {
					account.Extra[k] = v
				}
				upstream := &httpUpstreamRecorder{}
				switch tc.status {
				case UpstreamModelSyncSuccess:
					// Keep the explicitly mapped model complete as well.
					upstream.resp = &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"data":[{"id":"old","reasoning":false,"input_modalities":["text"],"context_window":64000}]}`))}
				case UpstreamModelSyncPartial:
					upstream.responses = []*http.Response{
						{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"data":[{"id":"old"}]}`))},
						{StatusCode: 502, Body: io.NopCloser(strings.NewReader(`{}`))},
					}
				case UpstreamModelSyncFailed:
					upstream.err = context.DeadlineExceeded
				case UpstreamModelSyncSkipped:
					account.Platform = "unsupported-platform"
				}
				svc := &AccountTestService{accountRepo: repo, httpUpstream: upstream, cfg: upstreamModelSyncTestConfig()}
				var err error
				if periodic {
					scheduler := NewUpstreamModelSyncScheduler(&upstreamModelSyncListerStub{accounts: []Account{*account}}, svc)
					defer scheduler.Stop()
					err = scheduler.RunOnce(context.Background())
				} else {
					var catalog *UpstreamModelCatalog
					catalog, err = svc.SyncUpstreamModelCatalog(context.Background(), account)
					if tc.status == UpstreamModelSyncSuccess || tc.status == UpstreamModelSyncPartial {
						require.NoError(t, err)
						require.Equal(t, tc.status, catalog.Status)
						require.Equal(t, catalog.Warnings, extra[UpstreamModelSyncWarningsExtraKey])
					}
				}
				if tc.status == UpstreamModelSyncFailed || (tc.status == UpstreamModelSyncSkipped && !periodic) {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, string(tc.status), extra[UpstreamModelSyncStatusExtraKey])
				attempted, parseErr := time.Parse(time.RFC3339Nano, extra[UpstreamModelSyncAttemptExtraKey].(string))
				require.NoError(t, parseErr)
				require.WithinDuration(t, time.Now(), attempted, time.Second)
				require.Equal(t, "keep", extra["unrelated"])
				require.Equal(t, map[string]any{"old": "old"}, account.Credentials["model_mapping"])
				require.Len(t, repo.patches, 1)
				require.NotContains(t, repo.patches[0], "unrelated")
				if tc.status == UpstreamModelSyncFailed || tc.status == UpstreamModelSyncSkipped {
					require.Equal(t, old, extra[UpstreamModelMetadataExtraKey])
					require.Equal(t, "2025-01-01T00:00:00Z", extra[UpstreamModelSyncSuccessExtraKey])
					require.Equal(t, 7, extra[UpstreamModelSyncCountExtraKey])
					require.NotContains(t, repo.patches[0], UpstreamModelSyncSuccessExtraKey)
					require.NotContains(t, repo.patches[0], UpstreamModelSyncCountExtraKey)
				} else {
					require.Equal(t, 1, extra[UpstreamModelSyncCountExtraKey])
					success, parseErr := time.Parse(time.RFC3339Nano, extra[UpstreamModelSyncSuccessExtraKey].(string))
					require.NoError(t, parseErr)
					require.False(t, success.Before(attempted))
					if tc.status == UpstreamModelSyncPartial {
						require.Equal(t, old, extra[UpstreamModelMetadataExtraKey])
						require.NotEmpty(t, extra[UpstreamModelSyncWarningsExtraKey])
					}
				}
			})
		}
	}
}

func TestUpstreamModelSyncCanceledContextRecordsFailure(t *testing.T) {
	repo := &upstreamSyncStateRepo{extra: map[string]any{}}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "fake"}}
	svc := &AccountTestService{accountRepo: repo, httpUpstream: &httpUpstreamRecorder{err: context.Canceled}, cfg: upstreamModelSyncTestConfig()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := svc.SyncUpstreamModelCatalog(ctx, account)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, string(UpstreamModelSyncFailed), repo.extra[UpstreamModelSyncStatusExtraKey])
	require.NotContains(t, repo.extra, UpstreamModelSyncSuccessExtraKey)
}

func TestUpstreamModelSyncPersistenceFailurePreservesMemoryAndSnapshot(t *testing.T) {
	old := UpstreamModelMetadataSnapshot{Models: map[string]UpstreamModelMetadata{"old": {ID: "old"}}}
	repo := &upstreamSyncStateRepo{extra: map[string]any{UpstreamModelMetadataExtraKey: old}, err: errors.New("db unavailable")}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Credentials: map[string]any{"api_key": "fake"}, Extra: map[string]any{UpstreamModelMetadataExtraKey: old}}
	svc := &AccountTestService{accountRepo: repo, cfg: upstreamModelSyncTestConfig(), httpUpstream: &httpUpstreamRecorder{
		resp: &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"data":[{"id":"new","reasoning":false,"input_modalities":["text"],"context_window":64000}]}`))},
	}}
	catalog, err := svc.SyncUpstreamModelCatalog(context.Background(), account)
	require.Nil(t, catalog)
	var syncErr *UpstreamModelSyncError
	require.ErrorAs(t, err, &syncErr)
	require.Equal(t, UpstreamModelSyncErrorInternal, syncErr.Kind)
	require.Equal(t, old, repo.extra[UpstreamModelMetadataExtraKey])
	require.Equal(t, old, account.Extra[UpstreamModelMetadataExtraKey])
	require.NotContains(t, account.Extra, UpstreamModelSyncSuccessExtraKey)
}

type upstreamSyncCancelOnRegistry struct {
	httpUpstreamRecorder
	cancel context.CancelFunc
}

func (u *upstreamSyncCancelOnRegistry) Do(req *http.Request, proxy string, id int64, concurrency int) (*http.Response, error) {
	if req.URL.String() == modelsDevRegistryURL {
		u.cancel()
		return nil, req.Context().Err()
	}
	return u.httpUpstreamRecorder.Do(req, proxy, id, concurrency)
}

func TestUpstreamModelSyncCancellationDuringEnrichmentDiscardsPendingSnapshot(t *testing.T) {
	old := UpstreamModelMetadataSnapshot{Models: map[string]UpstreamModelMetadata{"old": {ID: "old"}}}
	repo := &upstreamSyncStateRepo{extra: map[string]any{UpstreamModelMetadataExtraKey: old}}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Credentials: map[string]any{"api_key": "fake", "base_url": "https://provider.example/v1"}, Extra: map[string]any{UpstreamModelMetadataExtraKey: old}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	upstream := &upstreamSyncCancelOnRegistry{cancel: cancel, httpUpstreamRecorder: httpUpstreamRecorder{
		resp: &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"data":[{"id":"new","reasoning":false,"input_modalities":["text"],"context_window":64000},{"id":"incomplete"}]}`))},
	}}
	svc := &AccountTestService{accountRepo: repo, httpUpstream: upstream, cfg: upstreamModelSyncTestConfig()}
	catalog, err := svc.SyncUpstreamModelCatalog(ctx, account)
	require.Nil(t, catalog)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, string(UpstreamModelSyncFailed), repo.extra[UpstreamModelSyncStatusExtraKey])
	require.Equal(t, old, repo.extra[UpstreamModelMetadataExtraKey])
	require.Equal(t, old, account.Extra[UpstreamModelMetadataExtraKey])
	require.NotContains(t, repo.patches[0], UpstreamModelMetadataExtraKey)
	require.NotContains(t, repo.patches[0], UpstreamModelSyncSuccessExtraKey)
}

func (u *upstreamSyncCancelOnRegistry) DoWithTLS(req *http.Request, proxy string, id int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxy, id, concurrency)
}
