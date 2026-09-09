package service

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// listerStub 只需要 ListActive 的窄桩（scheduler 依赖的是窄接口）。
type upstreamModelSyncListerStub struct {
	accounts []Account
	err      error
}

func (s *upstreamModelSyncListerStub) ListActive(context.Context) ([]Account, error) {
	return s.accounts, s.err
}

func TestUpstreamModelSyncScheduler_RunOnce_IsolatesFailures(t *testing.T) {
	lister := &upstreamModelSyncListerStub{accounts: []Account{
		{ID: 1, Name: "ok", Platform: PlatformOpenAI, Type: AccountTypeAPIKey},
		{ID: 2, Name: "bad", Platform: PlatformOpenAI, Type: AccountTypeAPIKey},
		{ID: 3, Name: "ok2", Platform: PlatformOpenAI, Type: AccountTypeAPIKey},
	}}
	s := NewUpstreamModelSyncScheduler(lister, nil)
	var visited []int64
	s.syncAccount = func(_ context.Context, account *Account) error {
		visited = append(visited, account.ID)
		if account.ID == 2 {
			return errors.New("upstream exploded")
		}
		return nil
	}

	err := s.RunOnce(context.Background())
	// 失败被聚合成返回错误，但不中断其他账号
	require.Error(t, err)
	require.Contains(t, err.Error(), "1/3")
	require.Equal(t, []int64{1, 2, 3}, visited)
}

func TestUpstreamModelSyncScheduler_RunOnce_ListError(t *testing.T) {
	lister := &upstreamModelSyncListerStub{err: errors.New("db down")}
	s := NewUpstreamModelSyncScheduler(lister, nil)
	s.syncAccount = func(context.Context, *Account) error { return nil }
	err := s.RunOnce(context.Background())
	require.ErrorContains(t, err, "list active accounts")
}

func TestAccount_UpstreamUserAgent(t *testing.T) {
	a := &Account{Extra: map[string]any{UpstreamUserAgentExtraKey: "codex_cli_rs/0.153.4 (test)"}}
	require.Equal(t, "codex_cli_rs/0.153.4 (test)", a.GetUpstreamUserAgent())

	h := http.Header{}
	a.ApplyUpstreamUserAgent(h)
	require.Equal(t, "codex_cli_rs/0.153.4 (test)", h.Get("User-Agent"))

	// 未配置 → no-op
	empty := &Account{}
	require.Equal(t, "", empty.GetUpstreamUserAgent())
	h2 := http.Header{}
	empty.ApplyUpstreamUserAgent(h2)
	_, exists := h2["User-Agent"]
	require.False(t, exists)

	// 非法值（控制字符）被拒
	bad := &Account{Extra: map[string]any{UpstreamUserAgentExtraKey: "bad\nvalue"}}
	require.Equal(t, "", bad.GetUpstreamUserAgent())
}

func TestBuildOpenAIAPIKeyModelsRequest_CarriesUpstreamUserAgent(t *testing.T) {
	account := &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"api_key":  "sk-test",
			"base_url": "https://bridge.local",
		},
		Extra: map[string]any{UpstreamUserAgentExtraKey: "codex_cli_rs/0.153.4"},
	}
	req, err := buildOpenAIAPIKeyModelsRequest(context.Background(), account, func(s string) (string, error) { return s, nil })
	require.NoError(t, err)
	require.Equal(t, "codex_cli_rs/0.153.4", req.Header.Get("User-Agent"))
	require.Equal(t, "Bearer sk-test", req.Header.Get("Authorization"))
}

func TestBuildOpenAIAPIKeyModelsRequest_HeaderOverrideWinsOverUpstreamUserAgent(t *testing.T) {
	account := &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"api_key":                    "sk-test",
			"base_url":                   "https://bridge.local",
			credKeyHeaderOverrideEnabled: true,
			credKeyHeaderOverrides:       map[string]any{"user-agent": "explicit/1.0"},
		},
		Extra: map[string]any{UpstreamUserAgentExtraKey: "codex_cli_rs/0.153.4"},
	}
	req, err := buildOpenAIAPIKeyModelsRequest(context.Background(), account, func(s string) (string, error) { return s, nil })
	require.NoError(t, err)
	require.Equal(t, "explicit/1.0", req.Header.Get("User-Agent"))
}

func TestUpstreamModelSyncScheduler_UnsupportedDoesNotCountAsFailure(t *testing.T) {
	s := NewUpstreamModelSyncScheduler(&upstreamModelSyncListerStub{accounts: []Account{{ID: 1}, {ID: 2}, {ID: 3}}}, nil)
	defer s.Stop()
	var visited []int64
	s.syncAccount = func(_ context.Context, a *Account) error {
		visited = append(visited, a.ID)
		if a.ID == 1 {
			return newUpstreamModelSyncUnsupportedError("unsupported", nil)
		}
		if a.ID == 2 {
			return context.DeadlineExceeded
		}
		return nil
	}
	require.ErrorContains(t, s.RunOnce(context.Background()), "1/3 accounts failed")
	require.Equal(t, []int64{1, 2, 3}, visited)
	s.syncAccount = func(context.Context, *Account) error { return newUpstreamModelSyncUnsupportedError("unsupported", nil) }
	require.NoError(t, s.RunOnce(context.Background()))
}

func TestUpstreamModelSyncScheduler_AccountTimeoutContinues(t *testing.T) {
	t.Setenv("UPSTREAM_MODEL_SYNC_ACCOUNT_TIMEOUT_SECONDS", "1")
	s := NewUpstreamModelSyncScheduler(&upstreamModelSyncListerStub{accounts: []Account{{ID: 1}, {ID: 2}}}, nil)
	defer s.Stop()
	var visited []int64
	s.syncAccount = func(ctx context.Context, a *Account) error {
		visited = append(visited, a.ID)
		if a.ID == 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		require.NoError(t, ctx.Err())
		return nil
	}
	require.ErrorContains(t, s.RunOnce(context.Background()), "1/2 accounts failed")
	require.Equal(t, []int64{1, 2}, visited)
}

func TestUpstreamModelSyncScheduler_StopCancelsAndWaitsForRunOnce(t *testing.T) {
	s := NewUpstreamModelSyncScheduler(&upstreamModelSyncListerStub{accounts: []Account{{ID: 1}, {ID: 2}}}, nil)
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	s.syncAccount = func(ctx context.Context, a *Account) error {
		if a.ID != 1 {
			return errors.New("unexpected subsequent account")
		}
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return ctx.Err()
	}
	runDone := make(chan error, 1)
	go func() { runDone <- s.RunOnce(context.Background()) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("sync did not start")
	}
	stops := make(chan struct{}, 2)
	go func() { s.Stop(); stops <- struct{}{} }()
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not cancel sync")
	}
	go func() { s.Stop(); stops <- struct{}{} }()
	select {
	case <-stops:
		t.Fatal("Stop returned before sync finished")
	case <-time.After(20 * time.Millisecond):
	}
	release <- struct{}{}
	for i := 0; i < 2; i++ {
		select {
		case <-stops:
		case <-time.After(3 * time.Second):
			t.Fatal("Stop did not finish")
		}
	}
	require.ErrorIs(t, <-runDone, context.Canceled)
	require.ErrorIs(t, s.RunOnce(context.Background()), context.Canceled)
	s.Start()
	require.False(t, s.started, "a stopped scheduler must not restart")
}
