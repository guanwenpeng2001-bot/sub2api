package service

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The lister models the stable ID cursor used by the production repository.
type upstreamModelSyncListerStub struct {
	accounts []Account
	err      error
}

func (s *upstreamModelSyncListerStub) ListActiveModelSyncPage(_ context.Context, afterID int64, limit int) ([]Account, error) {
	if s.err != nil {
		return nil, s.err
	}
	var page []Account
	for _, account := range s.accounts {
		if account.ID > afterID {
			page = append(page, account)
		}
		if len(page) == limit {
			break
		}
	}
	return page, nil
}

func TestUpstreamModelSyncScheduler_RunOnce_IsolatesFailures(t *testing.T) {
	lister := &upstreamModelSyncListerStub{accounts: []Account{
		{ID: 1, Name: "ok", Platform: PlatformOpenAI, Type: AccountTypeAPIKey},
		{ID: 2, Name: "bad", Platform: PlatformOpenAI, Type: AccountTypeAPIKey},
		{ID: 3, Name: "ok2", Platform: PlatformOpenAI, Type: AccountTypeAPIKey},
	}}
	s := newTestModelSyncScheduler(lister, nil)
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
	s := newTestModelSyncScheduler(lister, nil)
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
	s := newTestModelSyncScheduler(&upstreamModelSyncListerStub{accounts: []Account{{ID: 1}, {ID: 2}, {ID: 3}}}, nil)
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
	s := newTestModelSyncScheduler(&upstreamModelSyncListerStub{accounts: []Account{{ID: 1}, {ID: 2}}}, nil)
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
	s := newTestModelSyncScheduler(&upstreamModelSyncListerStub{accounts: []Account{{ID: 1}, {ID: 2}}}, nil)
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

func TestUpstreamModelSyncEmptyEnvironment(t *testing.T) {
	for _, raw := range []string{"", "  "} {
		for _, key := range []string{"UPSTREAM_MODEL_SYNC_ENABLED", "UPSTREAM_MODEL_SYNC_INTERVAL_HOURS", "UPSTREAM_MODEL_SYNC_ACCOUNT_TIMEOUT_SECONDS"} {
			t.Setenv(key, raw)
		}
		got, err := resolveUpstreamModelSyncConfig(nil, defaultUpstreamModelSyncConfig())
		require.NoError(t, err)
		require.Equal(t, defaultUpstreamModelSyncConfig(), got)
	}
}

type recoveringSyncSettings struct {
	SettingRepository
	calls    atomic.Int32
	disabled bool
}

func (r *recoveringSyncSettings) GetAll(context.Context) (map[string]string, error) {
	if r.calls.Add(1) <= 2 {
		return nil, errors.New("temporary settings failure")
	}
	enabled := "true"
	if r.disabled {
		enabled = "false"
	}
	return map[string]string{SettingKeyUpstreamModelSyncEnabled: enabled, SettingKeyUpstreamModelSyncInterval: "10ms"}, nil
}
func TestUpstreamModelSyncSchedulerRecoversWithoutNotification(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "enabled", true: "disabled"}[disabled], func(t *testing.T) {
			repo := &recoveringSyncSettings{disabled: disabled}
			s := newTestModelSyncSchedulerWithConfig(&upstreamModelSyncListerStub{accounts: []Account{{ID: 1}}}, nil, defaultUpstreamModelSyncConfig())
			s.settings = &SettingService{settingRepo: repo}
			var synced atomic.Int32
			s.syncAccount = func(context.Context, *Account) error { synced.Add(1); return nil }
			s.Start()
			defer s.Stop()
			require.Eventually(t, func() bool { return repo.calls.Load() >= 3 }, 5*time.Second, 10*time.Millisecond)
			if disabled {
				time.Sleep(30 * time.Millisecond)
				require.Zero(t, synced.Load())
			} else {
				require.Eventually(t, func() bool { return synced.Load() > 0 }, time.Second, 10*time.Millisecond)
			}
		})
	}
}

func TestUpstreamModelSyncDefaultDisabled(t *testing.T) {
	require.False(t, defaultUpstreamModelSyncConfig().enabled)
}

func TestUpstreamModelSyncUnchangedWakeKeepsDeadline(t *testing.T) {
	t.Setenv("UPSTREAM_MODEL_SYNC_ENABLED", "")
	t.Setenv("UPSTREAM_MODEL_SYNC_INTERVAL_HOURS", "")
	t.Setenv("UPSTREAM_MODEL_SYNC_ACCOUNT_TIMEOUT_SECONDS", "")
	s := newTestModelSyncSchedulerWithConfig(&upstreamModelSyncListerStub{accounts: []Account{{ID: 1}}}, nil,
		upstreamModelSyncConfig{true, 100 * time.Millisecond, time.Second})
	var synced atomic.Int32
	s.syncAccount = func(context.Context, *Account) error { synced.Add(1); return nil }
	s.Start()
	defer s.Stop()
	// Notifications keep arriving faster than the interval. Resetting on each
	// notification would starve the scheduled cycle throughout this window.
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(time.Second)
	for synced.Load() == 0 {
		select {
		case <-ticker.C:
			select {
			case s.wake <- struct{}{}:
			default:
			}
		case <-deadline:
			t.Fatal("unchanged settings postponed every cycle")
		}
	}
}

func TestUpstreamModelSyncReloadDetectsOnlySyncConfigChanges(t *testing.T) {
	t.Setenv("UPSTREAM_MODEL_SYNC_ENABLED", "")
	t.Setenv("UPSTREAM_MODEL_SYNC_INTERVAL_HOURS", "")
	t.Setenv("UPSTREAM_MODEL_SYNC_ACCOUNT_TIMEOUT_SECONDS", "")
	s := newTestModelSyncSchedulerWithConfig(nil, nil, defaultUpstreamModelSyncConfig())
	defer s.Stop()
	changed, err := s.reloadConfig()
	require.NoError(t, err)
	require.False(t, changed)
	for _, setting := range []struct{ key, value string }{
		{"UPSTREAM_MODEL_SYNC_ENABLED", "true"},
		{"UPSTREAM_MODEL_SYNC_INTERVAL_HOURS", "12"},
		{"UPSTREAM_MODEL_SYNC_ACCOUNT_TIMEOUT_SECONDS", "60"},
		{"UPSTREAM_MODEL_SYNC_ENABLED", "false"},
	} {
		t.Setenv(setting.key, setting.value)
		changed, err = s.reloadConfig()
		require.NoError(t, err)
		require.True(t, changed, setting.key)
		changed, err = s.reloadConfig()
		require.NoError(t, err)
		require.False(t, changed, setting.key)
	}
}

// Shared fake ownership makes cross-runner exclusion testable without Redis.
type modelSyncLockStub struct {
	mu          sync.Mutex
	owner       string
	err         error
	loseRenewal bool
	renewals    int
}

func (l *modelSyncLockStub) TryAcquireLeaderLock(_ context.Context, _, owner string, _ time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return false, l.err
	}
	if l.owner != "" {
		return false, nil
	}
	l.owner = owner
	return true, nil
}
func (l *modelSyncLockStub) ReleaseLeaderLock(_ context.Context, _, owner string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.owner == owner {
		l.owner = ""
	}
	return nil
}
func (l *modelSyncLockStub) RenewLeaderLock(_ context.Context, _, owner string, _ time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.renewals++
	return !l.loseRenewal && l.owner == owner, l.err
}
func newTestModelSyncScheduler(l upstreamModelSyncAccountLister, a *AccountTestService) *UpstreamModelSyncScheduler {
	s := NewUpstreamModelSyncScheduler(l, a, &modelSyncLockStub{})
	return s
}
func newTestModelSyncSchedulerWithConfig(l upstreamModelSyncAccountLister, a *AccountTestService, c upstreamModelSyncConfig) *UpstreamModelSyncScheduler {
	s := newUpstreamModelSyncScheduler(l, a, c)
	s.lockCache = &modelSyncLockStub{}
	return s
}

func TestModelSyncPaginationAndLease(t *testing.T) {
	lister := &upstreamModelSyncListerStub{}
	for id := int64(1); id <= 205; id++ {
		lister.accounts = append(lister.accounts, Account{ID: id})
	}
	s := newTestModelSyncScheduler(lister, nil)
	defer s.Stop()
	var visited []int64
	s.syncAccount = func(_ context.Context, a *Account) error { visited = append(visited, a.ID); return nil }
	require.NoError(t, s.RunOnce(context.Background()))
	require.Len(t, visited, 205)
	for i, id := range visited {
		require.Equal(t, int64(i+1), id)
	}
}

func TestModelSyncSkipsOverlappingRunnersAndTriggers(t *testing.T) {
	lister := &upstreamModelSyncListerStub{accounts: []Account{{ID: 1}}}
	first := newTestModelSyncScheduler(lister, nil)
	second := newTestModelSyncScheduler(lister, nil)
	defer first.Stop()
	defer second.Stop()
	second.lockCache = first.lockCache
	started, finish := make(chan struct{}), make(chan struct{})
	first.syncAccount = func(ctx context.Context, _ *Account) error {
		close(started)
		select {
		case <-finish:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var secondCalls atomic.Int32
	second.syncAccount = func(context.Context, *Account) error { secondCalls.Add(1); return nil }
	done := make(chan error, 1)
	go func() { done <- first.RunOnce(context.Background()) }()
	<-started
	require.NoError(t, first.RunOnce(context.Background()))
	require.NoError(t, second.RunOnce(context.Background()))
	require.Zero(t, secondCalls.Load())
	close(finish)
	require.NoError(t, <-done)
	require.NoError(t, second.RunOnce(context.Background()))
	require.EqualValues(t, 1, secondCalls.Load())
}

func TestModelSyncLeaseLossCancelsWork(t *testing.T) {
	s := newTestModelSyncScheduler(&upstreamModelSyncListerStub{accounts: []Account{{ID: 1}, {ID: 2}}}, nil)
	defer s.Stop()
	s.lockTTL = 30 * time.Millisecond
	s.lockCache = &modelSyncLockStub{loseRenewal: true}
	var visited []int64
	s.syncAccount = func(ctx context.Context, a *Account) error {
		visited = append(visited, a.ID)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorIs(t, s.RunOnce(ctx), context.Canceled)
	require.Equal(t, []int64{1}, visited)
}

func TestModelSyncLockFailureDoesNotRun(t *testing.T) {
	for _, lock := range []LeaderLockCache{nil, &modelSyncLockStub{err: errors.New("redis unavailable")}} {
		s := newTestModelSyncScheduler(&upstreamModelSyncListerStub{accounts: []Account{{ID: 1}}}, nil)
		s.lockCache = lock
		s.syncAccount = func(context.Context, *Account) error { t.Error("must not sync without ownership"); return nil }
		require.Error(t, s.RunOnce(context.Background()))
		s.Stop()
	}
}
