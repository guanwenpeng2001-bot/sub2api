package service

import (
	"context"
	"errors"
	"net/http"
	"testing"

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
