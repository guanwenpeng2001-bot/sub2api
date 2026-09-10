//go:build unit

package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestReactiveCooldownSettingsDefaultAndStoredValue(t *testing.T) {
	repo := newMockSettingRepo()
	svc := NewSettingService(repo, &config.Config{})

	settings, err := svc.GetReactiveCooldownSettings(context.Background())
	require.NoError(t, err)
	require.Equal(t, DefaultReactiveCooldownSettings(), settings)

	want := &ReactiveCooldownSettings{
		PlanGatedMinutes:            15,
		ModelNotFoundMinutes:        20,
		OpenAI403Minutes:            5,
		KimiConcurrencyLimitSeconds: 45,
		ImageCapabilityLossMinutes:  12,
	}
	require.NoError(t, svc.SetReactiveCooldownSettings(context.Background(), want))
	settings, err = svc.GetReactiveCooldownSettings(context.Background())
	require.NoError(t, err)
	require.Equal(t, want, settings)
}

func TestSetReactiveCooldownSettingsBoundaries(t *testing.T) {
	svc := NewSettingService(newMockSettingRepo(), &config.Config{})
	valid := DefaultReactiveCooldownSettings()
	require.NoError(t, svc.SetReactiveCooldownSettings(context.Background(), valid))

	invalid := *valid
	invalid.KimiConcurrencyLimitSeconds = 0
	require.ErrorContains(t, svc.SetReactiveCooldownSettings(context.Background(), &invalid), "kimi_concurrency_limit_seconds")
	invalid = *valid
	invalid.PlanGatedMinutes = maxReactiveCooldownMinutes + 1
	require.ErrorContains(t, svc.SetReactiveCooldownSettings(context.Background(), &invalid), "plan_gated_minutes")
}

func TestGetReactiveCooldownSettingsNormalizesCorruptValues(t *testing.T) {
	repo := newMockSettingRepo()
	svc := NewSettingService(repo, &config.Config{})
	data, err := json.Marshal(ReactiveCooldownSettings{PlanGatedMinutes: -1, OpenAI403Minutes: 99999})
	require.NoError(t, err)
	repo.data[SettingKeyReactiveCooldownSettings] = string(data)

	settings, err := svc.GetReactiveCooldownSettings(context.Background())
	require.NoError(t, err)
	require.Equal(t, DefaultReactiveCooldownSettings(), settings)
}

func TestRateLimitServiceUsesConfiguredReactiveCooldowns(t *testing.T) {
	repo := &rateLimitAccountRepoStub{}
	settingRepo := newMockSettingRepo()
	settingSvc := NewSettingService(settingRepo, &config.Config{})
	require.NoError(t, settingSvc.SetReactiveCooldownSettings(context.Background(), &ReactiveCooldownSettings{
		PlanGatedMinutes:            7,
		ModelNotFoundMinutes:        8,
		OpenAI403Minutes:            3,
		KimiConcurrencyLimitSeconds: 12,
		ImageCapabilityLossMinutes:  9,
	}))
	svc := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
	svc.SetSettingService(settingSvc)

	require.Equal(t, 7*time.Minute, svc.planGatedCooldown(context.Background()))
	require.Equal(t, 8*time.Minute, svc.modelNotFoundCooldown(context.Background()))
	require.Equal(t, 3*time.Minute, svc.openAI403Cooldown(context.Background()))
	require.Equal(t, 12*time.Second, svc.kimiConcurrencyLimitCooldown(context.Background()))
	require.Equal(t, 9*time.Minute, svc.imageCapabilityLossCooldown(context.Background()))
}
