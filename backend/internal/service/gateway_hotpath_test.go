package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	gocache "github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/require"
)

type fixQSnapshotCache struct {
	SchedulerCache
	accounts []*Account
	calls    int
	buckets  []SchedulerBucket
}

func (c *fixQSnapshotCache) GetSnapshot(_ context.Context, b SchedulerBucket) ([]*Account, bool, error) {
	c.calls++
	c.buckets = append(c.buckets, b)
	return c.accounts, true, nil
}

func TestFixQModelsReadsSnapshotAfterListCacheExpiry(t *testing.T) {
	for _, platform := range []string{PlatformOpenAI, PlatformAnthropic, PlatformGemini, PlatformGrok, PlatformKimi, PlatformDeepseek} {
		t.Run(platform, func(t *testing.T) {
			cache := &fixQSnapshotCache{accounts: []*Account{{ID: 1, Platform: platform, Type: AccountTypeAPIKey, Credentials: map[string]any{"model_mapping": map[string]any{"alias": "upstream"}}}}}
			svc := &GatewayService{schedulerSnapshot: &SchedulerSnapshotService{cache: cache}, modelsListCache: gocache.New(time.Minute, time.Minute), modelsListCacheTTL: time.Minute}
			groupID := int64(7)
			first := svc.GetAvailableModels(context.Background(), &groupID, platform)
			require.Contains(t, first, "alias")
			first[0] = "caller mutation"
			require.NotContains(t, svc.GetAvailableModels(context.Background(), &groupID, platform), "caller mutation")
			require.Equal(t, 1, cache.calls)
			// Delete simulates list TTL expiry; a nil accountRepo makes any SQL path panic.
			svc.modelsListCache.Delete(modelsListCacheKey(&groupID, platform))
			require.Contains(t, svc.GetAvailableModels(context.Background(), &groupID, platform), "alias")
			require.Equal(t, 2, cache.calls)
			require.Equal(t, SchedulerBucket{GroupID: groupID, Platform: platform, Mode: SchedulerModeForced}, cache.buckets[0])
		})
	}
	require.GreaterOrEqual(t, defaultModelsListCacheTTL, 30*time.Second)
}

func TestFixQModelsSnapshotPassthroughKeepsDefaultFallback(t *testing.T) {
	cache := &fixQSnapshotCache{accounts: []*Account{{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"model_mapping": map[string]any{"stale": "stale"}}, Extra: map[string]any{"openai_passthrough": true}}}}
	svc := &GatewayService{schedulerSnapshot: &SchedulerSnapshotService{cache: cache}}
	id := int64(1)
	require.Nil(t, svc.GetAvailableModels(context.Background(), &id, PlatformOpenAI))
}

type fixQRPMRepo struct {
	UserGroupRateRepository
	calls    int
	override *int
	err      error
}

func (r *fixQRPMRepo) GetRPMOverrideByUserAndGroup(context.Context, int64, int64) (*int, error) {
	r.calls++
	return r.override, r.err
}

type fixQRPMCache struct {
	UserRPMCache
	groups, users int
}

func (r *fixQRPMCache) IncrementUserGroupRPM(context.Context, int64, int64) (int, error) {
	r.groups++
	return r.groups, nil
}
func (r *fixQRPMCache) IncrementUserRPM(context.Context, int64) (int, error) {
	r.users++
	return r.users, nil
}

func TestFixQRPMAuthSnapshotCachesAbsence(t *testing.T) {
	id := int64(10)
	repo := &fixQRPMRepo{}
	auth := &APIKeyService{userGroupRateRepo: repo}
	snapshot := auth.snapshotFromAPIKey(context.Background(), &APIKey{ID: 1, UserID: 2, GroupID: &id, User: &User{ID: 2, RPMLimit: 10}})
	require.Equal(t, 1, repo.calls)
	require.True(t, snapshot.User.UserGroupRPMOverrideLoaded)
	require.Nil(t, snapshot.User.UserGroupRPMOverride)
	// Exercise the Redis JSON representation and materialization, not just structs.
	raw, err := json.Marshal(snapshot)
	require.NoError(t, err)
	var decoded APIKeyAuthSnapshot
	require.NoError(t, json.Unmarshal(raw, &decoded))
	user := auth.snapshotToAPIKey("test-key", &decoded).User
	cache := &fixQRPMCache{}
	billing := &BillingCacheService{userRPMCache: cache, userGroupRateRepo: repo}
	group := &Group{ID: id, RPMLimit: 2}
	require.NoError(t, billing.checkRPM(context.Background(), user, group))
	require.NoError(t, billing.checkRPM(context.Background(), user, group))
	require.ErrorIs(t, billing.checkRPM(context.Background(), user, group), ErrGroupRPMExceeded)
	require.Equal(t, 1, repo.calls, "negative snapshot must not query SQL per hop")
	require.Equal(t, 3, cache.groups, "nil override must retain the group RPM limit")
}

func TestFixQRPMFailedOldAndDifferentGroupSnapshotsRetry(t *testing.T) {
	id := int64(10)
	for _, scenario := range []string{"failed", "old", "different-group"} {
		t.Run(scenario, func(t *testing.T) {
			repo := &fixQRPMRepo{}
			user := &User{ID: 2}
			if scenario == "failed" {
				repo.err = errors.New("lookup unavailable")
				auth := &APIKeyService{userGroupRateRepo: repo}
				snapshot := auth.snapshotFromAPIKey(context.Background(), &APIKey{UserID: 2, GroupID: &id, User: user})
				require.False(t, snapshot.User.UserGroupRPMOverrideLoaded)
				user = auth.snapshotToAPIKey("test", snapshot).User
				repo.err = nil
				repo.calls = 0
			}
			if scenario == "different-group" {
				zero := 0
				user.UserGroupRPMOverride = &zero
				user.UserGroupRPMOverrideLoaded = true
				user.UserGroupRPMOverrideGroupID = id + 1
			}
			cache := &fixQRPMCache{}
			svc := &BillingCacheService{userRPMCache: cache, userGroupRateRepo: repo}
			require.NoError(t, svc.checkRPM(context.Background(), user, &Group{ID: id, RPMLimit: 2}))
			require.Equal(t, 1, repo.calls)
			require.Equal(t, 1, cache.groups)
		})
	}
}

func TestFixQRPMZeroOverrideStillChecksGlobalLimit(t *testing.T) {
	zero := 0
	repo := &fixQRPMRepo{}
	cache := &fixQRPMCache{}
	svc := &BillingCacheService{userRPMCache: cache, userGroupRateRepo: repo}
	user := &User{ID: 2, RPMLimit: 1, UserGroupRPMOverride: &zero, UserGroupRPMOverrideLoaded: true, UserGroupRPMOverrideGroupID: 10}
	group := &Group{ID: 10, RPMLimit: 1}
	require.NoError(t, svc.checkRPM(context.Background(), user, group))
	require.ErrorIs(t, svc.checkRPM(context.Background(), user, group), ErrUserRPMExceeded)
	require.Zero(t, repo.calls)
	require.Zero(t, cache.groups)
}

type fixQGroupRepo struct {
	GroupRepository
	group                *Group
	liteCalls, fullCalls int
}

func (r *fixQGroupRepo) GetByID(context.Context, int64) (*Group, error) {
	r.fullCalls++
	return nil, errors.New("full group query is forbidden")
}
func (r *fixQGroupRepo) GetByIDLite(context.Context, int64) (*Group, error) {
	r.liteCalls++
	return r.group, nil
}

func TestFixQLegacySelectionReusesLiteGroup(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		for _, cached := range []bool{false, true} {
			group := &Group{ID: 7, Hydrated: true, Status: StatusActive, Platform: PlatformAnthropic, RequirePrivacySet: true}
			repo := &fixQGroupRepo{group: group}
			svc := &GatewayService{groupRepo: repo, schedulerSnapshot: &SchedulerSnapshotService{cache: &fixQSnapshotCache{}}}
			ctx := context.Background()
			if cached {
				ctx = svc.withGroupContext(ctx, group)
			}
			if mixed {
				_, _ = svc.selectAccountWithMixedScheduling(ctx, &group.ID, "", "", nil, PlatformAnthropic)
			} else {
				_, _ = svc.selectAccountForModelWithPlatform(ctx, &group.ID, "", "", nil, PlatformAnthropic)
			}
			require.Zero(t, repo.fullCalls)
			if cached {
				require.Zero(t, repo.liteCalls)
			} else {
				require.Equal(t, 1, repo.liteCalls)
			}
		}
	}
}

type fixQDiscoveryProjection struct {
	AccountRepository
	calls    int
	accounts []Account
}

func (r *fixQDiscoveryProjection) ListModelDiscoveryAccounts(context.Context, *int64, string) ([]Account, error) {
	r.calls++
	return r.accounts, nil
}

func TestFixQUnscopedAndCompositeDiscoveryUseProjection(t *testing.T) {
	repo := &fixQDiscoveryProjection{accounts: []Account{{Platform: PlatformAnthropic, Credentials: map[string]any{"model_mapping": map[string]any{"alias": "upstream"}}}}}
	svc := &GatewayService{accountRepo: repo, modelsListCache: gocache.New(time.Minute, time.Minute), modelsListCacheTTL: time.Minute}
	require.Equal(t, []string{"alias"}, svc.GetAvailableModels(context.Background(), nil, ""))
	id := int64(7)
	require.Contains(t, svc.GetSchedulablePlatforms(context.Background(), &id), PlatformAnthropic)
	require.Contains(t, svc.GetSchedulablePlatforms(context.Background(), &id), PlatformAnthropic)
	require.Equal(t, 2, repo.calls)
	repo.accounts = []Account{{Platform: PlatformGemini}}
	svc.modelsListCache.Flush()
	require.Contains(t, svc.GetSchedulablePlatforms(context.Background(), &id), PlatformGemini)
	require.Equal(t, 3, repo.calls)
}

// Discovery errors must remain errors, while cached emptiness must retain its source.
type catalogSourceRepo struct {
	AccountRepository
	accounts []Account
	err      error
	calls    int
}

func (r *catalogSourceRepo) ListModelDiscoveryAccounts(context.Context, *int64, string) ([]Account, error) {
	r.calls++
	return r.accounts, r.err
}
func TestAvailableModelCatalog_CacheSourcesAndRecovery(t *testing.T) {
	for _, platform := range []string{PlatformKimi, PlatformZhipu, PlatformDeepseek, PlatformMiniMax, PlatformOpenAI} {
		t.Run(platform, func(t *testing.T) {
			repo := &catalogSourceRepo{err: errors.New("query failed")}
			svc := &GatewayService{accountRepo: repo, modelsListCache: gocache.New(time.Minute, 0), modelsListCacheTTL: time.Minute}
			groupID := int64(7)
			catalog, err := svc.GetAvailableModelCatalog(context.Background(), &groupID, platform)
			require.ErrorIs(t, err, repo.err)
			require.Equal(t, "query_failed", catalog.Source)
			repo.err = nil
			for i := 0; i < 2; i++ {
				catalog, err = svc.GetAvailableModelCatalog(context.Background(), &groupID, platform)
				require.NoError(t, err)
				require.Equal(t, "authoritative_empty", catalog.Source)
				require.Empty(t, catalog.Models)
			}
			require.Equal(t, 2, repo.calls)
			repo.accounts = []Account{{ID: 1, Platform: platform}}
			svc.modelsListCache.Flush()
			for i := 0; i < 2; i++ {
				catalog, err = svc.GetAvailableModelCatalog(context.Background(), &groupID, platform)
				require.NoError(t, err)
				want := "authoritative_empty"
				if platform == PlatformOpenAI {
					want = "static_default"
				}
				require.Equal(t, want, catalog.Source)
			}
			require.Equal(t, 3, repo.calls)
			repo.accounts[0].Credentials = map[string]any{"model_mapping": map[string]any{"alias": "upstream"}}
			svc.modelsListCache.Flush()
			catalog, err = svc.GetAvailableModelCatalog(context.Background(), &groupID, platform)
			require.NoError(t, err)
			require.Equal(t, "account_mapping", catalog.Source)
			require.Equal(t, []string{"alias"}, catalog.Models)
			catalog.Models[0] = "caller mutation"
			catalog, err = svc.GetAvailableModelCatalog(context.Background(), &groupID, platform)
			require.NoError(t, err)
			require.Equal(t, []string{"alias"}, catalog.Models)
			require.Equal(t, 4, repo.calls)
		})
	}
}
