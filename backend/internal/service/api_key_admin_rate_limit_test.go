package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

type adminMintUserRepo struct{ UserRepository }

func (*adminMintUserRepo) GetByID(context.Context, int64) (*User, error) { return &User{ID: 42}, nil }

type adminMintKeyRepo struct {
	APIKeyRepository
	exists          bool
	checks, creates int
}

func (r *adminMintKeyRepo) ExistsByKey(context.Context, string) (bool, error) {
	r.checks++
	return r.exists, nil
}
func (r *adminMintKeyRepo) Create(_ context.Context, k *APIKey) error {
	r.creates++
	k.ID = 1
	return nil
}

type adminMintCache struct {
	APIKeyCache
	count, reads, increments int
}

func (c *adminMintCache) GetCreateAttemptCount(context.Context, int64) (int, error) {
	c.reads++
	return c.count, nil
}
func (c *adminMintCache) IncrementCreateAttemptCount(context.Context, int64) error {
	c.increments++
	return nil
}
func (*adminMintCache) DeleteAuthCache(context.Context, string) error              { return nil }
func (*adminMintCache) PublishAuthCacheInvalidation(context.Context, string) error { return nil }

func TestAdminMintCustomKeyRateLimitIsolation(t *testing.T) {
	for _, admin := range []bool{false, true} {
		for _, exists := range []bool{false, true} {
			for _, count := range []int{0, 20} {
				repo := &adminMintKeyRepo{exists: exists}
				cache := &adminMintCache{count: count}
				s := &APIKeyService{apiKeyRepo: repo, userRepo: &adminMintUserRepo{}, cache: cache}
				credential := "sk-0123456789abcdef0123456789abcdef"
				_, err := s.Create(context.Background(), 42, CreateAPIKeyRequest{Name: "test", CustomKey: &credential, SkipCustomKeyRateLimit: admin})
				if !admin && count == 20 {
					require.ErrorIs(t, err, ErrAPIKeyRateLimited)
					require.Zero(t, repo.checks)
				} else if exists {
					require.ErrorIs(t, err, ErrAPIKeyExists)
				} else {
					require.NoError(t, err)
					require.Equal(t, 1, repo.creates)
				}
				if admin {
					require.Zero(t, cache.reads)
					require.Zero(t, cache.increments)
					require.Equal(t, 1, repo.checks)
				} else if exists && count == 0 {
					require.Equal(t, 1, cache.increments)
				}
			}
		}
	}
}
func TestCustomKeyRateLimitBypassCannotBeBoundFromJSON(t *testing.T) {
	var req CreateAPIKeyRequest
	require.NoError(t, json.Unmarshal([]byte(`{"SkipCustomKeyRateLimit":true,"skip_custom_key_rate_limit":true}`), &req))
	require.False(t, req.SkipCustomKeyRateLimit)
}
