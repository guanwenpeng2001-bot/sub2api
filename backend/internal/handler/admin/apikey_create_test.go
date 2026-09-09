package admin_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	adminhandler "github.com/Wei-Shaw/sub2api/internal/handler/admin"
	"github.com/Wei-Shaw/sub2api/internal/handler/dto"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/server/routes"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type mintKeyRepo struct {
	service.APIKeyRepository
	keys    []service.APIKey
	creates int
	err     error
}

func (r *mintKeyRepo) Create(_ context.Context, key *service.APIKey) error {
	r.creates++
	if r.err != nil {
		return r.err
	}
	key.ID = int64(len(r.keys) + 1)
	r.keys = append(r.keys, *key)
	return nil
}

type mintUserRepo struct {
	service.UserRepository
	target *service.User
	err    error
	reads  int
}

func (r *mintUserRepo) GetByID(_ context.Context, id int64) (*service.User, error) {
	if id == 1 {
		return &service.User{ID: 1, Role: service.RoleAdmin, Status: service.StatusActive}, nil
	}
	if id == 2 {
		return &service.User{ID: 2, Role: service.RoleUser, Status: service.StatusActive}, nil
	}
	r.reads++
	if r.err != nil {
		return nil, r.err
	}
	if id != r.target.ID {
		return nil, service.ErrUserNotFound
	}
	return r.target, nil
}
func (r *mintUserRepo) GetUserAvatar(_ context.Context, _ int64) (*service.UserAvatar, error) {
	return nil, nil
}

func (r *mintUserRepo) GetFirstAdmin(ctx context.Context) (*service.User, error) {
	return r.GetByID(ctx, 1)
}

type mintGroupRepo struct {
	service.GroupRepository
	group *service.Group
	err   error
}

func (r *mintGroupRepo) GetByID(_ context.Context, id int64) (*service.Group, error) {
	if r.err != nil {
		return nil, r.err
	}
	if id != r.group.ID {
		return nil, service.ErrGroupNotFound
	}
	return r.group, nil
}

type mintSubscriptionRepo struct {
	service.UserSubscriptionRepository
	err             error
	userID, groupID int64
	calls           int
}

func (r *mintSubscriptionRepo) GetActiveByUserIDAndGroupID(_ context.Context, userID, groupID int64) (*service.UserSubscription, error) {
	r.calls++
	r.userID, r.groupID = userID, groupID
	if r.err != nil {
		return nil, r.err
	}
	return &service.UserSubscription{UserID: userID, GroupID: groupID}, nil
}

type mintSettingRepo struct{ service.SettingRepository }

func (*mintSettingRepo) GetValue(_ context.Context, key string) (string, error) {
	if key == service.SettingKeyAdminAPIKey {
		return "test-admin-key", nil
	}
	return "false", nil
}

type mintAdminService struct {
	service.AdminService
	repo *mintKeyRepo
}

func (s *mintAdminService) GetUserAPIKeys(_ context.Context, userID int64, _, _ int, _, _ string) ([]service.APIKey, int64, error) {
	keys := []service.APIKey{}
	for _, key := range s.repo.keys {
		if key.UserID == userID {
			keys = append(keys, key)
		}
	}
	return keys, int64(len(keys)), nil
}

type mintFixture struct {
	router                *gin.Engine
	keys                  *mintKeyRepo
	users                 *mintUserRepo
	groups                *mintGroupRepo
	subs                  *mintSubscriptionRepo
	adminToken, userToken string
}

func newMintFixture(t *testing.T) *mintFixture {
	t.Helper()
	gin.SetMode(gin.TestMode)
	f := &mintFixture{
		keys:   &mintKeyRepo{},
		users:  &mintUserRepo{target: &service.User{ID: 42, Role: service.RoleUser, Status: service.StatusActive}},
		groups: &mintGroupRepo{group: &service.Group{ID: 7, Status: service.StatusActive}},
		subs:   &mintSubscriptionRepo{},
	}
	cfg := &config.Config{JWT: config.JWTConfig{Secret: "mint-test-secret", ExpireHour: 1}}
	auth := service.NewAuthService(nil, nil, nil, nil, cfg, nil, nil, nil, nil, nil, nil, nil, nil)
	var err error
	f.adminToken, err = auth.GenerateToken(context.Background(), &service.User{ID: 1, Role: service.RoleAdmin})
	require.NoError(t, err)
	f.userToken, err = auth.GenerateToken(context.Background(), &service.User{ID: 2, Role: service.RoleUser})
	require.NoError(t, err)
	settings := service.NewSettingService(&mintSettingRepo{}, cfg)
	users := service.NewUserService(f.users, nil, nil, nil)
	keys := service.NewAPIKeyService(f.keys, f.users, f.groups, f.subs, nil, nil, cfg)
	adminSvc := &mintAdminService{repo: f.keys}
	h := &handler.Handlers{Admin: &handler.AdminHandlers{
		APIKey: adminhandler.NewAdminAPIKeyHandler(adminSvc, keys),
		User:   adminhandler.NewUserHandler(adminSvc, nil, nil, nil, nil, nil, nil),
	}}
	f.router = gin.New()
	routes.RegisterAdminRoutes(f.router.Group("/api/v1"), h,
		middleware.NewAdminAuthMiddleware(auth, users, settings, nil),
		middleware.AuditLogMiddleware(func(c *gin.Context) { c.Next() }),
		middleware.StepUpAuthMiddleware(func(c *gin.Context) { c.Next() }), nil, nil)
	return f
}
func (f *mintFixture) request(method, id, body, auth string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/api/v1/admin/users/"+id+"/api-keys", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if strings.HasPrefix(auth, "Bearer ") {
		req.Header.Set("Authorization", auth)
	} else if auth != "" {
		req.Header.Set("x-api-key", auth)
	}
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)
	return rec
}

func TestAdminCreateAPIKey_InvalidInputDoesNotCreate(t *testing.T) {
	cases := []struct{ name, id, body string }{
		{"zero user", "0", `{"name":"key"}`},
		{"negative user", "-1", `{"name":"key"}`},
		{"non-numeric user", "abc", `{"name":"key"}`},
		{"overflow user", "9223372036854775808", `{"name":"key"}`},
		{"zero group", "42", `{"name":"key","group_id":0}`},
		{"negative group", "42", `{"name":"key","group_id":-1}`},
		{"string group", "42", `{"name":"key","group_id":"7"}`},
		{"fraction group", "42", `{"name":"key","group_id":1.5}`},
		{"overflow group", "42", `{"name":"key","group_id":9223372036854775808}`},
		{"missing name", "42", `{}`},
		{"empty name", "42", `{"name":""}`},
		{"long name", "42", `{"name":"` + strings.Repeat("a", 101) + `"}`},
		{"invalid json", "42", `{`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newMintFixture(t)
			rec := f.request(http.MethodPost, tc.id, tc.body, "test-admin-key")
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			require.Zero(t, f.users.reads)
			require.Zero(t, f.keys.creates)
			require.Empty(t, f.keys.keys)
		})
	}
}

func TestAdminCreateAPIKey_SuccessAndAdminReadBack(t *testing.T) {
	for _, tc := range []struct {
		name, body                            string
		grouped, exclusive, subscription, jwt bool
	}{
		{name: "omitted group", body: `{"name":"key"}`},
		{name: "null group", body: `{"name":"key","group_id":null}`},
		{name: "standard group", body: `{"name":"key","group_id":7}`, grouped: true},
		{name: "allowed exclusive group", body: `{"name":"key","group_id":7}`, grouped: true, exclusive: true},
		{name: "active subscription", body: `{"name":"key","group_id":7}`, grouped: true, subscription: true},
		{name: "admin JWT", body: `{"name":"key"}`, jwt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMintFixture(t)
			if tc.exclusive {
				f.groups.group.IsExclusive = true
				f.users.target.AllowedGroups = []int64{7}
			}
			if tc.subscription {
				f.groups.group.SubscriptionType = "subscription"
			}
			auth := "test-admin-key"
			if tc.jwt {
				auth = "Bearer " + f.adminToken
			}
			rec := f.request(http.MethodPost, "42", tc.body, auth)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var created struct {
				Code int        `json:"code"`
				Data dto.APIKey `json:"data"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
			require.Zero(t, created.Code)
			require.Equal(t, int64(42), created.Data.UserID)
			require.Equal(t, "key", created.Data.Name)
			require.Equal(t, service.StatusActive, created.Data.Status)
			require.Regexp(t, `^sk-[0-9a-f]{64}$`, created.Data.Key)
			require.Equal(t, 1, f.keys.creates)
			require.Len(t, f.keys.keys, 1)
			require.Equal(t, f.keys.keys[0].ID, created.Data.ID)
			require.Equal(t, f.keys.keys[0].Key, created.Data.Key)
			if tc.grouped {
				require.NotNil(t, created.Data.GroupID)
				require.Equal(t, int64(7), *created.Data.GroupID)
			} else {
				require.Nil(t, created.Data.GroupID)
				require.Nil(t, f.keys.keys[0].GroupID)
			}
			if tc.subscription {
				require.Equal(t, 1, f.subs.calls)
				require.Equal(t, int64(42), f.subs.userID)
				require.Equal(t, int64(7), f.subs.groupID)
			}
			rec = f.request(http.MethodGet, "42", "", auth)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var listed struct {
				Data struct {
					Items []dto.APIKey `json:"items"`
					Total int64        `json:"total"`
				} `json:"data"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listed))
			require.Equal(t, int64(1), listed.Data.Total)
			require.Len(t, listed.Data.Items, 1)
			require.Equal(t, created.Data, listed.Data.Items[0])
		})
	}
}

func TestAdminCreateAPIKey_BusinessErrorsDoNotCreate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		setup   func(*mintFixture)
		status  int
		reason  string
		creates int
	}{
		{"missing user", func(f *mintFixture) { f.users.err = service.ErrUserNotFound }, 404, "USER_NOT_FOUND", 0},
		{"user lookup failure", func(f *mintFixture) { f.users.err = errors.New("private backend failure") }, 500, "", 0},
		{"missing group", func(f *mintFixture) { f.groups.err = service.ErrGroupNotFound }, 404, "GROUP_NOT_FOUND", 0},
		{"group lookup failure", func(f *mintFixture) { f.groups.err = errors.New("private backend failure") }, 500, "", 0},
		{"exclusive group denied", func(f *mintFixture) { f.groups.group.IsExclusive = true }, 403, "GROUP_NOT_ALLOWED", 0},
		{"missing or expired subscription", func(f *mintFixture) {
			f.groups.group.SubscriptionType = "subscription"
			f.subs.err = service.ErrSubscriptionNotFound
		}, 403, "GROUP_NOT_ALLOWED", 0},
		{"subscription lookup failure", func(f *mintFixture) {
			f.groups.group.SubscriptionType = "subscription"
			f.subs.err = errors.New("private backend failure")
		}, 403, "GROUP_NOT_ALLOWED", 0},
		{"create failure", func(f *mintFixture) { f.keys.err = errors.New("private backend failure") }, 500, "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMintFixture(t)
			tc.setup(f)
			rec := f.request(http.MethodPost, "42", `{"name":"key","group_id":7}`, "test-admin-key")
			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			var result struct {
				Code   int    `json:"code"`
				Reason string `json:"reason"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
			require.Equal(t, tc.status, result.Code)
			if tc.reason != "" {
				require.Equal(t, tc.reason, result.Reason)
			}
			require.NotContains(t, rec.Body.String(), "private backend failure")
			require.Equal(t, tc.creates, f.keys.creates)
			require.Empty(t, f.keys.keys)
		})
	}
}

func TestAdminAPIKeyRoutes_RejectUnauthorizedIdentities(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		for _, identity := range []string{"anonymous", "ordinary JWT", "invalid JWT", "ordinary API key"} {
			t.Run(method+"/"+identity, func(t *testing.T) {
				f := newMintFixture(t)
				auth := ""
				status := http.StatusUnauthorized
				code := "UNAUTHORIZED"
				switch identity {
				case "ordinary JWT":
					auth = "Bearer " + f.userToken
					status = http.StatusForbidden
					code = "FORBIDDEN"
				case "invalid JWT":
					auth = "Bearer invalid-token"
					code = "INVALID_TOKEN"
				case "ordinary API key":
					auth = "sk-user-key"
					code = "INVALID_ADMIN_KEY"
				}
				rec := f.request(method, "42", `{"name":"key"}`, auth)
				require.Equal(t, status, rec.Code, rec.Body.String())
				require.Contains(t, rec.Body.String(), code)
				require.Zero(t, f.users.reads)
				require.Zero(t, f.keys.creates)
				require.Empty(t, f.keys.keys)
			})
		}
	}
}
