package admin

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler/dto"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

// AdminAPIKeyHandler handles admin API key management
type AdminAPIKeyHandler struct {
	config        *config.Config
	adminService  service.AdminService
	apiKeyService *service.APIKeyService
}

// NewAdminAPIKeyHandler creates a new admin API key handler
func NewAdminAPIKeyHandler(adminService service.AdminService, apiKeyService *service.APIKeyService, cfg *config.Config) *AdminAPIKeyHandler {
	return &AdminAPIKeyHandler{
		config:        cfg,
		adminService:  adminService,
		apiKeyService: apiKeyService,
	}
}

// AdminCreateUserAPIKeyRequest represents the request to mint an API key
// for a user, admin-side.
type AdminCreateUserAPIKeyRequest struct {
	Name    string `json:"name" binding:"required,max=100"`
	GroupID *int64 `json:"group_id" binding:"omitempty,gt=0"`
}

// CreateUserAPIKey mints an API key for the given user without logging in
// as that user — provisioning flows (e.g. cumora's per-signup mirroring)
// run purely on the admin x-api-key and never touch /auth/*, which keeps
// them working when Turnstile gates the auth endpoints.
//
// POST /api/v1/admin/users/:id/api-keys
//
// The plaintext key is returned in this response and remains available through
// the existing admin GET /api/v1/admin/users/:id/api-keys endpoint.
func (h *AdminAPIKeyHandler) CreateUserAPIKey(c *gin.Context) {
	userID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || userID <= 0 {
		response.BadRequest(c, "Invalid user ID")
		return
	}

	var req AdminCreateUserAPIKeyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}

	idempotencyKey, err := service.NormalizeIdempotencyKey(c.GetHeader("Idempotency-Key"))
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	if idempotencyKey == "" {
		key, createErr := h.apiKeyService.Create(c.Request.Context(), userID, service.CreateAPIKeyRequest{
			Name: req.Name, GroupID: req.GroupID,
		})
		if createErr != nil {
			response.ErrorFrom(c, createErr)
			return
		}
		response.Success(c, dto.APIKeyFromService(key))
		return
	}
	coordinator := service.DefaultIdempotencyCoordinator()
	if coordinator == nil || h.config == nil {
		response.ErrorFrom(c, service.ErrIdempotencyStoreUnavail)
		return
	}
	scope := "admin.user-api-key:" + strconv.FormatInt(userID, 10)
	// Successful replays use the stored row ID, never a newly derived credential.
	find := func(ctx context.Context, credential string) (*service.APIKey, error) {
		key, err := h.apiKeyService.GetStoredByKey(ctx, credential)
		if errors.Is(err, service.ErrAPIKeyNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if key == nil || key.UserID != userID {
			return nil, service.ErrAPIKeyExists
		}
		return key, nil
	}
	result, err := coordinator.Execute(c.Request.Context(), service.IdempotencyExecuteOptions{
		Scope: scope, ActorScope: scope, Method: c.Request.Method,
		Route: c.FullPath(), IdempotencyKey: idempotencyKey, Payload: req,
		RequireKey: true, ReclaimExpiredProcessing: true, RetainSucceeded: true, TTL: service.DefaultWriteIdempotencyTTL(),
	}, func(ctx context.Context) (any, error) {
		secret := strings.TrimSpace(h.config.APIKeyIdemHMACSecret)
		if secret == "" {
			if h.config.JWT.Secret == "" {
				return nil, service.ErrIdempotencyStoreUnavail
			}
			derived := hmac.New(sha256.New, []byte(h.config.JWT.Secret))
			derived.Write([]byte("sub2api/api-key-idempotency/hmac/v1"))
			secret = string(derived.Sum(nil))
		}
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(scope + ":" + idempotencyKey))
		credential := "sk-" + hex.EncodeToString(mac.Sum(nil))
		key, findErr := find(ctx, credential)
		if findErr != nil {
			return nil, findErr
		}
		if key == nil {
			key, findErr = h.apiKeyService.Create(ctx, userID, service.CreateAPIKeyRequest{
				Name: req.Name, GroupID: req.GroupID, CustomKey: &credential, SkipCustomKeyRateLimit: true,
			})
			if findErr != nil {
				// A duplicate writer or a lost database response may have won.
				recovered, recoveryErr := find(ctx, credential)
				if recoveryErr != nil || recovered == nil {
					return nil, findErr
				}
				key = recovered
			}
		}
		snapshot := dto.APIKeyFromService(key)
		snapshot.Key = ""
		return snapshot, nil
	})
	if err != nil {
		if retryAfter := service.RetryAfterSecondsFromError(err); retryAfter > 0 {
			c.Header("Retry-After", strconv.Itoa(retryAfter))
		}
		response.ErrorFrom(c, err)
		return
	}
	// Restore the secret from its owning row, never from the redacted cache.
	raw, err := json.Marshal(result.Data)
	if err != nil {
		response.ErrorFrom(c, service.ErrIdempotencyStoreUnavail)
		return
	}
	var snapshot dto.APIKey
	if err = json.Unmarshal(raw, &snapshot); err != nil {
		response.ErrorFrom(c, service.ErrIdempotencyStoreUnavail)
		return
	}
	key, err := h.apiKeyService.GetByID(c.Request.Context(), snapshot.ID)
	if err != nil || key == nil || key.UserID != userID {
		response.ErrorFrom(c, service.ErrIdempotencyStoreUnavail)
		return
	}
	snapshot.Key = key.Key
	if result.Replayed {
		c.Header("X-Idempotency-Replayed", "true")
	}
	response.Success(c, &snapshot)

}

// AdminUpdateAPIKeyGroupRequest represents the request to update an API key.
type AdminUpdateAPIKeyGroupRequest struct {
	GroupID             *int64 `json:"group_id"`               // nil=不修改, 0=解绑, >0=绑定到目标分组
	ResetRateLimitUsage *bool  `json:"reset_rate_limit_usage"` // true=重置 5h/1d/7d 限速用量
}

// UpdateGroup handles updating an API key's admin-managed fields.
// PUT /api/v1/admin/api-keys/:id
func (h *AdminAPIKeyHandler) UpdateGroup(c *gin.Context) {
	keyID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.BadRequest(c, "Invalid API key ID")
		return
	}

	var req AdminUpdateAPIKeyGroupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}

	var resetKey *service.APIKey
	if req.ResetRateLimitUsage != nil && *req.ResetRateLimitUsage {
		resetKey, err = h.adminService.AdminResetAPIKeyRateLimitUsage(c.Request.Context(), keyID)
		if err != nil {
			response.ErrorFrom(c, err)
			return
		}
	}

	result, err := h.adminService.AdminUpdateAPIKeyGroupID(c.Request.Context(), keyID, req.GroupID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	if resetKey != nil && req.GroupID == nil {
		result.APIKey = resetKey
	}

	resp := struct {
		APIKey                 *dto.APIKey `json:"api_key"`
		AutoGrantedGroupAccess bool        `json:"auto_granted_group_access"`
		GrantedGroupID         *int64      `json:"granted_group_id,omitempty"`
		GrantedGroupName       string      `json:"granted_group_name,omitempty"`
	}{
		APIKey:                 dto.APIKeyFromService(result.APIKey),
		AutoGrantedGroupAccess: result.AutoGrantedGroupAccess,
		GrantedGroupID:         result.GrantedGroupID,
		GrantedGroupName:       result.GrantedGroupName,
	}
	response.Success(c, resp)
}
