//go:build unit

package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestHandleOpenAIAccountUpstreamError_DashScopeThrottlingCoolsImageCapabilityNotAccount(t *testing.T) {
	repo := &modelNotFoundAccountRepoStub{}
	svc := &OpenAIGatewayService{rateLimitService: &RateLimitService{accountRepo: repo}}
	account := newDashScopeImageAccount()
	account.ID = 601
	body := []byte(`{"code":"Throttling.RateQuota","message":"Requests rate limit exceeded, please try again later."}`)

	disabled := svc.handleOpenAIAccountUpstreamError(
		WithOpenAIImagesEndpoint(context.Background()),
		account,
		http.StatusTooManyRequests,
		http.Header{},
		body,
		"qwen-image-plus",
	)

	require.False(t, disabled)
	require.Len(t, repo.modelRateLimitCalls, 1)
	require.Equal(t, openAIImageGenerationRateLimitKey, repo.modelRateLimitCalls[0].scope)
	require.Equal(t, openAIImageRateLimitReason, repo.modelRateLimitCalls[0].reason)
	require.Zero(t, repo.tempCalls)
	_, wholeAccountBlocked := svc.openaiAccountRuntimeBlockUntil.Load(account.ID)
	require.False(t, wholeAccountBlocked)
}

func TestHandleOpenAIAccountUpstreamError_DashScopeUnsupportedModelCoolsModelNotAccount(t *testing.T) {
	repo := &modelNotFoundAccountRepoStub{}
	svc := &OpenAIGatewayService{rateLimitService: &RateLimitService{accountRepo: repo}}
	account := newDashScopeImageAccount()
	account.ID = 602
	body := []byte(`{"code":"InvalidParameter","message":"model not exist."}`)

	disabled := svc.handleOpenAIAccountUpstreamError(
		WithOpenAIImagesEndpoint(context.Background()),
		account,
		http.StatusBadRequest,
		http.Header{},
		body,
		"wanx-v1",
	)

	require.True(t, disabled, "failover to another account, but only this model is cooled")
	require.Len(t, repo.modelRateLimitCalls, 1)
	require.Equal(t, "wanx-v1", repo.modelRateLimitCalls[0].scope)
	require.Equal(t, upstreamModelNotFoundReason, repo.modelRateLimitCalls[0].reason)
	require.Zero(t, repo.tempCalls)
	_, wholeAccountBlocked := svc.openaiAccountRuntimeBlockUntil.Load(account.ID)
	require.False(t, wholeAccountBlocked)
}

func TestOpenAIGatewayServiceForwardImages_DashScopeUnsupportedModelFailoversWithoutAccountCooldown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &modelNotFoundAccountRepoStub{}
	body := []byte(`{"model":"wanx-v1","prompt":"draw a cat","response_format":"url"}`)
	errorBody := `{"code":"Model.AccessDenied","message":"access denied for model wanx-v1"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req

	svc := &OpenAIGatewayService{
		cfg:              &config.Config{},
		rateLimitService: &RateLimitService{accountRepo: repo},
		httpUpstream: &httpUpstreamRecorder{
			resp: &http.Response{
				StatusCode: http.StatusForbidden,
				Header:     http.Header{"X-Request-Id": []string{"req_ds_denied"}},
				Body:       io.NopCloser(strings.NewReader(errorBody)),
			},
		},
	}
	parsed, err := svc.ParseOpenAIImagesRequest(c, body)
	require.NoError(t, err)

	result, err := svc.ForwardImages(WithOpenAIImagesEndpoint(context.Background()), c, newDashScopeImageAccount(), body, parsed, "")
	require.Nil(t, result)
	var failoverErr *UpstreamFailoverError
	require.ErrorAs(t, err, &failoverErr)
	require.Equal(t, http.StatusForbidden, failoverErr.StatusCode)
	require.Len(t, repo.modelRateLimitCalls, 1)
	require.Equal(t, "wanx-v1", repo.modelRateLimitCalls[0].scope)
	require.Zero(t, repo.tempCalls)
	require.False(t, c.Writer.Written())
}

func TestRateLimitService_HandleUpstreamModelNotFound_DashScopeUnsupportedModel(t *testing.T) {
	repo := &modelNotFoundAccountRepoStub{}
	svc := &RateLimitService{accountRepo: repo}
	account := newDashScopeImageAccount()
	account.ID = 603

	handled := svc.HandleUpstreamModelNotFound(
		WithOpenAIImagesEndpoint(context.Background()),
		account,
		"qwen-image-plus",
		http.StatusBadRequest,
		[]byte(`{"code":"InvalidParameter","message":"The model is not supported"}`),
	)

	require.True(t, handled)
	require.Zero(t, repo.tempCalls)
	require.Len(t, repo.modelRateLimitCalls, 1)
	require.Equal(t, "qwen-image-plus", repo.modelRateLimitCalls[0].scope)
	require.Equal(t, upstreamModelNotFoundReason, repo.modelRateLimitCalls[0].reason)
	require.WithinDuration(t, time.Now().Add(upstreamModelNotFoundCooldown), repo.modelRateLimitCalls[0].resetAt, 5*time.Second)
}
