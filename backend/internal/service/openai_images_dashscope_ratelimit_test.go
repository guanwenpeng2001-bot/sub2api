//go:build unit

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	body := []byte(`{"code":"Model.NotFound","message":"model not exist."}`)

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

func TestOpenAIGatewayServiceForwardImages_DashScopeAccessDeniedDoesNotCool(t *testing.T) {
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
	var upErr *OpenAIImagesUpstreamError
	require.ErrorAs(t, err, &upErr)
	require.Equal(t, http.StatusForbidden, upErr.StatusCode)
	require.Empty(t, repo.modelRateLimitCalls)
	require.Zero(t, repo.tempCalls)
	require.Equal(t, http.StatusForbidden, rec.Code)
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
		[]byte(`{"code":"Model.NotFound","message":"The model is not supported"}`),
	)

	require.True(t, handled)
	require.Zero(t, repo.tempCalls)
	require.Len(t, repo.modelRateLimitCalls, 1)
	require.Equal(t, "qwen-image-plus", repo.modelRateLimitCalls[0].scope)
	require.Equal(t, upstreamModelNotFoundReason, repo.modelRateLimitCalls[0].reason)
	require.WithinDuration(t, time.Now().Add(upstreamModelNotFoundCooldown), repo.modelRateLimitCalls[0].resetAt, 5*time.Second)
}

func TestDashScopeParameterErrorsDoNotCoolOrFailover(t *testing.T) {
	for _, message := range []string{
		"input image does not exist", "size is not supported", "input image is not accessible",
		"prompt is empty", "model not exist.", "The model is not supported",
		"input image for model qwen-image does not exist", "prompt must not contain rate limit text",
	} {
		for _, async := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/async=%v", message, async), func(t *testing.T) {
				repo := &modelNotFoundAccountRepoStub{}
				payload, err := json.Marshal(map[string]string{"code": "InvalidParameter", "message": message})
				require.NoError(t, err)
				require.False(t, isDashScopeUnsupportedModelError(400, payload))
				responses := []*http.Response{dashScopeJSONResponse(400, string(payload))}
				model := "qwen-image"
				if async {
					withDashScopeImageTestClock(t)
					model = "wanx-v1"
					failed, err := json.Marshal(map[string]any{"output": map[string]string{"task_status": "FAILED", "code": "InvalidParameter", "message": message}})
					require.NoError(t, err)
					responses = []*http.Response{dashScopeJSONResponse(200, `{"output":{"task_id":"task-1"}}`), dashScopeJSONResponse(200, string(failed))}
				}
				account := newDashScopeImageAccount()
				svc := &OpenAIGatewayService{cfg: &config.Config{}, rateLimitService: &RateLimitService{accountRepo: repo}, httpUpstream: &httpUpstreamRecorder{responses: responses}}
				body := []byte(fmt.Sprintf(`{"model":%q,"prompt":"cat"}`, model))
				c, rec := newDashScopeImagesTestContext(t, body)
				parsed, err := svc.ParseOpenAIImagesRequest(c, body)
				require.NoError(t, err)
				result, err := svc.ForwardImages(WithOpenAIImagesEndpoint(context.Background()), c, account, body, parsed, "")
				require.Nil(t, result)
				var upErr *OpenAIImagesUpstreamError
				require.ErrorAs(t, err, &upErr)
				require.Equal(t, 400, rec.Code)
				require.Contains(t, upErr.Message, message)
				require.Empty(t, repo.modelRateLimitCalls)
				require.Zero(t, repo.tempCalls)
				_, blocked := svc.openaiAccountRuntimeBlockUntil.Load(account.ID)
				require.False(t, blocked)
			})
		}
	}
}

func TestDashScopeForwardQuotaStillCoolsImageCapability(t *testing.T) {
	repo := &modelNotFoundAccountRepoStub{}
	upstream := &httpUpstreamRecorder{resp: dashScopeJSONResponse(429, `{"code":"Throttling.RateQuota","message":"Requests rate limit exceeded"}`)}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream, rateLimitService: &RateLimitService{accountRepo: repo}}
	c, rec := newDashScopeImagesTestContext(t, nil)
	account := newDashScopeImageAccount()
	parsed := &OpenAIImagesRequest{Model: "qwen-image", Prompt: "cat", N: 1}
	_, err := svc.ForwardImages(WithOpenAIImagesEndpoint(context.Background()), c, account, nil, parsed, "")
	require.Error(t, err)
	require.Equal(t, 429, rec.Code)
	require.Len(t, repo.modelRateLimitCalls, 1)
	require.Equal(t, openAIImageGenerationRateLimitKey, repo.modelRateLimitCalls[0].scope)
	require.Zero(t, repo.tempCalls)
}
