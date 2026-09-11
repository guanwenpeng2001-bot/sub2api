package service

import (
	"bytes"
	"context"
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
	"github.com/tidwall/gjson"
)

func newDashScopeImageAccount() *Account {
	return &Account{
		ID:       6,
		Name:     "dashscope-apikey",
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"api_key":  "sk-dashscope-test",
			"base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
		},
	}
}

func newDashScopeImagesTestContext(t *testing.T, body []byte) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req
	return c, rec
}

func dashScopeJSONResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
			"X-Request-Id": []string{"req_dashscope"},
		},
		Body: io.NopCloser(strings.NewReader(body)),
	}
}

func withDashScopeImageTestClock(t *testing.T) {
	t.Helper()
	oldInterval := dashScopeImageTaskPollInterval
	oldTimeout := dashScopeImageTaskTimeout
	dashScopeImageTaskPollInterval = 0
	dashScopeImageTaskTimeout = time.Second
	t.Cleanup(func() {
		dashScopeImageTaskPollInterval = oldInterval
		dashScopeImageTaskTimeout = oldTimeout
	})
}

func TestIsDashScopeImageGenerationModel(t *testing.T) {
	for _, model := range []string{"qwen-image", "Qwen-Image-Plus", "qwen-image-max", "qwen-image-edit", "z-image-turbo", "wanx-v1", "wanx2.1-t2i-turbo", "wan2.2-t2i-flash", "wan2.5-t2i-preview"} {
		require.True(t, isDashScopeImageGenerationModel(model), model)
	}
	for _, model := range []string{"qwen-plus", "qwen-max", "wandb", "wan", "gpt-image-2", "grok-imagine-image", ""} {
		require.False(t, isDashScopeImageGenerationModel(model), model)
	}
}

func TestNormalizeDashScopeNativeAPIBase(t *testing.T) {
	require.Equal(t, "https://dashscope.aliyuncs.com/api/v1", normalizeDashScopeNativeAPIBase("https://dashscope.aliyuncs.com/compatible-mode/v1"))
	require.Equal(t, "https://dashscope.aliyuncs.com/api/v1", normalizeDashScopeNativeAPIBase("https://dashscope.aliyuncs.com/api/v1"))
	require.Equal(t, "https://dashscope-intl.aliyuncs.com/api/v1", normalizeDashScopeNativeAPIBase("https://dashscope-intl.aliyuncs.com/compatible-mode/v1/"))
	require.Equal(t, "https://dashscope.aliyuncs.com/api/v1", normalizeDashScopeNativeAPIBase("https://dashscope.aliyuncs.com"))
	require.Equal(t, dashScopeDefaultAPIBase, normalizeDashScopeNativeAPIBase(""))
}

func TestMapDashScopeImageParameters(t *testing.T) {
	parsed := &OpenAIImagesRequest{N: 2, Size: "1024x1024", Quality: "high", Background: "transparent", Stream: true}
	syncParams := mapDashScopeImageParameters("qwen-image-plus", parsed)
	require.Equal(t, "1328*1328", syncParams.Size)
	require.Equal(t, 2, syncParams.N)
	require.NotNil(t, syncParams.PromptExtend)
	require.True(t, *syncParams.PromptExtend)
	require.Contains(t, syncParams.Ignored, "background")
	require.NotContains(t, syncParams.Ignored, "stream")

	asyncParams := mapDashScopeImageParameters("wanx-v1", parsed)
	require.Equal(t, "1024*1024", asyncParams.Size)
	require.Nil(t, asyncParams.PromptExtend)
	require.Contains(t, asyncParams.Ignored, "quality=high")
}

func TestParseDashScopeImageUsage(t *testing.T) {
	usage, ok := parseDashScopeImageUsage([]byte(`{"usage":{"input_tokens":16,"output_tokens":1290,"image_count":1}}`))
	require.True(t, ok)
	require.Equal(t, 16, usage.InputTokens)
	require.Equal(t, 1290, usage.OutputTokens)
	require.Equal(t, 1290, usage.ImageOutputTokens)

	usage, ok = parseDashScopeImageUsage([]byte(`{"usage":{"image_count":2}}`))
	require.True(t, ok)
	require.Zero(t, usage.InputTokens)
	require.Zero(t, usage.OutputTokens)
	require.Equal(t, 2, dashScopeUsageImageCount([]byte(`{"usage":{"image_count":2}}`)))
}

func TestIsDashScopeImageAccount(t *testing.T) {
	require.True(t, isDashScopeImageAccount(newDashScopeImageAccount()))
	require.True(t, isDashScopeImageAccount(&Account{
		Type: AccountTypeAPIKey, Platform: "dashscope",
		Credentials: map[string]any{"api_key": "sk-1"},
	}))
	require.False(t, isDashScopeImageAccount(&Account{
		Type: AccountTypeAPIKey, Platform: PlatformOpenAI,
		Credentials: map[string]any{"api_key": "sk-1", "base_url": "https://api.openai.com/v1"},
	}))
	require.False(t, isDashScopeImageAccount(&Account{
		Type: AccountTypeOAuth, Platform: PlatformOpenAI,
		Credentials: map[string]any{"base_url": "https://dashscope.aliyuncs.com/api/v1"},
	}))
}

func TestOpenAIGatewayServiceForwardImages_DashScopeSyncMultimodalGeneration(t *testing.T) {
	t.Setenv("SUB2API_IMAGES_MAIN_MODEL", "gpt-5.6-sol")
	body := []byte(`{"model":"qwen-image-plus","prompt":"a red cup","n":1,"size":"1024x1024","quality":"high","response_format":"url"}`)
	c, rec := newDashScopeImagesTestContext(t, body)
	upstream := &httpUpstreamRecorder{resp: dashScopeJSONResponse(http.StatusOK, `{
		"request_id":"req_sync",
		"output":{"choices":[{"message":{"content":[{"image":"https://cdn.example.com/a.png"}]}}]},
		"usage":{"input_tokens":16,"output_tokens":1290,"image_count":1}
	}`)}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
	parsed, err := svc.ParseOpenAIImagesRequest(c, body)
	require.NoError(t, err)

	result, err := svc.ForwardImages(context.Background(), c, newDashScopeImageAccount(), body, parsed, "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, result.ImageCount)
	require.Equal(t, "qwen-image-plus", result.Model)
	require.Equal(t, 16, result.Usage.InputTokens)
	require.Equal(t, 1290, result.Usage.OutputTokens)
	require.Equal(t, 1290, result.Usage.ImageOutputTokens)
	require.NotZero(t, result.Usage.InputTokens+result.Usage.OutputTokens+result.ImageCount)

	require.NotNil(t, upstream.lastReq)
	require.Equal(t, "https://dashscope.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation", upstream.lastReq.URL.String())
	require.Equal(t, "Bearer sk-dashscope-test", upstream.lastReq.Header.Get("Authorization"))
	require.Equal(t, "qwen-image-plus", gjson.GetBytes(upstream.lastBody, "model").String())
	require.NotEqual(t, "gpt-5.6-sol", gjson.GetBytes(upstream.lastBody, "model").String())
	require.Equal(t, "1328*1328", gjson.GetBytes(upstream.lastBody, "parameters.size").String())
	require.Equal(t, int64(1), gjson.GetBytes(upstream.lastBody, "parameters.n").Int())
	require.True(t, gjson.GetBytes(upstream.lastBody, "parameters.prompt_extend").Bool())
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "https://cdn.example.com/a.png", gjson.Get(rec.Body.String(), "data.0.url").String())
	require.Equal(t, int64(16), gjson.Get(rec.Body.String(), "usage.input_tokens").Int())
	require.Equal(t, int64(1290), gjson.Get(rec.Body.String(), "usage.output_tokens_details.image_tokens").Int())
}

func TestOpenAIGatewayServiceForwardImages_DashScopeAsyncText2ImagePoll(t *testing.T) {
	withDashScopeImageTestClock(t)
	body := []byte(`{"model":"wanx-v1","prompt":"a lighthouse","n":2,"size":"1280x1280","quality":"hd","response_format":"url"}`)
	c, rec := newDashScopeImagesTestContext(t, body)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		dashScopeJSONResponse(http.StatusOK, `{"request_id":"req_create","output":{"task_id":"task-1","task_status":"PENDING"}}`),
		dashScopeJSONResponse(http.StatusOK, `{"request_id":"req_poll","output":{"task_id":"task-1","task_status":"RUNNING"}}`),
		dashScopeJSONResponse(http.StatusOK, `{
			"request_id":"req_done",
			"output":{"task_id":"task-1","task_status":"SUCCEEDED","results":[{"url":"https://cdn.example.com/1.png"},{"url":"https://cdn.example.com/2.png"}]},
			"usage":{"image_count":2}
		}`),
	}}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
	parsed, err := svc.ParseOpenAIImagesRequest(c, body)
	require.NoError(t, err)

	result, err := svc.ForwardImages(context.Background(), c, newDashScopeImageAccount(), body, parsed, "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 2, result.ImageCount)
	require.Equal(t, "wanx-v1", result.Model)
	require.Len(t, upstream.requests, 3)
	require.Equal(t, "https://dashscope.aliyuncs.com/api/v1/services/aigc/text2image/image-synthesis", upstream.requests[0].URL.String())
	require.Equal(t, dashScopeAsyncHeaderValue, upstream.requests[0].Header.Get(dashScopeAsyncHeaderName))
	require.Equal(t, "wanx-v1", gjson.GetBytes(upstream.bodies[0], "model").String())
	require.Equal(t, "1280*1280", gjson.GetBytes(upstream.bodies[0], "parameters.size").String())
	require.False(t, gjson.GetBytes(upstream.bodies[0], "parameters.prompt_extend").Exists())
	require.Equal(t, http.MethodGet, upstream.requests[1].Method)
	require.Equal(t, "https://dashscope.aliyuncs.com/api/v1/tasks/task-1", upstream.requests[2].URL.String())
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "https://cdn.example.com/1.png", gjson.Get(rec.Body.String(), "data.0.url").String())
	require.Equal(t, "https://cdn.example.com/2.png", gjson.Get(rec.Body.String(), "data.1.url").String())
	require.Equal(t, int64(2), gjson.Get(rec.Body.String(), "usage.images").Int())
}

func TestOpenAIGatewayServiceForwardImages_DashScopeAccountDoesNotUseImagesMainModel(t *testing.T) {
	t.Setenv("SUB2API_IMAGES_MAIN_MODEL", "gpt-5.4-mini")
	body := []byte(`{"model":"qwen-image","prompt":"cat","response_format":"url"}`)
	c, _ := newDashScopeImagesTestContext(t, body)
	upstream := &httpUpstreamRecorder{resp: dashScopeJSONResponse(http.StatusOK, `{
		"output":{"choices":[{"message":{"content":[{"image":"https://cdn.example.com/cat.png"}]}}]},
		"usage":{"input_tokens":8,"output_tokens":400,"image_count":1}
	}`)}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
	parsed, err := svc.ParseOpenAIImagesRequest(c, body)
	require.NoError(t, err)
	_, err = svc.ForwardImages(context.Background(), c, newDashScopeImageAccount(), body, parsed, "")
	require.NoError(t, err)
	require.Equal(t, "qwen-image", gjson.GetBytes(upstream.lastBody, "model").String())
	require.False(t, gjson.GetBytes(upstream.lastBody, "tools").Exists())
}

func TestOpenAIGatewayServiceForwardImages_NonDashScopeAccountKeepsStandardImagesURL(t *testing.T) {
	body := []byte(`{"model":"qwen-image","prompt":"cat","response_format":"url"}`)
	c, _ := newDashScopeImagesTestContext(t, body)
	upstream := &httpUpstreamRecorder{resp: dashScopeJSONResponse(http.StatusOK, `{"created":1,"data":[{"url":"https://cdn.example.com/x.png"}]}`)}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
	parsed, err := svc.ParseOpenAIImagesRequest(c, body)
	require.NoError(t, err)
	account := &Account{
		ID:       9,
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"api_key":  "sk-openai",
			"base_url": "https://api.openai.com/v1",
		},
	}
	_, err = svc.ForwardImages(context.Background(), c, account, body, parsed, "")
	require.NoError(t, err)
	require.Equal(t, "https://api.openai.com/v1/images/generations", upstream.lastReq.URL.String())
}

func TestIsDashScopeUnsupportedModelError(t *testing.T) {
	require.False(t, isDashScopeUnsupportedModelError(http.StatusBadRequest, []byte(`{"code":"InvalidParameter","message":"url error, please check url parameter. model not exist."}`)))
	require.True(t, isDashScopeUnsupportedModelError(http.StatusForbidden, []byte(`{"code":"Model.AccessDenied","message":"access denied for model wanx-v1"}`)))
	require.False(t, isDashScopeUnsupportedModelError(http.StatusBadRequest, []byte(`{"code":"InvalidParameter","message":"prompt is empty"}`)))
	require.False(t, isDashScopeUnsupportedModelError(http.StatusTooManyRequests, []byte(`{"code":"Throttling.RateQuota","message":"Requests rate limit exceeded"}`)))
}

func TestIsDashScopeThrottlingPayload(t *testing.T) {
	require.True(t, isDashScopeThrottlingPayload([]byte(`{"code":"Throttling.RateQuota","message":"Requests rate limit exceeded, please try again later."}`)))
	require.True(t, isImageCapabilityRateLimitError(context.Background(), http.StatusTooManyRequests, []byte(`{"code":"Throttling.RateQuota","message":"Requests rate limit exceeded"}`), "qwen-image"))
	require.False(t, isImageCapabilityRateLimitError(context.Background(), http.StatusTooManyRequests, []byte(`{"code":"Throttling.RateQuota","message":"Requests rate limit exceeded"}`), "qwen-plus"))
}

func TestOpenAIGatewayServiceForwardImages_DashScopeRateLimitClassified(t *testing.T) {
	body := []byte(`{"model":"qwen-image","prompt":"cat","response_format":"url"}`)
	c, rec := newDashScopeImagesTestContext(t, body)
	errorBody := `{"code":"Throttling.RateQuota","message":"Requests rate limit exceeded, please try again later.","request_id":"rl-1"}`
	svc := &OpenAIGatewayService{
		cfg: &config.Config{},
		httpUpstream: &httpUpstreamRecorder{
			resp: &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
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
	require.Equal(t, http.StatusTooManyRequests, upErr.StatusCode)
	require.Equal(t, "Throttling.RateQuota", upErr.Code)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
}

func TestOpenAIGatewayServiceForwardImages_DashScopeUnsupportedModelDoesNotUseResponsesDriver(t *testing.T) {
	t.Setenv("SUB2API_IMAGES_MAIN_MODEL", "gpt-5.6-luna")
	body := []byte(`{"model":"wan2.2-t2i-flash","prompt":"waves","response_format":"url"}`)
	c, rec := newDashScopeImagesTestContext(t, body)
	errorBody := `{"code":"InvalidParameter","message":"model not exist.","request_id":"nf-1"}`
	svc := &OpenAIGatewayService{
		cfg: &config.Config{},
		httpUpstream: &httpUpstreamRecorder{
			resp: &http.Response{
				StatusCode: http.StatusBadRequest,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
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
	require.Equal(t, http.StatusBadRequest, upErr.StatusCode)
	require.Contains(t, upErr.Message, "model not exist")
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.NotContains(t, rec.Body.String(), "gpt-5.6-luna")
}

func TestValidateOpenAIImagesModel_DashScopeFamilies(t *testing.T) {
	require.NoError(t, validateOpenAIImagesModel("qwen-image-max"))
	require.NoError(t, validateOpenAIImagesModel("wanx2.1-t2i-plus"))
	require.NoError(t, validateOpenAIImagesModel("wan2.6-t2i"))
	require.Error(t, validateOpenAIImagesModel("qwen-plus"))
	require.Error(t, validateOpenAIImagesModel("gpt-5.4"))
}

func TestBuildDashScopeMultimodalPayload_EditIncludesInputImage(t *testing.T) {
	parsed := &OpenAIImagesRequest{
		Endpoint:       openAIImagesEditsEndpoint,
		Prompt:         "make it blue",
		InputImageURLs: []string{"https://example.com/in.png"},
		N:              1,
		Size:           "1024x1024",
	}
	params := mapDashScopeImageParameters("qwen-image-edit", parsed)
	payload, err := buildDashScopeMultimodalPayload("qwen-image-edit", parsed, params)
	require.NoError(t, err)
	require.Equal(t, "https://example.com/in.png", gjson.GetBytes(payload, "input.messages.0.content.0.image").String())
	require.Equal(t, "make it blue", gjson.GetBytes(payload, "input.messages.0.content.1.text").String())
}

func TestOpenAIGatewayServiceForwardImages_DashScopeAsyncEditsRejected(t *testing.T) {
	body := []byte(`{"model":"wanx-v1","prompt":"edit me","images":[{"image_url":"https://example.com/in.png"}],"response_format":"url"}`)
	gin.SetMode(gin.TestMode)
	req := httptest.NewRequest(http.MethodPost, "/v1/images/edits", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: &httpUpstreamRecorder{}}
	parsed, err := svc.ParseOpenAIImagesRequest(c, body)
	require.NoError(t, err)
	result, err := svc.ForwardImages(context.Background(), c, newDashScopeImageAccount(), body, parsed, "")
	require.Nil(t, result)
	var upErr *OpenAIImagesUpstreamError
	require.ErrorAs(t, err, &upErr)
	require.Equal(t, http.StatusBadRequest, upErr.StatusCode)
	require.Contains(t, upErr.Message, "not edits")
	require.Empty(t, svc.httpUpstream.(*httpUpstreamRecorder).requests)
}

func TestDashScopeUsageToOpenAIMapPreservesImageCount(t *testing.T) {
	out := dashScopeUsageToOpenAIMap(OpenAIUsage{InputTokens: 10, OutputTokens: 20, ImageOutputTokens: 20}, 3)
	require.Equal(t, 3, out["images"])
	require.Equal(t, 10, out["input_tokens"])
	require.Equal(t, 20, out["output_tokens"])
	details, ok := out["output_tokens_details"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, 20, details["image_tokens"])
}

func TestShouldForwardDashScopeImages(t *testing.T) {
	account := newDashScopeImageAccount()
	require.True(t, shouldForwardDashScopeImages(account, &OpenAIImagesRequest{Model: "qwen-image"}, ""))
	require.True(t, shouldForwardDashScopeImages(account, &OpenAIImagesRequest{Model: "gpt-image-2"}, "wanx-v1"))
	require.False(t, shouldForwardDashScopeImages(account, &OpenAIImagesRequest{Model: "gpt-image-2"}, ""))
	require.False(t, shouldForwardDashScopeImages(&Account{
		Type: AccountTypeAPIKey, Platform: PlatformOpenAI,
		Credentials: map[string]any{"base_url": "https://api.openai.com/v1"},
	}, &OpenAIImagesRequest{Model: "qwen-image"}, ""))
}

func TestParseDashScopeImageValue_DataURI(t *testing.T) {
	got := parseDashScopeImageValue("data:image/png;base64,YQ==")
	require.Equal(t, "YQ==", got.B64)
}

func TestExtractDashScopeSyncAndAsyncImages(t *testing.T) {
	syncImages := extractDashScopeSyncImages([]byte(`{"output":{"choices":[{"message":{"content":[{"text":"ok"},{"image":"https://cdn.example.com/a.png"}]}}]}}`))
	require.Len(t, syncImages, 1)
	require.Equal(t, "https://cdn.example.com/a.png", syncImages[0].URL)

	asyncImages := extractDashScopeAsyncImages([]byte(`{"output":{"results":[{"url":"https://cdn.example.com/b.png"}]}}`))
	require.Len(t, asyncImages, 1)
	require.Equal(t, "https://cdn.example.com/b.png", asyncImages[0].URL)
}

func TestOpenAIGatewayServiceForwardImages_DashScopeSyncDataURIReturnsB64(t *testing.T) {
	pngB64 := "YQ=="
	body := []byte(`{"model":"qwen-image","prompt":"dot","response_format":"b64_json"}`)
	c, rec := newDashScopeImagesTestContext(t, body)
	upstreamBody := fmt.Sprintf(`{"output":{"choices":[{"message":{"content":[{"image":"data:image/png;base64,%s"}]}}]},"usage":{"input_tokens":4,"output_tokens":80,"image_count":1}}`, pngB64)
	require.Len(t, extractDashScopeSyncImages([]byte(upstreamBody)), 1)
	upstream := &httpUpstreamRecorder{resp: dashScopeJSONResponse(http.StatusOK, upstreamBody)}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
	parsed, err := svc.ParseOpenAIImagesRequest(c, body)
	require.NoError(t, err)
	result, err := svc.ForwardImages(context.Background(), c, newDashScopeImageAccount(), body, parsed, "")
	require.NoError(t, err)
	require.Equal(t, 1, result.ImageCount)
	require.Equal(t, pngB64, gjson.Get(rec.Body.String(), "data.0.b64_json").String())
}

func TestMapDashScopeModelImageSize(t *testing.T) {
	for _, tc := range []struct{ model, size, want string }{
		{"qwen-image-max", "1024x1024", "1328*1328"},
		{"qwen-image-max-2025-12-30", "1536x1024", "1472*1104"},
		{"qwen-image-plus", "1024x1536", "1104*1472"},
		{"qwen-image", "1920x1080", "1664*928"},
		{"qwen-image-max", "1080x1920", "928*1664"},
		{"qwen-image-max", "1664*928", "1664*928"},
		{"qwen-image-max", "auto", "1328*1328"},
		{"qwen-image-max", "", "1328*1328"},
		{"qwen-image-max", "0x1024", "0*1024"},
		{"qwen-image-2.0-pro", "1024x1024", "1024*1024"},
		{"qwen-image-edit-max", "1024x1024", "1024*1024"},
		{"z-image-turbo", "1024x1024", "1024*1024"},
		{"wan2.2-t2i-flash", "1024x1024", "1024*1024"},
	} {
		t.Run(tc.model+"/"+tc.size, func(t *testing.T) {
			require.Equal(t, tc.want, mapDashScopeImageParameters(tc.model, &OpenAIImagesRequest{Size: tc.size}).Size)
		})
	}
	require.Equal(t, "1328*1328", mapDashScopeImageParameters("qwen-image-max", nil).Size)
}

func TestDashScopeZImageRejectsEditsAndMultipleImages(t *testing.T) {
	for _, parsed := range []*OpenAIImagesRequest{
		{Model: "z-image-turbo", Prompt: "cat", N: 2},
		{Model: "z-image-turbo", Prompt: "cat", N: 1, Endpoint: "/v1/images/edits"},
	} {
		c, rec := newDashScopeImagesTestContext(t, nil)
		upstream := &httpUpstreamRecorder{}
		svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
		_, err := svc.ForwardImages(context.Background(), c, newDashScopeImageAccount(), nil, parsed, "")
		require.Error(t, err)
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Empty(t, upstream.requests)
	}
}
