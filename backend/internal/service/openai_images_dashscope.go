package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

const (
	dashScopeDefaultAPIBase               = "https://dashscope.aliyuncs.com/api/v1"
	dashScopeMultimodalGenerationPath     = "/services/aigc/multimodal-generation/generation"
	dashScopeText2ImagePath               = "/services/aigc/text2image/image-synthesis"
	dashScopeTaskPathPrefix               = "/tasks/"
	dashScopeAsyncHeaderName              = "X-DashScope-Async"
	dashScopeAsyncHeaderValue             = "enable"
	dashScopeDefaultImageSize             = "1024*1024"
	dashScopeImageTaskStatusSucceeded     = "SUCCEEDED"
	dashScopeImageTaskStatusFailed        = "FAILED"
	dashScopeImageTaskStatusCanceled      = "CANCELED"
	dashScopeImageTaskStatusCancelled     = "CANCELLED"
	dashScopeUnsupportedModelReasonPhrase = "model is not supported"
)

var (
	dashScopeImageTaskPollInterval = 2 * time.Second
	dashScopeImageTaskTimeout      = 180 * time.Second
)

func shouldForwardDashScopeImages(account *Account, parsed *OpenAIImagesRequest, channelMappedModel string) bool {
	if !isDashScopeImageAccount(account) {
		return false
	}
	requestModel := ""
	if parsed != nil {
		requestModel = strings.TrimSpace(parsed.Model)
	}
	if mapped := strings.TrimSpace(channelMappedModel); mapped != "" {
		requestModel = mapped
	}
	upstreamModel := requestModel
	if account != nil {
		upstreamModel = account.GetMappedModel(requestModel)
	}
	return isDashScopeImageGenerationModel(requestModel) || isDashScopeImageGenerationModel(upstreamModel)
}

func isDashScopeImageAccount(account *Account) bool {
	if account == nil || account.Type != AccountTypeAPIKey {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(account.Platform)) {
	case "dashscope", "aliyun", "alibaba", "aliyun-bailian", "bailian":
		return true
	}
	if isDashScopeHostURL(account.GetOpenAIBaseURL()) || isDashScopeHostURL(account.GetCredential("base_url")) {
		return true
	}
	for _, key := range []string{"provider", "vendor", "protocol"} {
		value := strings.ToLower(strings.TrimSpace(account.GetCredential(key)))
		if strings.Contains(value, "dashscope") || value == "aliyun" || value == "alibaba" {
			return true
		}
	}
	return false
}

func isDashScopeSyncImageModel(model string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(model)), "qwen-image")
}

func isDashScopeHostURL(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return strings.Contains(strings.ToLower(raw), "dashscope")
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	if host == "" {
		host = strings.ToLower(strings.TrimSpace(parsed.Host))
	}
	return strings.Contains(host, "dashscope")
}

func dashScopeImageAPIKey(account *Account) string {
	if account == nil {
		return ""
	}
	if key := strings.TrimSpace(account.GetOpenAIProtocolAPIKey()); key != "" {
		return key
	}
	return strings.TrimSpace(account.GetCredential("api_key"))
}

func dashScopeNativeAPIBase(account *Account) string {
	raw := ""
	if account != nil {
		raw = strings.TrimSpace(account.GetOpenAIBaseURL())
		if raw == "" {
			raw = strings.TrimSpace(account.GetCredential("base_url"))
		}
	}
	return normalizeDashScopeNativeAPIBase(raw)
}

func normalizeDashScopeNativeAPIBase(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return dashScopeDefaultAPIBase
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return dashScopeDefaultAPIBase
	}
	path := strings.TrimSuffix(parsed.Path, "/")
	lower := strings.ToLower(path)
	switch {
	case strings.Contains(lower, "/compatible-mode"):
		path = "/api/v1"
	case strings.HasPrefix(lower, "/api/v1"):
		path = "/api/v1"
	case path == "" || path == "/v1":
		path = "/api/v1"
	default:
		if strings.Contains(strings.ToLower(parsed.Host), "dashscope") {
			path = "/api/v1"
		}
	}
	parsed.Path = path
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimRight(parsed.String(), "/")
}

func joinDashScopeAPIURL(base, path string) string {
	return strings.TrimRight(strings.TrimSpace(base), "/") + path
}

type dashScopeImageParameters struct {
	Size         string
	N            int
	PromptExtend *bool
	Ignored      []string
}

func mapDashScopeImageSize(size string) (string, bool) {
	size = strings.TrimSpace(size)
	if size == "" {
		return dashScopeDefaultImageSize, false
	}
	normalized := strings.ToLower(size)
	normalized = strings.ReplaceAll(normalized, "×", "x")
	if strings.Contains(normalized, "*") {
		return normalized, false
	}
	if strings.Contains(normalized, "x") {
		return strings.ReplaceAll(normalized, "x", "*"), false
	}
	return dashScopeDefaultImageSize, true
}

func mapDashScopeImageParameters(model string, parsed *OpenAIImagesRequest) dashScopeImageParameters {
	params := dashScopeImageParameters{N: 1, Size: dashScopeDefaultImageSize}
	if parsed == nil {
		return params
	}
	if parsed.N > 0 {
		params.N = parsed.N
	}
	size, sizeIgnored := mapDashScopeImageSize(parsed.Size)
	params.Size = size
	if sizeIgnored {
		params.Ignored = append(params.Ignored, "size="+strings.TrimSpace(parsed.Size))
	}
	quality := strings.ToLower(strings.TrimSpace(parsed.Quality))
	if quality != "" {
		if isDashScopeSyncImageModel(model) {
			switch quality {
			case "high", "hd", "xhigh", "max":
				enabled := true
				params.PromptExtend = &enabled
			case "low", "standard":
				enabled := false
				params.PromptExtend = &enabled
			default:
				params.Ignored = append(params.Ignored, "quality="+parsed.Quality)
			}
		} else {
			params.Ignored = append(params.Ignored, "quality="+parsed.Quality)
		}
	}
	appendIgnored := func(name, value string) {
		if strings.TrimSpace(value) != "" {
			params.Ignored = append(params.Ignored, name)
		}
	}
	appendIgnored("background", parsed.Background)
	appendIgnored("output_format", parsed.OutputFormat)
	appendIgnored("moderation", parsed.Moderation)
	appendIgnored("input_fidelity", parsed.InputFidelity)
	appendIgnored("style", parsed.Style)
	if parsed.OutputCompression != nil {
		params.Ignored = append(params.Ignored, "output_compression")
	}
	if parsed.PartialImages != nil {
		params.Ignored = append(params.Ignored, "partial_images")
	}
	return params
}

func logDashScopeIgnoredImageOptions(model string, ignored []string) {
	if len(ignored) == 0 {
		return
	}
	logger.LegacyPrintf(
		"service.openai_gateway",
		"[DashScope] Images ignored unsupported options model=%s options=%s",
		model,
		strings.Join(ignored, ","),
	)
}

func (s *OpenAIGatewayService) forwardDashScopeImages(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	parsed *OpenAIImagesRequest,
	channelMappedModel string,
) (*OpenAIForwardResult, error) {
	startTime := time.Now()
	if parsed.Stream || parsed.MaskUpload != nil || strings.TrimSpace(parsed.MaskImageURL) != "" {
		option := "mask"
		if parsed.Stream {
			option = "stream=true"
		}
		upErr := &OpenAIImagesUpstreamError{
			StatusCode: http.StatusBadRequest,
			ErrorType:  "invalid_request_error",
			Code:       "unsupported_parameter",
			Message:    "DashScope images do not support " + option,
		}
		writeOpenAIImagesUpstreamErrorResponse(c, upErr)
		return nil, upErr
	}
	requestModel := strings.TrimSpace(parsed.Model)
	if mapped := strings.TrimSpace(channelMappedModel); mapped != "" {
		requestModel = mapped
	}
	if err := validateOpenAIImagesModel(requestModel); err != nil {
		return nil, err
	}
	upstreamModel := account.GetMappedModel(requestModel)
	if err := validateOpenAIImagesModel(upstreamModel); err != nil {
		return nil, err
	}
	if !isDashScopeImageGenerationModel(upstreamModel) {
		return nil, fmt.Errorf("images endpoint requires an image model, got %q", upstreamModel)
	}
	SetOpsUpstreamModel(c, upstreamModel)
	logger.LegacyPrintf(
		"service.openai_gateway",
		"[DashScope] Images request routing request_model=%s upstream_model=%s endpoint=%s account_type=%s",
		strings.TrimSpace(parsed.Model),
		upstreamModel,
		parsed.Endpoint,
		account.Type,
	)

	token := dashScopeImageAPIKey(account)
	if token == "" {
		return nil, fmt.Errorf("api_key not found in credentials")
	}
	nativeBase := dashScopeNativeAPIBase(account)
	validatedBase, err := s.validateUpstreamBaseURL(nativeBase)
	if err != nil {
		return nil, err
	}

	// Keep collecting usage after a downstream disconnect, but bound the entire
	// operation (generation, polling, body reads and downloads) from its start.
	// HTTPUpstream passes this context to net/http.Client.Do and its Transport.
	upstreamCtx, releaseUpstreamCtx := context.WithTimeout(context.WithoutCancel(ctx), dashScopeImageTaskTimeout)
	defer releaseUpstreamCtx()

	var (
		images      []dashScopeImageResult
		usageBody   []byte
		requestID   string
		respHeader  http.Header
		upstreamURL string
	)
	if isDashScopeSyncImageModel(upstreamModel) {
		images, usageBody, requestID, respHeader, upstreamURL, err = s.generateDashScopeImageSync(upstreamCtx, c, account, parsed, upstreamModel, validatedBase, token)
	} else {
		if parsed.IsEdits() {
			upErr := &OpenAIImagesUpstreamError{
				StatusCode: http.StatusBadRequest,
				ErrorType:  "invalid_request_error",
				Code:       "unsupported_endpoint",
				Message:    "wan/wanx DashScope models only support image generation, not edits",
			}
			writeOpenAIImagesUpstreamErrorResponse(c, upErr)
			return nil, upErr
		}
		images, usageBody, requestID, respHeader, upstreamURL, err = s.generateDashScopeImageAsync(upstreamCtx, c, account, parsed, upstreamModel, validatedBase, token)
	}
	if err != nil {
		if upstreamCtx.Err() != nil {
			upErr := &OpenAIImagesUpstreamError{
				StatusCode: http.StatusGatewayTimeout,
				ErrorType:  "timeout",
				Code:       "image_generation_timeout",
				Message:    "DashScope image operation exceeded its time limit",
			}
			writeOpenAIImagesUpstreamErrorResponse(c, upErr)
			return nil, upErr
		}
		return nil, err
	}

	openaiBody, usage, imageCount, deliveryErr := s.buildDashScopeOpenAIImagesResponse(upstreamCtx, account, parsed, images, usageBody)
	if respHeader == nil {
		respHeader = make(http.Header)
	}
	if requestID != "" && respHeader.Get("x-request-id") == "" {
		respHeader.Set("x-request-id", requestID)
	}
	result := &OpenAIForwardResult{
		RequestID:        requestID,
		UpstreamHeaders:  respHeader,
		Usage:            usage,
		Model:            requestModel,
		UpstreamModel:    upstreamModel,
		UpstreamEndpoint: upstreamURL,
		ResponseHeaders:  respHeader.Clone(),
		Duration:         time.Since(startTime),
		ImageCount:       imageCount,
		ImageSize:        parsed.SizeTier,
		ImageInputSize:   parsed.Size,
		ClientDisconnect: ctx.Err() != nil,
	}
	if deliveryErr != nil {
		upErr := &OpenAIImagesUpstreamError{
			StatusCode:        http.StatusBadGateway,
			ErrorType:         "upstream_error",
			Code:              "image_download_failed",
			Message:           sanitizeUpstreamErrorMessage(deliveryErr.Error()),
			UpstreamRequestID: requestID,
		}
		setOpsUpstreamError(c, upErr.StatusCode, upErr.Message, upErr.Code)
		writeOpenAIImagesUpstreamErrorResponse(c, upErr)
		// The handler records usage for nonempty image results even on error.
		return result, upErr
	}
	responseheaders.WriteFilteredHeaders(c.Writer.Header(), respHeader, s.responseHeaderFilter)
	c.Data(http.StatusOK, "application/json", openaiBody)
	return result, nil
}

type dashScopeImageResult struct {
	URL    string
	B64    string
	Prompt string
}

func (s *OpenAIGatewayService) generateDashScopeImageSync(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	parsed *OpenAIImagesRequest,
	model, base, token string,
) ([]dashScopeImageResult, []byte, string, http.Header, string, error) {
	params := mapDashScopeImageParameters(model, parsed)
	logDashScopeIgnoredImageOptions(model, params.Ignored)
	payload, err := buildDashScopeMultimodalPayload(model, parsed, params)
	if err != nil {
		return nil, nil, "", nil, "", err
	}
	targetURL := joinDashScopeAPIURL(base, dashScopeMultimodalGenerationPath)
	resp, err := s.doDashScopeImageJSON(ctx, c, account, http.MethodPost, targetURL, token, payload, nil)
	if err != nil {
		return nil, nil, "", nil, targetURL, err
	}
	if resp.StatusCode >= 400 {
		_, err = s.handleDashScopeImagesErrorResponse(ctx, resp, c, account, model)
		return nil, nil, "", nil, targetURL, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := ReadUpstreamResponseBody(resp.Body, s.cfg, c, openAITooLargeError)
	if err != nil {
		return nil, nil, "", nil, targetURL, err
	}
	images := extractDashScopeSyncImages(body)
	if len(images) == 0 {
		upErr := &OpenAIImagesUpstreamError{
			StatusCode:        http.StatusBadGateway,
			ErrorType:         "upstream_error",
			Message:           "dashscope multimodal-generation returned no image",
			UpstreamRequestID: dashScopeRequestID(resp.Header, body),
		}
		writeOpenAIImagesUpstreamErrorResponse(c, upErr)
		return nil, nil, "", nil, targetURL, upErr
	}
	return images, body, dashScopeRequestID(resp.Header, body), resp.Header.Clone(), targetURL, nil
}

func (s *OpenAIGatewayService) generateDashScopeImageAsync(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	parsed *OpenAIImagesRequest,
	model, base, token string,
) ([]dashScopeImageResult, []byte, string, http.Header, string, error) {
	params := mapDashScopeImageParameters(model, parsed)
	logDashScopeIgnoredImageOptions(model, params.Ignored)
	payload := buildDashScopeText2ImagePayload(model, parsed, params)
	createURL := joinDashScopeAPIURL(base, dashScopeText2ImagePath)
	resp, err := s.doDashScopeImageJSON(ctx, c, account, http.MethodPost, createURL, token, payload, http.Header{
		dashScopeAsyncHeaderName: []string{dashScopeAsyncHeaderValue},
	})
	if err != nil {
		return nil, nil, "", nil, createURL, err
	}
	if resp.StatusCode >= 400 {
		_, err = s.handleDashScopeImagesErrorResponse(ctx, resp, c, account, model)
		return nil, nil, "", nil, createURL, err
	}
	createBody, err := ReadUpstreamResponseBody(resp.Body, s.cfg, c, openAITooLargeError)
	_ = resp.Body.Close()
	if err != nil {
		return nil, nil, "", nil, createURL, err
	}
	taskID := strings.TrimSpace(gjson.GetBytes(createBody, "output.task_id").String())
	if taskID == "" {
		upErr := &OpenAIImagesUpstreamError{
			StatusCode:        http.StatusBadGateway,
			ErrorType:         "upstream_error",
			Message:           "dashscope task create returned no task_id",
			UpstreamRequestID: dashScopeRequestID(resp.Header, createBody),
		}
		writeOpenAIImagesUpstreamErrorResponse(c, upErr)
		return nil, nil, "", nil, createURL, upErr
	}
	taskURL := joinDashScopeAPIURL(base, dashScopeTaskPathPrefix+url.PathEscape(taskID))
	var lastHeader http.Header
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, "", nil, taskURL, err
		}
		pollResp, pollErr := s.doDashScopeImageJSON(ctx, c, account, http.MethodGet, taskURL, token, nil, nil)
		if pollErr != nil {
			return nil, nil, "", nil, taskURL, pollErr
		}
		lastHeader = pollResp.Header.Clone()
		if pollResp.StatusCode >= 400 {
			_, err = s.handleDashScopeImagesErrorResponse(ctx, pollResp, c, account, model)
			return nil, nil, "", nil, taskURL, err
		}
		pollBody, readErr := ReadUpstreamResponseBody(pollResp.Body, s.cfg, c, openAITooLargeError)
		_ = pollResp.Body.Close()
		if readErr != nil {
			return nil, nil, "", nil, taskURL, readErr
		}
		state := strings.ToUpper(strings.TrimSpace(gjson.GetBytes(pollBody, "output.task_status").String()))
		switch state {
		case dashScopeImageTaskStatusSucceeded:
			images := extractDashScopeAsyncImages(pollBody)
			if len(images) == 0 {
				upErr := &OpenAIImagesUpstreamError{
					StatusCode:        http.StatusBadGateway,
					ErrorType:         "upstream_error",
					Message:           "dashscope task succeeded with no result url",
					UpstreamRequestID: dashScopeRequestID(lastHeader, pollBody),
				}
				writeOpenAIImagesUpstreamErrorResponse(c, upErr)
				return nil, nil, "", nil, taskURL, upErr
			}
			return images, pollBody, dashScopeRequestID(lastHeader, pollBody), lastHeader, createURL, nil
		case dashScopeImageTaskStatusFailed, dashScopeImageTaskStatusCanceled, dashScopeImageTaskStatusCancelled:
			message := strings.TrimSpace(gjson.GetBytes(pollBody, "output.message").String())
			if message == "" {
				message = "dashscope task " + strings.ToLower(state)
			}
			status, code := dashScopeTaskFailureStatus(pollBody)
			upErr := &OpenAIImagesUpstreamError{
				StatusCode:        status,
				ErrorType:         openAIImagesErrorTypeForStatus(status),
				Code:              code,
				Message:           sanitizeUpstreamErrorMessage(message),
				UpstreamRequestID: dashScopeRequestID(lastHeader, pollBody),
			}
			if isDashScopeUnsupportedModelError(status, pollBody) || isDashScopeThrottlingPayload(pollBody) {
				fake := &http.Response{StatusCode: status, Header: lastHeader, Body: io.NopCloser(bytes.NewReader(pollBody))}
				_, err = s.handleDashScopeImagesErrorResponse(ctx, fake, c, account, model)
				return nil, nil, "", nil, taskURL, err
			}
			writeOpenAIImagesUpstreamErrorResponse(c, upErr)
			return nil, nil, "", nil, taskURL, upErr
		}
		timer := time.NewTimer(dashScopeImageTaskPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, "", nil, taskURL, ctx.Err()
		case <-timer.C:
		}
	}
}

func dashScopeTaskFailureStatus(body []byte) (int, string) {
	code := dashScopeErrorCode(body)
	if isDashScopeThrottlingPayload(body) {
		return http.StatusTooManyRequests, code
	}
	if isDashScopeUnsupportedModelError(http.StatusBadRequest, body) {
		return http.StatusBadRequest, code
	}
	if code != "" {
		return http.StatusBadRequest, code
	}
	return http.StatusBadRequest, ""
}

func buildDashScopeMultimodalPayload(model string, parsed *OpenAIImagesRequest, params dashScopeImageParameters) ([]byte, error) {
	content := make([]map[string]string, 0, 2+len(parsed.InputImageURLs)+len(parsed.Uploads))
	if parsed != nil && parsed.IsEdits() {
		for _, imageURL := range parsed.InputImageURLs {
			if imageURL = strings.TrimSpace(imageURL); imageURL != "" {
				content = append(content, map[string]string{"image": imageURL})
			}
		}
		for _, upload := range parsed.Uploads {
			if dataURL := upload.ModerationDataURL(); dataURL != "" {
				content = append(content, map[string]string{"image": dataURL})
			}
		}
		if len(content) == 0 {
			return nil, fmt.Errorf("image file is required")
		}
	}
	prompt := ""
	if parsed != nil {
		prompt = strings.TrimSpace(parsed.Prompt)
	}
	if prompt != "" {
		content = append(content, map[string]string{"text": prompt})
	}
	parameters := map[string]any{
		"size": params.Size,
		"n":    params.N,
	}
	if params.PromptExtend != nil {
		parameters["prompt_extend"] = *params.PromptExtend
	}
	payload := map[string]any{
		"model": model,
		"input": map[string]any{
			"messages": []map[string]any{
				{"role": "user", "content": content},
			},
		},
		"parameters": parameters,
	}
	return json.Marshal(payload)
}

func buildDashScopeText2ImagePayload(model string, parsed *OpenAIImagesRequest, params dashScopeImageParameters) []byte {
	prompt := ""
	if parsed != nil {
		prompt = strings.TrimSpace(parsed.Prompt)
	}
	parameters := map[string]any{
		"size": params.Size,
		"n":    params.N,
	}
	payload := map[string]any{
		"model": model,
		"input": map[string]any{
			"prompt": prompt,
		},
		"parameters": parameters,
	}
	body, _ := json.Marshal(payload)
	return body
}

func (s *OpenAIGatewayService) doDashScopeImageJSON(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	method, targetURL, token string,
	body []byte,
	extraHeader http.Header,
) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, targetURL, reader)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(WithHTTPUpstreamProfile(req.Context(), HTTPUpstreamProfileOpenAI))
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, values := range extraHeader {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	if c != nil && c.Request != nil {
		for key, values := range c.Request.Header {
			if !openaiPassthroughAllowedHeaders[strings.ToLower(key)] {
				continue
			}
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
	}
	account.ApplyHeaderOverrides(req.Header)
	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	upstreamStart := time.Now()
	resp, err := s.doOpenAIUpstream(req, proxyURL, account)
	SetOpsLatencyMs(c, OpsUpstreamLatencyMsKey, time.Since(upstreamStart).Milliseconds())
	if err != nil {
		safeErr := sanitizeUpstreamErrorMessage(err.Error())
		setOpsUpstreamError(c, 0, safeErr, "")
		return nil, fmt.Errorf("upstream request failed: %s", safeErr)
	}
	return resp, nil
}

func extractDashScopeSyncImages(body []byte) []dashScopeImageResult {
	var images []dashScopeImageResult
	choices := gjson.GetBytes(body, "output.choices")
	if !choices.IsArray() {
		if image := parseDashScopeImageValue(gjson.GetBytes(body, "output.choices.0.message.content.0.image").String()); image.URL != "" || image.B64 != "" {
			return []dashScopeImageResult{image}
		}
		return nil
	}
	for _, choice := range choices.Array() {
		content := choice.Get("message.content")
		if !content.IsArray() {
			continue
		}
		for _, item := range content.Array() {
			image := parseDashScopeImageValue(item.Get("image").String())
			if image.URL == "" && image.B64 == "" {
				continue
			}
			images = append(images, image)
		}
	}
	return images
}

func extractDashScopeAsyncImages(body []byte) []dashScopeImageResult {
	var images []dashScopeImageResult
	results := gjson.GetBytes(body, "output.results")
	if !results.IsArray() {
		return nil
	}
	for _, item := range results.Array() {
		image := parseDashScopeImageValue(item.Get("url").String())
		if image.URL == "" && image.B64 == "" {
			image = parseDashScopeImageValue(item.Get("b64_image").String())
		}
		if image.URL == "" && image.B64 == "" {
			continue
		}
		images = append(images, image)
	}
	return images
}

func parseDashScopeImageValue(raw string) dashScopeImageResult {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return dashScopeImageResult{}
	}
	if strings.HasPrefix(strings.ToLower(raw), "data:") {
		if idx := strings.Index(raw, ","); idx >= 0 && idx+1 < len(raw) {
			raw = strings.TrimSpace(raw[idx+1:])
		}
		if decoded := decodeDashScopeImageBase64(raw); decoded != "" {
			return dashScopeImageResult{B64: decoded}
		}
		// Preserve a generated result even when its base64 cannot be delivered.
		return dashScopeImageResult{B64: raw}
	}
	if !strings.Contains(raw, "://") && isLikelyRawBase64(raw) {
		if decoded := decodeDashScopeImageBase64(raw); decoded != "" {
			return dashScopeImageResult{B64: decoded}
		}
	}
	return dashScopeImageResult{URL: raw}
}

func decodeDashScopeImageBase64(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if _, err := base64.StdEncoding.DecodeString(raw); err == nil {
		return raw
	}
	return normalizeOpenAIImageBase64(raw)
}

func isLikelyRawBase64(raw string) bool {
	if len(raw) < 32 {
		return false
	}
	for _, r := range raw {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '+' || r == '/' || r == '=' {
			continue
		}
		return false
	}
	return true
}

func (s *OpenAIGatewayService) buildDashScopeOpenAIImagesResponse(
	ctx context.Context,
	account *Account,
	parsed *OpenAIImagesRequest,
	images []dashScopeImageResult,
	upstreamBody []byte,
) ([]byte, OpenAIUsage, int, error) {
	wantB64 := parsed != nil && strings.EqualFold(strings.TrimSpace(parsed.ResponseFormat), "b64_json")
	// Capture all upstream consumption before any delivery operation can fail.
	usage, usageOK := parseDashScopeImageUsage(upstreamBody)
	imageCount := len(images)
	if usageCount := dashScopeUsageImageCount(upstreamBody); usageCount > imageCount {
		imageCount = usageCount
	}
	data := make([]map[string]any, 0, len(images))
	for _, image := range images {
		item := map[string]any{}
		b64 := strings.TrimSpace(image.B64)
		if b64 != "" {
			b64 = decodeDashScopeImageBase64(b64)
			if b64 == "" {
				return nil, usage, imageCount, fmt.Errorf("decode dashscope image: invalid base64")
			}
		}
		imageURL := strings.TrimSpace(image.URL)
		if wantB64 && b64 == "" && imageURL != "" {
			encoded, err := s.fetchOpenAIImageURLBase64(ctx, account, imageURL)
			if err != nil {
				return nil, usage, imageCount, fmt.Errorf("download dashscope image: %w", err)
			}
			b64 = encoded
		}
		if b64 != "" {
			item["b64_json"] = b64
		}
		if imageURL != "" {
			item["url"] = imageURL
		}
		if len(item) == 0 {
			continue
		}
		data = append(data, item)
	}
	if len(data) == 0 {
		return nil, usage, imageCount, fmt.Errorf("dashscope returned no image")
	}
	resp := map[string]any{
		"created": time.Now().Unix(),
		"data":    data,
	}
	if usageOK || imageCount > 0 {
		resp["usage"] = dashScopeUsageToOpenAIMap(usage, imageCount)
	}
	body, err := json.Marshal(resp)
	if err != nil {
		return nil, usage, imageCount, err
	}
	if !usageOK {
		if extracted, ok := extractOpenAIUsageFromJSONBytes(body); ok {
			usage = extracted
		}
	}
	return body, usage, imageCount, nil
}

func parseDashScopeImageUsage(body []byte) (OpenAIUsage, bool) {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return OpenAIUsage{}, false
	}
	usageNode := gjson.GetBytes(body, "usage")
	if !usageNode.Exists() || !usageNode.IsObject() {
		return OpenAIUsage{}, false
	}
	if usage, ok := openAIUsageFromGJSON(usageNode); ok {
		if usage.ImageOutputTokens == 0 {
			if imageTokens := usageNode.Get("image_tokens").Int(); imageTokens > 0 {
				usage.ImageOutputTokens = int(imageTokens)
			} else if usage.OutputTokens > 0 {
				usage.ImageOutputTokens = usage.OutputTokens
			}
		}
		if usage.ImageInputTokens == 0 {
			if imageTokens := usageNode.Get("input_tokens_details.image_tokens").Int(); imageTokens > 0 {
				usage.ImageInputTokens = int(imageTokens)
			}
		}
		return usage, usage.InputTokens > 0 || usage.OutputTokens > 0 || usage.ImageInputTokens > 0 || usage.ImageOutputTokens > 0 || usageNode.Get("image_count").Int() > 0
	}
	return OpenAIUsage{}, false
}

func dashScopeUsageImageCount(body []byte) int {
	if count := int(gjson.GetBytes(body, "usage.image_count").Int()); count > 0 {
		return count
	}
	if count := int(gjson.GetBytes(body, "usage.images").Int()); count > 0 {
		return count
	}
	return 0
}

func dashScopeUsageToOpenAIMap(usage OpenAIUsage, imageCount int) map[string]any {
	out := map[string]any{
		"input_tokens":  usage.InputTokens,
		"output_tokens": usage.OutputTokens,
		"total_tokens":  usage.InputTokens + usage.OutputTokens,
	}
	if imageCount > 0 {
		out["images"] = imageCount
	}
	inputDetails := map[string]any{}
	if usage.ImageInputTokens > 0 {
		inputDetails["image_tokens"] = usage.ImageInputTokens
		textTokens := usage.InputTokens - usage.ImageInputTokens
		if textTokens < 0 {
			textTokens = 0
		}
		inputDetails["text_tokens"] = textTokens
	} else if usage.InputTokens > 0 {
		inputDetails["text_tokens"] = usage.InputTokens
		inputDetails["image_tokens"] = 0
	}
	if len(inputDetails) > 0 {
		out["input_tokens_details"] = inputDetails
	}
	if usage.ImageOutputTokens > 0 {
		out["output_tokens_details"] = map[string]any{"image_tokens": usage.ImageOutputTokens}
	}
	return out
}

func dashScopeRequestID(header http.Header, body []byte) string {
	if header != nil {
		if id := strings.TrimSpace(header.Get("x-request-id")); id != "" {
			return id
		}
		if id := strings.TrimSpace(header.Get("request-id")); id != "" {
			return id
		}
	}
	if id := strings.TrimSpace(gjson.GetBytes(body, "request_id").String()); id != "" {
		return id
	}
	return strings.TrimSpace(gjson.GetBytes(body, "output.task_id").String())
}

func dashScopeErrorCode(body []byte) string {
	if code := strings.TrimSpace(gjson.GetBytes(body, "code").String()); code != "" {
		return code
	}
	if code := strings.TrimSpace(extractUpstreamErrorCode(body)); code != "" {
		return code
	}
	return strings.TrimSpace(gjson.GetBytes(body, "output.code").String())
}

func isDashScopeThrottlingPayload(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	code := strings.ToLower(dashScopeErrorCode(body))
	if strings.Contains(code, "invalidparameter") {
		return false
	}
	if strings.Contains(code, "throttl") || strings.Contains(code, "ratequota") || strings.Contains(code, "limitrequests") {
		return true
	}
	msg := strings.ToLower(extractUpstreamErrorMessage(body))
	return strings.Contains(msg, "rate limit") || strings.Contains(msg, "throttl") || strings.Contains(msg, "too many requests")
}

func isDashScopeUnsupportedModelError(statusCode int, body []byte) bool {
	if len(body) == 0 {
		return false
	}
	switch statusCode {
	case 0, http.StatusBadRequest, http.StatusForbidden, http.StatusNotFound:
	default:
		return false
	}
	code := strings.ToLower(dashScopeErrorCode(body))
	// InvalidParameter is a request failure, including a bad model parameter.
	if strings.Contains(code, "invalidparameter") {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(extractUpstreamErrorMessage(body)))
	if msg == "" {
		msg = strings.ToLower(string(body))
	}
	if strings.Contains(msg, "when using codex") {
		return false
	}
	switch {
	case strings.Contains(code, "model.accessdenied"),
		strings.Contains(code, "modelnotexist"),
		strings.Contains(code, "invalidmodel"),
		strings.Contains(code, "model.notfound"):
		return true
	case strings.Contains(msg, "model not exist"),
		strings.Contains(msg, "model does not exist"),
		strings.Contains(msg, dashScopeUnsupportedModelReasonPhrase),
		strings.Contains(msg, "unknown model"),
		strings.Contains(msg, "not have access to the model"),
		(strings.Contains(msg, "access denied") && strings.Contains(msg, "model")):
		return true
	default:
		return false
	}
}

func isImageCapabilityRateLimitError(ctx context.Context, statusCode int, body []byte, models ...string) bool {
	if isOpenAIImageRateLimitError(statusCode, body) {
		return true
	}
	if statusCode != http.StatusTooManyRequests || !isDashScopeThrottlingPayload(body) {
		return false
	}
	if OpenAIImagesEndpointFromContext(ctx) || OpenAIImageGenerationIntentFromContext(ctx) {
		return true
	}
	for _, model := range models {
		if isDashScopeImageGenerationModel(model) {
			return true
		}
	}
	return false
}

// DashScope request/configuration failures must not mutate account scheduling.
// Only quota/throttling and overload responses enter the shared cooldown path.
func (s *OpenAIGatewayService) handleDashScopeImagesErrorResponse(ctx context.Context, resp *http.Response, c *gin.Context, account *Account, model string) (*OpenAIForwardResult, error) {
	body := s.readUpstreamErrorBody(resp)
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	code := strings.ToLower(dashScopeErrorCode(body))
	parameterError := strings.Contains(code, "invalidparameter")
	coolable := !parameterError && (resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable || isDashScopeThrottlingPayload(body))
	if coolable {
		resp.Body = io.NopCloser(bytes.NewReader(body))
		return s.handleOpenAIImagesErrorResponse(ctx, resp, c, account, model)
	}
	upErr := openAIImagesUpstreamErrorFromHTTP(resp.StatusCode, resp.Header, body)
	upErr.Code = dashScopeErrorCode(body)
	upErr.UpstreamRequestID = dashScopeRequestID(resp.Header, body)
	if message := strings.TrimSpace(gjson.GetBytes(body, "output.message").String()); message != "" {
		upErr.Message = sanitizeUpstreamErrorMessage(message)
	}
	setOpsUpstreamError(c, resp.StatusCode, upErr.Message, "")
	if status, errType, message, matched := applyErrorPassthroughRule(c, account.Platform, resp.StatusCode, body, http.StatusBadGateway, "upstream_error", "Upstream request failed"); matched {
		upErr.StatusCode, upErr.ErrorType, upErr.Message = status, errType, message
	}
	writeOpenAIImagesUpstreamErrorResponse(c, upErr)
	return nil, upErr
}
