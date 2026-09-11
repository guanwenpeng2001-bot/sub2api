package service

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Use a real net/http client with no Client.Timeout: cancellation must reach
// both the blocked RoundTrip and the response body's Read through the context.
type dashScopeNetworkUpstream struct {
	HTTPUpstream
	client *http.Client
	target *url.URL
}

func (u *dashScopeNetworkUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.URL.Scheme, clone.URL.Host = u.target.Scheme, u.target.Host
	return u.client.Do(clone)
}

func TestDashScopeOperationDeadlineCancelsNetwork(t *testing.T) {
	for _, phase := range []string{"sync_headers", "sync_body", "create_headers", "create_body", "poll_headers", "poll_body", "poll_sleep"} {
		t.Run(phase, func(t *testing.T) {
			withDashScopeImageTestClock(t)
			dashScopeImageTaskTimeout = 150 * time.Millisecond
			dashScopeImageTaskPollInterval = time.Hour
			canceled := make(chan struct{}, 1)
			stop := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if (phase == "poll_headers" || phase == "poll_body" || phase == "poll_sleep") && r.Method == http.MethodPost {
					_, _ = io.WriteString(w, `{"output":{"task_id":"task-1"}}`)
					return
				}
				if phase == "poll_sleep" {
					_, _ = io.WriteString(w, `{"output":{"task_status":"RUNNING"}}`)
					return
				}
				if phase == "sync_body" || phase == "create_body" || phase == "poll_body" {
					_, _ = io.WriteString(w, `{"output":`)
					if f, ok := w.(http.Flusher); ok {
						f.Flush()
					}
				}
				select {
				case <-r.Context().Done():
					canceled <- struct{}{}
				case <-stop:
				}
			}))
			defer server.Close()
			defer close(stop)
			target, err := url.Parse(server.URL)
			require.NoError(t, err)
			svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: &dashScopeNetworkUpstream{client: server.Client(), target: target}}
			model := "wanx-v1"
			if phase == "sync_headers" || phase == "sync_body" {
				model = "qwen-image"
			}
			body := []byte(fmt.Sprintf(`{"model":%q,"prompt":"cat"}`, model))
			c, rec := newDashScopeImagesTestContext(t, body)
			parsed, err := svc.ParseOpenAIImagesRequest(c, body)
			require.NoError(t, err)
			// Downstream disconnect must not remove the independent operation deadline.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			started := time.Now()
			result, err := svc.ForwardImages(ctx, c, newDashScopeImageAccount(), body, parsed, "")
			require.Error(t, err)
			require.Nil(t, result)
			require.Equal(t, http.StatusGatewayTimeout, rec.Code)
			require.Less(t, time.Since(started), 2*time.Second)
			if phase != "poll_sleep" {
				select {
				case <-canceled:
				case <-time.After(time.Second):
					t.Fatal("HTTP request remained blocked after the operation deadline")
				}
			}
		})
	}
}

func TestDashScopeRejectsUnsupportedOptionsBeforeUpstream(t *testing.T) {
	for _, option := range []string{"stream", "mask_url", "mask_upload"} {
		t.Run(option, func(t *testing.T) {
			c, rec := newDashScopeImagesTestContext(t, nil)
			upstream := &httpUpstreamRecorder{}
			svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
			parsed := &OpenAIImagesRequest{Model: "qwen-image-edit", Prompt: "cat", N: 1}
			switch option {
			case "stream":
				parsed.Stream = true
			case "mask_url":
				parsed.MaskImageURL = "https://example.com/mask.png"
			case "mask_upload":
				parsed.MaskUpload = &OpenAIImagesUpload{}
			}
			result, err := svc.ForwardImages(context.Background(), c, newDashScopeImageAccount(), nil, parsed, "")
			require.Nil(t, result)
			var upErr *OpenAIImagesUpstreamError
			require.ErrorAs(t, err, &upErr)
			require.Equal(t, "unsupported_parameter", upErr.Code)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Contains(t, upErr.Message, "do not support")
			require.Empty(t, upstream.requests)
		})
	}
}

func TestDashScopeDeliveryFailurePreservesGeneratedUsage(t *testing.T) {
	for _, model := range []string{"qwen-image", "wanx-v1"} {
		for _, firstDelivered := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/partial=%v", model, firstDelivered), func(t *testing.T) {
				withDashScopeImageTestClock(t)
				// The second generated image is blocked by download URL validation.
				first := "http://127.0.0.1/first.png"
				if firstDelivered {
					first = "data:image/png;base64,aGVsbG8="
				}
				usage := `"usage":{"input_tokens":16,"output_tokens":1290,"image_count":3}`
				output := fmt.Sprintf(`{"output":{"choices":[{"message":{"content":[{"image":%q},{"image":"http://127.0.0.1/second.png"}]}}]},%s}`, first, usage)
				responses := []*http.Response{}
				if model == "wanx-v1" {
					responses = append(responses, dashScopeJSONResponse(200, `{"output":{"task_id":"task-1"}}`))
					output = fmt.Sprintf(`{"output":{"task_status":"SUCCEEDED","results":[{"url":%q},{"url":"http://127.0.0.1/second.png"}]},%s}`, first, usage)
				}
				responses = append(responses, dashScopeJSONResponse(200, output))
				upstream := &httpUpstreamRecorder{responses: responses}
				svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
				body := []byte(fmt.Sprintf(`{"model":%q,"prompt":"cat","response_format":"b64_json"}`, model))
				c, rec := newDashScopeImagesTestContext(t, body)
				parsed, err := svc.ParseOpenAIImagesRequest(c, body)
				require.NoError(t, err)
				result, err := svc.ForwardImages(context.Background(), c, newDashScopeImageAccount(), body, parsed, "")
				var upErr *OpenAIImagesUpstreamError
				require.ErrorAs(t, err, &upErr)
				require.Equal(t, "image_download_failed", upErr.Code)
				require.Equal(t, 502, rec.Code)
				require.NotNil(t, result)
				require.Equal(t, 3, result.ImageCount)
				require.Equal(t, 16, result.Usage.InputTokens)
				require.Equal(t, 1290, result.Usage.OutputTokens)
				require.Equal(t, 1290, result.Usage.ImageOutputTokens)
				require.Equal(t, "req_dashscope", result.RequestID)
				require.Equal(t, model, result.UpstreamModel)
				require.NotEmpty(t, result.UpstreamEndpoint)
			})
		}
	}
}

func TestDashScopeInvalidBase64RetainsUsage(t *testing.T) {
	upstream := &httpUpstreamRecorder{resp: dashScopeJSONResponse(200, `{"output":{"choices":[{"message":{"content":[{"image":"data:image/png;base64,!!!invalid!!!"}]}}]},"usage":{"input_tokens":7,"output_tokens":42,"image_count":1}}`)}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
	c, rec := newDashScopeImagesTestContext(t, nil)
	parsed := &OpenAIImagesRequest{Model: "qwen-image", Prompt: "cat", N: 1, ResponseFormat: "b64_json"}
	result, err := svc.ForwardImages(context.Background(), c, newDashScopeImageAccount(), nil, parsed, "")
	require.Error(t, err)
	require.Equal(t, 502, rec.Code)
	require.NotNil(t, result)
	require.Equal(t, 1, result.ImageCount)
	require.Equal(t, 7, result.Usage.InputTokens)
	require.Equal(t, 42, result.Usage.OutputTokens)
}

func TestDashScopeDownloadHTTPFailurePreservesAllUsage(t *testing.T) {
	for _, failure := range []string{"expired_url", "invalid_content", "truncated_body"} {
		t.Run(failure, func(t *testing.T) {
			failed := b64BackfillImageResponse(http.StatusForbidden, "text/plain", []byte("expired"))
			if failure == "invalid_content" {
				failed = b64BackfillImageResponse(200, "image/png", []byte("not an image"))
			}
			if failure == "truncated_body" {
				failed = &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(dashScopeFailedImageReader{})}
			}
			upstream := &httpUpstreamRecorder{responses: []*http.Response{
				dashScopeJSONResponse(200, `{"output":{"choices":[{"message":{"content":[{"image":"https://cdn.example.com/first.png"},{"image":"https://cdn.example.com/second.png"}]}}]},"usage":{"input_tokens":12,"output_tokens":99,"image_count":2}}`),
				b64BackfillImageResponse(200, "image/png", b64BackfillPNGBytes), failed,
			}}
			svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
			c, rec := newDashScopeImagesTestContext(t, nil)
			parsed := &OpenAIImagesRequest{Model: "qwen-image", Prompt: "cat", N: 2, ResponseFormat: "b64_json"}
			result, err := svc.ForwardImages(context.Background(), c, newDashScopeImageAccount(), nil, parsed, "")
			require.Error(t, err)
			require.Equal(t, 502, rec.Code)
			require.NotNil(t, result)
			require.Equal(t, 2, result.ImageCount)
			require.Equal(t, 12, result.Usage.InputTokens)
			require.Equal(t, 99, result.Usage.OutputTokens)
			require.Len(t, upstream.requests, 3)
		})
	}
}

type dashScopeFailedImageReader struct{}

func (dashScopeFailedImageReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

// Exercise wire headers with a real HTTP transport: Header.Get only sees the
// first value and missed the duplicate Content-Type that DashScope rejects.
func TestDashScopeNativeWireProtocol(t *testing.T) {
	for _, model := range []string{"qwen-image-max", "z-image-turbo", "wan2.2-t2i-flash"} {
		t.Run(model, func(t *testing.T) {
			withDashScopeImageTestClock(t)
			type wireRequest struct {
				method, path string
				header       http.Header
				body         []byte
			}
			requests := make(chan wireRequest, 3)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				requests <- wireRequest{r.Method, r.URL.Path, r.Header.Clone(), body}
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/v1/services/aigc/multimodal-generation/generation":
					_, _ = io.WriteString(w, `{"output":{"choices":[{"message":{"content":[{"image":"https://cdn.example.com/test.png"}]}}]},"usage":{"image_count":1}}`)
				case "/api/v1/services/aigc/text2image/image-synthesis":
					_, _ = io.WriteString(w, `{"output":{"task_id":"task-1","task_status":"PENDING"}}`)
				case "/api/v1/tasks/task-1":
					_, _ = io.WriteString(w, `{"output":{"task_status":"SUCCEEDED","results":[{"url":"https://cdn.example.com/test.png"}]},"usage":{"image_count":1}}`)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			target, err := url.Parse(server.URL)
			require.NoError(t, err)
			svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: &dashScopeNetworkUpstream{client: server.Client(), target: target}}
			body := []byte(fmt.Sprintf(`{"model":%q,"prompt":"a cube","n":1,"size":"1024x1024","response_format":"url"}`, model))
			c, rec := newDashScopeImagesTestContext(t, body)
			parsed, err := svc.ParseOpenAIImagesRequest(c, body)
			require.NoError(t, err)
			// Native encoding must also replace multipart and repeated SDK headers.
			c.Request.Header.Add("Content-Type", "multipart/form-data; boundary=test")
			c.Request.Header["accept"] = []string{"application/json", "text/event-stream"}
			c.Request.Header.Set("X-DashScope-Async", "disable")
			c.Request.Header.Set("User-Agent", "dashscope-regression")
			account := newDashScopeImageAccount()
			account.Credentials["header_override_enabled"] = true
			account.Credentials["header_overrides"] = map[string]any{"accept": "text/event-stream", "content-type": "text/plain", "user-agent": "native-test"}
			result, err := svc.ForwardImages(context.Background(), c, account, body, parsed, "")
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, 1, result.ImageCount)
			close(requests)
			count := 0
			for req := range requests {
				count++
				require.Equal(t, []string{"application/json"}, req.header.Values("Accept"))
				require.Equal(t, []string{"Bearer sk-dashscope-test"}, req.header.Values("Authorization"))
				if req.method == http.MethodPost {
					require.Equal(t, []string{"application/json"}, req.header.Values("Content-Type"))
					require.Equal(t, model, gjson.GetBytes(req.body, "model").String())
					if model == "wan2.2-t2i-flash" {
						require.Equal(t, "/api/v1"+dashScopeText2ImagePath, req.path)
						require.Equal(t, []string{"enable"}, req.header.Values(dashScopeAsyncHeaderName))
						require.Equal(t, "a cube", gjson.GetBytes(req.body, "input.prompt").String())
					} else {
						require.Equal(t, "/api/v1"+dashScopeMultimodalGenerationPath, req.path)
						require.Empty(t, req.header.Values(dashScopeAsyncHeaderName))
						require.Equal(t, "a cube", gjson.GetBytes(req.body, "input.messages.0.content.0.text").String())
						if model == "qwen-image-max" {
							require.Equal(t, "1328*1328", gjson.GetBytes(req.body, "parameters.size").String())
						} else {
							require.Equal(t, "1024*1024", gjson.GetBytes(req.body, "parameters.size").String())
							require.False(t, gjson.GetBytes(req.body, "parameters.n").Exists())
						}
					}
				} else {
					require.Equal(t, http.MethodGet, req.method)
					require.Empty(t, req.header.Values("Content-Type"))
					require.Empty(t, req.header.Values(dashScopeAsyncHeaderName))
				}
			}
			if model == "wan2.2-t2i-flash" {
				require.Equal(t, 2, count)
			} else {
				require.Equal(t, 1, count)
			}
		})
	}
}

func TestDashScopeDefaultResponseIncludesImageBytes(t *testing.T) {
	for _, format := range []string{"", "b64_json", "url"} {
		t.Run("format="+format, func(t *testing.T) {
			upstream := &httpUpstreamRecorder{responses: []*http.Response{
				dashScopeJSONResponse(200, `{"output":{"choices":[{"message":{"content":[{"image":"https://cdn.example.com/image.png"}]}}]},"usage":{"image_count":1}}`),
				b64BackfillImageResponse(200, "image/png", b64BackfillPNGBytes),
			}}
			svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
			c, rec := newDashScopeImagesTestContext(t, nil)
			parsed := &OpenAIImagesRequest{Model: "qwen-image-max", Prompt: "a cube", N: 1, ResponseFormat: format}
			result, err := svc.ForwardImages(context.Background(), c, newDashScopeImageAccount(), nil, parsed, "")
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, 1, result.ImageCount)
			require.Equal(t, "https://cdn.example.com/image.png", gjson.Get(rec.Body.String(), "data.0.url").String())
			if format == "url" {
				require.Len(t, upstream.requests, 1)
				require.False(t, gjson.Get(rec.Body.String(), "data.0.b64_json").Exists())
			} else {
				require.Len(t, upstream.requests, 2)
				require.Equal(t, base64.StdEncoding.EncodeToString(b64BackfillPNGBytes), gjson.Get(rec.Body.String(), "data.0.b64_json").String())
				require.Empty(t, upstream.requests[1].Header.Get("Authorization"))
				require.True(t, HTTPUpstreamPublicHostsOnly(upstream.requests[1].Context()))
			}
		})
	}
}
