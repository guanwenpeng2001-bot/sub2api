package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/claude"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/geminicli"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// RetrieveModel handles OpenAI-compatible GET /v1/models/{model}.
// The catalog is the same visible list as GET /v1/models. Matching is exact
// after lowercasing; unknown IDs are 404. Codex client_version and list ETags
// are intentionally not used.
func (h *GatewayHandler) RetrieveModel(c *gin.Context) {
	requested := strings.TrimSpace(c.Param("model"))
	if requested == "" {
		writeRetrieveModelNotFound(c, requested)
		return
	}

	apiKey, _ := middleware2.GetAPIKeyFromContext(c)
	var groupID *int64
	var platform string
	if apiKey != nil && apiKey.Group != nil {
		groupID = &apiKey.Group.ID
		platform = apiKey.Group.Platform
	}
	if forcedPlatform, ok := middleware2.GetForcePlatformFromContext(c); ok && strings.TrimSpace(forcedPlatform) != "" {
		platform = forcedPlatform
	}

	if platform == service.PlatformOpenAI && apiKey != nil && apiKey.Group != nil &&
		apiKey.Group.Platform == service.PlatformOpenAI && apiKey.Group.CodexModelsManifestConfig.Enabled {
		h.retrievePinnedOpenAIModel(c, apiKey.Group, requested)
		return
	}

	var modelIDs []string
	if platform == service.PlatformComposite {
		availableModels := h.compositeAvailableModels(c.Request.Context(), groupID)
		if apiKey != nil && apiKey.Group != nil && apiKey.Group.ModelAllowlistEnabled() {
			source := availableModels
			if len(source) == 0 {
				source = defaultModelIDsForPlatform(service.PlatformComposite)
			}
			modelIDs = apiKey.Group.ModelAllowlist.FilterForListing(source)
		} else if len(availableModels) > 0 {
			modelIDs = availableModels
		} else {
			modelIDs = defaultModelIDsForPlatform(service.PlatformComposite)
		}
		writeRetrievedModel(c, platform, modelIDs, requested)
		return
	}

	availableModels := h.gatewayService.GetAvailableModels(c.Request.Context(), groupID, platform)
	if apiKey != nil && apiKey.Group != nil && apiKey.Group.ModelAllowlistEnabled() {
		source := modelListingSource(platform, availableModels, defaultModelIDsForPlatform(platform))
		modelIDs = apiKey.Group.ModelAllowlist.FilterForListing(source)
		writeRetrievedModel(c, platform, modelIDs, requested)
		return
	}
	if len(availableModels) > 0 {
		writeRetrievedModel(c, platform, availableModels, requested)
		return
	}
	writeRetrievedModel(c, platform, defaultModelIDsForPlatform(platform), requested)
}

func (h *GatewayHandler) retrievePinnedOpenAIModel(c *gin.Context, group *service.Group, requested string) {
	if c.Request.Context().Err() != nil {
		return
	}
	if h.openAIGatewayService == nil {
		writeOpenAIModelsError(c, http.StatusInternalServerError, "api_error", "OpenAI model discovery is not configured")
		return
	}
	response, account, err := h.openAIGatewayService.FetchPinnedOpenAIModelsList(
		c.Request.Context(), group, h.maxAccountSwitches, "",
	)
	if c.Request.Context().Err() != nil {
		return
	}
	if err != nil {
		if errors.Is(err, service.ErrNoPinnedCodexModelsAccounts) {
			writeOpenAIModelsError(c, http.StatusServiceUnavailable, "upstream_error", "No available OpenAI model discovery accounts")
			return
		}
		writeOpenAIModelsError(c, infraerrors.Code(err), "upstream_error", infraerrors.Message(err))
		return
	}
	setOpsSelectedAccount(c, account.ID, account.Platform)
	if !writePinnedRetrievedModel(c, response.Body, requested) {
		writeRetrieveModelNotFound(c, requested)
	}
}

func writePinnedRetrievedModel(c *gin.Context, body []byte, requested string) bool {
	var parsed struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return false
	}
	want := strings.ToLower(strings.TrimSpace(requested))
	for _, raw := range parsed.Data {
		var item struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &item); err != nil {
			continue
		}
		if strings.ToLower(strings.TrimSpace(item.ID)) == want {
			c.Data(http.StatusOK, "application/json", raw)
			return true
		}
	}
	return false
}

func writeRetrievedModel(c *gin.Context, platform string, modelIDs []string, requested string) {
	matched, ok := matchListedModelID(modelIDs, requested)
	if !ok {
		writeRetrieveModelNotFound(c, requested)
		return
	}
	switch platform {
	case service.PlatformOpenAI:
		c.JSON(http.StatusOK, openAIModelForID(matched))
	case service.PlatformGrok:
		c.JSON(http.StatusOK, grokModelListItemForID(matched))
	case service.PlatformGemini:
		c.JSON(http.StatusOK, geminiModelForID(matched))
	default:
		c.JSON(http.StatusOK, claudeModelForID(matched))
	}
}

func matchListedModelID(modelIDs []string, requested string) (string, bool) {
	want := strings.ToLower(strings.TrimSpace(requested))
	if want == "" {
		return "", false
	}
	for _, id := range modelIDs {
		id = strings.TrimSpace(id)
		if id != "" && strings.ToLower(id) == want {
			return id, true
		}
	}
	return "", false
}

func openAIModelForID(modelID string) openai.Model {
	for _, model := range openai.DefaultModels {
		if model.ID == modelID {
			return model
		}
	}
	return openai.Model{
		ID:          modelID,
		Object:      "model",
		Created:     1704067200,
		OwnedBy:     "openai",
		Type:        "model",
		DisplayName: modelID,
	}
}

func geminiModelForID(modelID string) geminicli.Model {
	for _, model := range geminicli.DefaultModels {
		if model.ID == modelID {
			return model
		}
	}
	return geminicli.Model{ID: modelID, Type: "model", DisplayName: modelID}
}

func claudeModelForID(modelID string) claude.Model {
	for _, model := range claude.DefaultModels {
		if model.ID == modelID {
			return model
		}
	}
	return claude.Model{
		ID:          modelID,
		Type:        "model",
		DisplayName: modelID,
		CreatedAt:   "2024-01-01T00:00:00Z",
	}
}

func grokModelListItemForID(modelID string) grokModelListItem {
	defaults := xai.DefaultModels()
	model := xai.Model{ID: modelID, Object: "model", OwnedBy: "xai", DisplayName: modelID}
	for _, candidate := range defaults {
		if candidate.ID == modelID {
			model = candidate
			break
		}
	}
	item := grokModelListItem{Model: model}
	if grokModelSupportsConfigurableReasoning(modelID) {
		item.SupportsReasoningEffort = true
		item.ReasoningEffort = "high"
		efforts := []grokReasoningEffortOption{
			{Value: "low", Label: "Low"},
			{Value: "medium", Label: "Medium"},
			{Value: "high", Label: "High", Default: true},
		}
		if service.GrokSupportsXHighReasoningEffort(modelID) {
			efforts = append(efforts, grokReasoningEffortOption{Value: "xhigh", Label: "xHigh"})
		}
		item.ReasoningEfforts = efforts
	}
	return item
}

func writeRetrieveModelNotFound(c *gin.Context, modelID string) {
	service.MarkOpsClientBusinessLimited(c, service.OpsClientBusinessLimitedReasonLocalFeatureGate)
	c.JSON(http.StatusNotFound, gin.H{
		"error": gin.H{
			"message": "The model '" + modelID + "' does not exist",
			"type":    "invalid_request_error",
			"param":   "model",
			"code":    "model_not_found",
		},
	})
}
