package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type KiroExecutor struct {
	cfg *config.Config
}

func NewKiroExecutor(cfg *config.Config) *KiroExecutor {
	return &KiroExecutor{cfg: cfg}
}

func (e *KiroExecutor) Identifier() string {
	return "kiro"
}

func (e *KiroExecutor) PrepareRequest(req *http.Request, auth *cliproxyauth.Auth) error {
	if req == nil {
		return nil
	}
	token, _, err := helps.EnsureKiroAccessToken(req.Context(), e.cfg, auth)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	util.ApplyCustomHeadersFromAttrs(req, authAttributes(auth))
	return nil
}

func (e *KiroExecutor) HttpRequest(ctx context.Context, auth *cliproxyauth.Auth, req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, fmt.Errorf("kiro executor: request is nil")
	}
	if ctx == nil {
		ctx = req.Context()
	}
	httpReq := req.WithContext(ctx)
	if err := e.PrepareRequest(httpReq, auth); err != nil {
		return nil, err
	}
	creds := helps.KiroCredentialsFromAuth(auth)
	httpClient := helps.NewKiroHTTPClient(ctx, e.cfg, auth, creds, 0)
	return httpClient.Do(httpReq)
}

func (e *KiroExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	reporter := helps.NewUsageReporter(ctx, e.Identifier(), baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	to := sdktranslator.FromString("claude")
	originalPayloadSource := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayloadSource = opts.OriginalRequest
	}
	originalPayloadSource = normalizeKiroSourcePayload(from, originalPayloadSource)
	requestPayload := normalizeKiroSourcePayload(from, req.Payload)
	originalTranslated := sdktranslator.TranslateRequest(from, to, baseModel, originalPayloadSource, false)
	body := sdktranslator.TranslateRequest(from, to, baseModel, requestPayload, false)
	body, err = thinking.ApplyThinking(body, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return resp, err
	}
	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	body = helps.ApplyPayloadConfigWithRoot(e.cfg, baseModel, to.String(), "", body, originalTranslated, requestedModel)

	kiroBody, err := buildKiroRequestBody(body, auth, baseModel)
	if err != nil {
		return resp, err
	}

	events, headers, err := e.executeKiroEvents(ctx, auth, kiroBody, baseModel)
	if err != nil {
		return resp, err
	}
	reporter.EnsurePublished(ctx)

	if from == to {
		return cliproxyexecutor.Response{
			Payload: buildClaudeResponsePayload(baseModel, events),
			Headers: headers,
		}, nil
	}

	dataLines, err := buildClaudeDataLines(baseModel, events)
	if err != nil {
		return resp, err
	}
	out := sdktranslator.TranslateNonStream(ctx, to, from, req.Model, opts.OriginalRequest, body, joinClaudeDataLines(dataLines), nil)
	if opts.Alt == "responses/compact" && from == sdktranslator.FromString("openai-response") {
		out = rewriteOpenAIResponsesCompactPayload(out)
	}
	return cliproxyexecutor.Response{Payload: out, Headers: headers}, nil
}

func (e *KiroExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	if opts.Alt == "responses/compact" {
		return nil, statusErr{code: http.StatusBadRequest, msg: "streaming not supported for /responses/compact"}
	}
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	reporter := helps.NewUsageReporter(ctx, e.Identifier(), baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	to := sdktranslator.FromString("claude")
	originalPayloadSource := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayloadSource = opts.OriginalRequest
	}
	originalPayloadSource = normalizeKiroSourcePayload(from, originalPayloadSource)
	requestPayload := normalizeKiroSourcePayload(from, req.Payload)
	originalTranslated := sdktranslator.TranslateRequest(from, to, baseModel, originalPayloadSource, true)
	body := sdktranslator.TranslateRequest(from, to, baseModel, requestPayload, true)
	body, err = thinking.ApplyThinking(body, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return nil, err
	}
	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	body = helps.ApplyPayloadConfigWithRoot(e.cfg, baseModel, to.String(), "", body, originalTranslated, requestedModel)

	kiroBody, err := buildKiroRequestBody(body, auth, baseModel)
	if err != nil {
		return nil, err
	}
	events, headers, err := e.executeKiroEvents(ctx, auth, kiroBody, baseModel)
	if err != nil {
		return nil, err
	}
	reporter.EnsurePublished(ctx)

	out := make(chan cliproxyexecutor.StreamChunk, 16)
	go func() {
		defer close(out)
		if from == to {
			chunks, errBuild := buildClaudeSSEChunks(baseModel, events)
			if errBuild != nil {
				log.Errorf("kiro executor: build claude sse chunks failed: %v", errBuild)
				return
			}
			for _, chunk := range chunks {
				out <- cliproxyexecutor.StreamChunk{Payload: chunk}
			}
			return
		}

		dataLines, errBuild := buildClaudeDataLines(baseModel, events)
		if errBuild != nil {
			log.Errorf("kiro executor: build claude data lines failed: %v", errBuild)
			return
		}
		var param any
		for _, line := range dataLines {
			chunks := sdktranslator.TranslateStream(ctx, to, from, req.Model, opts.OriginalRequest, body, line, &param)
			for i := range chunks {
				out <- cliproxyexecutor.StreamChunk{Payload: chunks[i]}
			}
		}
	}()

	streamHeaders := headers.Clone()
	if streamHeaders == nil {
		streamHeaders = make(http.Header)
	}
	streamHeaders.Set("Content-Type", "text/event-stream")
	return &cliproxyexecutor.StreamResult{Headers: streamHeaders, Chunks: out}, nil
}

func (e *KiroExecutor) Refresh(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	log.Debugf("kiro executor: refresh called")
	return helps.RefreshKiroAuth(ctx, e.cfg, auth)
}

func (e *KiroExecutor) CountTokens(_ context.Context, _ *cliproxyauth.Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, statusErr{code: http.StatusNotImplemented, msg: "kiro count tokens not supported"}
}

func (e *KiroExecutor) executeKiroEvents(ctx context.Context, auth *cliproxyauth.Auth, body []byte, baseModel string) ([]helps.KiroEvent, http.Header, error) {
	token, updatedAuth, err := helps.EnsureKiroAccessToken(ctx, e.cfg, auth)
	if err != nil {
		return nil, nil, err
	}
	targetAuth := auth
	if updatedAuth != nil {
		targetAuth = updatedAuth
	}
	creds := helps.KiroCredentialsFromAuth(targetAuth)
	host := fmt.Sprintf("q.%s.amazonaws.com", credsEffectiveAPIRegion(creds))
	requestURL := fmt.Sprintf("https://%s/generateAssistantResponse", host)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	if err := helps.ApplyKiroGenerateHeaders(httpReq, creds, token, host, helps.KiroAgentMode(baseModel)); err != nil {
		return nil, nil, err
	}
	util.ApplyCustomHeadersFromAttrs(httpReq, authAttributes(targetAuth))
	var authID, authLabel, authType, authValue string
	if targetAuth != nil {
		authID = targetAuth.ID
		authLabel = targetAuth.Label
		authType, authValue = targetAuth.AccountInfo()
	}
	helps.RecordAPIRequest(ctx, e.cfg, helps.UpstreamRequestLog{
		URL:       requestURL,
		Method:    http.MethodPost,
		Headers:   httpReq.Header.Clone(),
		Body:      body,
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	})
	httpResp, err := helps.NewKiroHTTPClient(ctx, e.cfg, targetAuth, creds, 0).Do(httpReq)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return nil, nil, err
	}
	defer func() {
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("kiro executor: close response body error: %v", errClose)
		}
	}()
	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())
	bodyBytes, err := io.ReadAll(httpResp.Body)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return nil, nil, err
	}
	helps.AppendAPIResponseChunk(ctx, e.cfg, bodyBytes)
	if httpResp.StatusCode < http.StatusOK || httpResp.StatusCode >= http.StatusMultipleChoices {
		return nil, nil, statusErr{code: httpResp.StatusCode, msg: strings.TrimSpace(string(bodyBytes))}
	}
	events, err := helps.CollectKiroEventsFromEventStream(bodyBytes)
	if err != nil {
		return nil, nil, err
	}
	return events, httpResp.Header.Clone(), nil
}

type anthropicMessagesRequest struct {
	System   json.RawMessage    `json:"system"`
	Messages []anthropicMessage `json:"messages"`
	Tools    []anthropicTool    `json:"tools"`
}

type anthropicMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type anthropicTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

type anthropicContentBlock struct {
	Type      string           `json:"type"`
	Text      string           `json:"text"`
	Thinking  string           `json:"thinking"`
	ID        string           `json:"id"`
	Name      string           `json:"name"`
	Input     json.RawMessage  `json:"input"`
	ToolUseID string           `json:"tool_use_id"`
	Content   json.RawMessage  `json:"content"`
	IsError   bool             `json:"is_error"`
	Source    *anthropicSource `json:"source,omitempty"`
}

type anthropicSource struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type"`
	Data      string `json:"data"`
	URL       string `json:"url"`
}

type kiroRequestPayload struct {
	ConversationState kiroConversationState `json:"conversationState"`
	ProfileARN        string                `json:"profileArn,omitempty"`
}

type kiroConversationState struct {
	AgentTaskType   string               `json:"agentTaskType,omitempty"`
	ChatTriggerType string               `json:"chatTriggerType,omitempty"`
	ConversationID  string               `json:"conversationId"`
	CurrentMessage  kiroCurrentMessage   `json:"currentMessage"`
	History         []kiroHistoryMessage `json:"history,omitempty"`
}

type kiroCurrentMessage struct {
	UserInputMessage kiroUserMessage `json:"userInputMessage"`
}

type kiroHistoryMessage struct {
	UserInputMessage         *kiroUserMessage      `json:"userInputMessage,omitempty"`
	AssistantResponseMessage *kiroAssistantMessage `json:"assistantResponseMessage,omitempty"`
}

type kiroUserMessage struct {
	Content                 string                 `json:"content"`
	ModelID                 string                 `json:"modelId"`
	Origin                  string                 `json:"origin,omitempty"`
	UserInputMessageContext kiroUserMessageContext `json:"userInputMessageContext"`
	Images                  []kiroImage            `json:"images,omitempty"`
}

type kiroUserMessageContext struct {
	Tools       []kiroTool       `json:"tools,omitempty"`
	ToolResults []kiroToolResult `json:"toolResults,omitempty"`
}

type kiroAssistantMessage struct {
	Content  string             `json:"content"`
	ToolUses []kiroToolUseEntry `json:"toolUses,omitempty"`
}

type kiroImage struct {
	Format string          `json:"format"`
	Source kiroImageSource `json:"source"`
}

type kiroImageSource struct {
	Bytes string `json:"bytes"`
}

type kiroTool struct {
	ToolSpecification kiroToolSpecification `json:"toolSpecification"`
}

type kiroToolSpecification struct {
	Description string          `json:"description"`
	InputSchema kiroInputSchema `json:"inputSchema"`
	Name        string          `json:"name"`
}

type kiroInputSchema struct {
	JSON any `json:"json"`
}

type kiroToolResult struct {
	Content   []map[string]string `json:"content"`
	Status    string              `json:"status,omitempty"`
	ToolUseID string              `json:"toolUseId"`
	IsError   bool                `json:"isError,omitempty"`
}

type kiroToolUseEntry struct {
	Input     any    `json:"input"`
	Name      string `json:"name"`
	ToolUseID string `json:"toolUseId"`
}

type parsedAnthropicUserMessage struct {
	Text        string
	Images      []kiroImage
	ToolResults []kiroToolResult
}

type parsedAnthropicAssistantMessage struct {
	Text     string
	ToolUses []kiroToolUseEntry
}

func buildKiroRequestBody(body []byte, auth *cliproxyauth.Auth, baseModel string) ([]byte, error) {
	modelID, ok := helps.KiroMapModel(baseModel)
	if !ok {
		return nil, statusErr{code: http.StatusBadRequest, msg: fmt.Sprintf("kiro model unsupported: %s", baseModel)}
	}

	var req anthropicMessagesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, statusErr{code: http.StatusBadRequest, msg: "invalid anthropic request body"}
	}
	if len(req.Messages) == 0 {
		return nil, statusErr{code: http.StatusBadRequest, msg: "kiro requires at least one message"}
	}

	messages, err := trimTrailingAssistantPrefill(req.Messages)
	if err != nil {
		return nil, err
	}

	history := make([]kiroHistoryMessage, 0, len(messages)+2)
	systemText, err := extractAnthropicSystemText(req.System)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(systemText) != "" {
		history = append(history, buildKiroHistoryUserMessage(systemText, modelID))
		history = append(history, buildKiroHistoryAssistantMessage("I will follow these instructions.", nil))
	}

	for _, message := range messages[:len(messages)-1] {
		switch {
		case strings.EqualFold(message.Role, "user"):
			parsed, err := parseAnthropicUserMessage(message.Content)
			if err != nil {
				return nil, err
			}
			history = append(history, buildKiroHistoryUserMessageWithResults(parsed.Text, modelID, parsed.ToolResults, parsed.Images))
		case strings.EqualFold(message.Role, "assistant"):
			parsed, err := parseAnthropicAssistantMessage(message.Content)
			if err != nil {
				return nil, err
			}
			history = append(history, buildKiroHistoryAssistantMessage(parsed.Text, parsed.ToolUses))
		default:
			return nil, statusErr{code: http.StatusBadRequest, msg: fmt.Sprintf("unsupported kiro message role: %s", message.Role)}
		}
	}

	currentParsed, err := parseAnthropicUserMessage(messages[len(messages)-1].Content)
	if err != nil {
		return nil, err
	}
	validToolResults, orphanedToolUses := validateToolPairing(history, currentParsed.ToolResults)
	removeOrphanedToolUses(history, orphanedToolUses)

	tools := convertAnthropicTools(req.Tools)
	tools = appendHistoryPlaceholderTools(tools, history)

	currentText := currentParsed.Text
	if strings.TrimSpace(currentText) == "" {
		currentText = "."
	}
	currentMessage := buildKiroUserInputMessage(currentText, modelID, currentParsed.Images)
	currentMessage.UserInputMessageContext.Tools = tools
	currentMessage.UserInputMessageContext.ToolResults = validToolResults

	payload := kiroRequestPayload{
		ConversationState: kiroConversationState{
			AgentTaskType:   helps.KiroAgentMode(baseModel),
			ChatTriggerType: "MANUAL",
			ConversationID:  "conv-" + uuid.NewString(),
			CurrentMessage: kiroCurrentMessage{
				UserInputMessage: currentMessage,
			},
			History: history,
		},
	}
	if creds := helps.KiroCredentialsFromAuth(auth); strings.TrimSpace(creds.ProfileARN) != "" {
		payload.ProfileARN = strings.TrimSpace(creds.ProfileARN)
	}
	return json.Marshal(payload)
}

func trimTrailingAssistantPrefill(messages []anthropicMessage) ([]anthropicMessage, error) {
	if len(messages) == 0 {
		return nil, statusErr{code: http.StatusBadRequest, msg: "kiro requires at least one message"}
	}
	if strings.EqualFold(messages[len(messages)-1].Role, "user") {
		return messages, nil
	}
	lastUser := -1
	for i := len(messages) - 1; i >= 0; i-- {
		if strings.EqualFold(messages[i].Role, "user") {
			lastUser = i
			break
		}
	}
	if lastUser < 0 {
		return nil, statusErr{code: http.StatusBadRequest, msg: "kiro requires a trailing user message"}
	}
	return messages[:lastUser+1], nil
}

func extractAnthropicSystemText(raw json.RawMessage) (string, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return "", nil
	}
	blocks, err := decodeAnthropicContentBlocks(raw)
	if err != nil {
		return "", statusErr{code: http.StatusBadRequest, msg: "unsupported kiro system format"}
	}
	parts := make([]string, 0, len(blocks))
	for _, block := range blocks {
		if block.Type == "" || strings.EqualFold(block.Type, "text") {
			if trimmed := strings.TrimSpace(block.Text); trimmed != "" {
				parts = append(parts, trimmed)
			}
			continue
		}
		return "", statusErr{code: http.StatusBadRequest, msg: fmt.Sprintf("unsupported kiro system block type: %s", block.Type)}
	}
	return strings.Join(parts, "\n"), nil
}

func parseAnthropicUserMessage(raw json.RawMessage) (parsedAnthropicUserMessage, error) {
	blocks, err := decodeAnthropicContentBlocks(raw)
	if err != nil {
		return parsedAnthropicUserMessage{}, statusErr{code: http.StatusBadRequest, msg: "unsupported kiro message content"}
	}
	parts := make([]string, 0, len(blocks))
	images := make([]kiroImage, 0)
	results := make([]kiroToolResult, 0)
	for _, block := range blocks {
		switch strings.ToLower(block.Type) {
		case "", "text":
			if block.Text != "" {
				parts = append(parts, block.Text)
			}
		case "image":
			if image, ok := buildKiroImage(block); ok {
				images = append(images, image)
			}
		case "tool_result":
			if strings.TrimSpace(block.ToolUseID) == "" {
				continue
			}
			result := kiroToolResult{
				Content:   []map[string]string{{"text": extractAnthropicToolResultContent(block.Content)}},
				Status:    "success",
				ToolUseID: block.ToolUseID,
			}
			if block.IsError {
				result.Status = "error"
				result.IsError = true
			}
			results = append(results, result)
		case "thinking":
			continue
		default:
			return parsedAnthropicUserMessage{}, statusErr{code: http.StatusBadRequest, msg: fmt.Sprintf("kiro does not support content block type %s yet", block.Type)}
		}
	}
	return parsedAnthropicUserMessage{
		Text:        strings.Join(parts, "\n"),
		Images:      images,
		ToolResults: results,
	}, nil
}

func parseAnthropicAssistantMessage(raw json.RawMessage) (parsedAnthropicAssistantMessage, error) {
	blocks, err := decodeAnthropicContentBlocks(raw)
	if err != nil {
		return parsedAnthropicAssistantMessage{}, statusErr{code: http.StatusBadRequest, msg: "unsupported kiro message content"}
	}
	parts := make([]string, 0, len(blocks))
	toolUses := make([]kiroToolUseEntry, 0)
	for _, block := range blocks {
		switch strings.ToLower(block.Type) {
		case "", "text":
			if block.Text != "" {
				parts = append(parts, block.Text)
			}
		case "thinking":
			continue
		case "tool_use":
			if strings.TrimSpace(block.ID) == "" || strings.TrimSpace(block.Name) == "" {
				continue
			}
			toolUses = append(toolUses, kiroToolUseEntry{
				Input:     parseRawJSONValue(block.Input, map[string]any{}),
				Name:      block.Name,
				ToolUseID: block.ID,
			})
		default:
			return parsedAnthropicAssistantMessage{}, statusErr{code: http.StatusBadRequest, msg: fmt.Sprintf("kiro does not support content block type %s yet", block.Type)}
		}
	}
	return parsedAnthropicAssistantMessage{
		Text:     strings.Join(parts, "\n"),
		ToolUses: toolUses,
	}, nil
}

func decodeAnthropicContentBlocks(raw json.RawMessage) ([]anthropicContentBlock, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return []anthropicContentBlock{{Type: "text", Text: text}}, nil
	}
	var blocks []anthropicContentBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil, err
	}
	return blocks, nil
}

func extractAnthropicToolResultContent(raw json.RawMessage) string {
	if len(bytes.TrimSpace(raw)) == 0 {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text
	}
	var blocks []map[string]any
	if err := json.Unmarshal(raw, &blocks); err == nil {
		parts := make([]string, 0, len(blocks))
		for _, block := range blocks {
			textValue, ok := block["text"].(string)
			if ok && textValue != "" {
				parts = append(parts, textValue)
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, "\n")
		}
	}
	return string(bytes.TrimSpace(raw))
}

func parseRawJSONValue(raw json.RawMessage, fallback any) any {
	if len(bytes.TrimSpace(raw)) == 0 {
		return fallback
	}
	var parsed any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return fallback
	}
	return parsed
}

func normalizeKiroSourcePayload(from sdktranslator.Format, raw []byte) []byte {
	if from != sdktranslator.FromString("openai-response") {
		return raw
	}
	return normalizeKiroOpenAIResponsesInput(raw)
}

func normalizeKiroOpenAIResponsesInput(raw []byte) []byte {
	if len(bytes.TrimSpace(raw)) == 0 {
		return raw
	}
	input := gjson.GetBytes(raw, "input")
	if !input.Exists() || input.Type != gjson.String {
		return raw
	}
	message := []byte(`[{"type":"message","role":"user","content":[{"type":"input_text","text":""}]}]`)
	message, err := sjson.SetBytes(message, "0.content.0.text", input.String())
	if err != nil {
		return raw
	}
	normalized, err := sjson.SetRawBytes(raw, "input", message)
	if err != nil {
		return raw
	}
	return normalized
}

func buildKiroImage(block anthropicContentBlock) (kiroImage, bool) {
	if block.Source == nil || !strings.EqualFold(block.Source.Type, "base64") {
		return kiroImage{}, false
	}
	format, ok := normalizeKiroImageFormat(block.Source.MediaType)
	if !ok {
		return kiroImage{}, false
	}
	data := strings.TrimSpace(block.Source.Data)
	if data == "" {
		return kiroImage{}, false
	}
	return kiroImage{
		Format: format,
		Source: kiroImageSource{
			Bytes: data,
		},
	}, true
}

func normalizeKiroImageFormat(mediaType string) (string, bool) {
	mediaType = strings.TrimSpace(mediaType)
	if mediaType == "" {
		return "", false
	}
	if parsedMediaType, _, err := mime.ParseMediaType(mediaType); err == nil {
		mediaType = parsedMediaType
	}
	switch strings.ToLower(mediaType) {
	case "image/jpeg":
		return "jpeg", true
	case "image/png":
		return "png", true
	case "image/gif":
		return "gif", true
	case "image/webp":
		return "webp", true
	default:
		return "", false
	}
}

func convertAnthropicTools(tools []anthropicTool) []kiroTool {
	out := make([]kiroTool, 0, len(tools))
	seen := make(map[string]struct{}, len(tools))
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Name)
		if name == "" {
			continue
		}
		lower := strings.ToLower(name)
		if _, ok := seen[lower]; ok {
			continue
		}
		seen[lower] = struct{}{}
		schema := parseRawJSONValue(tool.InputSchema, map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		})
		out = append(out, kiroTool{
			ToolSpecification: kiroToolSpecification{
				Description: strings.TrimSpace(tool.Description),
				InputSchema: kiroInputSchema{JSON: schema},
				Name:        name,
			},
		})
	}
	return out
}

func appendHistoryPlaceholderTools(tools []kiroTool, history []kiroHistoryMessage) []kiroTool {
	seen := make(map[string]struct{}, len(tools))
	for _, tool := range tools {
		seen[strings.ToLower(tool.ToolSpecification.Name)] = struct{}{}
	}
	for _, message := range history {
		if message.AssistantResponseMessage == nil {
			continue
		}
		for _, toolUse := range message.AssistantResponseMessage.ToolUses {
			lower := strings.ToLower(toolUse.Name)
			if _, ok := seen[lower]; ok {
				continue
			}
			seen[lower] = struct{}{}
			tools = append(tools, placeholderKiroTool(toolUse.Name))
		}
	}
	return tools
}

func placeholderKiroTool(name string) kiroTool {
	return kiroTool{
		ToolSpecification: kiroToolSpecification{
			Description: "Placeholder tool definition for history replay.",
			InputSchema: kiroInputSchema{
				JSON: map[string]any{
					"type":       "object",
					"properties": map[string]any{},
				},
			},
			Name: name,
		},
	}
}

func buildKiroUserInputMessage(content, modelID string, images []kiroImage) kiroUserMessage {
	return kiroUserMessage{
		Content: content,
		ModelID: modelID,
		Origin:  "AI_EDITOR",
		UserInputMessageContext: kiroUserMessageContext{
			Tools:       nil,
			ToolResults: nil,
		},
		Images: append([]kiroImage(nil), images...),
	}
}

func buildKiroHistoryUserMessage(content, modelID string) kiroHistoryMessage {
	msg := buildKiroUserInputMessage(content, modelID, nil)
	return kiroHistoryMessage{UserInputMessage: &msg}
}

func buildKiroHistoryUserMessageWithResults(content, modelID string, results []kiroToolResult, images []kiroImage) kiroHistoryMessage {
	content = strings.TrimSpace(content)
	if content == "" {
		content = "."
	}
	msg := buildKiroUserInputMessage(content, modelID, images)
	msg.UserInputMessageContext.ToolResults = results
	return kiroHistoryMessage{UserInputMessage: &msg}
}

func buildKiroHistoryAssistantMessage(content string, toolUses []kiroToolUseEntry) kiroHistoryMessage {
	msg := kiroAssistantMessage{
		Content:  content,
		ToolUses: toolUses,
	}
	return kiroHistoryMessage{AssistantResponseMessage: &msg}
}

func validateToolPairing(history []kiroHistoryMessage, currentToolResults []kiroToolResult) ([]kiroToolResult, map[string]struct{}) {
	allToolUseIDs := make(map[string]struct{})
	historyToolResultIDs := make(map[string]struct{})
	for _, message := range history {
		if message.AssistantResponseMessage != nil {
			for _, toolUse := range message.AssistantResponseMessage.ToolUses {
				allToolUseIDs[toolUse.ToolUseID] = struct{}{}
			}
		}
		if message.UserInputMessage != nil {
			for _, result := range message.UserInputMessage.UserInputMessageContext.ToolResults {
				historyToolResultIDs[result.ToolUseID] = struct{}{}
			}
		}
	}

	unpairedToolUses := make(map[string]struct{})
	for toolUseID := range allToolUseIDs {
		if _, alreadyPaired := historyToolResultIDs[toolUseID]; alreadyPaired {
			continue
		}
		unpairedToolUses[toolUseID] = struct{}{}
	}

	filtered := make([]kiroToolResult, 0, len(currentToolResults))
	for _, result := range currentToolResults {
		if _, ok := unpairedToolUses[result.ToolUseID]; ok {
			filtered = append(filtered, result)
			delete(unpairedToolUses, result.ToolUseID)
			continue
		}
		if _, ok := allToolUseIDs[result.ToolUseID]; ok {
			continue
		}
	}
	return filtered, unpairedToolUses
}

func removeOrphanedToolUses(history []kiroHistoryMessage, orphaned map[string]struct{}) {
	if len(orphaned) == 0 {
		return
	}
	for i := range history {
		assistant := history[i].AssistantResponseMessage
		if assistant == nil || len(assistant.ToolUses) == 0 {
			continue
		}
		filtered := assistant.ToolUses[:0]
		for _, toolUse := range assistant.ToolUses {
			if _, ok := orphaned[toolUse.ToolUseID]; ok {
				continue
			}
			filtered = append(filtered, toolUse)
		}
		assistant.ToolUses = filtered
	}
}

type kiroClaudeBlock struct {
	Type      string
	Text      string
	ToolUseID string
	Name      string
	Input     any
	RawInput  string
}

type kiroClaudeResponse struct {
	Blocks     []kiroClaudeBlock
	StopReason string
}

type kiroPendingToolUse struct {
	BlockIndex int
	Name       string
	Builder    strings.Builder
}

type claudeStreamEvent struct {
	Name string
	Data []byte
}

func kiroStopReasonPriority(reason string) int {
	switch strings.TrimSpace(reason) {
	case "model_context_window_exceeded":
		return 0
	case "max_tokens":
		return 1
	case "tool_use":
		return 2
	case "end_turn", "":
		return 3
	default:
		return 4
	}
}

func setKiroStopReason(current *string, candidate string) {
	if current == nil {
		return
	}
	if kiroStopReasonPriority(candidate) < kiroStopReasonPriority(*current) {
		*current = candidate
	}
}

func parseKiroToolInput(raw string) (any, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]any{}, "{}"
	}
	parsed := parseRawJSONValue(json.RawMessage(raw), map[string]any{})
	normalized := normalizeJSON(raw)
	if normalized == "" {
		return map[string]any{}, "{}"
	}
	return parsed, normalized
}

func collectKiroClaudeResponse(events []helps.KiroEvent) kiroClaudeResponse {
	response := kiroClaudeResponse{
		Blocks:     make([]kiroClaudeBlock, 0, len(events)),
		StopReason: "end_turn",
	}
	pendingTools := make(map[string]*kiroPendingToolUse)

	appendText := func(text string) {
		if text == "" {
			return
		}
		if len(response.Blocks) > 0 && response.Blocks[len(response.Blocks)-1].Type == "text" {
			response.Blocks[len(response.Blocks)-1].Text += text
			return
		}
		response.Blocks = append(response.Blocks, kiroClaudeBlock{
			Type: "text",
			Text: text,
		})
	}

	for _, event := range events {
		switch event.Type {
		case helps.KiroEventAssistantResponse:
			if event.AssistantResponse != nil {
				appendText(event.AssistantResponse.Content)
			}
		case helps.KiroEventToolUse:
			if event.ToolUse == nil {
				continue
			}
			setKiroStopReason(&response.StopReason, "tool_use")
			state, ok := pendingTools[event.ToolUse.ToolUseID]
			if !ok {
				response.Blocks = append(response.Blocks, kiroClaudeBlock{
					Type:      "tool_use",
					ToolUseID: event.ToolUse.ToolUseID,
					Name:      event.ToolUse.Name,
					Input:     map[string]any{},
					RawInput:  "{}",
				})
				state = &kiroPendingToolUse{
					BlockIndex: len(response.Blocks) - 1,
					Name:       event.ToolUse.Name,
				}
				pendingTools[event.ToolUse.ToolUseID] = state
			}
			state.Name = event.ToolUse.Name
			state.Builder.WriteString(event.ToolUse.Input)
			if !event.ToolUse.Stop {
				continue
			}
			parsedInput, normalizedRaw := parseKiroToolInput(state.Builder.String())
			response.Blocks[state.BlockIndex].Name = event.ToolUse.Name
			response.Blocks[state.BlockIndex].Input = parsedInput
			response.Blocks[state.BlockIndex].RawInput = normalizedRaw
			delete(pendingTools, event.ToolUse.ToolUseID)
		case helps.KiroEventContextUsage:
			if event.ContextUsage != nil && event.ContextUsage.ContextUsagePercentage >= 100 {
				setKiroStopReason(&response.StopReason, "model_context_window_exceeded")
			}
		}
	}

	for _, state := range pendingTools {
		parsedInput, normalizedRaw := parseKiroToolInput(state.Builder.String())
		response.Blocks[state.BlockIndex].Name = state.Name
		response.Blocks[state.BlockIndex].Input = parsedInput
		response.Blocks[state.BlockIndex].RawInput = normalizedRaw
	}

	return response
}

func buildClaudeResponsePayload(model string, events []helps.KiroEvent) []byte {
	response := collectKiroClaudeResponse(events)
	content := make([]map[string]any, 0, len(response.Blocks))
	for _, block := range response.Blocks {
		switch block.Type {
		case "text":
			content = append(content, map[string]any{
				"type": "text",
				"text": block.Text,
			})
		case "tool_use":
			content = append(content, map[string]any{
				"type":  "tool_use",
				"id":    block.ToolUseID,
				"name":  block.Name,
				"input": block.Input,
			})
		}
	}
	if len(content) == 0 {
		content = append(content, map[string]any{
			"type": "text",
			"text": "",
		})
	}
	payload, _ := json.Marshal(map[string]any{
		"id":            "msg_" + strings.ReplaceAll(uuid.NewString(), "-", ""),
		"type":          "message",
		"role":          "assistant",
		"model":         model,
		"stop_reason":   response.StopReason,
		"stop_sequence": nil,
		"content":       content,
		"usage": map[string]any{
			"input_tokens":  0,
			"output_tokens": 0,
		},
	})
	return payload
}

func buildClaudeDataLines(model string, events []helps.KiroEvent) ([][]byte, error) {
	streamEvents, err := buildClaudeStreamEvents(model, events)
	if err != nil {
		return nil, err
	}
	lines := make([][]byte, 0, len(streamEvents))
	for _, event := range streamEvents {
		lines = append(lines, append([]byte("data: "), event.Data...))
	}
	return lines, nil
}

func joinClaudeDataLines(lines [][]byte) []byte {
	return bytes.Join(lines, []byte("\n"))
}

func rewriteOpenAIResponsesCompactPayload(raw []byte) []byte {
	if len(raw) == 0 {
		return raw
	}
	updated, err := sjson.SetBytes(raw, "object", "response.compaction")
	if err != nil {
		return raw
	}
	return updated
}

func buildClaudeSSEChunks(model string, events []helps.KiroEvent) ([][]byte, error) {
	streamEvents, err := buildClaudeStreamEvents(model, events)
	if err != nil {
		return nil, err
	}
	chunks := make([][]byte, 0, len(streamEvents))
	for _, event := range streamEvents {
		chunk := fmt.Sprintf("event: %s\ndata: %s\n\n", event.Name, event.Data)
		chunks = append(chunks, []byte(chunk))
	}
	return chunks, nil
}

func buildClaudeStreamEvents(model string, events []helps.KiroEvent) ([]claudeStreamEvent, error) {
	messageID := "msg_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	streamEvents := make([]claudeStreamEvent, 0, len(events)*3+3)

	messageStart, err := json.Marshal(map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id":            messageID,
			"type":          "message",
			"role":          "assistant",
			"model":         model,
			"stop_reason":   nil,
			"stop_sequence": nil,
			"content":       []any{},
			"usage": map[string]any{
				"input_tokens":  0,
				"output_tokens": 0,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	streamEvents = append(streamEvents, claudeStreamEvent{Name: "message_start", Data: messageStart})

	openTextIndex := -1
	nextBlockIndex := 0
	stopReason := "end_turn"
	type pendingToolBlock struct {
		Index   int
		Stopped bool
	}
	pendingTools := make(map[string]*pendingToolBlock)

	closeTextBlock := func() error {
		if openTextIndex < 0 {
			return nil
		}
		stopData, errMarshal := json.Marshal(map[string]any{
			"type":  "content_block_stop",
			"index": openTextIndex,
		})
		if errMarshal != nil {
			return errMarshal
		}
		streamEvents = append(streamEvents, claudeStreamEvent{Name: "content_block_stop", Data: stopData})
		openTextIndex = -1
		return nil
	}
	closeToolBlock := func(state *pendingToolBlock) error {
		if state == nil || state.Stopped {
			return nil
		}
		stopData, errMarshal := json.Marshal(map[string]any{
			"type":  "content_block_stop",
			"index": state.Index,
		})
		if errMarshal != nil {
			return errMarshal
		}
		streamEvents = append(streamEvents, claudeStreamEvent{Name: "content_block_stop", Data: stopData})
		state.Stopped = true
		return nil
	}

	for _, event := range events {
		switch event.Type {
		case helps.KiroEventAssistantResponse:
			if event.AssistantResponse == nil || event.AssistantResponse.Content == "" {
				continue
			}
			if openTextIndex < 0 {
				openTextIndex = nextBlockIndex
				nextBlockIndex++
				startData, errMarshal := json.Marshal(map[string]any{
					"type":  "content_block_start",
					"index": openTextIndex,
					"content_block": map[string]any{
						"type": "text",
						"text": "",
					},
				})
				if errMarshal != nil {
					return nil, errMarshal
				}
				streamEvents = append(streamEvents, claudeStreamEvent{Name: "content_block_start", Data: startData})
			}
			deltaData, errMarshal := json.Marshal(map[string]any{
				"type":  "content_block_delta",
				"index": openTextIndex,
				"delta": map[string]any{
					"type": "text_delta",
					"text": event.AssistantResponse.Content,
				},
			})
			if errMarshal != nil {
				return nil, errMarshal
			}
			streamEvents = append(streamEvents, claudeStreamEvent{Name: "content_block_delta", Data: deltaData})
		case helps.KiroEventToolUse:
			if event.ToolUse == nil {
				continue
			}
			setKiroStopReason(&stopReason, "tool_use")
			if err = closeTextBlock(); err != nil {
				return nil, err
			}
			state, ok := pendingTools[event.ToolUse.ToolUseID]
			if !ok {
				state = &pendingToolBlock{Index: nextBlockIndex}
				nextBlockIndex++
				pendingTools[event.ToolUse.ToolUseID] = state
				startData, errMarshal := json.Marshal(map[string]any{
					"type":  "content_block_start",
					"index": state.Index,
					"content_block": map[string]any{
						"type":  "tool_use",
						"id":    event.ToolUse.ToolUseID,
						"name":  event.ToolUse.Name,
						"input": map[string]any{},
					},
				})
				if errMarshal != nil {
					return nil, errMarshal
				}
				streamEvents = append(streamEvents, claudeStreamEvent{Name: "content_block_start", Data: startData})
			}
			if event.ToolUse.Input != "" {
				deltaData, errMarshal := json.Marshal(map[string]any{
					"type":  "content_block_delta",
					"index": state.Index,
					"delta": map[string]any{
						"type":         "input_json_delta",
						"partial_json": event.ToolUse.Input,
					},
				})
				if errMarshal != nil {
					return nil, errMarshal
				}
				streamEvents = append(streamEvents, claudeStreamEvent{Name: "content_block_delta", Data: deltaData})
			}
			if event.ToolUse.Stop {
				if err = closeToolBlock(state); err != nil {
					return nil, err
				}
			}
		case helps.KiroEventContextUsage:
			if event.ContextUsage != nil && event.ContextUsage.ContextUsagePercentage >= 100 {
				setKiroStopReason(&stopReason, "model_context_window_exceeded")
			}
		}
	}

	if err = closeTextBlock(); err != nil {
		return nil, err
	}
	for _, state := range pendingTools {
		if err = closeToolBlock(state); err != nil {
			return nil, err
		}
	}

	messageDelta, err := json.Marshal(map[string]any{
		"type": "message_delta",
		"delta": map[string]any{
			"stop_reason":   stopReason,
			"stop_sequence": nil,
		},
		"usage": map[string]any{
			"input_tokens":  0,
			"output_tokens": 0,
		},
	})
	if err != nil {
		return nil, err
	}
	streamEvents = append(streamEvents, claudeStreamEvent{Name: "message_delta", Data: messageDelta})

	messageStop, err := json.Marshal(map[string]any{
		"type": "message_stop",
	})
	if err != nil {
		return nil, err
	}
	streamEvents = append(streamEvents, claudeStreamEvent{Name: "message_stop", Data: messageStop})
	return streamEvents, nil
}

func normalizeJSON(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	var parsed any
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return ""
	}
	normalized, err := json.Marshal(parsed)
	if err != nil {
		return ""
	}
	return string(normalized)
}

func authAttributes(auth *cliproxyauth.Auth) map[string]string {
	if auth == nil {
		return nil
	}
	return auth.Attributes
}

func credsEffectiveAPIRegion(creds helps.KiroCredentials) string {
	if strings.TrimSpace(creds.APIRegion) != "" {
		return strings.TrimSpace(creds.APIRegion)
	}
	if strings.TrimSpace(creds.Region) != "" {
		return strings.TrimSpace(creds.Region)
	}
	return "us-east-1"
}
