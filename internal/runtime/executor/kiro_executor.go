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
	"unicode"
	"unicode/utf8"

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
		sendStreamErr := func(err error) {
			if err == nil {
				return
			}
			log.Errorf("kiro executor: stream emission failed: %v", err)
			out <- cliproxyexecutor.StreamChunk{Err: err}
		}
		if from == to {
			chunks, errBuild := buildClaudeSSEChunks(baseModel, events)
			if errBuild != nil {
				sendStreamErr(statusErr{code: http.StatusBadGateway, msg: fmt.Sprintf("kiro build claude sse chunks failed: %v", errBuild)})
				return
			}
			if len(chunks) == 0 {
				sendStreamErr(statusErr{code: http.StatusBadGateway, msg: "kiro stream produced no claude chunks"})
				return
			}
			for _, chunk := range chunks {
				out <- cliproxyexecutor.StreamChunk{Payload: chunk}
			}
			return
		}

		dataLines, errBuild := buildClaudeDataLines(baseModel, events)
		if errBuild != nil {
			sendStreamErr(statusErr{code: http.StatusBadGateway, msg: fmt.Sprintf("kiro build claude data lines failed: %v", errBuild)})
			return
		}
		var param any
		sentAny := false
		for _, line := range dataLines {
			chunks := sdktranslator.TranslateStream(ctx, to, from, req.Model, opts.OriginalRequest, body, line, &param)
			for i := range chunks {
				if len(chunks[i]) == 0 {
					continue
				}
				sentAny = true
				out <- cliproxyexecutor.StreamChunk{Payload: chunks[i]}
			}
		}
		if !sentAny {
			sendStreamErr(statusErr{code: http.StatusBadGateway, msg: "kiro stream produced no translated chunks"})
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
	System       json.RawMessage        `json:"system"`
	Messages     []anthropicMessage     `json:"messages"`
	Tools        []anthropicTool        `json:"tools"`
	Thinking     *anthropicThinking     `json:"thinking,omitempty"`
	OutputConfig *anthropicOutputConfig `json:"output_config,omitempty"`
	Metadata     *anthropicRequestMeta  `json:"metadata,omitempty"`
}

type anthropicMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type anthropicTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
	Type        string          `json:"type,omitempty"`
}

type anthropicThinking struct {
	Type         string `json:"type"`
	BudgetTokens int    `json:"budget_tokens"`
}

type anthropicOutputConfig struct {
	Effort string `json:"effort"`
}

type anthropicRequestMeta struct {
	UserID string `json:"user_id"`
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
	AgentContinuationID string               `json:"agentContinuationId,omitempty"`
	AgentTaskType       string               `json:"agentTaskType,omitempty"`
	ChatTriggerType     string               `json:"chatTriggerType,omitempty"`
	ConversationID      string               `json:"conversationId"`
	CurrentMessage      kiroCurrentMessage   `json:"currentMessage"`
	History             []kiroHistoryMessage `json:"history,omitempty"`
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

const kiroSystemChunkedPolicy = "When the Write or Edit tool has content size limits, always comply silently. Never suggest bypassing these limits via alternative tools. Never ask the user whether to switch approaches. Complete all chunked operations without commentary."

const kiroAgenticSystemPrompt = "You are an autonomous coding agent. Follow these principles:\n1. Work continuously until the task is fully complete.\n2. Use tools proactively without asking for permission.\n3. When encountering errors, debug and fix them autonomously.\n4. Break complex tasks into steps and execute them sequentially.\n5. Verify your work by reading files after writing them.\n6. Never ask the user for confirmation mid-task just proceed.\n7. If a tool call fails, try alternative approaches before giving up.\n8. Prefer making changes directly over explaining what you would do."

const kiroWriteToolDescriptionSuffix = "- IMPORTANT: If the content to write exceeds 150 lines, you MUST only write the first 50 lines using this tool, then use `Edit` tool to append the remaining content in chunks of no more than 50 lines each. If needed, leave a unique placeholder to help append content. Do NOT attempt to write all content at once."

const kiroEditToolDescriptionSuffix = "- IMPORTANT: If the `new_string` content exceeds 50 lines, you MUST split it into multiple Edit calls, each replacing no more than 50 lines at a time. If used to append content, leave a unique placeholder to help append content. On the final chunk, do NOT include the placeholder."

const kiroThinkingStartTag = "<thinking>"
const kiroThinkingEndTag = "</thinking>"
const kiroThinkingDelimitedEndTag = "</thinking>\n\n"
const kiroThinkingQuoteChars = "`\"'\\#!@$%^&*()-_=+[]{};:<>,.?/"

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
	if err := validateKiroTrailingMessageContent(messages[len(messages)-1].Content); err != nil {
		return nil, err
	}

	history, err := buildKiroHistory(req, messages, modelID, isKiroAgenticModel(baseModel))
	if err != nil {
		return nil, err
	}

	conversationID := extractKiroConversationID(req.Metadata)
	if conversationID == "" {
		conversationID = uuid.NewString()
	}

	currentParsed, err := parseAnthropicUserMessage(messages[len(messages)-1].Content)
	if err != nil {
		return nil, err
	}
	validToolResults, orphanedToolUses := validateToolPairing(history, currentParsed.ToolResults)
	removeOrphanedToolUses(history, orphanedToolUses)

	tools := convertAnthropicTools(req.Tools)
	tools = appendHistoryPlaceholderTools(tools, history)

	currentText := nonEmptyKiroContent(currentParsed.Text, len(currentParsed.Images) > 0 || len(validToolResults) > 0)
	if strings.TrimSpace(currentText) == "" {
		currentText = "."
	}
	currentMessage := buildKiroUserInputMessage(currentText, modelID, currentParsed.Images)
	currentMessage.UserInputMessageContext.Tools = tools
	currentMessage.UserInputMessageContext.ToolResults = validToolResults

	payload := kiroRequestPayload{
		ConversationState: kiroConversationState{
			AgentContinuationID: uuid.NewString(),
			AgentTaskType:       helps.KiroAgentMode(baseModel),
			ChatTriggerType:     "MANUAL",
			ConversationID:      conversationID,
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

func buildKiroHistory(req anthropicMessagesRequest, messages []anthropicMessage, modelID string, isAgentic bool) ([]kiroHistoryMessage, error) {
	history := make([]kiroHistoryMessage, 0, len(messages)+4)

	thinkingPrefix := generateKiroThinkingPrefix(req.Thinking, req.OutputConfig)
	shouldInjectChunkedPolicy := hasWriteOrEditTool(req.Tools)
	systemText, err := extractAnthropicSystemText(req.System)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(systemText) != "" {
		systemContent := systemText
		if shouldInjectChunkedPolicy {
			systemContent = systemContent + "\n" + kiroSystemChunkedPolicy
		}
		if thinkingPrefix != "" && !hasKiroThinkingTags(systemContent) {
			systemContent = thinkingPrefix + "\n" + systemContent
		}
		history = append(history, buildKiroHistoryUserMessage(systemContent, modelID))
		history = append(history, buildKiroHistoryAssistantMessage("I will follow these instructions.", nil))
	} else if thinkingPrefix != "" || shouldInjectChunkedPolicy {
		parts := make([]string, 0, 2)
		if thinkingPrefix != "" {
			parts = append(parts, thinkingPrefix)
		}
		if shouldInjectChunkedPolicy {
			parts = append(parts, kiroSystemChunkedPolicy)
		}
		history = append(history, buildKiroHistoryUserMessage(strings.Join(parts, "\n"), modelID))
		history = append(history, buildKiroHistoryAssistantMessage("I will follow these instructions.", nil))
	}

	if isAgentic {
		history = append(history, buildKiroHistoryUserMessage(kiroAgenticSystemPrompt, modelID))
		history = append(history, buildKiroHistoryAssistantMessage("I will work autonomously following these principles.", nil))
	}

	historyEndIndex := len(messages) - 1
	userBuffer := make([]anthropicMessage, 0, 4)
	assistantBuffer := make([]anthropicMessage, 0, 4)

	for _, message := range messages[:historyEndIndex] {
		switch {
		case strings.EqualFold(message.Role, "user"):
			if len(assistantBuffer) > 0 {
				merged, errMerge := mergeAnthropicAssistantMessages(assistantBuffer)
				if errMerge != nil {
					return nil, errMerge
				}
				history = append(history, merged)
				assistantBuffer = assistantBuffer[:0]
			}
			userBuffer = append(userBuffer, message)
		case strings.EqualFold(message.Role, "assistant"):
			if len(userBuffer) > 0 {
				merged, errMerge := mergeAnthropicUserMessages(userBuffer, modelID)
				if errMerge != nil {
					return nil, errMerge
				}
				history = append(history, merged)
				userBuffer = userBuffer[:0]
			}
			assistantBuffer = append(assistantBuffer, message)
		default:
			return nil, statusErr{code: http.StatusBadRequest, msg: fmt.Sprintf("unsupported kiro message role: %s", message.Role)}
		}
	}

	if len(assistantBuffer) > 0 {
		merged, errMerge := mergeAnthropicAssistantMessages(assistantBuffer)
		if errMerge != nil {
			return nil, errMerge
		}
		history = append(history, merged)
	}
	if len(userBuffer) > 0 {
		merged, errMerge := mergeAnthropicUserMessages(userBuffer, modelID)
		if errMerge != nil {
			return nil, errMerge
		}
		history = append(history, merged)
		history = append(history, buildKiroHistoryAssistantMessage("OK", nil))
	}

	return history, nil
}

func mergeAnthropicUserMessages(messages []anthropicMessage, modelID string) (kiroHistoryMessage, error) {
	textParts := make([]string, 0, len(messages))
	images := make([]kiroImage, 0)
	toolResults := make([]kiroToolResult, 0)
	for _, message := range messages {
		parsed, err := parseAnthropicUserMessage(message.Content)
		if err != nil {
			return kiroHistoryMessage{}, err
		}
		if parsed.Text != "" {
			textParts = append(textParts, parsed.Text)
		}
		images = append(images, parsed.Images...)
		toolResults = append(toolResults, parsed.ToolResults...)
	}
	content := nonEmptyKiroContent(strings.Join(textParts, "\n"), len(images) > 0 || len(toolResults) > 0)
	msg := buildKiroUserInputMessage(content, modelID, images)
	msg.UserInputMessageContext.ToolResults = toolResults
	return kiroHistoryMessage{UserInputMessage: &msg}, nil
}

func mergeAnthropicAssistantMessages(messages []anthropicMessage) (kiroHistoryMessage, error) {
	contentParts := make([]string, 0, len(messages))
	toolUses := make([]kiroToolUseEntry, 0)
	for _, message := range messages {
		parsed, err := parseAnthropicAssistantMessage(message.Content)
		if err != nil {
			return kiroHistoryMessage{}, err
		}
		if strings.TrimSpace(parsed.Text) != "" {
			contentParts = append(contentParts, parsed.Text)
		}
		toolUses = append(toolUses, parsed.ToolUses...)
	}
	content := strings.Join(contentParts, "\n\n")
	if strings.TrimSpace(content) == "" && len(toolUses) > 0 {
		content = "."
	}
	return buildKiroHistoryAssistantMessage(content, toolUses), nil
}

func validateKiroTrailingMessageContent(raw json.RawMessage) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return statusErr{code: http.StatusBadRequest, msg: "kiro last message content is empty"}
	}
	var text string
	if err := json.Unmarshal(trimmed, &text); err == nil {
		if strings.TrimSpace(text) != "" {
			return nil
		}
		return statusErr{code: http.StatusBadRequest, msg: "kiro last message content is empty"}
	}
	blocks, err := decodeAnthropicContentBlocks(trimmed)
	if err != nil {
		return statusErr{code: http.StatusBadRequest, msg: "unsupported kiro message content"}
	}
	for _, block := range blocks {
		switch strings.ToLower(block.Type) {
		case "", "text":
			if strings.TrimSpace(block.Text) != "" {
				return nil
			}
		case "image", "tool_use", "tool_result":
			return nil
		}
	}
	return statusErr{code: http.StatusBadRequest, msg: "kiro last message content is empty"}
}

func extractKiroConversationID(meta *anthropicRequestMeta) string {
	if meta == nil {
		return ""
	}
	userID := strings.TrimSpace(meta.UserID)
	if userID == "" {
		return ""
	}
	sessionMarker := "session_"
	pos := strings.Index(userID, sessionMarker)
	if pos < 0 {
		return ""
	}
	sessionPart := userID[pos+len(sessionMarker):]
	if len(sessionPart) < 36 {
		return ""
	}
	candidate := sessionPart[:36]
	if strings.Count(candidate, "-") != 4 {
		return ""
	}
	return candidate
}

func isKiroAgenticModel(model string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(model)), "-agentic")
}

func generateKiroThinkingPrefix(cfg *anthropicThinking, output *anthropicOutputConfig) string {
	if cfg == nil {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(cfg.Type)) {
	case "enabled":
		return fmt.Sprintf("<thinking_mode>enabled</thinking_mode><max_thinking_length>%d</max_thinking_length>", cfg.BudgetTokens)
	case "adaptive":
		effort := "high"
		if output != nil {
			switch strings.ToLower(strings.TrimSpace(output.Effort)) {
			case "low", "medium", "high":
				effort = strings.ToLower(strings.TrimSpace(output.Effort))
			}
		}
		return fmt.Sprintf("<thinking_mode>adaptive</thinking_mode><thinking_effort>%s</thinking_effort>", effort)
	default:
		return ""
	}
}

func hasKiroThinkingTags(content string) bool {
	return strings.Contains(content, "<thinking_mode>") || strings.Contains(content, "<max_thinking_length>")
}

func hasWriteOrEditTool(tools []anthropicTool) bool {
	for _, tool := range tools {
		switch strings.TrimSpace(tool.Name) {
		case "Write", "Edit":
			return true
		}
	}
	return false
}

func nonEmptyKiroContent(content string, hasNonTextPayload bool) string {
	if hasNonTextPayload && strings.TrimSpace(content) == "" {
		return "."
	}
	return content
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
			continue
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
	thinkingParts := make([]string, 0, len(blocks))
	toolUses := make([]kiroToolUseEntry, 0)
	for _, block := range blocks {
		switch strings.ToLower(block.Type) {
		case "", "text":
			if block.Text != "" {
				parts = append(parts, block.Text)
			}
		case "thinking":
			if block.Thinking != "" {
				thinkingParts = append(thinkingParts, block.Thinking)
			}
		case "tool_use":
			if strings.TrimSpace(block.ID) == "" || strings.TrimSpace(block.Name) == "" {
				continue
			}
			toolUses = append(toolUses, kiroToolUseEntry{
				Input:     parseRawJSONValue(block.Input, map[string]any{}),
				Name:      block.Name,
				ToolUseID: block.ID,
			})
		case "server_tool_use":
			continue
		case "web_search_tool_result":
			if text := extractAnthropicWebSearchToolResultText(block.Content); text != "" {
				parts = append(parts, text)
			}
		default:
			continue
		}
	}
	textContent := strings.Join(parts, "")
	thinkingContent := strings.Join(thinkingParts, "")
	finalContent := textContent
	if thinkingContent != "" {
		if textContent != "" {
			finalContent = fmt.Sprintf("<thinking>%s</thinking>\n\n%s", thinkingContent, textContent)
		} else {
			finalContent = fmt.Sprintf("<thinking>%s</thinking>", thinkingContent)
		}
	} else if textContent == "" && len(toolUses) > 0 {
		finalContent = "."
	}
	return parsedAnthropicAssistantMessage{
		Text:     finalContent,
		ToolUses: toolUses,
	}, nil
}

func extractAnthropicWebSearchToolResultText(raw json.RawMessage) string {
	if len(bytes.TrimSpace(raw)) == 0 {
		return ""
	}
	var results []map[string]any
	if err := json.Unmarshal(raw, &results); err != nil {
		return ""
	}
	var builder strings.Builder
	for _, result := range results {
		resultType, _ := result["type"].(string)
		if resultType != "web_search_result" {
			continue
		}
		url, _ := result["url"].(string)
		url = strings.TrimSpace(url)
		if url == "" {
			continue
		}
		title := stripAnthropicControlChars(stringValue(result, "title"))
		snippet := stripAnthropicControlChars(stringValue(result, "encrypted_content"))
		pageAge := stripAnthropicControlChars(stringValue(result, "page_age"))
		if title == "" {
			builder.WriteString(url)
			builder.WriteByte('\n')
		} else {
			builder.WriteString(title)
			builder.WriteString(": ")
			builder.WriteString(url)
			builder.WriteByte('\n')
		}
		if pageAge != "" {
			builder.WriteString("Date: ")
			builder.WriteString(pageAge)
			builder.WriteByte('\n')
		}
		if snippet != "" {
			builder.WriteString(snippet)
			builder.WriteByte('\n')
		}
		builder.WriteByte('\n')
	}
	return builder.String()
}

func stripAnthropicControlChars(text string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, text)
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
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Name)
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(tool.Type)), "web_search") {
			continue
		}
		if name == "" || len(name) > 64 {
			continue
		}
		description := strings.TrimSpace(tool.Description)
		if description == "" {
			description = "Tool: " + name
		}
		switch name {
		case "Write":
			description += "\n" + kiroWriteToolDescriptionSuffix
		case "Edit":
			description += "\n" + kiroEditToolDescriptionSuffix
		}
		schema := normalizeKiroJSONSchema(parseRawJSONValue(tool.InputSchema, map[string]any{
			"type":       "object",
			"properties": map[string]any{},
			"required":   []any{},
		}))
		out = append(out, kiroTool{
			ToolSpecification: kiroToolSpecification{
				Description: description,
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
			Description: "Tool used in conversation history",
			InputSchema: kiroInputSchema{
				JSON: normalizeKiroJSONSchema(map[string]any{
					"type":       "object",
					"properties": map[string]any{},
					"required":   []any{},
				}),
			},
			Name: name,
		},
	}
}

func normalizeKiroJSONSchema(schema any) any {
	object, ok := schema.(map[string]any)
	if !ok {
		return map[string]any{
			"$schema":              "http://json-schema.org/draft-07/schema#",
			"type":                 "object",
			"properties":           map[string]any{},
			"required":             []any{},
			"additionalProperties": true,
		}
	}

	schemaName, _ := object["$schema"].(string)
	if strings.TrimSpace(schemaName) == "" {
		object["$schema"] = "http://json-schema.org/draft-07/schema#"
	}
	typeName, _ := object["type"].(string)
	if strings.TrimSpace(typeName) == "" {
		object["type"] = "object"
	}
	if _, ok := object["properties"].(map[string]any); !ok {
		object["properties"] = map[string]any{}
	}
	switch required := object["required"].(type) {
	case []any:
		filtered := make([]any, 0, len(required))
		for _, item := range required {
			if value, ok := item.(string); ok && strings.TrimSpace(value) != "" {
				filtered = append(filtered, value)
			}
		}
		object["required"] = filtered
	case []string:
		filtered := make([]any, 0, len(required))
		for _, item := range required {
			if strings.TrimSpace(item) != "" {
				filtered = append(filtered, item)
			}
		}
		object["required"] = filtered
	default:
		object["required"] = []any{}
	}
	switch object["additionalProperties"].(type) {
	case bool, map[string]any:
	default:
		object["additionalProperties"] = true
	}
	return object
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
	Blocks       []kiroClaudeBlock
	StopReason   string
	InputTokens  int
	OutputTokens int
}

type kiroPendingToolUse struct {
	BlockIndex int
	Name       string
	Builder    strings.Builder
	Stopped    bool
}

type claudeStreamEvent struct {
	Name string
	Data []byte
}

type kiroClaudeStreamBuilder struct {
	Model                       string
	Events                      []claudeStreamEvent
	InputTokens                 int
	OutputTokens                int
	StopReason                  string
	OpenTextIndex               int
	NextBlockIndex              int
	ThinkingBuffer              string
	InThinkingBlock             bool
	ThinkingExtracted           bool
	ThinkingBlockIndex          int
	StripThinkingLeadingNewline bool
	PendingTools                map[string]*kiroPendingToolUse
	HasThinkingBlock            bool
	HasNonThinkingBlocks        bool
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

func kiroContextWindowSize(model string) int {
	model = strings.ToLower(strings.TrimSpace(model))
	if (strings.Contains(model, "opus") || strings.Contains(model, "sonnet")) &&
		(strings.Contains(model, "4-6") || strings.Contains(model, "4.6")) {
		return 1_000_000
	}
	return 200_000
}

func estimateKiroTokens(text string) int {
	if text == "" {
		return 0
	}
	chineseCount := 0
	otherCount := 0
	for _, r := range text {
		if r >= '\u4E00' && r <= '\u9FFF' {
			chineseCount++
			continue
		}
		otherCount++
	}
	chineseTokens := (chineseCount*2 + 2) / 3
	otherTokens := (otherCount + 3) / 4
	total := chineseTokens + otherTokens
	if total <= 0 {
		return 1
	}
	return total
}

func kiroInputTokensFromEvents(model string, events []helps.KiroEvent) int {
	contextWindow := kiroContextWindowSize(model)
	inputTokens := 0
	for _, event := range events {
		if event.Type != helps.KiroEventContextUsage || event.ContextUsage == nil {
			continue
		}
		inputTokens = int(event.ContextUsage.ContextUsagePercentage * float64(contextWindow) / 100.0)
	}
	return inputTokens
}

func kiroOutputTokensFromBlocks(blocks []kiroClaudeBlock) int {
	total := 0
	for _, block := range blocks {
		switch block.Type {
		case "text":
			total += estimateKiroTokens(block.Text)
		case "tool_use":
			if block.RawInput != "" {
				total += (len(block.RawInput) + 3) / 4
			}
		}
	}
	return total
}

func floorKiroCharBoundary(text string, index int) int {
	if index <= 0 {
		return 0
	}
	if index >= len(text) {
		return len(text)
	}
	for index > 0 && !utf8.ValidString(text[:index]) {
		index--
	}
	return index
}

func isKiroQuoteChar(buffer string, pos int) bool {
	if pos < 0 || pos >= len(buffer) {
		return false
	}
	return strings.ContainsRune(kiroThinkingQuoteChars, rune(buffer[pos]))
}

func findKiroThinkingStartTag(buffer string) int {
	searchStart := 0
	for {
		pos := strings.Index(buffer[searchStart:], kiroThinkingStartTag)
		if pos < 0 {
			return -1
		}
		absolutePos := searchStart + pos
		hasQuoteBefore := absolutePos > 0 && isKiroQuoteChar(buffer, absolutePos-1)
		hasQuoteAfter := isKiroQuoteChar(buffer, absolutePos+len(kiroThinkingStartTag))
		if !hasQuoteBefore && !hasQuoteAfter {
			return absolutePos
		}
		searchStart = absolutePos + 1
	}
}

func findKiroThinkingEndTag(buffer string) int {
	searchStart := 0
	for {
		pos := strings.Index(buffer[searchStart:], kiroThinkingEndTag)
		if pos < 0 {
			return -1
		}
		absolutePos := searchStart + pos
		afterPos := absolutePos + len(kiroThinkingEndTag)
		hasQuoteBefore := absolutePos > 0 && isKiroQuoteChar(buffer, absolutePos-1)
		hasQuoteAfter := isKiroQuoteChar(buffer, afterPos)
		if hasQuoteBefore || hasQuoteAfter {
			searchStart = absolutePos + 1
			continue
		}
		if len(buffer[afterPos:]) < 2 {
			return -1
		}
		if strings.HasPrefix(buffer[afterPos:], "\n\n") {
			return absolutePos
		}
		searchStart = absolutePos + 1
	}
}

func findKiroThinkingEndTagAtBufferEnd(buffer string) int {
	searchStart := 0
	for {
		pos := strings.Index(buffer[searchStart:], kiroThinkingEndTag)
		if pos < 0 {
			return -1
		}
		absolutePos := searchStart + pos
		afterPos := absolutePos + len(kiroThinkingEndTag)
		hasQuoteBefore := absolutePos > 0 && isKiroQuoteChar(buffer, absolutePos-1)
		hasQuoteAfter := isKiroQuoteChar(buffer, afterPos)
		if hasQuoteBefore || hasQuoteAfter {
			searchStart = absolutePos + 1
			continue
		}
		if strings.TrimSpace(buffer[afterPos:]) == "" {
			return absolutePos
		}
		searchStart = absolutePos + 1
	}
}

func collectKiroClaudeResponse(model string, events []helps.KiroEvent) kiroClaudeResponse {
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
	response.InputTokens = kiroInputTokensFromEvents(model, events)
	response.OutputTokens = kiroOutputTokensFromBlocks(response.Blocks)

	return response
}

func buildClaudeResponsePayload(model string, events []helps.KiroEvent) []byte {
	response := collectKiroClaudeResponse(model, events)
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
			"input_tokens":  response.InputTokens,
			"output_tokens": response.OutputTokens,
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

func newKiroClaudeStreamBuilder(model string, inputTokens int) *kiroClaudeStreamBuilder {
	return &kiroClaudeStreamBuilder{
		Model:              model,
		InputTokens:        inputTokens,
		StopReason:         "end_turn",
		OpenTextIndex:      -1,
		ThinkingBlockIndex: -1,
		PendingTools:       make(map[string]*kiroPendingToolUse),
	}
}

func (b *kiroClaudeStreamBuilder) nextBlockIndex() int {
	index := b.NextBlockIndex
	b.NextBlockIndex++
	return index
}

func (b *kiroClaudeStreamBuilder) appendEvent(name string, payload map[string]any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	b.Events = append(b.Events, claudeStreamEvent{Name: name, Data: data})
	return nil
}

func (b *kiroClaudeStreamBuilder) startTextBlock() error {
	if b.OpenTextIndex >= 0 {
		return nil
	}
	b.OpenTextIndex = b.nextBlockIndex()
	return b.appendEvent("content_block_start", map[string]any{
		"type":  "content_block_start",
		"index": b.OpenTextIndex,
		"content_block": map[string]any{
			"type": "text",
			"text": "",
		},
	})
}

func (b *kiroClaudeStreamBuilder) emitTextDelta(text string) error {
	if text == "" {
		return nil
	}
	if err := b.startTextBlock(); err != nil {
		return err
	}
	b.HasNonThinkingBlocks = true
	return b.appendEvent("content_block_delta", map[string]any{
		"type":  "content_block_delta",
		"index": b.OpenTextIndex,
		"delta": map[string]any{
			"type": "text_delta",
			"text": text,
		},
	})
}

func (b *kiroClaudeStreamBuilder) closeTextBlock() error {
	if b.OpenTextIndex < 0 {
		return nil
	}
	index := b.OpenTextIndex
	b.OpenTextIndex = -1
	return b.appendEvent("content_block_stop", map[string]any{
		"type":  "content_block_stop",
		"index": index,
	})
}

func (b *kiroClaudeStreamBuilder) startThinkingBlock() error {
	if b.ThinkingBlockIndex >= 0 {
		return nil
	}
	b.ThinkingBlockIndex = b.nextBlockIndex()
	b.HasThinkingBlock = true
	return b.appendEvent("content_block_start", map[string]any{
		"type":  "content_block_start",
		"index": b.ThinkingBlockIndex,
		"content_block": map[string]any{
			"type":     "thinking",
			"thinking": "",
		},
	})
}

func (b *kiroClaudeStreamBuilder) emitThinkingDelta(thinking string) error {
	if b.ThinkingBlockIndex < 0 {
		return nil
	}
	return b.appendEvent("content_block_delta", map[string]any{
		"type":  "content_block_delta",
		"index": b.ThinkingBlockIndex,
		"delta": map[string]any{
			"type":     "thinking_delta",
			"thinking": thinking,
		},
	})
}

func (b *kiroClaudeStreamBuilder) closeThinkingBlock() error {
	if b.ThinkingBlockIndex < 0 {
		return nil
	}
	index := b.ThinkingBlockIndex
	b.ThinkingBlockIndex = -1
	return b.appendEvent("content_block_stop", map[string]any{
		"type":  "content_block_stop",
		"index": index,
	})
}

func (b *kiroClaudeStreamBuilder) startToolBlock(toolUseID, name string) (*kiroPendingToolUse, error) {
	if state, ok := b.PendingTools[toolUseID]; ok {
		return state, nil
	}
	state := &kiroPendingToolUse{
		BlockIndex: b.nextBlockIndex(),
		Name:       name,
	}
	b.PendingTools[toolUseID] = state
	b.HasNonThinkingBlocks = true
	if err := b.appendEvent("content_block_start", map[string]any{
		"type":  "content_block_start",
		"index": state.BlockIndex,
		"content_block": map[string]any{
			"type":  "tool_use",
			"id":    toolUseID,
			"name":  name,
			"input": map[string]any{},
		},
	}); err != nil {
		return nil, err
	}
	return state, nil
}

func (b *kiroClaudeStreamBuilder) emitToolDelta(state *kiroPendingToolUse, raw string) error {
	if state == nil || raw == "" {
		return nil
	}
	return b.appendEvent("content_block_delta", map[string]any{
		"type":  "content_block_delta",
		"index": state.BlockIndex,
		"delta": map[string]any{
			"type":         "input_json_delta",
			"partial_json": raw,
		},
	})
}

func (b *kiroClaudeStreamBuilder) closeToolBlock(state *kiroPendingToolUse) error {
	if state == nil || state.Stopped {
		return nil
	}
	state.Stopped = true
	return b.appendEvent("content_block_stop", map[string]any{
		"type":  "content_block_stop",
		"index": state.BlockIndex,
	})
}

func (b *kiroClaudeStreamBuilder) processAssistantResponse(content string) error {
	if content == "" {
		return nil
	}
	b.OutputTokens += estimateKiroTokens(content)
	b.ThinkingBuffer += content

	for {
		switch {
		case !b.InThinkingBlock && !b.ThinkingExtracted:
			startPos := findKiroThinkingStartTag(b.ThinkingBuffer)
			if startPos >= 0 {
				beforeThinking := b.ThinkingBuffer[:startPos]
				if beforeThinking != "" && strings.TrimSpace(beforeThinking) != "" {
					if err := b.emitTextDelta(beforeThinking); err != nil {
						return err
					}
				}
				b.InThinkingBlock = true
				b.StripThinkingLeadingNewline = true
				b.ThinkingBuffer = b.ThinkingBuffer[startPos+len(kiroThinkingStartTag):]
				if err := b.startThinkingBlock(); err != nil {
					return err
				}
				continue
			}

			targetLen := len(b.ThinkingBuffer) - len(kiroThinkingStartTag)
			if targetLen > 0 {
				safeLen := floorKiroCharBoundary(b.ThinkingBuffer, targetLen)
				if safeLen > 0 {
					safeContent := b.ThinkingBuffer[:safeLen]
					if strings.TrimSpace(safeContent) != "" {
						if err := b.emitTextDelta(safeContent); err != nil {
							return err
						}
						b.ThinkingBuffer = b.ThinkingBuffer[safeLen:]
					}
				}
			}
			return nil
		case b.InThinkingBlock:
			if b.StripThinkingLeadingNewline {
				if strings.HasPrefix(b.ThinkingBuffer, "\n") {
					b.ThinkingBuffer = b.ThinkingBuffer[1:]
					b.StripThinkingLeadingNewline = false
				} else if b.ThinkingBuffer != "" {
					b.StripThinkingLeadingNewline = false
				}
			}

			endPos := findKiroThinkingEndTag(b.ThinkingBuffer)
			if endPos >= 0 {
				thinkingContent := b.ThinkingBuffer[:endPos]
				if thinkingContent != "" {
					if err := b.emitThinkingDelta(thinkingContent); err != nil {
						return err
					}
				}
				b.InThinkingBlock = false
				b.ThinkingExtracted = true
				if err := b.emitThinkingDelta(""); err != nil {
					return err
				}
				if err := b.closeThinkingBlock(); err != nil {
					return err
				}
				b.ThinkingBuffer = b.ThinkingBuffer[endPos+len(kiroThinkingDelimitedEndTag):]
				continue
			}

			targetLen := len(b.ThinkingBuffer) - len(kiroThinkingDelimitedEndTag)
			if targetLen > 0 {
				safeLen := floorKiroCharBoundary(b.ThinkingBuffer, targetLen)
				if safeLen > 0 {
					safeContent := b.ThinkingBuffer[:safeLen]
					if safeContent != "" {
						if err := b.emitThinkingDelta(safeContent); err != nil {
							return err
						}
						b.ThinkingBuffer = b.ThinkingBuffer[safeLen:]
					}
				}
			}
			return nil
		default:
			if b.ThinkingBuffer != "" {
				remaining := b.ThinkingBuffer
				b.ThinkingBuffer = ""
				if err := b.emitTextDelta(remaining); err != nil {
					return err
				}
			}
			return nil
		}
	}
}

func (b *kiroClaudeStreamBuilder) flushThinkingAtBoundary() error {
	if !b.InThinkingBlock {
		return nil
	}
	endPos := findKiroThinkingEndTagAtBufferEnd(b.ThinkingBuffer)
	if endPos < 0 {
		return nil
	}
	thinkingContent := b.ThinkingBuffer[:endPos]
	if thinkingContent != "" {
		if err := b.emitThinkingDelta(thinkingContent); err != nil {
			return err
		}
	}
	b.InThinkingBlock = false
	b.ThinkingExtracted = true
	if err := b.emitThinkingDelta(""); err != nil {
		return err
	}
	if err := b.closeThinkingBlock(); err != nil {
		return err
	}
	afterPos := endPos + len(kiroThinkingEndTag)
	remaining := strings.TrimLeftFunc(b.ThinkingBuffer[afterPos:], unicode.IsSpace)
	b.ThinkingBuffer = ""
	if remaining != "" {
		if err := b.emitTextDelta(remaining); err != nil {
			return err
		}
	}
	return nil
}

func (b *kiroClaudeStreamBuilder) processToolUse(toolUse *helps.KiroToolUseEvent) error {
	if toolUse == nil {
		return nil
	}
	setKiroStopReason(&b.StopReason, "tool_use")
	if err := b.flushThinkingAtBoundary(); err != nil {
		return err
	}
	if !b.InThinkingBlock && !b.ThinkingExtracted && b.ThinkingBuffer != "" {
		buffered := b.ThinkingBuffer
		b.ThinkingBuffer = ""
		if err := b.emitTextDelta(buffered); err != nil {
			return err
		}
	}
	if err := b.closeTextBlock(); err != nil {
		return err
	}

	state, err := b.startToolBlock(toolUse.ToolUseID, toolUse.Name)
	if err != nil {
		return err
	}
	state.Name = toolUse.Name
	if toolUse.Input != "" {
		state.Builder.WriteString(toolUse.Input)
	}
	if !toolUse.Stop {
		return nil
	}

	raw := state.Builder.String()
	if strings.TrimSpace(raw) != "" {
		b.OutputTokens += (len(raw) + 3) / 4
		if err := b.emitToolDelta(state, raw); err != nil {
			return err
		}
	}
	return b.closeToolBlock(state)
}

func (b *kiroClaudeStreamBuilder) finalize() error {
	if b.ThinkingBuffer != "" {
		if b.InThinkingBlock {
			endPos := findKiroThinkingEndTagAtBufferEnd(b.ThinkingBuffer)
			if endPos >= 0 {
				thinkingContent := b.ThinkingBuffer[:endPos]
				if thinkingContent != "" {
					if err := b.emitThinkingDelta(thinkingContent); err != nil {
						return err
					}
				}
				b.InThinkingBlock = false
				b.ThinkingExtracted = true
				if err := b.emitThinkingDelta(""); err != nil {
					return err
				}
				if err := b.closeThinkingBlock(); err != nil {
					return err
				}
				afterPos := endPos + len(kiroThinkingEndTag)
				remaining := strings.TrimLeftFunc(b.ThinkingBuffer[afterPos:], unicode.IsSpace)
				b.ThinkingBuffer = ""
				if remaining != "" {
					if err := b.emitTextDelta(remaining); err != nil {
						return err
					}
				}
			} else {
				if err := b.emitThinkingDelta(b.ThinkingBuffer); err != nil {
					return err
				}
				b.ThinkingBuffer = ""
				b.InThinkingBlock = false
				b.ThinkingExtracted = true
				if err := b.emitThinkingDelta(""); err != nil {
					return err
				}
				if err := b.closeThinkingBlock(); err != nil {
					return err
				}
			}
		} else {
			if err := b.emitTextDelta(b.ThinkingBuffer); err != nil {
				return err
			}
			b.ThinkingBuffer = ""
		}
	}

	if b.HasThinkingBlock && !b.HasNonThinkingBlocks {
		setKiroStopReason(&b.StopReason, "max_tokens")
		if err := b.emitTextDelta(" "); err != nil {
			return err
		}
	}

	if err := b.closeTextBlock(); err != nil {
		return err
	}
	for _, state := range b.PendingTools {
		if state.Stopped {
			continue
		}
		raw := state.Builder.String()
		if strings.TrimSpace(raw) != "" {
			b.OutputTokens += (len(raw) + 3) / 4
			if err := b.emitToolDelta(state, raw); err != nil {
				return err
			}
		}
		if err := b.closeToolBlock(state); err != nil {
			return err
		}
	}
	if err := b.appendEvent("message_delta", map[string]any{
		"type": "message_delta",
		"delta": map[string]any{
			"stop_reason":   b.StopReason,
			"stop_sequence": nil,
		},
		"usage": map[string]any{
			"input_tokens":  b.InputTokens,
			"output_tokens": b.OutputTokens,
		},
	}); err != nil {
		return err
	}
	return b.appendEvent("message_stop", map[string]any{
		"type": "message_stop",
	})
}

func buildClaudeStreamEvents(model string, events []helps.KiroEvent) ([]claudeStreamEvent, error) {
	inputTokens := kiroInputTokensFromEvents(model, events)
	builder := newKiroClaudeStreamBuilder(model, inputTokens)
	messageID := "msg_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if err := builder.appendEvent("message_start", map[string]any{
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
				"input_tokens":  inputTokens,
				"output_tokens": 1,
			},
		},
	}); err != nil {
		return nil, err
	}

	for _, event := range events {
		switch event.Type {
		case helps.KiroEventAssistantResponse:
			if event.AssistantResponse == nil {
				continue
			}
			if err := builder.processAssistantResponse(event.AssistantResponse.Content); err != nil {
				return nil, err
			}
		case helps.KiroEventToolUse:
			if err := builder.processToolUse(event.ToolUse); err != nil {
				return nil, err
			}
		case helps.KiroEventContextUsage:
			if event.ContextUsage != nil && event.ContextUsage.ContextUsagePercentage >= 100 {
				setKiroStopReason(&builder.StopReason, "model_context_window_exceeded")
			}
		}
	}

	if err := builder.finalize(); err != nil {
		return nil, err
	}
	return builder.Events, nil
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
