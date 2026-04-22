package executor

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestBuildKiroRequestBodySupportsDirectKiroModel(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"messages": [
			{"role":"user","content":"hello"}
		]
	}`)

	payload, err := buildKiroRequestBody(body, nil, "glm-5")
	if err != nil {
		t.Fatalf("buildKiroRequestBody: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	conversationState, ok := decoded["conversationState"].(map[string]any)
	if !ok {
		t.Fatalf("missing conversationState: %#v", decoded)
	}
	currentMessage, ok := conversationState["currentMessage"].(map[string]any)
	if !ok {
		t.Fatalf("missing currentMessage: %#v", conversationState)
	}
	userInput, ok := currentMessage["userInputMessage"].(map[string]any)
	if !ok {
		t.Fatalf("missing userInputMessage: %#v", currentMessage)
	}
	if got, _ := userInput["modelId"].(string); got != "glm-5" {
		t.Fatalf("expected direct modelId glm-5, got %q", got)
	}
}

func TestBuildKiroRequestBodyIncludesToolsToolUsesAndToolResults(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"tools": [
			{
				"name": "get_weather",
				"description": "Look up weather",
				"input_schema": {
					"type": "object",
					"properties": {
						"city": {"type": "string"}
					}
				}
			}
		],
		"messages": [
			{"role":"user","content":[{"type":"text","text":"Weather in Shanghai?"}]},
			{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"get_weather","input":{"city":"Shanghai"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":[{"type":"text","text":"Sunny"}]}]}
		]
	}`)

	payload, err := buildKiroRequestBody(body, nil, "glm-5")
	if err != nil {
		t.Fatalf("buildKiroRequestBody: %v", err)
	}

	var decoded struct {
		ConversationState struct {
			History []struct {
				UserInputMessage struct {
					Content                 string `json:"content"`
					UserInputMessageContext struct {
						ToolResults []struct {
							ToolUseID string `json:"toolUseId"`
						} `json:"toolResults"`
					} `json:"userInputMessageContext"`
				} `json:"userInputMessage"`
				AssistantResponseMessage struct {
					ToolUses []struct {
						Name      string         `json:"name"`
						ToolUseID string         `json:"toolUseId"`
						Input     map[string]any `json:"input"`
					} `json:"toolUses"`
				} `json:"assistantResponseMessage"`
			} `json:"history"`
			CurrentMessage struct {
				UserInputMessage struct {
					Content                 string `json:"content"`
					UserInputMessageContext struct {
						Tools []struct {
							ToolSpecification struct {
								Name string `json:"name"`
							} `json:"toolSpecification"`
						} `json:"tools"`
						ToolResults []struct {
							ToolUseID string `json:"toolUseId"`
							Status    string `json:"status"`
						} `json:"toolResults"`
					} `json:"userInputMessageContext"`
				} `json:"userInputMessage"`
			} `json:"currentMessage"`
		} `json:"conversationState"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if len(decoded.ConversationState.History) != 2 {
		t.Fatalf("expected 2 history messages, got %d", len(decoded.ConversationState.History))
	}
	if decoded.ConversationState.History[1].AssistantResponseMessage.ToolUses[0].Name != "get_weather" {
		t.Fatalf("expected assistant tool use to be preserved, got %#v", decoded.ConversationState.History[1].AssistantResponseMessage.ToolUses)
	}
	if got := decoded.ConversationState.History[1].AssistantResponseMessage.ToolUses[0].Input["city"]; got != "Shanghai" {
		t.Fatalf("expected tool input city Shanghai, got %#v", got)
	}
	if decoded.ConversationState.CurrentMessage.UserInputMessage.Content != "." {
		t.Fatalf("expected tool-result-only current message content to be '.', got %q", decoded.ConversationState.CurrentMessage.UserInputMessage.Content)
	}
	if len(decoded.ConversationState.CurrentMessage.UserInputMessage.UserInputMessageContext.Tools) != 1 {
		t.Fatalf("expected 1 tool definition, got %d", len(decoded.ConversationState.CurrentMessage.UserInputMessage.UserInputMessageContext.Tools))
	}
	if got := decoded.ConversationState.CurrentMessage.UserInputMessage.UserInputMessageContext.Tools[0].ToolSpecification.Name; got != "get_weather" {
		t.Fatalf("expected tool name get_weather, got %q", got)
	}
	if len(decoded.ConversationState.CurrentMessage.UserInputMessage.UserInputMessageContext.ToolResults) != 1 {
		t.Fatalf("expected 1 tool result, got %d", len(decoded.ConversationState.CurrentMessage.UserInputMessage.UserInputMessageContext.ToolResults))
	}
	if got := decoded.ConversationState.CurrentMessage.UserInputMessage.UserInputMessageContext.ToolResults[0].ToolUseID; got != "toolu_1" {
		t.Fatalf("expected tool result toolUseId toolu_1, got %q", got)
	}
}

func TestBuildKiroRequestBodyIncludesCurrentMessageImages(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"messages": [
			{
				"role":"user",
				"content":[
					{"type":"image","source":{"type":"base64","media_type":"image/png","data":"iVBORw0KGgoAAAANSUhEUg=="}}
				]
			}
		]
	}`)

	payload, err := buildKiroRequestBody(body, nil, "glm-5")
	if err != nil {
		t.Fatalf("buildKiroRequestBody: %v", err)
	}

	var decoded struct {
		ConversationState struct {
			CurrentMessage struct {
				UserInputMessage struct {
					Content string `json:"content"`
					Images  []struct {
						Format string `json:"format"`
						Source struct {
							Bytes string `json:"bytes"`
						} `json:"source"`
					} `json:"images"`
				} `json:"userInputMessage"`
			} `json:"currentMessage"`
		} `json:"conversationState"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	current := decoded.ConversationState.CurrentMessage.UserInputMessage
	if current.Content != "." {
		t.Fatalf("expected image-only current message content '.', got %q", current.Content)
	}
	if len(current.Images) != 1 {
		t.Fatalf("expected 1 current image, got %d", len(current.Images))
	}
	if current.Images[0].Format != "png" {
		t.Fatalf("expected png image format, got %q", current.Images[0].Format)
	}
	if current.Images[0].Source.Bytes != "iVBORw0KGgoAAAANSUhEUg==" {
		t.Fatalf("unexpected image bytes: %q", current.Images[0].Source.Bytes)
	}
}

func TestBuildKiroRequestBodyIncludesHistoryImages(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"messages": [
			{
				"role":"user",
				"content":[
					{"type":"image","source":{"type":"base64","media_type":"image/webp","data":"UklGRlIAAABXRUJQVlA4"}}
				]
			},
			{"role":"assistant","content":"Describe the next image."},
			{"role":"user","content":"continue"}
		]
	}`)

	payload, err := buildKiroRequestBody(body, nil, "glm-5")
	if err != nil {
		t.Fatalf("buildKiroRequestBody: %v", err)
	}

	var decoded struct {
		ConversationState struct {
			History []struct {
				UserInputMessage struct {
					Content string `json:"content"`
					Images  []struct {
						Format string `json:"format"`
						Source struct {
							Bytes string `json:"bytes"`
						} `json:"source"`
					} `json:"images"`
				} `json:"userInputMessage"`
			} `json:"history"`
		} `json:"conversationState"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if len(decoded.ConversationState.History) != 2 {
		t.Fatalf("expected 2 history messages, got %d", len(decoded.ConversationState.History))
	}
	historyUser := decoded.ConversationState.History[0].UserInputMessage
	if historyUser.Content != "." {
		t.Fatalf("expected image-only history content '.', got %q", historyUser.Content)
	}
	if len(historyUser.Images) != 1 {
		t.Fatalf("expected 1 history image, got %d", len(historyUser.Images))
	}
	if historyUser.Images[0].Format != "webp" {
		t.Fatalf("expected webp history image format, got %q", historyUser.Images[0].Format)
	}
	if historyUser.Images[0].Source.Bytes != "UklGRlIAAABXRUJQVlA4" {
		t.Fatalf("unexpected history image bytes: %q", historyUser.Images[0].Source.Bytes)
	}
}

func TestBuildKiroRequestBodySupportsOpenAIResponsesStringInput(t *testing.T) {
	t.Parallel()

	request := []byte(`{"model":"glm-5","input":"hello from compact"}`)
	normalized := normalizeKiroSourcePayload(sdktranslator.FromString("openai-response"), request)
	translated := sdktranslator.TranslateRequest(
		sdktranslator.FromString("openai-response"),
		sdktranslator.FromString("claude"),
		"glm-5",
		normalized,
		false,
	)

	payload, err := buildKiroRequestBody(translated, nil, "glm-5")
	if err != nil {
		t.Fatalf("buildKiroRequestBody: %v", err)
	}

	var decoded struct {
		ConversationState struct {
			CurrentMessage struct {
				UserInputMessage struct {
					Content string `json:"content"`
				} `json:"userInputMessage"`
			} `json:"currentMessage"`
		} `json:"conversationState"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got := decoded.ConversationState.CurrentMessage.UserInputMessage.Content; got != "hello from compact" {
		t.Fatalf("expected compact string input to reach current message, got %q", got)
	}
}

func TestBuildKiroRequestBodyAddsPlaceholderToolForHistoryToolUse(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"messages": [
			{"role":"user","content":[{"type":"text","text":"Call the tool"}]},
			{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"local_tool","input":{"path":"a.txt"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"ok"}]}
		]
	}`)

	payload, err := buildKiroRequestBody(body, nil, "glm-5")
	if err != nil {
		t.Fatalf("buildKiroRequestBody: %v", err)
	}

	var decoded struct {
		ConversationState struct {
			CurrentMessage struct {
				UserInputMessage struct {
					UserInputMessageContext struct {
						Tools []struct {
							ToolSpecification struct {
								Name string `json:"name"`
							} `json:"toolSpecification"`
						} `json:"tools"`
					} `json:"userInputMessageContext"`
				} `json:"userInputMessage"`
			} `json:"currentMessage"`
		} `json:"conversationState"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if len(decoded.ConversationState.CurrentMessage.UserInputMessage.UserInputMessageContext.Tools) != 1 {
		t.Fatalf("expected placeholder tool to be added, got %d tools", len(decoded.ConversationState.CurrentMessage.UserInputMessage.UserInputMessageContext.Tools))
	}
	if got := decoded.ConversationState.CurrentMessage.UserInputMessage.UserInputMessageContext.Tools[0].ToolSpecification.Name; got != "local_tool" {
		t.Fatalf("expected placeholder tool local_tool, got %q", got)
	}
}

func TestBuildKiroRequestBodyToolOnlyAssistantHistoryUsesDotContent(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"messages": [
			{"role":"user","content":[{"type":"text","text":"Call the tool"}]},
			{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"local_tool","input":{"path":"a.txt"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"ok"}]}
		]
	}`)

	payload, err := buildKiroRequestBody(body, nil, "glm-5")
	if err != nil {
		t.Fatalf("buildKiroRequestBody: %v", err)
	}

	var decoded struct {
		ConversationState struct {
			History []struct {
				AssistantResponseMessage struct {
					Content string `json:"content"`
				} `json:"assistantResponseMessage"`
			} `json:"history"`
		} `json:"conversationState"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if len(decoded.ConversationState.History) != 2 {
		t.Fatalf("expected 2 history messages, got %d", len(decoded.ConversationState.History))
	}
	if got := decoded.ConversationState.History[1].AssistantResponseMessage.Content; got != "." {
		t.Fatalf("expected tool-only assistant history content '.', got %q", got)
	}
}

func TestBuildKiroRequestBodyNormalizesAndFiltersTools(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"tools": [
			{"type":"web_search_20250305","name":"web_search"},
			{"name":"` + strings.Repeat("a", 65) + `","description":"too long","input_schema":{"type":"object"}},
			{"name":"Write","description":"","input_schema":{"required":null,"properties":null,"additionalProperties":null}}
		],
		"messages": [
			{"role":"user","content":"hello"}
		]
	}`)

	payload, err := buildKiroRequestBody(body, nil, "glm-5")
	if err != nil {
		t.Fatalf("buildKiroRequestBody: %v", err)
	}

	var decoded struct {
		ConversationState struct {
			CurrentMessage struct {
				UserInputMessage struct {
					UserInputMessageContext struct {
						Tools []struct {
							ToolSpecification struct {
								Name        string         `json:"name"`
								Description string         `json:"description"`
								InputSchema map[string]any `json:"inputSchema"`
							} `json:"toolSpecification"`
						} `json:"tools"`
					} `json:"userInputMessageContext"`
				} `json:"userInputMessage"`
			} `json:"currentMessage"`
		} `json:"conversationState"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	tools := decoded.ConversationState.CurrentMessage.UserInputMessage.UserInputMessageContext.Tools
	if len(tools) != 1 {
		t.Fatalf("expected only the supported Write tool to remain, got %#v", tools)
	}
	tool := tools[0].ToolSpecification
	if tool.Name != "Write" {
		t.Fatalf("expected tool name Write, got %q", tool.Name)
	}
	if !strings.Contains(tool.Description, "Tool: Write") {
		t.Fatalf("expected fallback tool description, got %q", tool.Description)
	}
	if !strings.Contains(tool.Description, "Do NOT attempt to write all content at once.") {
		t.Fatalf("expected Write suffix in description, got %q", tool.Description)
	}
	schema, ok := tool.InputSchema["json"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested input schema payload, got %#v", tool.InputSchema)
	}
	if got, _ := schema["$schema"].(string); got != "http://json-schema.org/draft-07/schema#" {
		t.Fatalf("expected normalized $schema, got %#v", schema["$schema"])
	}
	if got, _ := schema["type"].(string); got != "object" {
		t.Fatalf("expected normalized type object, got %#v", schema["type"])
	}
	if _, ok := schema["properties"].(map[string]any); !ok {
		t.Fatalf("expected normalized properties object, got %#v", schema["properties"])
	}
	if got, ok := schema["additionalProperties"].(bool); !ok || !got {
		t.Fatalf("expected additionalProperties=true, got %#v", schema["additionalProperties"])
	}
	required, ok := schema["required"].([]any)
	if !ok {
		t.Fatalf("expected required array, got %#v", schema["required"])
	}
	if len(required) != 0 {
		t.Fatalf("expected empty required array, got %#v", required)
	}
}

func TestBuildKiroRequestBodyInjectsThinkingAndChunkedPolicyIntoHistory(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"thinking": {"type":"adaptive"},
		"output_config": {"effort":"medium"},
		"tools": [
			{"name":"Write","description":"write file","input_schema":{"type":"object"}}
		],
		"messages": [
			{"role":"user","content":"hello"}
		]
	}`)

	payload, err := buildKiroRequestBody(body, nil, "glm-5-agentic")
	if err != nil {
		t.Fatalf("buildKiroRequestBody: %v", err)
	}

	var decoded struct {
		ConversationState struct {
			History []struct {
				UserInputMessage struct {
					Content string `json:"content"`
				} `json:"userInputMessage"`
				AssistantResponseMessage struct {
					Content string `json:"content"`
				} `json:"assistantResponseMessage"`
			} `json:"history"`
		} `json:"conversationState"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if len(decoded.ConversationState.History) != 4 {
		t.Fatalf("expected system and agentic bootstrap pairs, got %d history entries", len(decoded.ConversationState.History))
	}
	systemUser := decoded.ConversationState.History[0].UserInputMessage.Content
	if !strings.Contains(systemUser, "<thinking_mode>adaptive</thinking_mode><thinking_effort>medium</thinking_effort>") {
		t.Fatalf("expected adaptive thinking prefix in history, got %q", systemUser)
	}
	if !strings.Contains(systemUser, "Complete all chunked operations without commentary.") {
		t.Fatalf("expected chunked policy in history, got %q", systemUser)
	}
	if got := decoded.ConversationState.History[1].AssistantResponseMessage.Content; got != "I will follow these instructions." {
		t.Fatalf("unexpected system assistant bootstrap: %q", got)
	}
	if got := decoded.ConversationState.History[3].AssistantResponseMessage.Content; got != "I will work autonomously following these principles." {
		t.Fatalf("unexpected agentic assistant bootstrap: %q", got)
	}
}

func TestBuildClaudeResponsePayloadIncludesToolUse(t *testing.T) {
	t.Parallel()

	payload := buildClaudeResponsePayload("glm-5", []helps.KiroEvent{
		{
			Type: helps.KiroEventAssistantResponse,
			AssistantResponse: &helps.KiroAssistantResponseEvent{
				Content: "Need a tool.",
			},
		},
		{
			Type: helps.KiroEventToolUse,
			ToolUse: &helps.KiroToolUseEvent{
				Name:      "get_weather",
				ToolUseID: "toolu_1",
				Input:     `{"city":"Shanghai"}`,
				Stop:      true,
			},
		},
	})

	var decoded struct {
		StopReason string `json:"stop_reason"`
		Content    []struct {
			Type  string         `json:"type"`
			ID    string         `json:"id"`
			Name  string         `json:"name"`
			Text  string         `json:"text"`
			Input map[string]any `json:"input"`
		} `json:"content"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if decoded.StopReason != "tool_use" {
		t.Fatalf("expected stop_reason tool_use, got %q", decoded.StopReason)
	}
	if len(decoded.Content) != 2 {
		t.Fatalf("expected 2 content blocks, got %d", len(decoded.Content))
	}
	if decoded.Content[0].Type != "text" || decoded.Content[0].Text != "Need a tool." {
		t.Fatalf("unexpected first content block: %#v", decoded.Content[0])
	}
	if decoded.Content[1].Type != "tool_use" || decoded.Content[1].Name != "get_weather" {
		t.Fatalf("unexpected tool_use block: %#v", decoded.Content[1])
	}
	if got := decoded.Content[1].Input["city"]; got != "Shanghai" {
		t.Fatalf("expected tool_use input city Shanghai, got %#v", got)
	}
}

func TestBuildClaudeResponsePayloadPrefersContextExceededOverToolUse(t *testing.T) {
	t.Parallel()

	payload := buildClaudeResponsePayload("glm-5", []helps.KiroEvent{
		{
			Type: helps.KiroEventContextUsage,
			ContextUsage: &helps.KiroContextUsageEvent{
				ContextUsagePercentage: 100,
			},
		},
		{
			Type: helps.KiroEventToolUse,
			ToolUse: &helps.KiroToolUseEvent{
				Name:      "get_weather",
				ToolUseID: "toolu_1",
				Input:     `{"city":"Shanghai"}`,
				Stop:      true,
			},
		},
	})

	var decoded struct {
		StopReason string `json:"stop_reason"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if decoded.StopReason != "model_context_window_exceeded" {
		t.Fatalf("expected stop_reason model_context_window_exceeded, got %q", decoded.StopReason)
	}
}

func TestBuildClaudeResponsePayloadIncludesUsageFromContextAndOutput(t *testing.T) {
	t.Parallel()

	payload := buildClaudeResponsePayload("claude-sonnet-4-6", []helps.KiroEvent{
		{
			Type: helps.KiroEventContextUsage,
			ContextUsage: &helps.KiroContextUsageEvent{
				ContextUsagePercentage: 50,
			},
		},
		{
			Type: helps.KiroEventAssistantResponse,
			AssistantResponse: &helps.KiroAssistantResponseEvent{
				Content: "Need a tool.",
			},
		},
		{
			Type: helps.KiroEventToolUse,
			ToolUse: &helps.KiroToolUseEvent{
				Name:      "get_weather",
				ToolUseID: "toolu_1",
				Input:     `{"city":"Shanghai"}`,
				Stop:      true,
			},
		},
	})

	var decoded struct {
		Usage struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if decoded.Usage.InputTokens != 500000 {
		t.Fatalf("expected input_tokens 500000, got %d", decoded.Usage.InputTokens)
	}
	if decoded.Usage.OutputTokens <= 0 {
		t.Fatalf("expected output_tokens > 0, got %d", decoded.Usage.OutputTokens)
	}
}

func TestBuildClaudeStreamEventsMergeToolUseDeltaOnStop(t *testing.T) {
	t.Parallel()

	streamEvents, err := buildClaudeStreamEvents("glm-5", []helps.KiroEvent{
		{
			Type: helps.KiroEventAssistantResponse,
			AssistantResponse: &helps.KiroAssistantResponseEvent{
				Content: "Need a tool.",
			},
		},
		{
			Type: helps.KiroEventToolUse,
			ToolUse: &helps.KiroToolUseEvent{
				Name:      "get_weather",
				ToolUseID: "toolu_1",
				Input:     `{"city":"Shang`,
				Stop:      false,
			},
		},
		{
			Type: helps.KiroEventToolUse,
			ToolUse: &helps.KiroToolUseEvent{
				Name:      "get_weather",
				ToolUseID: "toolu_1",
				Input:     `hai"}`,
				Stop:      true,
			},
		},
		{
			Type: helps.KiroEventAssistantResponse,
			AssistantResponse: &helps.KiroAssistantResponseEvent{
				Content: "Done.",
			},
		},
	})
	if err != nil {
		t.Fatalf("buildClaudeStreamEvents: %v", err)
	}

	type contentBlockStart struct {
		Type         string `json:"type"`
		Index        int    `json:"index"`
		ContentBlock struct {
			Type string `json:"type"`
		} `json:"content_block"`
	}
	type contentBlockDelta struct {
		Type  string `json:"type"`
		Index int    `json:"index"`
		Delta struct {
			Type        string `json:"type"`
			Text        string `json:"text"`
			PartialJSON string `json:"partial_json"`
		} `json:"delta"`
	}
	type messageDelta struct {
		Type  string `json:"type"`
		Delta struct {
			StopReason string `json:"stop_reason"`
		} `json:"delta"`
	}

	var starts []contentBlockStart
	var toolDeltas []contentBlockDelta
	var final messageDelta
	for _, event := range streamEvents {
		switch event.Name {
		case "content_block_start":
			var decoded contentBlockStart
			if err := json.Unmarshal(event.Data, &decoded); err != nil {
				t.Fatalf("unmarshal content_block_start: %v", err)
			}
			starts = append(starts, decoded)
		case "content_block_delta":
			var decoded contentBlockDelta
			if err := json.Unmarshal(event.Data, &decoded); err != nil {
				t.Fatalf("unmarshal content_block_delta: %v", err)
			}
			if decoded.Delta.Type == "input_json_delta" {
				toolDeltas = append(toolDeltas, decoded)
			}
		case "message_delta":
			if err := json.Unmarshal(event.Data, &final); err != nil {
				t.Fatalf("unmarshal message_delta: %v", err)
			}
		}
	}

	if len(starts) != 3 {
		t.Fatalf("expected 3 content_block_start events, got %d", len(starts))
	}
	if starts[0].ContentBlock.Type != "text" || starts[0].Index != 0 {
		t.Fatalf("unexpected first block start: %#v", starts[0])
	}
	if starts[1].ContentBlock.Type != "tool_use" || starts[1].Index != 1 {
		t.Fatalf("unexpected tool block start: %#v", starts[1])
	}
	if starts[2].ContentBlock.Type != "text" || starts[2].Index != 2 {
		t.Fatalf("unexpected second text block start: %#v", starts[2])
	}
	if len(toolDeltas) != 1 {
		t.Fatalf("expected 1 merged tool delta, got %d", len(toolDeltas))
	}
	if toolDeltas[0].Index != 1 || toolDeltas[0].Delta.PartialJSON != `{"city":"Shanghai"}` {
		t.Fatalf("unexpected first tool delta: %#v", toolDeltas[0])
	}
	if final.Delta.StopReason != "tool_use" {
		t.Fatalf("expected final stop_reason tool_use, got %q", final.Delta.StopReason)
	}
}

func TestBuildClaudeStreamEventsEmitThinkingBlocks(t *testing.T) {
	t.Parallel()

	streamEvents, err := buildClaudeStreamEvents("claude-sonnet-4-6", []helps.KiroEvent{
		{
			Type: helps.KiroEventAssistantResponse,
			AssistantResponse: &helps.KiroAssistantResponseEvent{
				Content: "<thinking>\nNeed to inspect files</thinking>\n\nI can help.",
			},
		},
	})
	if err != nil {
		t.Fatalf("buildClaudeStreamEvents: %v", err)
	}

	type contentBlockStart struct {
		Type         string `json:"type"`
		Index        int    `json:"index"`
		ContentBlock struct {
			Type string `json:"type"`
		} `json:"content_block"`
	}
	type contentBlockDelta struct {
		Type  string `json:"type"`
		Index int    `json:"index"`
		Delta struct {
			Type     string `json:"type"`
			Thinking string `json:"thinking"`
			Text     string `json:"text"`
		} `json:"delta"`
	}

	var starts []contentBlockStart
	var thinking string
	var text string
	for _, event := range streamEvents {
		switch event.Name {
		case "content_block_start":
			var decoded contentBlockStart
			if err := json.Unmarshal(event.Data, &decoded); err != nil {
				t.Fatalf("unmarshal content_block_start: %v", err)
			}
			starts = append(starts, decoded)
		case "content_block_delta":
			var decoded contentBlockDelta
			if err := json.Unmarshal(event.Data, &decoded); err != nil {
				t.Fatalf("unmarshal content_block_delta: %v", err)
			}
			switch decoded.Delta.Type {
			case "thinking_delta":
				thinking += decoded.Delta.Thinking
			case "text_delta":
				text += decoded.Delta.Text
			}
		}
	}

	if len(starts) != 2 {
		t.Fatalf("expected 2 content_block_start events, got %d", len(starts))
	}
	if starts[0].ContentBlock.Type != "thinking" || starts[0].Index != 0 {
		t.Fatalf("unexpected thinking block start: %#v", starts[0])
	}
	if starts[1].ContentBlock.Type != "text" || starts[1].Index != 1 {
		t.Fatalf("unexpected text block start: %#v", starts[1])
	}
	if thinking != "Need to inspect files" {
		t.Fatalf("unexpected thinking content: %q", thinking)
	}
	if text != "I can help." {
		t.Fatalf("unexpected text content: %q", text)
	}
}

func TestNormalizeClaudeStreamEventIndicesDropsOrphansAndRenumbersVisibleBlocks(t *testing.T) {
	t.Parallel()

	events := []claudeStreamEvent{
		{
			Name: "message_start",
			Data: []byte(`{"type":"message_start","message":{"content":[]}}`),
		},
		{
			Name: "content_block_start",
			Data: []byte(`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`),
		},
		{
			Name: "content_block_delta",
			Data: []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}`),
		},
		{
			Name: "content_block_stop",
			Data: []byte(`{"type":"content_block_stop","index":0}`),
		},
		{
			Name: "content_block_start",
			Data: []byte(`{"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_1","name":"Read","input":{}}}`),
		},
		{
			Name: "content_block_delta",
			Data: []byte(`{"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"file_path\":\"/tmp/a\"}"}}`),
		},
		{
			Name: "content_block_start",
			Data: []byte(`{"type":"content_block_start","index":3,"content_block":{"type":"tool_use","id":"toolu_2","name":"Bash","input":{}}}`),
		},
		{
			Name: "content_block_delta",
			Data: []byte(`{"type":"content_block_delta","index":3,"delta":{"type":"input_json_delta","partial_json":"{\"command\":\"pwd\"}"}}`),
		},
		{
			Name: "content_block_start",
			Data: []byte(`{"type":"content_block_start","index":4,"content_block":{"type":"tool_use","id":"toolu_3","name":"ToolSearch","input":{}}}`),
		},
		{
			Name: "content_block_delta",
			Data: []byte(`{"type":"content_block_delta","index":4,"delta":{"type":"input_json_delta","partial_json":"{\"query\":\"select:Read\"}"}}`),
		},
		{
			Name: "content_block_stop",
			Data: []byte(`{"type":"content_block_stop","index":1}`),
		},
		{
			Name: "content_block_stop",
			Data: []byte(`{"type":"content_block_stop","index":2}`),
		},
		{
			Name: "content_block_stop",
			Data: []byte(`{"type":"content_block_stop","index":3}`),
		},
		{
			Name: "content_block_stop",
			Data: []byte(`{"type":"content_block_stop","index":4}`),
		},
		{
			Name: "message_delta",
			Data: []byte(`{"type":"message_delta","delta":{"stop_reason":"tool_use"}}`),
		},
		{
			Name: "message_stop",
			Data: []byte(`{"type":"message_stop"}`),
		},
	}

	normalized, err := normalizeClaudeStreamEventIndices(events)
	if err != nil {
		t.Fatalf("normalizeClaudeStreamEventIndices: %v", err)
	}

	type contentBlockStart struct {
		Index        int `json:"index"`
		ContentBlock struct {
			Type string `json:"type"`
			Name string `json:"name"`
		} `json:"content_block"`
	}
	type contentBlockDelta struct {
		Index int `json:"index"`
		Delta struct {
			Type string `json:"type"`
		} `json:"delta"`
	}
	type contentBlockStop struct {
		Index int `json:"index"`
	}

	var starts []contentBlockStart
	var deltas []contentBlockDelta
	var stops []contentBlockStop
	for _, event := range normalized {
		switch event.Name {
		case "content_block_start":
			var decoded contentBlockStart
			if err := json.Unmarshal(event.Data, &decoded); err != nil {
				t.Fatalf("unmarshal content_block_start: %v", err)
			}
			starts = append(starts, decoded)
		case "content_block_delta":
			var decoded contentBlockDelta
			if err := json.Unmarshal(event.Data, &decoded); err != nil {
				t.Fatalf("unmarshal content_block_delta: %v", err)
			}
			deltas = append(deltas, decoded)
		case "content_block_stop":
			var decoded contentBlockStop
			if err := json.Unmarshal(event.Data, &decoded); err != nil {
				t.Fatalf("unmarshal content_block_stop: %v", err)
			}
			stops = append(stops, decoded)
		}
	}

	if len(starts) != 4 {
		t.Fatalf("expected 4 visible content_block_start events, got %d", len(starts))
	}
	for i, start := range starts {
		if start.Index != i {
			t.Fatalf("expected start index %d, got %d", i, start.Index)
		}
	}

	if len(stops) != 4 {
		t.Fatalf("expected 4 content_block_stop events after dropping orphan, got %d", len(stops))
	}
	for i, stop := range stops {
		if stop.Index != i {
			t.Fatalf("expected stop index %d, got %d", i, stop.Index)
		}
	}

	if len(deltas) != 4 {
		t.Fatalf("expected 4 content_block_delta events, got %d", len(deltas))
	}
	for i, delta := range deltas {
		if delta.Index != i {
			t.Fatalf("expected delta index %d, got %d", i, delta.Index)
		}
	}
}

func TestBuildClaudeStreamEventsUsesContextUsageTokens(t *testing.T) {
	t.Parallel()

	streamEvents, err := buildClaudeStreamEvents("claude-sonnet-4-6", []helps.KiroEvent{
		{
			Type: helps.KiroEventContextUsage,
			ContextUsage: &helps.KiroContextUsageEvent{
				ContextUsagePercentage: 50,
			},
		},
		{
			Type: helps.KiroEventAssistantResponse,
			AssistantResponse: &helps.KiroAssistantResponseEvent{
				Content: "pong",
			},
		},
	})
	if err != nil {
		t.Fatalf("buildClaudeStreamEvents: %v", err)
	}

	var messageStart struct {
		Message struct {
			Usage struct {
				InputTokens  int `json:"input_tokens"`
				OutputTokens int `json:"output_tokens"`
			} `json:"usage"`
		} `json:"message"`
	}
	var messageDelta struct {
		Usage struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}
	for _, event := range streamEvents {
		switch event.Name {
		case "message_start":
			if err := json.Unmarshal(event.Data, &messageStart); err != nil {
				t.Fatalf("unmarshal message_start: %v", err)
			}
		case "message_delta":
			if err := json.Unmarshal(event.Data, &messageDelta); err != nil {
				t.Fatalf("unmarshal message_delta: %v", err)
			}
		}
	}
	if messageStart.Message.Usage.InputTokens != 500000 {
		t.Fatalf("expected message_start input_tokens 500000, got %d", messageStart.Message.Usage.InputTokens)
	}
	if messageStart.Message.Usage.OutputTokens != 1 {
		t.Fatalf("expected message_start output_tokens 1, got %d", messageStart.Message.Usage.OutputTokens)
	}
	if messageDelta.Usage.InputTokens != 500000 {
		t.Fatalf("expected message_delta input_tokens 500000, got %d", messageDelta.Usage.InputTokens)
	}
	if messageDelta.Usage.OutputTokens <= 0 {
		t.Fatalf("expected message_delta output_tokens > 0, got %d", messageDelta.Usage.OutputTokens)
	}
}

func TestBuildClaudeStreamEventsThinkingOnlyAddsPlaceholderTextAndMaxTokens(t *testing.T) {
	t.Parallel()

	streamEvents, err := buildClaudeStreamEvents("claude-sonnet-4-6", []helps.KiroEvent{
		{
			Type: helps.KiroEventAssistantResponse,
			AssistantResponse: &helps.KiroAssistantResponseEvent{
				Content: "<thinking>\nNeed more time</thinking>",
			},
		},
	})
	if err != nil {
		t.Fatalf("buildClaudeStreamEvents: %v", err)
	}

	var stopReason string
	var text string
	for _, event := range streamEvents {
		switch event.Name {
		case "content_block_delta":
			var decoded struct {
				Delta struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"delta"`
			}
			if err := json.Unmarshal(event.Data, &decoded); err != nil {
				t.Fatalf("unmarshal content_block_delta: %v", err)
			}
			if decoded.Delta.Type == "text_delta" {
				text += decoded.Delta.Text
			}
		case "message_delta":
			var decoded struct {
				Delta struct {
					StopReason string `json:"stop_reason"`
				} `json:"delta"`
			}
			if err := json.Unmarshal(event.Data, &decoded); err != nil {
				t.Fatalf("unmarshal message_delta: %v", err)
			}
			stopReason = decoded.Delta.StopReason
		}
	}

	if stopReason != "max_tokens" {
		t.Fatalf("expected stop_reason max_tokens, got %q", stopReason)
	}
	if text != " " {
		t.Fatalf("expected placeholder text delta, got %q", text)
	}
}

func TestParseAnthropicAssistantMessagePreservesWebSearchHistoryText(t *testing.T) {
	t.Parallel()

	parsed, err := parseAnthropicAssistantMessage(json.RawMessage(`[
		{"type":"server_tool_use","id":"srvtoolu_1","name":"web_search","input":{"query":"rust async"}},
		{"type":"web_search_tool_result","content":[
			{"type":"web_search_result","title":"Rust Async","url":"https://example.com/rust","encrypted_content":"Async guide","page_age":"April 21, 2026"}
		]}
	]`))
	if err != nil {
		t.Fatalf("parseAnthropicAssistantMessage: %v", err)
	}
	if !strings.Contains(parsed.Text, "Rust Async: https://example.com/rust") {
		t.Fatalf("expected web search title/url in parsed text, got %q", parsed.Text)
	}
	if !strings.Contains(parsed.Text, "Date: April 21, 2026") {
		t.Fatalf("expected page age in parsed text, got %q", parsed.Text)
	}
	if !strings.Contains(parsed.Text, "Async guide") {
		t.Fatalf("expected snippet in parsed text, got %q", parsed.Text)
	}
}

func TestBuildClaudeDataLinesTranslateToOpenAINonStream(t *testing.T) {
	t.Parallel()

	lines, err := buildClaudeDataLines("glm-5", []helps.KiroEvent{
		{
			Type: helps.KiroEventAssistantResponse,
			AssistantResponse: &helps.KiroAssistantResponseEvent{
				Content: "pong",
			},
		},
	})
	if err != nil {
		t.Fatalf("buildClaudeDataLines: %v", err)
	}

	out := sdktranslator.TranslateNonStream(
		context.Background(),
		sdktranslator.FromString("claude"),
		sdktranslator.FromString("openai"),
		"glm-5",
		nil,
		nil,
		joinClaudeDataLines(lines),
		nil,
	)

	var decoded struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("unmarshal openai response: %v\nraw=%s", err, string(out))
	}
	if len(decoded.Choices) != 1 || decoded.Choices[0].Message.Content != "pong" {
		t.Fatalf("expected translated content pong, got %#v", decoded)
	}
}

func TestBuildClaudeDataLinesTranslateToolUseToOpenAINonStream(t *testing.T) {
	t.Parallel()

	lines, err := buildClaudeDataLines("glm-5", []helps.KiroEvent{
		{
			Type: helps.KiroEventToolUse,
			ToolUse: &helps.KiroToolUseEvent{
				Name:      "get_weather",
				ToolUseID: "toolu_1",
				Input:     `{"city":"Shanghai"}`,
				Stop:      true,
			},
		},
	})
	if err != nil {
		t.Fatalf("buildClaudeDataLines: %v", err)
	}

	out := sdktranslator.TranslateNonStream(
		context.Background(),
		sdktranslator.FromString("claude"),
		sdktranslator.FromString("openai"),
		"glm-5",
		nil,
		nil,
		joinClaudeDataLines(lines),
		nil,
	)

	var decoded struct {
		Choices []struct {
			FinishReason string `json:"finish_reason"`
			Message      struct {
				ToolCalls []struct {
					ID       string `json:"id"`
					Type     string `json:"type"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("unmarshal openai response: %v\nraw=%s", err, string(out))
	}
	if len(decoded.Choices) != 1 {
		t.Fatalf("expected 1 choice, got %d", len(decoded.Choices))
	}
	if decoded.Choices[0].FinishReason != "tool_calls" {
		t.Fatalf("expected finish_reason tool_calls, got %q", decoded.Choices[0].FinishReason)
	}
	if len(decoded.Choices[0].Message.ToolCalls) != 1 {
		t.Fatalf("expected 1 tool call, got %#v", decoded.Choices[0].Message.ToolCalls)
	}
	call := decoded.Choices[0].Message.ToolCalls[0]
	if call.ID != "toolu_1" || call.Function.Name != "get_weather" {
		t.Fatalf("unexpected tool call: %#v", call)
	}
	if !strings.Contains(call.Function.Arguments, `"city":"Shanghai"`) {
		t.Fatalf("expected tool arguments to contain city, got %q", call.Function.Arguments)
	}
}

func TestBuildClaudeDataLinesTranslateSplitToolUseToOpenAINonStream(t *testing.T) {
	t.Parallel()

	lines, err := buildClaudeDataLines("glm-5", []helps.KiroEvent{
		{
			Type: helps.KiroEventToolUse,
			ToolUse: &helps.KiroToolUseEvent{
				Name:      "get_weather",
				ToolUseID: "toolu_1",
				Input:     `{"city":"Shang`,
				Stop:      false,
			},
		},
		{
			Type: helps.KiroEventToolUse,
			ToolUse: &helps.KiroToolUseEvent{
				Name:      "get_weather",
				ToolUseID: "toolu_1",
				Input:     `hai"}`,
				Stop:      true,
			},
		},
	})
	if err != nil {
		t.Fatalf("buildClaudeDataLines: %v", err)
	}

	out := sdktranslator.TranslateNonStream(
		context.Background(),
		sdktranslator.FromString("claude"),
		sdktranslator.FromString("openai"),
		"glm-5",
		nil,
		nil,
		joinClaudeDataLines(lines),
		nil,
	)

	var decoded struct {
		Choices []struct {
			Message struct {
				ToolCalls []struct {
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("unmarshal openai response: %v\nraw=%s", err, string(out))
	}
	if len(decoded.Choices) != 1 || len(decoded.Choices[0].Message.ToolCalls) != 1 {
		t.Fatalf("expected 1 translated tool call, got %#v", decoded)
	}
	call := decoded.Choices[0].Message.ToolCalls[0]
	if call.Function.Name != "get_weather" {
		t.Fatalf("expected tool name get_weather, got %q", call.Function.Name)
	}
	if call.Function.Arguments != `{"city":"Shanghai"}` {
		t.Fatalf("expected merged tool arguments, got %q", call.Function.Arguments)
	}
}

func TestRewriteOpenAIResponsesCompactPayload(t *testing.T) {
	t.Parallel()

	out := rewriteOpenAIResponsesCompactPayload([]byte(`{"id":"resp_1","object":"response","status":"completed"}`))

	var decoded struct {
		Object string `json:"object"`
	}
	if err := json.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("unmarshal compact payload: %v", err)
	}
	if decoded.Object != "response.compaction" {
		t.Fatalf("expected object response.compaction, got %q", decoded.Object)
	}
}

func TestNormalizeKiroOpenAIResponsesInputPreservesArrayInput(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"model":"glm-5","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}]}`)
	normalized := normalizeKiroOpenAIResponsesInput(raw)
	if string(normalized) != string(raw) {
		t.Fatalf("expected array input to remain unchanged, got %s", string(normalized))
	}
}
