package executor

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	translatorcommon "github.com/router-for-me/CLIProxyAPI/v8/internal/translator/common"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestKiroExecutorUsesResponseFormat(t *testing.T) {
	tests := []struct {
		name        string
		source      sdktranslator.Format
		response    sdktranslator.Format
		payload     string
		alt         string
		objectPath  string
		wantObject  string
		contentPath string
	}{
		{
			name: "claude to chat completions", source: sdktranslator.FormatClaude, response: sdktranslator.FormatOpenAI,
			payload:    `{"messages":[{"role":"user","content":"ping"}]}`,
			objectPath: "object", wantObject: "chat.completion", contentPath: "choices.0.message.content",
		},
		{
			name: "responses string input to claude", source: sdktranslator.FormatOpenAIResponse, response: sdktranslator.FormatClaude,
			payload:    `{"input":"ping"}`,
			objectPath: "type", wantObject: "message", contentPath: "content.0.text",
		},
		{
			name: "claude to responses", source: sdktranslator.FormatClaude, response: sdktranslator.FormatOpenAIResponse,
			payload:    `{"messages":[{"role":"user","content":"ping"}]}`,
			objectPath: "object", wantObject: "response", contentPath: "output.0.content.0.text",
		},
		{
			name: "claude to responses compact", source: sdktranslator.FormatClaude, response: sdktranslator.FormatOpenAIResponse,
			payload: `{"messages":[{"role":"user","content":"ping"}]}`, alt: "responses/compact",
			objectPath: "object", wantObject: "response.compaction", contentPath: "output.0.content.0.text",
		},
		{
			name: "default response format", source: sdktranslator.FormatClaude,
			payload:    `{"messages":[{"role":"user","content":"ping"}]}`,
			objectPath: "type", wantObject: "message", contentPath: "content.0.text",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := []byte(tt.payload)
			ctx := kiroCompatContext(func(req *http.Request) {
				if req.Header.Get("Authorization") != "Bearer kiro-test-token" {
					t.Error("Kiro upstream request is missing its credential")
				}
			})
			response, err := NewKiroExecutor(&config.Config{}).Execute(ctx, kiroCompatAuth(), cliproxyexecutor.Request{
				Model: "glm-5", Payload: payload,
			}, cliproxyexecutor.Options{
				SourceFormat: tt.source, ResponseFormat: tt.response, OriginalRequest: payload, Alt: tt.alt,
			})
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if got := gjson.GetBytes(response.Payload, tt.objectPath).String(); got != tt.wantObject {
				t.Fatalf("response object = %q, want %q; body = %s", got, tt.wantObject, response.Payload)
			}
			if got := gjson.GetBytes(response.Payload, tt.contentPath).String(); got != "pong" {
				t.Fatalf("response text = %q, want pong; body = %s", got, response.Payload)
			}
		})
	}
}

func TestKiroExecutorStreamUsesResponseFormat(t *testing.T) {
	tests := []struct {
		name      string
		source    sdktranslator.Format
		response  sdktranslator.Format
		payload   string
		wantEvent string
	}{
		{
			name: "claude to chat completions", source: sdktranslator.FormatClaude, response: sdktranslator.FormatOpenAI,
			payload: `{"messages":[{"role":"user","content":"ping"}]}`, wantEvent: "chat.completion.chunk",
		},
		{
			name: "responses string input to claude", source: sdktranslator.FormatOpenAIResponse, response: sdktranslator.FormatClaude,
			payload: `{"input":"ping"}`, wantEvent: "content_block_delta",
		},
		{
			name: "claude to responses", source: sdktranslator.FormatClaude, response: sdktranslator.FormatOpenAIResponse,
			payload: `{"messages":[{"role":"user","content":"ping"}]}`, wantEvent: "response.output_text.delta",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := []byte(tt.payload)
			stream, err := NewKiroExecutor(&config.Config{}).ExecuteStream(kiroCompatContext(nil), kiroCompatAuth(), cliproxyexecutor.Request{
				Model: "glm-5", Payload: payload,
			}, cliproxyexecutor.Options{
				SourceFormat: tt.source, ResponseFormat: tt.response, OriginalRequest: payload, Stream: true,
			})
			if err != nil {
				t.Fatalf("ExecuteStream: %v", err)
			}
			if got := stream.Headers.Get("Content-Type"); got != "text/event-stream" {
				t.Fatalf("stream content type = %q", got)
			}
			var body strings.Builder
			for chunk := range stream.Chunks {
				if chunk.Err != nil {
					t.Fatalf("stream chunk: %v", chunk.Err)
				}
				body.Write(chunk.Payload)
			}
			if !strings.Contains(body.String(), tt.wantEvent) || !strings.Contains(body.String(), "pong") {
				t.Fatalf("stream missing %q or response text: %s", tt.wantEvent, body.String())
			}
		})
	}
}

func TestKiroExecutorReturnsTranslationErrorBeforeUpstream(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "non-stream", true: "stream"}[stream], func(t *testing.T) {
			called := false
			ctx := kiroCompatContext(func(*http.Request) { called = true })
			payload := []byte(`{"messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{"format":"wav","data":"UklGRg=="}}]}]}`)
			req := cliproxyexecutor.Request{Model: "glm-5", Payload: payload}
			opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAI, OriginalRequest: payload, Stream: stream}
			executor := NewKiroExecutor(&config.Config{})
			var err error
			if stream {
				_, err = executor.ExecuteStream(ctx, kiroCompatAuth(), req, opts)
			} else {
				_, err = executor.Execute(ctx, kiroCompatAuth(), req, opts)
			}
			var unsupported *translatorcommon.UnsupportedPartError
			if !errors.As(err, &unsupported) || unsupported.Type != "input_audio" {
				t.Fatalf("error = %v, want unsupported input_audio", err)
			}
			if called {
				t.Fatal("unsupported request reached the Kiro upstream")
			}
		})
	}
}

func TestKiroExecutorPayloadRulesFinalizeNativeBodyOnce(t *testing.T) {
	const currentMessage = "conversationState.currentMessage.userInputMessage"
	models := []config.PayloadModelRule{{Name: "glm-5", Protocol: "claude", Exist: []string{currentMessage}}}
	cfg := &config.Config{Payload: config.PayloadConfig{
		Default: []config.PayloadRule{{Models: models, Params: map[string]any{
			currentMessage + ".origin": "must-not-replace-existing-origin",
		}}},
		Override: []config.PayloadRule{{Models: models, Params: map[string]any{
			currentMessage + ".content": "configured prompt",
			currentMessage + ".modelId": "configured-model",
		}}},
		Filter: []config.PayloadFilterRule{{Models: models, Params: []string{
			"profileArn", "conversationState.history.0", currentMessage + ".userInputMessageContext.tools",
		}}},
	}}
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "non-stream", true: "stream"}[stream], func(t *testing.T) {
			called := false
			ctx := kiroCompatContext(func(req *http.Request) {
				called = true
				body, errRead := io.ReadAll(req.Body)
				if errRead != nil {
					t.Fatal(errRead)
				}
				if gjson.GetBytes(body, currentMessage+".content").String() != "configured prompt" ||
					gjson.GetBytes(body, currentMessage+".modelId").String() != "configured-model" {
					t.Fatalf("native Kiro overrides were replaced: %s", body)
				}
				if gjson.GetBytes(body, currentMessage+".origin").String() != "AI_EDITOR" {
					t.Fatalf("default rule did not inspect the original native Kiro payload: %s", body)
				}
				if gjson.GetBytes(body, "profileArn").Exists() || gjson.GetBytes(body, currentMessage+".userInputMessageContext.tools").Exists() {
					t.Fatalf("filtered profile or history placeholder tools were restored: %s", body)
				}
				if got := gjson.GetBytes(body, "conversationState.history.#").Int(); got != 3 {
					t.Fatalf("history length = %d, want 3 after removing its first item exactly once: %s", got, body)
				}
			})
			payload := []byte(`{"system":"test instructions","messages":[
				{"role":"user","content":"first"},
				{"role":"assistant","content":[{"type":"tool_use","id":"read-1","name":"Read","input":{"path":"test.txt"}}]},
				{"role":"user","content":[{"type":"tool_result","tool_use_id":"read-1","content":"test content"},{"type":"text","text":"ping"}]}
			]}`)
			auth := kiroCompatAuth()
			auth.Metadata["profile_arn"] = "test-profile"
			req := cliproxyexecutor.Request{Model: "glm-5", Payload: payload}
			opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatClaude, OriginalRequest: payload, Stream: stream}
			executor := NewKiroExecutor(cfg)
			if stream {
				result, err := executor.ExecuteStream(ctx, auth, req, opts)
				if err != nil {
					t.Fatal(err)
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
				}
			} else if _, err := executor.Execute(ctx, auth, req, opts); err != nil {
				t.Fatal(err)
			}
			if !called {
				t.Fatal("request did not reach the Kiro upstream")
			}
		})
	}
}

func TestKiroExecutorAppliesSourceAndHeaderPayloadRules(t *testing.T) {
	cfg := &config.Config{Payload: config.PayloadConfig{Override: []config.PayloadRule{{
		Models: []config.PayloadModelRule{{
			Name: "glm-5", Protocol: "claude", FromProtocol: "openai-response", Headers: map[string]string{"X-Kiro-Test": "override"},
		}},
		Params: map[string]any{"conversationState.currentMessage.userInputMessage.content": "configured prompt"},
	}}}}
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "non-stream", true: "stream"}[stream], func(t *testing.T) {
			called := false
			ctx := kiroCompatContext(func(req *http.Request) {
				called = true
				body, errRead := io.ReadAll(req.Body)
				if errRead != nil {
					t.Fatalf("read upstream request: %v", errRead)
				}
				if got := gjson.GetBytes(body, "conversationState.currentMessage.userInputMessage.content").String(); got != "configured prompt" {
					t.Fatalf("upstream prompt = %q, want configured prompt", got)
				}
			})
			payload := []byte(`{"input":"ping"}`)
			req := cliproxyexecutor.Request{Model: "glm-5", Payload: payload}
			opts := cliproxyexecutor.Options{
				SourceFormat: sdktranslator.FormatOpenAIResponse, OriginalRequest: payload, Stream: stream,
				Headers: http.Header{"X-Kiro-Test": []string{"override"}},
			}
			executor := NewKiroExecutor(cfg)
			if stream {
				result, err := executor.ExecuteStream(ctx, kiroCompatAuth(), req, opts)
				if err != nil {
					t.Fatalf("ExecuteStream: %v", err)
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatalf("stream chunk: %v", chunk.Err)
					}
				}
			} else if _, err := executor.Execute(ctx, kiroCompatAuth(), req, opts); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if !called {
				t.Fatal("request did not reach the Kiro upstream")
			}
		})
	}
}

type kiroCompatRoundTripper func(*http.Request) (*http.Response, error)

func (f kiroCompatRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func kiroCompatContext(inspect func(*http.Request)) context.Context {
	transport := kiroCompatRoundTripper(func(req *http.Request) (*http.Response, error) {
		if inspect != nil {
			inspect(req)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/vnd.amazon.eventstream"}},
			Body:       io.NopCloser(bytes.NewReader(kiroCompatEventStream())),
		}, nil
	})
	return context.WithValue(context.Background(), "cliproxy.roundtripper", transport)
}

func kiroCompatAuth() *cliproxyauth.Auth {
	return &cliproxyauth.Auth{ID: "kiro-compat-test", Provider: "kiro", Metadata: map[string]any{
		"access_token": "kiro-test-token", "refresh_token": "kiro-test-refresh", "region": "us-east-1",
	}}
}

func kiroCompatEventStream() []byte {
	var headers []byte
	for _, header := range [][2]string{{":message-type", "event"}, {":event-type", "assistantResponseEvent"}} {
		headers = append(headers, byte(len(header[0])))
		headers = append(headers, header[0]...)
		headers = append(headers, 7)
		headers = binary.BigEndian.AppendUint16(headers, uint16(len(header[1])))
		headers = append(headers, header[1]...)
	}
	payload := []byte(`{"content":"pong"}`)
	frame := make([]byte, 12, 16+len(headers)+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(cap(frame)))
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(headers)))
	binary.BigEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(frame[:8]))
	frame = append(frame, headers...)
	frame = append(frame, payload...)
	return binary.BigEndian.AppendUint32(frame, crc32.ChecksumIEEE(frame))
}
