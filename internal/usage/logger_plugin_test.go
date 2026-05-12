package usage

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
)

func TestRequestStatisticsRecordBuildsEndpointSnapshot(t *testing.T) {
	stats := NewRequestStatistics()
	stats.Record(context.Background(), coreusage.Record{
		Provider:    "kiro",
		Model:       "claude-opus-4.7",
		Source:      "user@example.com",
		AuthIndex:   "kiro.json#1",
		RequestedAt: time.Date(2026, 5, 12, 8, 0, 0, 0, time.UTC),
		Detail: coreusage.Detail{
			InputTokens:         10,
			OutputTokens:        5,
			ReasoningTokens:     2,
			CacheReadTokens:     3,
			CacheCreationTokens: 4,
		},
	})

	snapshot := stats.Snapshot()
	if snapshot.TotalRequests != 1 {
		t.Fatalf("total requests = %d, want 1", snapshot.TotalRequests)
	}
	if snapshot.TotalTokens != 17 {
		t.Fatalf("total tokens = %d, want 17", snapshot.TotalTokens)
	}
	api := snapshot.APIs["kiro"]
	model := api.Models["claude-opus-4.7"]
	if len(model.Details) != 1 {
		t.Fatalf("details = %d, want 1", len(model.Details))
	}
	detail := model.Details[0]
	if detail.Source != "use***@example.com" {
		t.Fatalf("source = %q, want masked email", detail.Source)
	}
	if detail.Tokens.CacheReadTokens != 3 || detail.Tokens.CacheCreationTokens != 4 {
		t.Fatalf("cache breakdown = %+v", detail.Tokens)
	}
}

func TestParseImportPayloadAcceptsLegacyUsageExport(t *testing.T) {
	payload := map[string]any{
		"version": 1,
		"usage": map[string]any{
			"apis": map[string]any{
				"POST /v1/chat/completions": map[string]any{
					"models": map[string]any{
						"glm-5": map[string]any{
							"details": []any{
								map[string]any{
									"timestamp": "2026-05-12T00:00:00Z",
									"source":    "sk-test-secret-value-that-is-long",
									"tokens": map[string]any{
										"input_tokens":  1,
										"output_tokens": 2,
										"total_tokens":  3,
									},
								},
							},
						},
					},
				},
			},
		},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	parsed, err := ParseImportPayload(data)
	if err != nil {
		t.Fatalf("ParseImportPayload: %v", err)
	}
	if parsed.Format != ImportFormatLegacyExport {
		t.Fatalf("format = %q, want %q", parsed.Format, ImportFormatLegacyExport)
	}
	if len(parsed.Events) != 1 {
		t.Fatalf("events = %d, want 1", len(parsed.Events))
	}
	event := parsed.Events[0]
	if event.Endpoint != "POST /v1/chat/completions" || event.Model != "glm-5" {
		t.Fatalf("event target = %s %s", event.Endpoint, event.Model)
	}
	if event.Source != "m:sk-t...long" {
		t.Fatalf("source = %q, want masked key", event.Source)
	}
}
