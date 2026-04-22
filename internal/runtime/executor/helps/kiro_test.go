package helps

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestResolveKiroProfileContextUsesFixedBuilderProfile(t *testing.T) {
	t.Parallel()

	serialized, err := json.Marshal(kiroClientSecretSerialized{
		InitiateLoginURI: kiroBuilderIDStartURL + "/",
	})
	if err != nil {
		t.Fatalf("marshal serialized client secret: %v", err)
	}
	payload, err := json.Marshal(kiroClientSecretPayload{
		Serialized: string(serialized),
	})
	if err != nil {
		t.Fatalf("marshal client secret payload: %v", err)
	}
	clientSecret := "header." + base64.RawURLEncoding.EncodeToString(payload) + ".signature"

	resolved, err := resolveKiroProfileContext(context.Background(), nil, nil, KiroCredentials{
		ClientSecret: clientSecret,
	}, "token")
	if err != nil {
		t.Fatalf("resolve profile context: %v", err)
	}
	if resolved.Provider != "BuilderId" {
		t.Fatalf("expected provider BuilderId, got %q", resolved.Provider)
	}
	if resolved.ProfileARN != kiroBuilderIDProfileARN {
		t.Fatalf("expected fixed builder profile ARN, got %q", resolved.ProfileARN)
	}
}

func TestApplyKiroResolvedBuilderProfileToAuth(t *testing.T) {
	t.Parallel()

	serialized, err := json.Marshal(kiroClientSecretSerialized{
		InitiateLoginURI: kiroBuilderIDStartURL + "/",
	})
	if err != nil {
		t.Fatalf("marshal serialized client secret: %v", err)
	}
	payload, err := json.Marshal(kiroClientSecretPayload{
		Serialized: string(serialized),
	})
	if err != nil {
		t.Fatalf("marshal client secret payload: %v", err)
	}
	clientSecret := "header." + base64.RawURLEncoding.EncodeToString(payload) + ".signature"

	auth := &cliproxyauth.Auth{}
	ApplyKiroCredentialsToAuth(auth, KiroCredentials{
		AccessToken:  "token",
		ExpiresAt:    time.Now().Add(10 * time.Minute).UTC().Format(time.RFC3339),
		ClientSecret: clientSecret,
	})

	resolved, err := resolveKiroProfileContext(context.Background(), nil, nil, KiroCredentials{
		ClientSecret: clientSecret,
	}, "token")
	if err != nil {
		t.Fatalf("resolve profile context: %v", err)
	}

	ApplyKiroCredentialsToAuth(auth, resolved)

	gotCreds := KiroCredentialsFromAuth(auth)
	if gotCreds.Provider != "BuilderId" {
		t.Fatalf("expected provider BuilderId, got %q", gotCreds.Provider)
	}
	if gotCreds.ProfileARN != kiroBuilderIDProfileARN {
		t.Fatalf("expected fixed builder profile ARN, got %q", gotCreds.ProfileARN)
	}
	if got := KiroCredentialsFromAuth(auth).ProfileARN; got != kiroBuilderIDProfileARN {
		t.Fatalf("expected auth metadata profile ARN %q, got %q", kiroBuilderIDProfileARN, got)
	}
}

func TestBuildKiroRegistryModelsFiltersUnsupportedModels(t *testing.T) {
	t.Parallel()

	maxInput := int64(200000)
	maxOutput := int64(32000)
	models := buildKiroRegistryModels([]kiroAvailableModel{
		{
			ModelID:             "claude-sonnet-4.6",
			ModelName:           "Claude Sonnet 4.6",
			Description:         "Latest Kiro Claude model",
			SupportedInputTypes: []string{"TEXT", "IMAGE"},
			TokenLimits: &kiroModelTokenLimits{
				MaxInputTokens:  &maxInput,
				MaxOutputTokens: &maxOutput,
			},
		},
		{
			ModelID:             "glm-5",
			ModelName:           "GLM 5",
			SupportedInputTypes: []string{"TEXT"},
		},
		{ModelID: "simple-task", ModelName: "Simple Task"},
		{ModelID: "gpt-4o", ModelName: "GPT 4o"},
		{ModelID: "claude-sonnet-4.6", ModelName: "Duplicate"},
	})

	if len(models) != 2 {
		t.Fatalf("expected 2 supported models, got %d", len(models))
	}
	if models[0].ID != "claude-sonnet-4.6" {
		t.Fatalf("expected model id claude-sonnet-4.6, got %q", models[0].ID)
	}
	if models[0].OwnedBy != kiroProviderKey {
		t.Fatalf("expected owned_by %q, got %q", kiroProviderKey, models[0].OwnedBy)
	}
	if models[0].Type != "claude" {
		t.Fatalf("expected type claude, got %q", models[0].Type)
	}
	if models[0].ContextLength != int(maxInput) {
		t.Fatalf("expected context length %d, got %d", maxInput, models[0].ContextLength)
	}
	if models[0].MaxCompletionTokens != int(maxOutput) {
		t.Fatalf("expected max completion tokens %d, got %d", maxOutput, models[0].MaxCompletionTokens)
	}
	if len(models[0].SupportedInputModalities) != 2 {
		t.Fatalf("expected supported input modalities to be preserved, got %v", models[0].SupportedInputModalities)
	}
	if models[1].ID != "glm-5" {
		t.Fatalf("expected second model id glm-5, got %q", models[1].ID)
	}
	if models[1].Type != "openai" {
		t.Fatalf("expected glm-5 type openai, got %q", models[1].Type)
	}
}

func TestKiroMapModelSupportsDirectCatalogModels(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input string
		want  string
	}{
		{input: "glm-5", want: "glm-5"},
		{input: "qwen3-coder-next", want: "qwen3-coder-next"},
		{input: "minimax-m2.5", want: "minimax-m2.5"},
		{input: "claude-sonnet-4-20250514", want: "claude-sonnet-4"},
	}

	for _, tc := range cases {
		got, ok := KiroMapModel(tc.input)
		if !ok {
			t.Fatalf("expected %q to be supported", tc.input)
		}
		if got != tc.want {
			t.Fatalf("KiroMapModel(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}

	if _, ok := KiroMapModel("auto"); ok {
		t.Fatal("expected auto to remain excluded from direct catalog support")
	}
}
