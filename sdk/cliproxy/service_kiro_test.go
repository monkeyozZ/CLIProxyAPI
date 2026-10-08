package cliproxy

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestKiroExecutorAndModelRegistration(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	cfg := &config.Config{
		OAuthExcludedModels: map[string][]string{"kiro": {"claude-*"}},
		OAuthModelAlias:     map[string][]config.OAuthModelAlias{"kiro": {{Name: "glm-5", Alias: "fast"}}},
	}
	cfg.ForceModelPrefix = true
	service := &Service{cfg: cfg, coreManager: manager}
	service.registerAvailableExecutors(context.Background(), executorRegistrationOptions{includeBaseline: true})
	executor, ok := manager.Executor("kiro")
	if _, isKiro := executor.(*runtimeexecutor.KiroExecutor); !ok || !isKiro {
		t.Fatalf("Kiro executor registration = %T (present=%t)", executor, ok)
	}
	auth, err := manager.Register(context.Background(), &coreauth.Auth{
		ID: "kiro-registration-test", Provider: "kiro", Prefix: "tenant", Status: coreauth.StatusActive,
		Metadata: map[string]any{
			"access_token": "test-access", "refresh_token": "test-refresh", "profile_arn": "test-profile",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })
	calls := 0
	transport := kiroCatalogTestTransport(func(req *http.Request) (*http.Response, error) {
		calls++
		if req.URL.Path != "/ListAvailableModels" || req.URL.Query().Get("profileArn") != "test-profile" {
			t.Fatalf("unexpected Kiro catalog request: %s", req.URL)
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(
			`{"models":[{"modelId":"glm-5","tokenLimits":{"maxInputTokens":200000}},{"modelId":"claude-sonnet-4.6"},{"modelId":"unsupported-model"}]}`,
		))}, nil
	})
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", transport)
	service.registerModelsForAuth(ctx, auth)
	models := GlobalModelRegistry().GetModelsForClient(auth.ID)
	if calls != 1 || len(models) != 1 {
		t.Fatalf("catalog calls = %d, models = %+v", calls, models)
	}
	if model := models[0]; model.ID != "tenant/fast" || model.OwnedBy != "kiro" || model.ContextLength != 200000 {
		t.Fatalf("Kiro alias, prefix, or capabilities lost: %+v", model)
	}
}

type kiroCatalogTestTransport func(*http.Request) (*http.Response, error)

func (f kiroCatalogTestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
