package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestKiroOAuthV8Dispatch(t *testing.T) {
	handler := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, nil)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v8/management/oauth/auth-url?provider=kiro&idp=Google", nil)
	handler.StartOAuthV8(ctx)
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "BuilderId/Internal") {
		t.Fatalf("Kiro OAuth dispatch returned %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestKiroAuthFieldsSurvivePagination(t *testing.T) {
	for _, source := range []string{"manager", "disk"} {
		t.Run(source, func(t *testing.T) {
			dir := t.TempDir()
			name := "kiro-test.json"
			path := filepath.Join(dir, name)
			metadata := map[string]any{
				"type": "kiro", "auth_method": "idc", "region": "us-east-1", "api_region": "us-west-2",
				"access_token": "test-access", "refresh_token": "test-refresh", "client_secret": "test-client-secret",
				"profile_arn": "test-profile", "client_id": "test-client", "subscription_title": "Kiro Pro",
				"machine_id": "test-machine", "proxy_url": "http://localhost:8181", "proxy_username": "test-user", "proxy_password": "test-password",
			}
			data, err := json.Marshal(metadata)
			if err != nil {
				t.Fatal(err)
			}
			if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
				t.Fatal(errWrite)
			}
			var manager *coreauth.Manager
			if source == "manager" {
				manager = coreauth.NewManager(nil, nil, nil)
				_, errRegister := manager.Register(context.Background(), &coreauth.Auth{
					ID: name, FileName: name, Provider: "kiro", Status: coreauth.StatusActive,
					Metadata: metadata, Attributes: map[string]string{"path": path},
				})
				if errRegister != nil {
					t.Fatal(errRegister)
				}
			}
			handler := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: dir}, manager)
			result := requestAuthFilesPage(t, handler, "/v8/management/credentials?page=1&page_size=1")
			if result.Total != 1 || len(result.Files) != 1 {
				t.Fatalf("unexpected page: %#v", result)
			}
			entry := result.Files[0]
			for _, field := range []string{"auth_method", "region", "api_region", "profile_arn", "client_id", "machine_id", "subscription_title", "proxy_url", "proxy_username"} {
				if entry[field] != metadata[field] {
					t.Errorf("%s = %v, want %v", field, entry[field], metadata[field])
				}
			}
			for _, field := range []string{"access_token", "refresh_token", "client_secret", "proxy_password"} {
				if entry["has_"+field] != true {
					t.Errorf("missing has_%s", field)
				}
				if _, exposed := entry[field]; exposed {
					t.Errorf("credential listing exposed %s", field)
				}
			}
		})
	}
}
