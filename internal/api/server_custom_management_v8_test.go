package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/api/handlers/management"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestManagementV8KiroAndUsageRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	routes := []struct {
		method, path, body string
		status             int
	}{
		{http.MethodGet, "/observability/usage", "", http.StatusOK},
		{http.MethodDelete, "/observability/usage?startMs=2&endMs=1", "", http.StatusBadRequest},
		{http.MethodGet, "/observability/usage/export", "", http.StatusOK},
		{http.MethodPost, "/observability/usage/import", "{", http.StatusBadRequest},
		{http.MethodGet, "/observability/usage/model-prices", "", http.StatusOK},
		{http.MethodPut, "/observability/usage/model-prices", "{}", http.StatusBadRequest},
		{http.MethodPost, "/observability/usage/model-prices/sync", "invalid", http.StatusBadRequest},
		{http.MethodGet, "/credentials/kiro/balance", "", http.StatusBadRequest},
		{http.MethodPatch, "/credentials/kiro", "{}", http.StatusBadRequest},
		{http.MethodGet, "/oauth/auth-url?provider=kiro&idp=Google", "", http.StatusBadRequest},
	}
	for _, access := range []struct {
		name                      string
		enabled, home, authorized bool
		status                    int
	}{
		{"authorized", true, false, true, 0},
		{"missing key", true, false, false, http.StatusUnauthorized},
		{"disabled", false, false, true, http.StatusNotFound},
		{"home", true, true, true, http.StatusNotFound},
	} {
		t.Run(access.name, func(t *testing.T) {
			cfg := &config.Config{AuthDir: t.TempDir()}
			cfg.RemoteManagement.SecretKey = "test-management-key"
			cfg.Home.Enabled = access.home
			for _, route := range routes {
				t.Run(route.method+" "+route.path, func(t *testing.T) {
					handler := management.NewHandler(cfg, "", auth.NewManager(nil, nil, nil))
					handler.SetLocalPassword("test-management-key")
					server := &Server{cfg: cfg, engine: gin.New(), mgmt: handler}
					server.managementRoutesEnabled.Store(access.enabled)
					server.registerManagementRoutes()
					req := httptest.NewRequest(route.method, "/v8/management"+route.path, strings.NewReader(route.body))
					req.RemoteAddr = "127.0.0.1:1234"
					req.Header.Set("Content-Type", "application/json")
					if access.authorized {
						req.Header.Set("Authorization", "Bearer test-management-key")
					}
					recorder := httptest.NewRecorder()
					server.engine.ServeHTTP(recorder, req)
					want := access.status
					if want == 0 {
						want = route.status
					}
					if recorder.Code != want {
						t.Fatalf("status = %d, want %d; body=%s", recorder.Code, want, recorder.Body.String())
					}
				})
			}
		})
	}
}
