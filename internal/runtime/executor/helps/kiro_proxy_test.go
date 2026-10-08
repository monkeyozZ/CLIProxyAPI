package helps

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func TestKiroHTTPClientUsesRequestProxyWithoutCredentialPassword(t *testing.T) {
	ctx := cliproxyexecutor.WithRequestProxyURL(context.Background(), "http://request-proxy.example:8080")
	client := NewKiroHTTPClient(ctx, nil, nil, KiroCredentials{
		ProxyURL: "http://credential-proxy.example:8080", ProxyUsername: "credential-user", ProxyPassword: "credential-password",
	}, 0)
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport.Proxy == nil {
		t.Fatalf("expected proxy transport, got %T", client.Transport)
	}
	req, err := http.NewRequest(http.MethodPost, "https://q.us-east-1.amazonaws.com/generateAssistantResponse", nil)
	if err != nil {
		t.Fatal(err)
	}
	proxyURL, err := transport.Proxy(req)
	if err != nil {
		t.Fatal(err)
	}
	if proxyURL == nil || proxyURL.Host != "request-proxy.example:8080" || proxyURL.User != nil {
		t.Fatal("request proxy did not replace the credential proxy and its credentials")
	}
	if client.Timeout != 0 {
		t.Fatalf("generation timeout = %v, want no timeout", client.Timeout)
	}
}

func TestKiroRefreshIgnoresRequestProxy(t *testing.T) {
	called := false
	transport := kiroRefreshTestTransport(func(req *http.Request) (*http.Response, error) {
		called = true
		if proxyURL := cliproxyexecutor.RequestProxyURL(req.Context()); proxyURL != "" {
			t.Errorf("credential refresh inherited request proxy %q", proxyURL)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(
			`{"accessToken":"refreshed-test-token","refreshToken":"next-test-refresh","expiresIn":3600}`,
		))}, nil
	})
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", transport)
	ctx = cliproxyexecutor.WithRequestProxyURL(ctx, "http://127.0.0.1:1")
	updated, err := RefreshKiroAuth(ctx, nil, &cliproxyauth.Auth{
		Provider: "kiro", Metadata: map[string]any{"auth_method": "social", "refresh_token": "test-refresh"},
	})
	if err != nil {
		t.Fatalf("RefreshKiroAuth: %v", err)
	}
	if !called || KiroCredentialsFromAuth(updated).AccessToken != "refreshed-test-token" {
		t.Fatal("credential refresh did not use the credential transport")
	}
}

type kiroRefreshTestTransport func(*http.Request) (*http.Response, error)

func (f kiroRefreshTestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
