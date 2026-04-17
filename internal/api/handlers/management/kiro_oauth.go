package management

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/misc"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
	"github.com/ugorji/go/codec"
)

const (
	kiroOAuthPortalURL         = "https://app.kiro.dev"
	kiroOAuthInitiateLoginPath = "/service/KiroWebPortalService/operation/InitiateLogin"
	kiroOAuthRedirectFrom      = "KiroIDE"
	kiroOAuthDefaultIDP        = "BuilderId"
	kiroOAuthDefaultAuthRegion = "us-east-1"
	kiroOAuthBuilderIDStartURL = "https://view.awsapps.com/start"
	kiroOAuthInternalStartURL  = "https://amzn.awsapps.com/start"
	kiroOAuthRegisterRedirect  = "http://127.0.0.1/oauth/callback"
	kiroOIDCUserAgent          = "aws-sdk-js/3.738.0 ua/2.1 os/other lang/js md/browser#unknown_unknown api/sso-oidc#3.738.0 m/E KiroIDE"
	kiroOAuthCallbackTimeout   = 5 * time.Minute
	kiroOAuthHTTPTimeout       = 20 * time.Second
	kiroOAuthPollInterval      = 500 * time.Millisecond
)

var kiroOAuthGrantScopes = []string{
	"codewhisperer:completions",
	"codewhisperer:analysis",
	"codewhisperer:conversations",
	"codewhisperer:transformations",
	"codewhisperer:taskassist",
}

var kiroOAuthCallbackPorts = []int{49153, 50153, 51153, 52153, 53153, 9091, 8008, 6588, 4649, 3128}

type kiroInitiateLoginResponse struct {
	RedirectURL    string `json:"redirectUrl" codec:"redirectUrl"`
	InstanceRegion string `json:"instanceRegion" codec:"instanceRegion"`
}

type kiroOIDCRegisterResponse struct {
	ClientID     string `json:"clientId"`
	ClientSecret string `json:"clientSecret"`
}

type kiroOIDCTokenResponse struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ExpiresIn    int64  `json:"expiresIn"`
	IDToken      string `json:"idToken"`
}

type kiroStage1Resolution struct {
	IssuerURL   string
	IDCRegion   string
	LoginOption string
}

func (h *Handler) RequestKiroToken(c *gin.Context) {
	if h == nil || h.cfg == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "handler not initialized"})
		return
	}
	if strings.TrimSpace(h.cfg.AuthDir) == "" {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "auth dir is not configured"})
		return
	}

	ctx := PopulateAuthContext(context.Background(), c)
	httpClient := util.SetProxy(&h.cfg.SDKConfig, &http.Client{Timeout: kiroOAuthHTTPTimeout})

	portalURL := firstNonEmptyTrim(c.Query("portal_url"), kiroOAuthPortalURL)
	idp := kiroCanonicalKiroProviderName(c.Query("idp"))
	if idp == "" {
		idp = kiroOAuthDefaultIDP
	}
	if !kiroProviderUsesIDC(idp) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "kiro management oauth currently supports BuilderId/Internal only"})
		return
	}

	startURL := strings.TrimSpace(c.Query("start_url"))
	if startURL == "" {
		startURL = kiroDefaultStartURLForProvider(idp)
	}

	authRegion := firstNonEmptyTrim(c.Query("auth_region"), c.Query("idc_region"), kiroOAuthDefaultAuthRegion)
	apiRegion := firstNonEmptyTrim(c.Query("api_region"), authRegion)
	callbackPort, err := kiroPickCallbackPort()
	if err != nil {
		log.WithError(err).Error("failed to allocate kiro callback port")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "no available callback port"})
		return
	}

	stage1State, err := misc.GenerateRandomState()
	if err != nil {
		log.WithError(err).Error("failed to generate kiro stage1 state")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate state"})
		return
	}
	stage1Verifier, err := kiroGenerateCodeVerifier()
	if err != nil {
		log.WithError(err).Error("failed to generate kiro stage1 verifier")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate pkce verifier"})
		return
	}
	stage1Challenge := kiroCodeChallenge(stage1Verifier)

	stage1RedirectURI := fmt.Sprintf("http://127.0.0.1:%d", callbackPort)
	stage2RedirectURI := fmt.Sprintf("http://127.0.0.1:%d/oauth/callback", callbackPort)

	initiateResp, err := kiroInitiateLogin(ctx, httpClient, portalURL, idp, stage1State, stage1Challenge, stage1RedirectURI, authRegion, startURL)
	if err != nil {
		log.WithError(err).Error("kiro initiate login failed")
		c.JSON(http.StatusBadGateway, gin.H{"error": "failed to initiate kiro login"})
		return
	}

	stage1, err := kiroResolveStage1(initiateResp.RedirectURL, stage1State, initiateResp.InstanceRegion, idp, startURL, authRegion)
	if err != nil {
		log.WithError(err).Error("failed to resolve kiro login metadata")
		c.JSON(http.StatusBadGateway, gin.H{"error": "failed to resolve kiro login metadata"})
		return
	}
	if !kiroProviderUsesIDC(stage1.LoginOption) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "kiro social login is not supported in management oauth yet; use BuilderId"})
		return
	}

	isWebUI := isWebUIRequest(c)
	var forwarder *callbackForwarder
	if isWebUI {
		targetURL, errTarget := h.managementCallbackURL("/kiro/callback")
		if errTarget != nil {
			log.WithError(errTarget).Error("failed to compute kiro callback target")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "callback server unavailable"})
			return
		}
		var errStart error
		if forwarder, errStart = startCallbackForwarder(callbackPort, "kiro", targetURL); errStart != nil {
			log.WithError(errStart).Error("failed to start kiro callback forwarder")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start callback server"})
			return
		}
	}

	registerResp, err := kiroRegisterOIDCClient(ctx, httpClient, stage1.IDCRegion, stage1.IssuerURL, kiroOAuthRegisterRedirect)
	if err != nil {
		log.WithError(err).Error("kiro oidc client registration failed")
		if isWebUI {
			stopCallbackForwarderInstance(callbackPort, forwarder)
		}
		c.JSON(http.StatusBadGateway, gin.H{"error": "failed to register kiro oidc client"})
		return
	}
	if strings.TrimSpace(registerResp.ClientID) == "" || strings.TrimSpace(registerResp.ClientSecret) == "" {
		if isWebUI {
			stopCallbackForwarderInstance(callbackPort, forwarder)
		}
		c.JSON(http.StatusBadGateway, gin.H{"error": "kiro oidc registration returned incomplete client data"})
		return
	}

	stage2State, err := misc.GenerateRandomState()
	if err != nil {
		log.WithError(err).Error("failed to generate kiro stage2 state")
		if isWebUI {
			stopCallbackForwarderInstance(callbackPort, forwarder)
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate state"})
		return
	}
	stage2Verifier, err := kiroGenerateCodeVerifier()
	if err != nil {
		log.WithError(err).Error("failed to generate kiro stage2 verifier")
		if isWebUI {
			stopCallbackForwarderInstance(callbackPort, forwarder)
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate pkce verifier"})
		return
	}
	stage2Challenge := kiroCodeChallenge(stage2Verifier)
	authURL := kiroBuildAuthorizeURL(stage1.IDCRegion, registerResp.ClientID, stage2RedirectURI, stage2State, stage2Challenge)

	RegisterOAuthSession(stage2State, "kiro")

	go func() {
		if isWebUI {
			defer stopCallbackForwarderInstance(callbackPort, forwarder)
		}
		waitFile := filepath.Join(h.cfg.AuthDir, fmt.Sprintf(".oauth-kiro-%s.oauth", stage2State))
		resultMap, errWait := waitForOAuthCallbackFile(stage2State, "kiro", waitFile, kiroOAuthCallbackTimeout)
		if errWait != nil {
			if errors.Is(errWait, errOAuthSessionNotPending) {
				return
			}
			log.WithError(errWait).Error("kiro oauth callback wait failed")
			return
		}

		if errStr := strings.TrimSpace(resultMap["error"]); errStr != "" {
			SetOAuthSessionError(stage2State, errStr)
			return
		}
		if strings.TrimSpace(resultMap["state"]) != stage2State {
			SetOAuthSessionError(stage2State, "State code error")
			return
		}

		code := strings.TrimSpace(resultMap["code"])
		if code == "" {
			SetOAuthSessionError(stage2State, "Authorization code missing")
			return
		}

		tokenResp, errExchange := kiroExchangeAuthorizationCode(ctx, httpClient, stage1.IDCRegion, registerResp.ClientID, registerResp.ClientSecret, code, stage2Verifier, stage2RedirectURI)
		if errExchange != nil {
			log.WithError(errExchange).Error("kiro token exchange failed")
			SetOAuthSessionError(stage2State, "Failed to exchange authorization code")
			return
		}

		refreshToken := strings.TrimSpace(tokenResp.RefreshToken)
		if refreshToken == "" {
			SetOAuthSessionError(stage2State, "Kiro token exchange returned empty refresh token")
			return
		}

		creds := helps.KiroCredentials{
			AccessToken:  strings.TrimSpace(tokenResp.AccessToken),
			RefreshToken: refreshToken,
			AuthMethod:   "idc",
			Provider:     kiroCanonicalKiroProviderName(stage1.LoginOption),
			ClientID:     strings.TrimSpace(registerResp.ClientID),
			ClientSecret: strings.TrimSpace(registerResp.ClientSecret),
			Region:       stage1.IDCRegion,
			APIRegion:    firstNonEmptyTrim(apiRegion, stage1.IDCRegion),
			MachineID:    kiroMachineIDFromRefreshToken(refreshToken),
			Email:        kiroExtractJWTString(tokenResp.IDToken, "email", "preferred_username", "upn"),
		}
		if creds.Provider == "" {
			creds.Provider = idp
		}
		if tokenResp.ExpiresIn > 0 {
			creds.ExpiresAt = time.Now().UTC().Add(time.Duration(tokenResp.ExpiresIn) * time.Second).Format(time.RFC3339)
		}
		if profileARN := kiroFixedProfileARN(creds.Provider); profileARN != "" {
			creds.ProfileARN = profileARN
		}

		fileName := kiroCredentialFileName(creds.Email, creds.Provider)
		record := &coreauth.Auth{
			ID:       fileName,
			FileName: fileName,
			Provider: "kiro",
			Metadata: map[string]any{},
		}
		helps.ApplyKiroCredentialsToAuth(record, creds)

		savedPath, errSave := h.saveTokenRecord(ctx, record)
		if errSave != nil {
			log.WithError(errSave).Error("failed to save kiro auth")
			SetOAuthSessionError(stage2State, "Failed to save kiro auth file")
			return
		}

		log.Infof("kiro authentication successful; token saved to %s", savedPath)
		CompleteOAuthSession(stage2State)
		CompleteOAuthSessionsByProvider("kiro")
	}()

	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
		"url":    authURL,
		"state":  stage2State,
	})
}

func waitForOAuthCallbackFile(state, provider, path string, timeout time.Duration) (map[string]string, error) {
	deadline := time.Now().Add(timeout)
	for {
		if !IsOAuthSessionPending(state, provider) {
			return nil, errOAuthSessionNotPending
		}
		if time.Now().After(deadline) {
			SetOAuthSessionError(state, "Timeout waiting for OAuth callback")
			return nil, fmt.Errorf("timeout waiting for oauth callback")
		}
		data, errRead := os.ReadFile(path)
		if errRead == nil {
			var payload map[string]string
			if errUnmarshal := json.Unmarshal(data, &payload); errUnmarshal != nil {
				SetOAuthSessionError(state, "Invalid OAuth callback payload")
				return nil, fmt.Errorf("invalid oauth callback payload: %w", errUnmarshal)
			}
			_ = os.Remove(path)
			return payload, nil
		}
		time.Sleep(kiroOAuthPollInterval)
	}
}

func kiroInitiateLogin(ctx context.Context, httpClient *http.Client, portalURL, idp, state, codeChallenge, redirectURI, authRegion, startURL string) (kiroInitiateLoginResponse, error) {
	endpoint := strings.TrimRight(portalURL, "/") + kiroOAuthInitiateLoginPath
	payload := map[string]any{
		"idp":                 idp,
		"state":               state,
		"codeChallenge":       codeChallenge,
		"codeChallengeMethod": "S256",
		"redirectUri":         redirectURI,
		"redirectFrom":        kiroOAuthRedirectFrom,
	}
	if provider := kiroCanonicalKiroProviderName(idp); provider == "Internal" {
		if strings.TrimSpace(authRegion) != "" {
			payload["idcRegion"] = strings.TrimSpace(authRegion)
		}
		if strings.TrimSpace(startURL) != "" {
			payload["startUrl"] = strings.TrimSpace(startURL)
		}
	}

	var (
		handle codec.CborHandle
		body   []byte
	)
	if err := codec.NewEncoderBytes(&body, &handle).Encode(payload); err != nil {
		return kiroInitiateLoginResponse{}, fmt.Errorf("encode kiro initiate login cbor: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return kiroInitiateLoginResponse{}, err
	}
	req.Header.Set("Content-Type", "application/cbor")
	req.Header.Set("Accept", "application/cbor")
	req.Header.Set("smithy-protocol", "rpc-v2-cbor")
	req.Header.Set("Origin", strings.TrimRight(portalURL, "/"))
	req.Header.Set("x-kiro-visitorid", kiroVisitorID())

	resp, err := httpClient.Do(req)
	if err != nil {
		return kiroInitiateLoginResponse{}, err
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.WithError(errClose).Error("failed to close kiro initiate login response body")
		}
	}()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return kiroInitiateLoginResponse{}, err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return kiroInitiateLoginResponse{}, fmt.Errorf("kiro initiate login failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
	}

	var result kiroInitiateLoginResponse
	if strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "application/cbor") {
		if err := codec.NewDecoderBytes(bodyBytes, &handle).Decode(&result); err != nil {
			return kiroInitiateLoginResponse{}, fmt.Errorf("decode kiro initiate login response: %w", err)
		}
	} else if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return kiroInitiateLoginResponse{}, fmt.Errorf("decode kiro initiate login response: %w", err)
	}
	if strings.TrimSpace(result.RedirectURL) == "" {
		return kiroInitiateLoginResponse{}, fmt.Errorf("kiro initiate login response missing redirectUrl")
	}
	return result, nil
}

func kiroResolveStage1(redirectURL, expectedState, fallbackRegion, requestedIDP, fallbackIssuerURL, fallbackAuthRegion string) (kiroStage1Resolution, error) {
	parsed, err := url.Parse(strings.TrimSpace(redirectURL))
	if err != nil {
		return kiroStage1Resolution{}, fmt.Errorf("parse kiro stage1 redirect url: %w", err)
	}
	params := parsed.Query()
	state := strings.TrimSpace(params.Get("state"))
	if state == "" {
		return kiroStage1Resolution{}, fmt.Errorf("kiro stage1 redirect missing state")
	}
	if state != strings.TrimSpace(expectedState) {
		return kiroStage1Resolution{}, fmt.Errorf("kiro stage1 state mismatch")
	}

	issuerURL := firstNonEmptyTrim(params.Get("issuer_url"), params.Get("issuerUrl"), fallbackIssuerURL)
	if issuerURL == "" {
		issuerURL = kiroDefaultStartURLForProvider(requestedIDP)
	}
	idcRegion := firstNonEmptyTrim(params.Get("idc_region"), params.Get("idcRegion"), fallbackRegion, fallbackAuthRegion, kiroOAuthDefaultAuthRegion)
	loginOption := firstNonEmptyTrim(params.Get("login_option"), requestedIDP)
	loginOption = kiroCanonicalKiroProviderName(loginOption)
	if loginOption == "" {
		loginOption = kiroOAuthDefaultIDP
	}
	if issuerURL == "" {
		return kiroStage1Resolution{}, fmt.Errorf("kiro stage1 redirect missing issuer url")
	}
	return kiroStage1Resolution{
		IssuerURL:   issuerURL,
		IDCRegion:   idcRegion,
		LoginOption: loginOption,
	}, nil
}

func kiroRegisterOIDCClient(ctx context.Context, httpClient *http.Client, idcRegion, issuerURL, redirectURI string) (kiroOIDCRegisterResponse, error) {
	endpoint := fmt.Sprintf("https://oidc.%s.amazonaws.com/client/register", strings.TrimSpace(idcRegion))
	payload := map[string]any{
		"clientName":   "Kiro IDE",
		"clientType":   "public",
		"scopes":       append([]string(nil), kiroOAuthGrantScopes...),
		"grantTypes":   []string{"authorization_code", "refresh_token"},
		"redirectUris": []string{redirectURI},
		"issuerUrl":    issuerURL,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return kiroOIDCRegisterResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return kiroOIDCRegisterResponse{}, err
	}
	kiroApplyOIDCHeaders(req, strings.TrimSpace(idcRegion))
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return kiroOIDCRegisterResponse{}, err
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.WithError(errClose).Error("failed to close kiro oidc register response body")
		}
	}()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return kiroOIDCRegisterResponse{}, err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return kiroOIDCRegisterResponse{}, fmt.Errorf("kiro oidc register failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
	}
	var result kiroOIDCRegisterResponse
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return kiroOIDCRegisterResponse{}, fmt.Errorf("decode kiro oidc register response: %w", err)
	}
	return result, nil
}

func kiroBuildAuthorizeURL(idcRegion, clientID, redirectURI, state, codeChallenge string) string {
	values := url.Values{}
	values.Set("response_type", "code")
	values.Set("client_id", clientID)
	values.Set("redirect_uri", redirectURI)
	values.Set("scopes", strings.Join(kiroOAuthGrantScopes, ","))
	values.Set("state", state)
	values.Set("code_challenge", codeChallenge)
	values.Set("code_challenge_method", "S256")
	return fmt.Sprintf("https://oidc.%s.amazonaws.com/authorize?%s", strings.TrimSpace(idcRegion), values.Encode())
}

func kiroExchangeAuthorizationCode(ctx context.Context, httpClient *http.Client, idcRegion, clientID, clientSecret, code, codeVerifier, redirectURI string) (kiroOIDCTokenResponse, error) {
	endpoint := fmt.Sprintf("https://oidc.%s.amazonaws.com/token", strings.TrimSpace(idcRegion))
	payload := map[string]string{
		"clientId":     clientID,
		"clientSecret": clientSecret,
		"grantType":    "authorization_code",
		"redirectUri":  redirectURI,
		"code":         code,
		"codeVerifier": codeVerifier,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return kiroOIDCTokenResponse{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return kiroOIDCTokenResponse{}, err
	}
	kiroApplyOIDCHeaders(req, strings.TrimSpace(idcRegion))
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return kiroOIDCTokenResponse{}, err
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.WithError(errClose).Error("failed to close kiro token response body")
		}
	}()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return kiroOIDCTokenResponse{}, err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return kiroOIDCTokenResponse{}, fmt.Errorf("kiro token exchange failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
	}
	var result kiroOIDCTokenResponse
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return kiroOIDCTokenResponse{}, fmt.Errorf("decode kiro token response: %w", err)
	}
	return result, nil
}

func kiroApplyOIDCHeaders(req *http.Request, idcRegion string) {
	if req == nil {
		return
	}
	req.Header.Set("Host", fmt.Sprintf("oidc.%s.amazonaws.com", idcRegion))
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("x-amz-user-agent", kiroOIDCUserAgent)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "*")
	req.Header.Set("sec-fetch-mode", "cors")
	req.Header.Set("User-Agent", "node")
	req.Header.Set("Accept-Encoding", "br, gzip, deflate")
}

func kiroGenerateCodeVerifier() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func kiroCodeChallenge(codeVerifier string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(codeVerifier)))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func kiroVisitorID() string {
	randomPart, err := misc.GenerateRandomState()
	if err != nil {
		randomPart = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	if len(randomPart) > 8 {
		randomPart = randomPart[:8]
	}
	return fmt.Sprintf("%d-%s", time.Now().Unix(), randomPart)
}

func kiroMachineIDFromRefreshToken(refreshToken string) string {
	sum := sha256.Sum256([]byte("KotlinNativeAPI/" + strings.TrimSpace(refreshToken)))
	return hex.EncodeToString(sum[:])
}

func kiroExtractJWTString(token string, keys ...string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		payload, err = base64.URLEncoding.DecodeString(parts[1])
		if err != nil {
			return ""
		}
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return ""
	}
	for _, key := range keys {
		if raw, ok := claims[key]; ok {
			if value, ok := raw.(string); ok && strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value)
			}
		}
	}
	return ""
}

func kiroCredentialFileName(email, provider string) string {
	email = kiroSanitizeFileSegment(email)
	if email != "" {
		return fmt.Sprintf("kiro-%s.json", email)
	}
	provider = kiroSanitizeFileSegment(provider)
	if provider == "" {
		provider = "oauth"
	}
	return fmt.Sprintf("kiro-%s-%s.json", provider, time.Now().UTC().Format("20060102-150405"))
}

func kiroSanitizeFileSegment(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	var builder strings.Builder
	builder.Grow(len(value))
	lastDash := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
			lastDash = false
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
			lastDash = false
		case r == '@':
			builder.WriteString("-at-")
			lastDash = false
		case r == '.' || r == '_' || r == '-':
			if !lastDash && builder.Len() > 0 {
				builder.WriteByte('-')
				lastDash = true
			}
		default:
			if !lastDash && builder.Len() > 0 {
				builder.WriteByte('-')
				lastDash = true
			}
		}
	}
	return strings.Trim(builder.String(), "-")
}

func kiroCanonicalKiroProviderName(provider string) string {
	normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(provider), "_", ""), "-", ""))
	switch normalized {
	case "builderid":
		return "BuilderId"
	case "internal", "awsidc":
		return "Internal"
	case "github":
		return "Github"
	case "google":
		return "Google"
	default:
		return ""
	}
}

func kiroProviderUsesIDC(provider string) bool {
	switch kiroCanonicalKiroProviderName(provider) {
	case "BuilderId", "Internal":
		return true
	default:
		return false
	}
}

func kiroDefaultStartURLForProvider(provider string) string {
	switch kiroCanonicalKiroProviderName(provider) {
	case "Internal":
		return kiroOAuthInternalStartURL
	default:
		return kiroOAuthBuilderIDStartURL
	}
}

func kiroFixedProfileARN(provider string) string {
	switch kiroCanonicalKiroProviderName(provider) {
	case "BuilderId":
		return "arn:aws:codewhisperer:us-east-1:638616132270:profile/AAAACCCCXXXX"
	case "Github", "Google":
		return "arn:aws:codewhisperer:us-east-1:699475941385:profile/EHGA3GRVQMUK"
	default:
		return ""
	}
}

func kiroPickCallbackPort() (int, error) {
	for _, port := range kiroOAuthCallbackPorts {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue
		}
		if errClose := ln.Close(); errClose != nil {
			log.WithError(errClose).Warn("failed to close provisional kiro callback listener")
		}
		return port, nil
	}
	return 0, fmt.Errorf("no available kiro callback port")
}

func firstNonEmptyTrim(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
