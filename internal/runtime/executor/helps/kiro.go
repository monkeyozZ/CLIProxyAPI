package helps

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
)

const (
	kiroProviderKey               = "kiro"
	kiroDefaultRegion             = "us-east-1"
	kiroDefaultVersion            = "0.11.130"
	kiroDefaultAWSSDKJSVersion    = "1.0.34"
	kiroDefaultNodeVersion        = "22.22.0"
	kiroDefaultUsageSDKVersion    = "1.0.0"
	kiroRefreshLead               = 5 * time.Minute
	kiroOriginAIEditor            = "AI_EDITOR"
	kiroResourceTypeAgentic       = "AGENTIC_REQUEST"
	kiroUsageLimitsAmzUserAgent   = "aws-sdk-js/1.0.0"
	kiroIDCAmzUserAgent           = "aws-sdk-js/3.738.0 ua/2.1 os/other lang/js md/browser#unknown_unknown api/sso-oidc#3.738.0 m/E KiroIDE"
	kiroAgentModeVibe             = "vibe"
	kiroAgentModeIntentClassifier = "intent-classification"
	kiroBuilderIDStartURL         = "https://view.awsapps.com/start"
	kiroInternalSSOStartURL       = "https://amzn.awsapps.com/start"
	kiroSocialSignInProfileARN    = "arn:aws:codewhisperer:us-east-1:699475941385:profile/EHGA3GRVQMUK"
	kiroBuilderIDProfileARN       = "arn:aws:codewhisperer:us-east-1:638616132270:profile/AAAACCCCXXXX"
	kiroProfileQueryMaxResults    = 50
)

type KiroCredentials struct {
	AccessToken       string
	RefreshToken      string
	ProfileARN        string
	ExpiresAt         string
	AuthMethod        string
	Provider          string
	ClientID          string
	ClientSecret      string
	Region            string
	APIRegion         string
	MachineID         string
	Email             string
	SubscriptionTitle string
	ProxyURL          string
	ProxyUsername     string
	ProxyPassword     string
	Disabled          bool
}

type KiroBalance struct {
	SubscriptionTitle string
	CurrentUsage      float64
	UsageLimit        float64
	Remaining         float64
	UsagePercentage   float64
	NextResetAt       *float64
}

type KiroHTTPStatusError struct {
	code int
	msg  string
}

func (e KiroHTTPStatusError) Error() string {
	if strings.TrimSpace(e.msg) != "" {
		return e.msg
	}
	return fmt.Sprintf("status %d", e.code)
}

func (e KiroHTTPStatusError) StatusCode() int {
	return e.code
}

type kiroSocialRefreshResponse struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ProfileARN   string `json:"profileArn"`
	ExpiresIn    int64  `json:"expiresIn"`
}

type kiroIDCRefreshResponse struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ExpiresIn    int64  `json:"expiresIn"`
}

type kiroUsageLimitsResponse struct {
	NextDateReset    *float64              `json:"nextDateReset"`
	SubscriptionInfo *kiroSubscriptionInfo `json:"subscriptionInfo"`
	Breakdowns       []kiroUsageBreakdown  `json:"usageBreakdownList"`
}

type kiroSubscriptionInfo struct {
	SubscriptionTitle string `json:"subscriptionTitle"`
}

type kiroUsageBreakdown struct {
	CurrentUsageWithPrecision float64            `json:"currentUsageWithPrecision"`
	UsageLimitWithPrecision   float64            `json:"usageLimitWithPrecision"`
	Bonuses                   []kiroUsageBonus   `json:"bonuses"`
	FreeTrialInfo             *kiroFreeTrialInfo `json:"freeTrialInfo"`
}

type kiroUsageBonus struct {
	CurrentUsage float64 `json:"currentUsage"`
	UsageLimit   float64 `json:"usageLimit"`
	Status       string  `json:"status"`
}

type kiroFreeTrialInfo struct {
	CurrentUsageWithPrecision float64 `json:"currentUsageWithPrecision"`
	UsageLimitWithPrecision   float64 `json:"usageLimitWithPrecision"`
	FreeTrialStatus           string  `json:"freeTrialStatus"`
}

type kiroAvailableModelsResponse struct {
	Models []kiroAvailableModel `json:"models"`
}

type kiroAvailableModel struct {
	ModelID             string                `json:"modelId"`
	ModelName           string                `json:"modelName"`
	Description         string                `json:"description"`
	SupportedInputTypes []string              `json:"supportedInputTypes"`
	RateMultiplier      *float64              `json:"rateMultiplier"`
	TokenLimits         *kiroModelTokenLimits `json:"tokenLimits"`
}

type kiroModelTokenLimits struct {
	MaxInputTokens  *int64 `json:"maxInputTokens"`
	MaxOutputTokens *int64 `json:"maxOutputTokens"`
}

type kiroAvailableProfilesRequest struct {
	MaxResults int    `json:"maxResults"`
	NextToken  string `json:"nextToken,omitempty"`
}

type kiroAvailableProfilesResponse struct {
	Profiles  []kiroProfileInfo `json:"profiles"`
	NextToken string            `json:"nextToken"`
}

type kiroProfileInfo struct {
	ARN  string `json:"arn"`
	Name string `json:"name"`
}

type kiroClientSecretPayload struct {
	Serialized string `json:"serialized"`
}

type kiroClientSecretSerialized struct {
	InitiateLoginURI string `json:"initiateLoginUri"`
}

var kiroDirectModels = map[string]string{
	"claude-sonnet-4.6": "claude-sonnet-4.6",
	"claude-sonnet-4.5": "claude-sonnet-4.5",
	"claude-sonnet-4":   "claude-sonnet-4",
	"claude-opus-4.6":   "claude-opus-4.6",
	"claude-opus-4.5":   "claude-opus-4.5",
	"claude-haiku-4.5":  "claude-haiku-4.5",
	"deepseek-3.2":      "deepseek-3.2",
	"minimax-m2.5":      "minimax-m2.5",
	"minimax-m2.1":      "minimax-m2.1",
	"glm-5":             "glm-5",
	"qwen3-coder-next":  "qwen3-coder-next",
}

func KiroCredentialsFromAuth(auth *cliproxyauth.Auth) KiroCredentials {
	if auth == nil {
		return KiroCredentials{}
	}
	meta := auth.Metadata
	creds := KiroCredentials{
		AccessToken:       metaString(meta, "access_token", "accessToken"),
		RefreshToken:      metaString(meta, "refresh_token", "refreshToken"),
		ProfileARN:        metaString(meta, "profile_arn", "profileArn"),
		ExpiresAt:         metaString(meta, "expires_at", "expiresAt", "expired", "expire"),
		AuthMethod:        canonicalizeKiroAuthMethod(metaString(meta, "auth_method", "authMethod")),
		Provider:          metaString(meta, "provider"),
		ClientID:          metaString(meta, "client_id", "clientId"),
		ClientSecret:      metaString(meta, "client_secret", "clientSecret"),
		Region:            metaString(meta, "region", "authRegion"),
		APIRegion:         metaString(meta, "api_region", "apiRegion"),
		MachineID:         metaString(meta, "machine_id", "machineId"),
		Email:             metaString(meta, "email"),
		SubscriptionTitle: metaString(meta, "subscription_title", "subscriptionTitle"),
		ProxyURL:          metaString(meta, "proxy_url", "proxyUrl"),
		ProxyUsername:     metaString(meta, "proxy_username", "proxyUsername"),
		ProxyPassword:     metaString(meta, "proxy_password", "proxyPassword"),
		Disabled:          metaBool(meta, "disabled"),
	}
	if creds.ProxyURL == "" {
		creds.ProxyURL = strings.TrimSpace(auth.ProxyURL)
	}
	return creds
}

func ApplyKiroCredentialsToAuth(auth *cliproxyauth.Auth, creds KiroCredentials) {
	if auth == nil {
		return
	}
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	meta := auth.Metadata
	auth.Provider = kiroProviderKey
	auth.ProxyURL = strings.TrimSpace(creds.ProxyURL)
	auth.Disabled = creds.Disabled
	if auth.Disabled {
		auth.Status = cliproxyauth.StatusDisabled
	} else if auth.Status == "" || auth.Status == cliproxyauth.StatusDisabled {
		auth.Status = cliproxyauth.StatusActive
	}
	if strings.TrimSpace(creds.Email) != "" {
		auth.Label = strings.TrimSpace(creds.Email)
	} else if strings.TrimSpace(auth.Label) == "" {
		auth.Label = kiroProviderKey
	}
	setOrDeleteMetaString(meta, "type", kiroProviderKey)
	setOrDeleteMetaString(meta, "access_token", creds.AccessToken)
	setOrDeleteMetaString(meta, "refresh_token", creds.RefreshToken)
	setOrDeleteMetaString(meta, "profile_arn", creds.ProfileARN)
	setOrDeleteMetaString(meta, "expires_at", creds.ExpiresAt)
	setOrDeleteMetaString(meta, "auth_method", canonicalizeKiroAuthMethod(creds.AuthMethod))
	setOrDeleteMetaString(meta, "provider", creds.Provider)
	setOrDeleteMetaString(meta, "client_id", creds.ClientID)
	setOrDeleteMetaString(meta, "client_secret", creds.ClientSecret)
	setOrDeleteMetaString(meta, "region", creds.Region)
	setOrDeleteMetaString(meta, "api_region", creds.APIRegion)
	setOrDeleteMetaString(meta, "machine_id", creds.MachineID)
	setOrDeleteMetaString(meta, "email", creds.Email)
	setOrDeleteMetaString(meta, "subscription_title", creds.SubscriptionTitle)
	setOrDeleteMetaString(meta, "proxy_url", creds.ProxyURL)
	setOrDeleteMetaString(meta, "proxy_username", creds.ProxyUsername)
	setOrDeleteMetaString(meta, "proxy_password", creds.ProxyPassword)
	meta["disabled"] = creds.Disabled
	auth.UpdatedAt = time.Now().UTC()
	cliproxyauth.ApplyCustomHeadersFromMetadata(auth)
}

func EnsureKiroAccessToken(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth) (string, *cliproxyauth.Auth, error) {
	if auth == nil {
		return "", nil, KiroHTTPStatusError{code: http.StatusUnauthorized, msg: "missing kiro auth"}
	}
	creds := KiroCredentialsFromAuth(auth)
	if strings.TrimSpace(creds.AccessToken) != "" {
		if expiry, ok := parseKiroExpiry(creds.ExpiresAt); !ok || expiry.After(time.Now().Add(kiroRefreshLead)) {
			return strings.TrimSpace(creds.AccessToken), nil, nil
		}
	}
	updated, err := RefreshKiroAuth(ctx, cfg, auth.Clone())
	if err != nil {
		return "", nil, err
	}
	token := strings.TrimSpace(KiroCredentialsFromAuth(updated).AccessToken)
	if token == "" {
		return "", nil, KiroHTTPStatusError{code: http.StatusUnauthorized, msg: "kiro refresh returned empty access token"}
	}
	return token, updated, nil
}

func RefreshKiroAuth(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	if auth == nil {
		return nil, KiroHTTPStatusError{code: http.StatusUnauthorized, msg: "missing kiro auth"}
	}
	creds := KiroCredentialsFromAuth(auth)
	refreshToken := strings.TrimSpace(creds.RefreshToken)
	if refreshToken == "" {
		return auth, KiroHTTPStatusError{code: http.StatusUnauthorized, msg: "missing kiro refresh token"}
	}
	authMethod := canonicalizeKiroAuthMethod(creds.AuthMethod)
	if authMethod == "" {
		if strings.TrimSpace(creds.ClientID) != "" && strings.TrimSpace(creds.ClientSecret) != "" {
			authMethod = "idc"
		} else {
			authMethod = "social"
		}
	}
	var (
		updatedCreds KiroCredentials
		err          error
	)
	switch authMethod {
	case "idc":
		updatedCreds, err = refreshKiroIDCToken(ctx, cfg, auth, creds)
	default:
		updatedCreds, err = refreshKiroSocialToken(ctx, cfg, auth, creds)
	}
	if err != nil {
		return auth, err
	}
	if strings.TrimSpace(updatedCreds.Email) == "" {
		updatedCreds.Email = creds.Email
	}
	if strings.TrimSpace(updatedCreds.ProfileARN) == "" {
		updatedCreds.ProfileARN = creds.ProfileARN
	}
	if strings.TrimSpace(updatedCreds.ProxyURL) == "" {
		updatedCreds.ProxyURL = creds.ProxyURL
	}
	if strings.TrimSpace(updatedCreds.ProxyUsername) == "" {
		updatedCreds.ProxyUsername = creds.ProxyUsername
	}
	if strings.TrimSpace(updatedCreds.ProxyPassword) == "" {
		updatedCreds.ProxyPassword = creds.ProxyPassword
	}
	if strings.TrimSpace(updatedCreds.Region) == "" {
		updatedCreds.Region = creds.Region
	}
	if strings.TrimSpace(updatedCreds.APIRegion) == "" {
		updatedCreds.APIRegion = creds.APIRegion
	}
	if strings.TrimSpace(updatedCreds.MachineID) == "" {
		updatedCreds.MachineID = creds.MachineID
	}
	updatedCreds.Disabled = creds.Disabled
	ApplyKiroCredentialsToAuth(auth, updatedCreds)
	markKiroAuthRefreshed(auth)
	return auth, nil
}

func FetchKiroBalance(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth) (KiroBalance, *cliproxyauth.Auth, error) {
	token, updatedAuth, err := EnsureKiroAccessToken(ctx, cfg, auth)
	if err != nil {
		return KiroBalance{}, nil, err
	}
	targetAuth := auth
	if updatedAuth != nil {
		targetAuth = updatedAuth
	}
	creds := KiroCredentialsFromAuth(targetAuth)
	host := fmt.Sprintf("q.%s.amazonaws.com", creds.effectiveAPIRegion())
	requestURL := fmt.Sprintf("https://%s/getUsageLimits?origin=%s&resourceType=%s", host, kiroOriginAIEditor, kiroResourceTypeAgentic)
	if strings.TrimSpace(creds.ProfileARN) != "" {
		requestURL += "&profileArn=" + url.QueryEscape(strings.TrimSpace(creds.ProfileARN))
	}
	req, errReq := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if errReq != nil {
		return KiroBalance{}, nil, errReq
	}
	applyKiroUsageHeaders(req, creds, host, token)
	resp, errDo := NewKiroHTTPClient(ctx, cfg, targetAuth, creds, 60*time.Second).Do(req)
	if errDo != nil {
		return KiroBalance{}, nil, errDo
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Errorf("kiro helpers: close response body error: %v", errClose)
		}
	}()
	body, errRead := io.ReadAll(resp.Body)
	if errRead != nil {
		return KiroBalance{}, nil, errRead
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return KiroBalance{}, nil, KiroHTTPStatusError{code: resp.StatusCode, msg: strings.TrimSpace(string(body))}
	}
	var payload kiroUsageLimitsResponse
	if errUnmarshal := json.Unmarshal(body, &payload); errUnmarshal != nil {
		return KiroBalance{}, nil, fmt.Errorf("kiro usage limits decode failed: %w", errUnmarshal)
	}
	current := payload.currentUsage()
	limit := payload.usageLimit()
	remaining := limit - current
	if remaining < 0 {
		remaining = 0
	}
	usagePercent := 0.0
	if limit > 0 {
		usagePercent = (current / limit) * 100
		if usagePercent > 100 {
			usagePercent = 100
		}
	}
	balance := KiroBalance{
		SubscriptionTitle: strings.TrimSpace(payload.subscriptionTitle()),
		CurrentUsage:      current,
		UsageLimit:        limit,
		Remaining:         remaining,
		UsagePercentage:   usagePercent,
		NextResetAt:       payload.NextDateReset,
	}
	if targetAuth != nil {
		updatedCreds := creds
		updatedCreds.SubscriptionTitle = balance.SubscriptionTitle
		ApplyKiroCredentialsToAuth(targetAuth, updatedCreds)
	}
	return balance, targetAuth, nil
}

func FetchKiroAvailableModels(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth) ([]*registry.ModelInfo, *cliproxyauth.Auth, error) {
	token, updatedAuth, err := EnsureKiroAccessToken(ctx, cfg, auth)
	if err != nil {
		return nil, nil, err
	}
	targetAuth := auth
	if updatedAuth != nil {
		targetAuth = updatedAuth
	}
	creds := KiroCredentialsFromAuth(targetAuth)
	resolvedCreds, err := resolveKiroProfileContext(ctx, cfg, targetAuth, creds, token)
	if err != nil {
		return nil, targetAuth, err
	}
	if targetAuth != nil {
		ApplyKiroCredentialsToAuth(targetAuth, resolvedCreds)
	}
	payload, err := listKiroAvailableModels(ctx, cfg, targetAuth, resolvedCreds, token)
	if err != nil {
		return nil, targetAuth, err
	}
	return buildKiroRegistryModels(payload.Models), targetAuth, nil
}

func KiroMapModel(model string) (string, bool) {
	model = strings.ToLower(strings.TrimSpace(model))
	if strings.HasSuffix(model, "-agentic") {
		model = strings.TrimSuffix(model, "-agentic")
	}
	if direct, ok := kiroDirectModels[model]; ok {
		return direct, true
	}
	switch {
	case strings.Contains(model, "sonnet"):
		switch {
		case strings.Contains(model, "4-6") || strings.Contains(model, "4.6"):
			return "claude-sonnet-4.6", true
		case strings.Contains(model, "4-5") || strings.Contains(model, "4.5") || strings.Contains(model, "3-5") || strings.Contains(model, "3.5"):
			return "claude-sonnet-4.5", true
		default:
			return "claude-sonnet-4", true
		}
	case strings.Contains(model, "opus"):
		if strings.Contains(model, "4-5") || strings.Contains(model, "4.5") {
			return "claude-opus-4.5", true
		}
		return "claude-opus-4.6", true
	case strings.Contains(model, "haiku"):
		return "claude-haiku-4.5", true
	default:
		return "", false
	}
}

func KiroCatalogModelSupported(model string) bool {
	model = strings.ToLower(strings.TrimSpace(model))
	_, ok := kiroDirectModels[model]
	return ok
}

func KiroModelType(model string) string {
	model = strings.ToLower(strings.TrimSpace(model))
	if strings.HasPrefix(model, "claude-") {
		return "claude"
	}
	return "openai"
}

func KiroAgentMode(requestedModel string) string {
	if strings.EqualFold(strings.TrimSpace(requestedModel), "simple-task") {
		return kiroAgentModeIntentClassifier
	}
	return kiroAgentModeVibe
}

func KiroSystemVersion() string {
	switch runtime.GOOS {
	case "darwin":
		return "darwin#23.6.0"
	case "windows":
		return "win32#10.0.22631"
	default:
		return "linux#6.8.0"
	}
}

func ApplyKiroGenerateHeaders(req *http.Request, creds KiroCredentials, token, host, agentMode string) error {
	if req == nil {
		return nil
	}
	machineID, ok := generateKiroMachineID(creds)
	if !ok {
		return fmt.Errorf("missing kiro machine_id and refresh_token")
	}
	sdkVersion := kiroDefaultAWSSDKJSVersion
	kiroVersion := kiroDefaultVersion
	systemVersion := KiroSystemVersion()
	nodeVersion := kiroDefaultNodeVersion
	xAmzUserAgent := fmt.Sprintf("aws-sdk-js/%s KiroIDE-%s-%s", sdkVersion, kiroVersion, machineID)
	userAgent := fmt.Sprintf("aws-sdk-js/%s ua/2.1 os/%s lang/js md/nodejs#%s api/codewhispererstreaming#%s m/E KiroIDE-%s-%s", sdkVersion, systemVersion, nodeVersion, sdkVersion, kiroVersion, machineID)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-amzn-codewhisperer-optout", "true")
	req.Header.Set("x-amzn-kiro-agent-mode", agentMode)
	req.Header.Set("x-amz-user-agent", xAmzUserAgent)
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Host", host)
	req.Header.Set("amz-sdk-invocation-id", uuid.NewString())
	req.Header.Set("amz-sdk-request", "attempt=1; max=3")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Connection", "close")
	return nil
}

func metaString(meta map[string]any, keys ...string) string {
	for _, key := range keys {
		if meta == nil {
			continue
		}
		raw, ok := meta[key]
		if !ok {
			continue
		}
		switch v := raw.(type) {
		case string:
			if trimmed := strings.TrimSpace(v); trimmed != "" {
				return trimmed
			}
		case json.Number:
			if trimmed := strings.TrimSpace(v.String()); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func metaBool(meta map[string]any, key string) bool {
	if meta == nil {
		return false
	}
	raw, ok := meta[key]
	if !ok {
		return false
	}
	switch v := raw.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "true")
	default:
		return false
	}
}

func setOrDeleteMetaString(meta map[string]any, key, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		delete(meta, key)
		return
	}
	meta[key] = value
}

func markKiroAuthRefreshed(auth *cliproxyauth.Auth) {
	if auth == nil {
		return
	}
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	now := time.Now().UTC()
	auth.Metadata["last_refresh"] = now.Format(time.RFC3339)
	auth.LastRefreshedAt = now
	auth.UpdatedAt = now
}

func canonicalizeKiroAuthMethod(v string) string {
	switch {
	case strings.EqualFold(strings.TrimSpace(v), "builder-id"), strings.EqualFold(strings.TrimSpace(v), "iam"), strings.EqualFold(strings.TrimSpace(v), "idc"):
		return "idc"
	case strings.TrimSpace(v) == "":
		return ""
	default:
		return strings.ToLower(strings.TrimSpace(v))
	}
}

func parseKiroExpiry(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	layouts := []string{time.RFC3339, time.RFC3339Nano, "2006-01-02 15:04:05"}
	for _, layout := range layouts {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts.UTC(), true
		}
	}
	return time.Time{}, false
}

func resolveKiroProfileContext(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, creds KiroCredentials, token string) (KiroCredentials, error) {
	resolved := creds
	if provider := inferKiroProvider(creds); provider != "" {
		resolved.Provider = provider
		if strings.TrimSpace(resolved.ProfileARN) == "" {
			if profileARN := fixedKiroProfileARN(provider); profileARN != "" {
				resolved.ProfileARN = profileARN
			}
		}
	}
	if strings.TrimSpace(resolved.ProfileARN) != "" {
		return resolved, nil
	}
	profiles, err := listKiroAvailableProfiles(ctx, cfg, auth, resolved, token)
	if err != nil {
		return resolved, err
	}
	for _, profile := range profiles.Profiles {
		if arn := strings.TrimSpace(profile.ARN); arn != "" {
			resolved.ProfileARN = arn
			break
		}
	}
	if strings.TrimSpace(resolved.ProfileARN) == "" {
		return resolved, fmt.Errorf("kiro available profiles returned no profile ARN")
	}
	return resolved, nil
}

func listKiroAvailableProfiles(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, creds KiroCredentials, token string) (kiroAvailableProfilesResponse, error) {
	host := fmt.Sprintf("q.%s.amazonaws.com", creds.effectiveAuthRegion())
	requestURL := fmt.Sprintf("https://%s/ListAvailableProfiles", host)
	client := NewKiroHTTPClient(ctx, cfg, auth, creds, 60*time.Second)
	var profiles []kiroProfileInfo
	var nextToken string
	for {
		payload := kiroAvailableProfilesRequest{
			MaxResults: kiroProfileQueryMaxResults,
			NextToken:  nextToken,
		}
		body, errMarshal := json.Marshal(payload)
		if errMarshal != nil {
			return kiroAvailableProfilesResponse{}, errMarshal
		}
		req, errReq := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(body))
		if errReq != nil {
			return kiroAvailableProfilesResponse{}, errReq
		}
		if errHeaders := applyKiroCatalogHeaders(req, creds, host, token); errHeaders != nil {
			return kiroAvailableProfilesResponse{}, errHeaders
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		resp, errDo := client.Do(req)
		if errDo != nil {
			return kiroAvailableProfilesResponse{}, errDo
		}
		page, errDecode := decodeKiroCatalogResponse[kiroAvailableProfilesResponse](resp, "kiro available profiles")
		if errDecode != nil {
			return kiroAvailableProfilesResponse{}, errDecode
		}
		profiles = append(profiles, page.Profiles...)
		nextToken = strings.TrimSpace(page.NextToken)
		if nextToken == "" {
			return kiroAvailableProfilesResponse{Profiles: profiles}, nil
		}
	}
}

func listKiroAvailableModels(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, creds KiroCredentials, token string) (kiroAvailableModelsResponse, error) {
	profileARN := strings.TrimSpace(creds.ProfileARN)
	if profileARN == "" {
		return kiroAvailableModelsResponse{}, fmt.Errorf("missing kiro profile ARN")
	}
	host := fmt.Sprintf("q.%s.amazonaws.com", creds.effectiveAPIRegion())
	requestURL := fmt.Sprintf(
		"https://%s/ListAvailableModels?origin=%s&maxResults=%d&profileArn=%s",
		host,
		kiroOriginAIEditor,
		kiroProfileQueryMaxResults,
		url.QueryEscape(profileARN),
	)
	req, errReq := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if errReq != nil {
		return kiroAvailableModelsResponse{}, errReq
	}
	if errHeaders := applyKiroCatalogHeaders(req, creds, host, token); errHeaders != nil {
		return kiroAvailableModelsResponse{}, errHeaders
	}
	req.Header.Set("Accept", "application/json")
	resp, errDo := NewKiroHTTPClient(ctx, cfg, auth, creds, 60*time.Second).Do(req)
	if errDo != nil {
		return kiroAvailableModelsResponse{}, errDo
	}
	return decodeKiroCatalogResponse[kiroAvailableModelsResponse](resp, "kiro available models")
}

func refreshKiroSocialToken(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, creds KiroCredentials) (KiroCredentials, error) {
	region := creds.effectiveAuthRegion()
	refreshURL := fmt.Sprintf("https://prod.%s.auth.desktop.kiro.dev/refreshToken", region)
	refreshHost := fmt.Sprintf("prod.%s.auth.desktop.kiro.dev", region)
	body, _ := json.Marshal(map[string]string{"refreshToken": creds.RefreshToken})
	req, errReq := http.NewRequestWithContext(ctx, http.MethodPost, refreshURL, bytes.NewReader(body))
	if errReq != nil {
		return KiroCredentials{}, errReq
	}
	machineID, ok := generateKiroMachineID(creds)
	if !ok {
		return KiroCredentials{}, fmt.Errorf("missing kiro machine_id and refresh_token")
	}
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", fmt.Sprintf("KiroIDE-%s-%s", kiroDefaultVersion, machineID))
	req.Header.Set("Accept-Encoding", "gzip, compress, deflate, br")
	req.Header.Set("Host", refreshHost)
	req.Header.Set("Connection", "close")
	resp, errDo := NewKiroHTTPClient(ctx, cfg, auth, creds, 60*time.Second).Do(req)
	if errDo != nil {
		return KiroCredentials{}, errDo
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Errorf("kiro helpers: close response body error: %v", errClose)
		}
	}()
	bodyBytes, errRead := io.ReadAll(resp.Body)
	if errRead != nil {
		return KiroCredentials{}, errRead
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return KiroCredentials{}, KiroHTTPStatusError{code: resp.StatusCode, msg: strings.TrimSpace(string(bodyBytes))}
	}
	var payload kiroSocialRefreshResponse
	if errUnmarshal := json.Unmarshal(bodyBytes, &payload); errUnmarshal != nil {
		return KiroCredentials{}, fmt.Errorf("kiro social refresh decode failed: %w", errUnmarshal)
	}
	next := creds
	next.AccessToken = strings.TrimSpace(payload.AccessToken)
	if strings.TrimSpace(payload.RefreshToken) != "" {
		next.RefreshToken = strings.TrimSpace(payload.RefreshToken)
	}
	if strings.TrimSpace(payload.ProfileARN) != "" {
		next.ProfileARN = strings.TrimSpace(payload.ProfileARN)
	}
	if payload.ExpiresIn > 0 {
		next.ExpiresAt = time.Now().UTC().Add(time.Duration(payload.ExpiresIn) * time.Second).Format(time.RFC3339)
	}
	next.AuthMethod = "social"
	return next, nil
}

func refreshKiroIDCToken(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, creds KiroCredentials) (KiroCredentials, error) {
	if strings.TrimSpace(creds.ClientID) == "" || strings.TrimSpace(creds.ClientSecret) == "" {
		return KiroCredentials{}, fmt.Errorf("kiro idc refresh requires client_id and client_secret")
	}
	region := creds.effectiveAuthRegion()
	refreshURL := fmt.Sprintf("https://oidc.%s.amazonaws.com/token", region)
	body, _ := json.Marshal(map[string]string{
		"clientId":     creds.ClientID,
		"clientSecret": creds.ClientSecret,
		"refreshToken": creds.RefreshToken,
		"grantType":    "refresh_token",
	})
	req, errReq := http.NewRequestWithContext(ctx, http.MethodPost, refreshURL, bytes.NewReader(body))
	if errReq != nil {
		return KiroCredentials{}, errReq
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Host", fmt.Sprintf("oidc.%s.amazonaws.com", region))
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("x-amz-user-agent", kiroIDCAmzUserAgent)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "*")
	req.Header.Set("sec-fetch-mode", "cors")
	req.Header.Set("User-Agent", "node")
	req.Header.Set("Accept-Encoding", "br, gzip, deflate")
	resp, errDo := NewKiroHTTPClient(ctx, cfg, auth, creds, 60*time.Second).Do(req)
	if errDo != nil {
		return KiroCredentials{}, errDo
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Errorf("kiro helpers: close response body error: %v", errClose)
		}
	}()
	bodyBytes, errRead := io.ReadAll(resp.Body)
	if errRead != nil {
		return KiroCredentials{}, errRead
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return KiroCredentials{}, KiroHTTPStatusError{code: resp.StatusCode, msg: strings.TrimSpace(string(bodyBytes))}
	}
	var payload kiroIDCRefreshResponse
	if errUnmarshal := json.Unmarshal(bodyBytes, &payload); errUnmarshal != nil {
		return KiroCredentials{}, fmt.Errorf("kiro idc refresh decode failed: %w", errUnmarshal)
	}
	next := creds
	next.AccessToken = strings.TrimSpace(payload.AccessToken)
	if strings.TrimSpace(payload.RefreshToken) != "" {
		next.RefreshToken = strings.TrimSpace(payload.RefreshToken)
	}
	if payload.ExpiresIn > 0 {
		next.ExpiresAt = time.Now().UTC().Add(time.Duration(payload.ExpiresIn) * time.Second).Format(time.RFC3339)
	}
	next.AuthMethod = "idc"
	return next, nil
}

func applyKiroUsageHeaders(req *http.Request, creds KiroCredentials, host, token string) {
	if req == nil {
		return
	}
	machineID, ok := generateKiroMachineID(creds)
	if !ok {
		return
	}
	systemVersion := KiroSystemVersion()
	userAgent := fmt.Sprintf("aws-sdk-js/%s ua/2.1 os/%s lang/js md/nodejs#%s api/codewhispererruntime#%s m/N,E KiroIDE-%s-%s", kiroDefaultUsageSDKVersion, systemVersion, kiroDefaultNodeVersion, kiroDefaultUsageSDKVersion, kiroDefaultVersion, machineID)
	amzUserAgent := fmt.Sprintf("%s KiroIDE-%s-%s", kiroUsageLimitsAmzUserAgent, kiroDefaultVersion, machineID)
	req.Header.Set("x-amz-user-agent", amzUserAgent)
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Host", host)
	req.Header.Set("amz-sdk-invocation-id", uuid.NewString())
	req.Header.Set("amz-sdk-request", "attempt=1; max=1")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Connection", "close")
}

func applyKiroCatalogHeaders(req *http.Request, creds KiroCredentials, host, token string) error {
	if req == nil {
		return nil
	}
	machineID, ok := generateKiroMachineID(creds)
	if !ok {
		return fmt.Errorf("missing kiro machine_id and refresh_token")
	}
	systemVersion := KiroSystemVersion()
	sdkVersion := kiroDefaultAWSSDKJSVersion
	userAgent := fmt.Sprintf("aws-sdk-js/%s ua/2.1 os/%s lang/js md/nodejs#%s api/codewhispererstreaming#%s m/E KiroIDE-%s-%s", sdkVersion, systemVersion, kiroDefaultNodeVersion, sdkVersion, kiroDefaultVersion, machineID)
	amzUserAgent := fmt.Sprintf("aws-sdk-js/%s KiroIDE-%s-%s", sdkVersion, kiroDefaultVersion, machineID)
	req.Header.Set("x-amzn-codewhisperer-optout", "true")
	req.Header.Set("x-amz-user-agent", amzUserAgent)
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Host", host)
	req.Header.Set("amz-sdk-invocation-id", uuid.NewString())
	req.Header.Set("amz-sdk-request", "attempt=1; max=1")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Connection", "close")
	return nil
}

func decodeKiroCatalogResponse[T any](resp *http.Response, label string) (T, error) {
	var zero T
	if resp == nil {
		return zero, fmt.Errorf("%s response is nil", label)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Errorf("kiro helpers: close response body error: %v", errClose)
		}
	}()
	body, errRead := io.ReadAll(resp.Body)
	if errRead != nil {
		return zero, errRead
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return zero, KiroHTTPStatusError{code: resp.StatusCode, msg: strings.TrimSpace(string(body))}
	}
	var payload T
	if errUnmarshal := json.Unmarshal(body, &payload); errUnmarshal != nil {
		return zero, fmt.Errorf("%s decode failed: %w", label, errUnmarshal)
	}
	return payload, nil
}

func NewKiroHTTPClient(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, creds KiroCredentials, timeout time.Duration) *http.Client {
	client := &http.Client{}
	if timeout > 0 {
		client.Timeout = timeout
	}
	proxyURL := effectiveKiroProxyURL(cfg, auth, creds)
	if proxyURL != "" {
		if transport, _, err := proxyutil.BuildHTTPTransport(proxyURL); err == nil && transport != nil {
			client.Transport = transport
			return client
		}
	}
	if ctx != nil {
		if rt, ok := ctx.Value("cliproxy.roundtripper").(http.RoundTripper); ok && rt != nil {
			client.Transport = rt
		}
	}
	return client
}

func effectiveKiroProxyURL(cfg *config.Config, auth *cliproxyauth.Auth, creds KiroCredentials) string {
	proxyURL := strings.TrimSpace(creds.ProxyURL)
	if strings.EqualFold(proxyURL, "direct") {
		return ""
	}
	if proxyURL == "" && auth != nil {
		proxyURL = strings.TrimSpace(auth.ProxyURL)
	}
	if proxyURL == "" && cfg != nil {
		proxyURL = strings.TrimSpace(cfg.ProxyURL)
	}
	if strings.EqualFold(proxyURL, "direct") || proxyURL == "" {
		return ""
	}
	if strings.TrimSpace(creds.ProxyUsername) == "" {
		return proxyURL
	}
	parsed, err := url.Parse(proxyURL)
	if err != nil {
		return proxyURL
	}
	parsed.User = url.UserPassword(strings.TrimSpace(creds.ProxyUsername), strings.TrimSpace(creds.ProxyPassword))
	return parsed.String()
}

func inferKiroProvider(creds KiroCredentials) string {
	if provider := canonicalKiroProviderName(creds.Provider); provider != "" {
		return provider
	}
	if uri := extractKiroClientSecretInitiateLoginURI(creds.ClientSecret); uri != "" {
		switch {
		case strings.HasPrefix(uri, kiroBuilderIDStartURL):
			return "BuilderId"
		case strings.HasPrefix(uri, kiroInternalSSOStartURL):
			return "Internal"
		}
	}
	return ""
}

func canonicalKiroProviderName(provider string) string {
	normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(provider), "_", ""), "-", ""))
	switch normalized {
	case "builderid":
		return "BuilderId"
	case "github":
		return "Github"
	case "google":
		return "Google"
	case "enterprise":
		return "Enterprise"
	case "internal":
		return "Internal"
	default:
		return ""
	}
}

func fixedKiroProfileARN(provider string) string {
	switch canonicalKiroProviderName(provider) {
	case "BuilderId":
		return kiroBuilderIDProfileARN
	case "Github", "Google":
		return kiroSocialSignInProfileARN
	default:
		return ""
	}
}

func extractKiroClientSecretInitiateLoginURI(clientSecret string) string {
	clientSecret = strings.TrimSpace(clientSecret)
	if clientSecret == "" {
		return ""
	}
	parts := strings.Split(clientSecret, ".")
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
	var tokenPayload kiroClientSecretPayload
	if errUnmarshal := json.Unmarshal(payload, &tokenPayload); errUnmarshal != nil {
		return ""
	}
	if strings.TrimSpace(tokenPayload.Serialized) == "" {
		return ""
	}
	var serialized kiroClientSecretSerialized
	if errUnmarshal := json.Unmarshal([]byte(tokenPayload.Serialized), &serialized); errUnmarshal != nil {
		return ""
	}
	return strings.TrimSpace(serialized.InitiateLoginURI)
}

func generateKiroMachineID(creds KiroCredentials) (string, bool) {
	if normalized := normalizeKiroMachineID(creds.MachineID); normalized != "" {
		return normalized, true
	}
	refreshToken := strings.TrimSpace(creds.RefreshToken)
	if refreshToken == "" {
		return "", false
	}
	sum := sha256.Sum256([]byte("KotlinNativeAPI/" + refreshToken))
	return hex.EncodeToString(sum[:]), true
}

func normalizeKiroMachineID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) == 64 && isHexString(value) {
		return strings.ToLower(value)
	}
	withoutDashes := strings.ReplaceAll(value, "-", "")
	if len(withoutDashes) == 32 && isHexString(withoutDashes) {
		return strings.ToLower(withoutDashes + withoutDashes)
	}
	return ""
}

func isHexString(value string) bool {
	for _, ch := range value {
		switch {
		case ch >= '0' && ch <= '9':
		case ch >= 'a' && ch <= 'f':
		case ch >= 'A' && ch <= 'F':
		default:
			return false
		}
	}
	return true
}

func (c KiroCredentials) effectiveAuthRegion() string {
	if strings.TrimSpace(c.Region) != "" {
		return strings.TrimSpace(c.Region)
	}
	return kiroDefaultRegion
}

func (c KiroCredentials) effectiveAPIRegion() string {
	if strings.TrimSpace(c.APIRegion) != "" {
		return strings.TrimSpace(c.APIRegion)
	}
	if strings.TrimSpace(c.Region) != "" {
		return strings.TrimSpace(c.Region)
	}
	return kiroDefaultRegion
}

func buildKiroRegistryModels(models []kiroAvailableModel) []*registry.ModelInfo {
	if len(models) == 0 {
		return nil
	}
	now := time.Now().Unix()
	out := make([]*registry.ModelInfo, 0, len(models))
	seen := make(map[string]struct{}, len(models))
	for _, model := range models {
		modelID := strings.TrimSpace(model.ModelID)
		if modelID == "" {
			continue
		}
		if !KiroCatalogModelSupported(modelID) {
			continue
		}
		if _, exists := seen[modelID]; exists {
			continue
		}
		seen[modelID] = struct{}{}
		item := &registry.ModelInfo{
			ID:          modelID,
			Object:      "model",
			Created:     now,
			OwnedBy:     kiroProviderKey,
			Type:        KiroModelType(modelID),
			DisplayName: strings.TrimSpace(model.ModelName),
			Description: strings.TrimSpace(model.Description),
		}
		if item.DisplayName == "" {
			item.DisplayName = modelID
		}
		if model.TokenLimits != nil {
			item.ContextLength = kiroTokenLimitToInt(model.TokenLimits.MaxInputTokens)
			item.MaxCompletionTokens = kiroTokenLimitToInt(model.TokenLimits.MaxOutputTokens)
		}
		if len(model.SupportedInputTypes) > 0 {
			item.SupportedInputModalities = append([]string(nil), model.SupportedInputTypes...)
		}
		out = append(out, item)
	}
	return out
}

func kiroTokenLimitToInt(value *int64) int {
	if value == nil || *value <= 0 {
		return 0
	}
	maxInt := int(^uint(0) >> 1)
	if *value > int64(maxInt) {
		return maxInt
	}
	return int(*value)
}

func (r kiroUsageLimitsResponse) subscriptionTitle() string {
	if r.SubscriptionInfo == nil {
		return ""
	}
	return strings.TrimSpace(r.SubscriptionInfo.SubscriptionTitle)
}

func (r kiroUsageLimitsResponse) usageLimit() float64 {
	if len(r.Breakdowns) == 0 {
		return 0
	}
	breakdown := r.Breakdowns[0]
	total := breakdown.UsageLimitWithPrecision
	if breakdown.FreeTrialInfo != nil && strings.EqualFold(strings.TrimSpace(breakdown.FreeTrialInfo.FreeTrialStatus), "ACTIVE") {
		total += breakdown.FreeTrialInfo.UsageLimitWithPrecision
	}
	for _, bonus := range breakdown.Bonuses {
		if strings.EqualFold(strings.TrimSpace(bonus.Status), "ACTIVE") {
			total += bonus.UsageLimit
		}
	}
	return total
}

func (r kiroUsageLimitsResponse) currentUsage() float64 {
	if len(r.Breakdowns) == 0 {
		return 0
	}
	breakdown := r.Breakdowns[0]
	total := breakdown.CurrentUsageWithPrecision
	if breakdown.FreeTrialInfo != nil && strings.EqualFold(strings.TrimSpace(breakdown.FreeTrialInfo.FreeTrialStatus), "ACTIVE") {
		total += breakdown.FreeTrialInfo.CurrentUsageWithPrecision
	}
	for _, bonus := range breakdown.Bonuses {
		if strings.EqualFold(strings.TrimSpace(bonus.Status), "ACTIVE") {
			total += bonus.CurrentUsage
		}
	}
	return total
}
