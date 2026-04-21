package management

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type kiroBalanceResponse struct {
	Name              string   `json:"name"`
	SubscriptionTitle string   `json:"subscription_title,omitempty"`
	CurrentUsage      float64  `json:"current_usage"`
	UsageLimit        float64  `json:"usage_limit"`
	Remaining         float64  `json:"remaining"`
	UsagePercentage   float64  `json:"usage_percentage"`
	NextResetAt       *float64 `json:"next_reset_at,omitempty"`
}

func (h *Handler) GetKiroAuthBalance(c *gin.Context) {
	if h == nil || h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}
	name := strings.TrimSpace(c.Query("name"))
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	auth := h.authByNameOrID(name)
	if auth == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "auth file not found"})
		return
	}
	if !isKiroAuth(auth) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "auth file is not kiro"})
		return
	}
	balance, updatedAuth, err := helps.FetchKiroBalance(c.Request.Context(), h.cfg, auth.Clone())
	if updatedAuth != nil {
		updatedAuth.FileName = auth.FileName
		updatedAuth.ID = auth.ID
		updatedAuth.Index = auth.Index
		if _, errUpdate := h.authManager.Update(c.Request.Context(), updatedAuth); errUpdate != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to update kiro auth: %v", errUpdate)})
			return
		}
	}
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	resolvedName := strings.TrimSpace(auth.FileName)
	if resolvedName == "" {
		resolvedName = strings.TrimSpace(auth.ID)
	}
	c.JSON(http.StatusOK, kiroBalanceResponse{
		Name:              resolvedName,
		SubscriptionTitle: balance.SubscriptionTitle,
		CurrentUsage:      balance.CurrentUsage,
		UsageLimit:        balance.UsageLimit,
		Remaining:         balance.Remaining,
		UsagePercentage:   balance.UsagePercentage,
		NextResetAt:       balance.NextResetAt,
	})
}

func (h *Handler) PatchKiroAuthFile(c *gin.Context) {
	if h == nil || h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}
	var req struct {
		Name          string  `json:"name"`
		AccessToken   *string `json:"access_token"`
		RefreshToken  *string `json:"refresh_token"`
		ProfileARN    *string `json:"profile_arn"`
		ExpiresAt     *string `json:"expires_at"`
		AuthMethod    *string `json:"auth_method"`
		Provider      *string `json:"provider"`
		ClientID      *string `json:"client_id"`
		ClientSecret  *string `json:"client_secret"`
		Region        *string `json:"region"`
		APIRegion     *string `json:"api_region"`
		MachineID     *string `json:"machine_id"`
		Email         *string `json:"email"`
		ProxyURL      *string `json:"proxy_url"`
		ProxyUsername *string `json:"proxy_username"`
		ProxyPassword *string `json:"proxy_password"`
		Priority      *int    `json:"priority"`
		Note          *string `json:"note"`
		Disabled      *bool   `json:"disabled"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	auth := h.authByNameOrID(name)
	if auth == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "auth file not found"})
		return
	}
	if !isKiroAuth(auth) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "auth file is not kiro"})
		return
	}
	updated := auth.Clone()
	creds := helps.KiroCredentialsFromAuth(updated)
	changed := false
	updateString := func(field **string, apply func(string)) {
		if *field == nil {
			return
		}
		apply(strings.TrimSpace(**field))
		changed = true
	}

	updateString(&req.AccessToken, func(v string) { creds.AccessToken = v })
	updateString(&req.RefreshToken, func(v string) { creds.RefreshToken = v })
	updateString(&req.ProfileARN, func(v string) { creds.ProfileARN = v })
	updateString(&req.ExpiresAt, func(v string) { creds.ExpiresAt = v })
	updateString(&req.AuthMethod, func(v string) { creds.AuthMethod = v })
	updateString(&req.Provider, func(v string) { creds.Provider = v })
	updateString(&req.ClientID, func(v string) { creds.ClientID = v })
	updateString(&req.ClientSecret, func(v string) { creds.ClientSecret = v })
	updateString(&req.Region, func(v string) { creds.Region = v })
	updateString(&req.APIRegion, func(v string) { creds.APIRegion = v })
	updateString(&req.MachineID, func(v string) { creds.MachineID = v })
	updateString(&req.Email, func(v string) { creds.Email = v })
	updateString(&req.ProxyURL, func(v string) { creds.ProxyURL = v })
	updateString(&req.ProxyUsername, func(v string) { creds.ProxyUsername = v })
	updateString(&req.ProxyPassword, func(v string) { creds.ProxyPassword = v })
	if req.Disabled != nil {
		creds.Disabled = *req.Disabled
		changed = true
	}
	if req.Priority != nil {
		if updated.Metadata == nil {
			updated.Metadata = make(map[string]any)
		}
		if updated.Attributes == nil {
			updated.Attributes = make(map[string]string)
		}
		if *req.Priority == 0 {
			delete(updated.Metadata, "priority")
			delete(updated.Attributes, "priority")
		} else {
			updated.Metadata["priority"] = *req.Priority
			updated.Attributes["priority"] = fmt.Sprintf("%d", *req.Priority)
		}
		changed = true
	}
	if req.Note != nil {
		if updated.Metadata == nil {
			updated.Metadata = make(map[string]any)
		}
		if updated.Attributes == nil {
			updated.Attributes = make(map[string]string)
		}
		note := strings.TrimSpace(*req.Note)
		if note == "" {
			delete(updated.Metadata, "note")
			delete(updated.Attributes, "note")
		} else {
			updated.Metadata["note"] = note
			updated.Attributes["note"] = note
		}
		changed = true
	}
	if !changed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no fields to update"})
		return
	}
	helps.ApplyKiroCredentialsToAuth(updated, creds)
	updated.FileName = auth.FileName
	updated.ID = auth.ID
	updated.Index = auth.Index
	updated.UpdatedAt = time.Now().UTC()
	if _, err := h.authManager.Update(c.Request.Context(), updated); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to update kiro auth: %v", err)})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func isKiroAuth(auth *coreauth.Auth) bool {
	if auth == nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(auth.Provider), "kiro") {
		return true
	}
	if auth.Metadata == nil {
		return false
	}
	if raw, ok := auth.Metadata["type"].(string); ok {
		return strings.EqualFold(strings.TrimSpace(raw), "kiro")
	}
	return false
}

func (h *Handler) authByNameOrID(name string) *coreauth.Auth {
	name = strings.TrimSpace(name)
	if name == "" || h == nil || h.authManager == nil {
		return nil
	}
	if auth, ok := h.authManager.GetByID(name); ok {
		return auth
	}
	for _, auth := range h.authManager.List() {
		if auth == nil {
			continue
		}
		if auth.FileName == name {
			return auth
		}
	}
	return nil
}
