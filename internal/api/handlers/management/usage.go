package management

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/redisqueue"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/usage"
)

type usageQueueRecord []byte

const maxUsageImportBytes int64 = 64 * 1024 * 1024

type usageExportPayload struct {
	Version    int                      `json:"version"`
	ExportedAt time.Time                `json:"exported_at"`
	Usage      usage.StatisticsSnapshot `json:"usage"`
}

type modelPricesRequest struct {
	Prices map[string]usage.ModelPrice `json:"prices"`
}

type modelPricesSyncRequest struct {
	Models []string `json:"models"`
}

func (r usageQueueRecord) MarshalJSON() ([]byte, error) {
	if json.Valid(r) {
		return append([]byte(nil), r...), nil
	}
	return json.Marshal(string(r))
}

// GetUsageStatistics returns the embedded request statistics snapshot.
func (h *Handler) GetUsageStatistics(c *gin.Context) {
	snapshot := usage.GetRequestStatistics().Snapshot()
	c.JSON(http.StatusOK, snapshot)
}

// ExportUsageStatistics returns a usage export compatible with both the legacy
// management panel and CPA-Manager's JSONL importer.
func (h *Handler) ExportUsageStatistics(c *gin.Context) {
	if strings.EqualFold(strings.TrimSpace(c.Query("format")), "jsonl") {
		data, err := usage.GetRequestStatistics().ExportJSONL()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.Header("Content-Type", "application/x-ndjson")
		c.Header("Content-Disposition", `attachment; filename="usage-events.jsonl"`)
		c.Data(http.StatusOK, "application/x-ndjson", data)
		return
	}

	c.JSON(http.StatusOK, usageExportPayload{
		Version:    1,
		ExportedAt: time.Now().UTC(),
		Usage:      usage.GetRequestStatistics().Snapshot(),
	})
}

// ImportUsageStatistics merges a usage export into the embedded store.
func (h *Handler) ImportUsageStatistics(c *gin.Context) {
	body := http.MaxBytesReader(c.Writer, c.Request.Body, maxUsageImportBytes)
	data, err := io.ReadAll(body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		return
	}

	parsed, err := usage.ParseImportPayload(data)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":       err.Error(),
			"format":      parsed.Format,
			"failed":      parsed.Failed,
			"unsupported": parsed.Unsupported,
			"warnings":    parsed.Warnings,
		})
		return
	}

	result := usage.GetRequestStatistics().ImportEvents(parsed)
	snapshot := usage.GetRequestStatistics().Snapshot()
	c.JSON(http.StatusOK, gin.H{
		"format":          result.Format,
		"added":           result.Added,
		"skipped":         result.Skipped,
		"total":           result.Total,
		"failed":          result.Failed,
		"unsupported":     result.Unsupported,
		"warnings":        result.Warnings,
		"total_requests":  snapshot.TotalRequests,
		"failed_requests": snapshot.FailureCount,
	})
}

// GetModelPrices returns persisted model prices used by the monitoring UI.
func (h *Handler) GetModelPrices(c *gin.Context) {
	prices, err := usage.LoadModelPrices(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"prices": prices})
}

// PutModelPrices replaces persisted model prices.
func (h *Handler) PutModelPrices(c *gin.Context) {
	var req modelPricesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	if req.Prices == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "prices are required"})
		return
	}
	prices, err := usage.SaveModelPrices(c.Request.Context(), req.Prices)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"prices": prices})
}

// SyncModelPrices imports model prices from LiteLLM's public price catalog.
func (h *Handler) SyncModelPrices(c *gin.Context) {
	var req modelPricesSyncRequest
	if err := c.ShouldBindJSON(&req); err != nil && !errors.Is(err, io.EOF) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	result, err := usage.SyncModelPrices(c.Request.Context(), req.Models)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"source":   result.Source,
		"imported": result.Imported,
		"skipped":  result.Skipped,
		"prices":   result.Prices,
	})
}

// GetUsageQueue pops queued usage records from the usage queue.
func (h *Handler) GetUsageQueue(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "handler unavailable"})
		return
	}

	count, errCount := parseUsageQueueCount(c.Query("count"))
	if errCount != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errCount.Error()})
		return
	}

	items := redisqueue.PopOldest(count)
	records := make([]usageQueueRecord, 0, len(items))
	for _, item := range items {
		records = append(records, usageQueueRecord(append([]byte(nil), item...)))
	}

	c.JSON(http.StatusOK, records)
}

func parseUsageQueueCount(value string) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 1, nil
	}
	count, errCount := strconv.Atoi(value)
	if errCount != nil || count <= 0 {
		return 0, errors.New("count must be a positive integer")
	}
	return count, nil
}
