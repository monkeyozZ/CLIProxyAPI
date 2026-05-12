// Package usage provides embedded usage statistics for the management API.
package usage

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	internallogging "github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	coreusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
)

const (
	ImportFormatJSONL         = "usage_service_jsonl"
	ImportFormatLegacyExport  = "legacy_usage_export"
	ImportFormatLegacyPayload = "legacy_usage_payload"

	modelPriceSyncSource = "litellm"
	modelPriceSyncURL    = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"
)

var (
	statisticsEnabled atomic.Bool
	endpointPattern   = regexp.MustCompile(`^(GET|POST|PUT|PATCH|DELETE|OPTIONS|HEAD)\s+(\S+)`)
	startedAtMS       = time.Now().UnixMilli()
)

func init() {
	statisticsEnabled.Store(true)
	coreusage.RegisterPlugin(NewLoggerPlugin())
}

// LoggerPlugin collects request statistics for the embedded management panel.
type LoggerPlugin struct {
	stats *RequestStatistics
}

// NewLoggerPlugin constructs a new logger plugin instance.
func NewLoggerPlugin() *LoggerPlugin { return &LoggerPlugin{stats: defaultRequestStatistics} }

// HandleUsage implements coreusage.Plugin.
func (p *LoggerPlugin) HandleUsage(ctx context.Context, record coreusage.Record) {
	if !statisticsEnabled.Load() || p == nil || p.stats == nil {
		return
	}
	p.stats.Record(ctx, record)
}

// SetStatisticsEnabled toggles whether embedded statistics are recorded.
func SetStatisticsEnabled(enabled bool) { statisticsEnabled.Store(enabled) }

// StatisticsEnabled reports the current recording state.
func StatisticsEnabled() bool { return statisticsEnabled.Load() }

// StartedAtMS returns the embedded usage service start timestamp in milliseconds.
func StartedAtMS() int64 { return startedAtMS }

// TokenStats captures the token usage breakdown for a request.
type TokenStats struct {
	InputTokens         int64 `json:"input_tokens"`
	OutputTokens        int64 `json:"output_tokens"`
	ReasoningTokens     int64 `json:"reasoning_tokens"`
	CachedTokens        int64 `json:"cached_tokens"`
	CacheReadTokens     int64 `json:"cache_read_tokens,omitempty"`
	CacheCreationTokens int64 `json:"cache_creation_tokens,omitempty"`
	CacheTokens         int64 `json:"cache_tokens,omitempty"`
	TotalTokens         int64 `json:"total_tokens"`
}

// RequestDetail stores the timestamp, latency, and token usage for a request.
type RequestDetail struct {
	Timestamp            time.Time  `json:"timestamp"`
	LatencyMs            int64      `json:"latency_ms,omitempty"`
	Source               string     `json:"source"`
	AuthIndex            string     `json:"auth_index"`
	AccountSnapshot      string     `json:"account_snapshot,omitempty"`
	AuthLabelSnapshot    string     `json:"auth_label_snapshot,omitempty"`
	AuthFileSnapshot     string     `json:"auth_file_snapshot,omitempty"`
	AuthProviderSnapshot string     `json:"auth_provider_snapshot,omitempty"`
	AuthSnapshotAtMS     int64      `json:"auth_snapshot_at_ms,omitempty"`
	Tokens               TokenStats `json:"tokens"`
	Failed               bool       `json:"failed"`
}

// StatisticsSnapshot represents an immutable view of the aggregated metrics.
type StatisticsSnapshot struct {
	TotalRequests int64 `json:"total_requests"`
	SuccessCount  int64 `json:"success_count"`
	FailureCount  int64 `json:"failure_count"`
	TotalTokens   int64 `json:"total_tokens"`

	APIs map[string]APISnapshot `json:"apis"`

	RequestsByDay  map[string]int64 `json:"requests_by_day"`
	RequestsByHour map[string]int64 `json:"requests_by_hour"`
	TokensByDay    map[string]int64 `json:"tokens_by_day"`
	TokensByHour   map[string]int64 `json:"tokens_by_hour"`
}

// APISnapshot summarises metrics for one endpoint.
type APISnapshot struct {
	TotalRequests int64                    `json:"total_requests"`
	TotalTokens   int64                    `json:"total_tokens"`
	Models        map[string]ModelSnapshot `json:"models"`
}

// ModelSnapshot summarises metrics for one model.
type ModelSnapshot struct {
	TotalRequests int64           `json:"total_requests"`
	TotalTokens   int64           `json:"total_tokens"`
	Details       []RequestDetail `json:"details"`
}

type apiStats struct {
	TotalRequests int64
	TotalTokens   int64
	Models        map[string]*modelStats
}

type modelStats struct {
	TotalRequests int64
	TotalTokens   int64
	Details       []RequestDetail
}

// Event is the persisted usage-service-compatible event format.
type Event struct {
	RequestID            string `json:"request_id,omitempty"`
	EventHash            string `json:"event_hash"`
	TimestampMS          int64  `json:"timestamp_ms"`
	Timestamp            string `json:"timestamp"`
	Provider             string `json:"provider,omitempty"`
	Model                string `json:"model"`
	Endpoint             string `json:"endpoint,omitempty"`
	Method               string `json:"method,omitempty"`
	Path                 string `json:"path,omitempty"`
	AuthType             string `json:"auth_type,omitempty"`
	AuthIndex            string `json:"auth_index,omitempty"`
	Source               string `json:"source,omitempty"`
	SourceHash           string `json:"source_hash,omitempty"`
	APIKeyHash           string `json:"api_key_hash,omitempty"`
	AccountSnapshot      string `json:"account_snapshot,omitempty"`
	AuthLabelSnapshot    string `json:"auth_label_snapshot,omitempty"`
	AuthFileSnapshot     string `json:"auth_file_snapshot,omitempty"`
	AuthProviderSnapshot string `json:"auth_provider_snapshot,omitempty"`
	AuthSnapshotAtMS     int64  `json:"auth_snapshot_at_ms,omitempty"`
	InputTokens          int64  `json:"input_tokens"`
	OutputTokens         int64  `json:"output_tokens"`
	ReasoningTokens      int64  `json:"reasoning_tokens"`
	CachedTokens         int64  `json:"cached_tokens"`
	CacheReadTokens      int64  `json:"cache_read_tokens,omitempty"`
	CacheCreationTokens  int64  `json:"cache_creation_tokens,omitempty"`
	CacheTokens          int64  `json:"cache_tokens"`
	TotalTokens          int64  `json:"total_tokens"`
	LatencyMS            *int64 `json:"latency_ms,omitempty"`
	Failed               bool   `json:"failed"`
	RawJSON              string `json:"raw_json,omitempty"`
	CreatedAtMS          int64  `json:"created_at_ms"`
}

// MergeResult summarises import results.
type MergeResult struct {
	Added       int64    `json:"added"`
	Skipped     int64    `json:"skipped"`
	Total       int      `json:"total"`
	Failed      int      `json:"failed"`
	Unsupported int      `json:"unsupported,omitempty"`
	Format      string   `json:"format,omitempty"`
	Warnings    []string `json:"warnings,omitempty"`
}

// ImportParseResult is returned by ParseImportPayload.
type ImportParseResult struct {
	Format      string
	Events      []Event
	Failed      int
	Unsupported int
	Warnings    []string
}

// ModelPrice stores model pricing in dollars per one million tokens.
type ModelPrice struct {
	Prompt        float64 `json:"prompt"`
	Completion    float64 `json:"completion"`
	Cache         float64 `json:"cache"`
	Source        string  `json:"source,omitempty"`
	SourceModelID string  `json:"sourceModelId,omitempty"`
	RawJSON       string  `json:"rawJson,omitempty"`
	UpdatedAtMS   int64   `json:"updatedAtMs,omitempty"`
	SyncedAtMS    *int64  `json:"syncedAtMs,omitempty"`
}

// ModelPriceSyncResult describes a model price sync.
type ModelPriceSyncResult struct {
	Imported int                   `json:"imported"`
	Skipped  int                   `json:"skipped"`
	Prices   map[string]ModelPrice `json:"prices"`
	Source   string                `json:"source,omitempty"`
}

// CollectorStatus mirrors CPA-Manager usage service collector status fields.
type CollectorStatus struct {
	Collector      string `json:"collector"`
	Upstream       string `json:"upstream"`
	Mode           string `json:"mode"`
	Transport      string `json:"transport"`
	Queue          string `json:"queue"`
	LastConsumedAt int64  `json:"lastConsumedAt"`
	LastInsertedAt int64  `json:"lastInsertedAt"`
	TotalInserted  int64  `json:"totalInserted"`
	TotalSkipped   int64  `json:"totalSkipped"`
	DeadLetters    int64  `json:"deadLetters"`
	LastError      string `json:"lastError,omitempty"`
}

// ServiceStatus mirrors CPA-Manager usage service status fields.
type ServiceStatus struct {
	Service     string          `json:"service"`
	DBPath      string          `json:"dbPath"`
	Events      int64           `json:"events"`
	DeadLetters int64           `json:"deadLetters"`
	Collector   CollectorStatus `json:"collector"`
}

// RequestStatistics maintains aggregated request metrics in memory and on disk.
type RequestStatistics struct {
	mu sync.RWMutex

	totalRequests int64
	successCount  int64
	failureCount  int64
	totalTokens   int64

	apis map[string]*apiStats

	requestsByDay  map[string]int64
	requestsByHour map[int]int64
	tokensByDay    map[string]int64
	tokensByHour   map[int]int64
	eventHashes    map[string]struct{}
	skippedEvents  int64

	storageDir            string
	dbPath                string
	legacyEventsPath      string
	legacyModelPricesPath string
	store                 *sqliteUsageStore
	modelPrices           map[string]ModelPrice
}

var defaultRequestStatistics = NewRequestStatistics()

// GetRequestStatistics returns the shared statistics store.
func GetRequestStatistics() *RequestStatistics { return defaultRequestStatistics }

// GetServiceStatus returns the embedded usage service status.
func GetServiceStatus(ctx context.Context) (ServiceStatus, error) {
	return defaultRequestStatistics.ServiceStatus(ctx)
}

// NewRequestStatistics constructs an empty statistics store.
func NewRequestStatistics() *RequestStatistics {
	return &RequestStatistics{
		apis:           make(map[string]*apiStats),
		requestsByDay:  make(map[string]int64),
		requestsByHour: make(map[int]int64),
		tokensByDay:    make(map[string]int64),
		tokensByHour:   make(map[int]int64),
		eventHashes:    make(map[string]struct{}),
		modelPrices:    make(map[string]ModelPrice),
	}
}

// ConfigureStorage sets the disk location used for usage events and model prices.
func ConfigureStorage(configFilePath string) error {
	return defaultRequestStatistics.ConfigureStorage(configFilePath)
}

// ConfigureStorage sets the disk location used for usage events and model prices.
func (s *RequestStatistics) ConfigureStorage(configFilePath string) error {
	if s == nil {
		return nil
	}
	dir := resolveStorageDir(configFilePath)
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("usage storage mkdir: %w", err)
	}

	dbPath := filepath.Join(dir, "usage.sqlite")
	legacyEventsPath := filepath.Join(dir, "usage-events.jsonl")
	legacyModelPricesPath := filepath.Join(dir, "model-prices.json")
	store, err := openSQLiteUsageStore(dbPath)
	if err != nil {
		return fmt.Errorf("usage sqlite open: %w", err)
	}

	s.mu.Lock()
	if s.dbPath == dbPath && s.store != nil {
		s.mu.Unlock()
		_ = store.Close()
		return nil
	}
	oldStore := s.store
	s.resetAggregatesLocked()
	s.storageDir = dir
	s.dbPath = dbPath
	s.legacyEventsPath = legacyEventsPath
	s.legacyModelPricesPath = legacyModelPricesPath
	s.store = store
	s.mu.Unlock()

	if oldStore != nil {
		_ = oldStore.Close()
	}

	if err := s.migrateLegacyEvents(legacyEventsPath); err != nil {
		return err
	}
	if err := s.migrateLegacyModelPrices(legacyModelPricesPath); err != nil {
		return err
	}
	if err := s.loadEventsFromStore(context.Background()); err != nil {
		return err
	}
	if err := s.loadModelPricesFromStore(context.Background()); err != nil {
		return err
	}
	return nil
}

func (s *RequestStatistics) resetAggregatesLocked() {
	s.totalRequests = 0
	s.successCount = 0
	s.failureCount = 0
	s.totalTokens = 0
	s.apis = make(map[string]*apiStats)
	s.requestsByDay = make(map[string]int64)
	s.requestsByHour = make(map[int]int64)
	s.tokensByDay = make(map[string]int64)
	s.tokensByHour = make(map[int]int64)
	s.eventHashes = make(map[string]struct{})
	s.skippedEvents = 0
	s.modelPrices = make(map[string]ModelPrice)
}

func resolveStorageDir(configFilePath string) string {
	if writable := util.WritablePath(); writable != "" {
		return filepath.Join(writable, "usage")
	}
	configFilePath = strings.TrimSpace(configFilePath)
	if configFilePath != "" {
		return filepath.Join(filepath.Dir(configFilePath), "usage")
	}
	if wd, err := os.Getwd(); err == nil {
		return filepath.Join(wd, "usage")
	}
	return ""
}

// Record ingests a new usage record and updates the aggregates.
func (s *RequestStatistics) Record(ctx context.Context, record coreusage.Record) {
	if s == nil || !statisticsEnabled.Load() {
		return
	}
	event := normalizeEvent(eventFromUsageRecord(ctx, record))
	inserted := s.insertEvent(event)
	if inserted {
		_ = s.persistEvent(ctx, event)
	} else {
		s.addSkipped(1)
	}
}

func eventFromUsageRecord(ctx context.Context, record coreusage.Record) Event {
	timestamp := record.RequestedAt
	if timestamp.IsZero() {
		timestamp = time.Now()
	}

	endpoint := strings.TrimSpace(internallogging.GetEndpoint(ctx))
	method, path := parseEndpoint(endpoint)
	if endpoint == "" {
		endpoint = resolveAPIIdentifier(ctx, record)
		method, path = parseEndpoint(endpoint)
	}
	if endpoint == "" {
		endpoint = "-"
	}

	modelName := strings.TrimSpace(record.Model)
	if modelName == "" {
		modelName = "unknown"
	}
	tokens := normaliseDetail(record.Detail)
	latencyMs := normaliseLatency(record.Latency)
	var latencyPtr *int64
	if latencyMs > 0 {
		latencyPtr = &latencyMs
	}

	failed := record.Failed
	if !failed {
		failed = !resolveSuccess(ctx)
	}
	sourceRaw := strings.TrimSpace(record.Source)
	apiKey := strings.TrimSpace(record.APIKey)

	event := Event{
		RequestID:           strings.TrimSpace(internallogging.GetRequestID(ctx)),
		TimestampMS:         timestamp.UnixMilli(),
		Timestamp:           timestamp.UTC().Format(time.RFC3339Nano),
		Provider:            nonEmpty(record.Provider, "unknown"),
		Model:               modelName,
		Endpoint:            endpoint,
		Method:              method,
		Path:                path,
		AuthType:            strings.TrimSpace(record.AuthType),
		AuthIndex:           strings.TrimSpace(record.AuthIndex),
		Source:              maskSource(sourceRaw),
		SourceHash:          hashString(sourceRaw),
		APIKeyHash:          hashString(apiKey),
		InputTokens:         tokens.InputTokens,
		OutputTokens:        tokens.OutputTokens,
		ReasoningTokens:     tokens.ReasoningTokens,
		CachedTokens:        tokens.CachedTokens,
		CacheReadTokens:     tokens.CacheReadTokens,
		CacheCreationTokens: tokens.CacheCreationTokens,
		CacheTokens:         tokens.CacheTokens,
		TotalTokens:         tokens.TotalTokens,
		LatencyMS:           latencyPtr,
		Failed:              failed,
		CreatedAtMS:         time.Now().UnixMilli(),
	}
	event.EventHash = buildEventHash(event)
	return event
}

func normalizeEvent(event Event) Event {
	if event.Timestamp == "" && event.TimestampMS > 0 {
		event.Timestamp = time.UnixMilli(event.TimestampMS).UTC().Format(time.RFC3339Nano)
	}
	if event.TimestampMS <= 0 {
		if event.Timestamp != "" {
			if parsed, err := time.Parse(time.RFC3339Nano, event.Timestamp); err == nil {
				event.TimestampMS = parsed.UnixMilli()
			}
		}
		if event.TimestampMS <= 0 {
			event.TimestampMS = time.Now().UnixMilli()
			event.Timestamp = time.UnixMilli(event.TimestampMS).UTC().Format(time.RFC3339Nano)
		}
	}
	if event.Timestamp == "" {
		event.Timestamp = time.UnixMilli(event.TimestampMS).UTC().Format(time.RFC3339Nano)
	}
	if event.TimestampMS <= 0 {
		if parsed, err := time.Parse(time.RFC3339Nano, event.Timestamp); err == nil {
			event.TimestampMS = parsed.UnixMilli()
		}
	}
	if event.Model == "" {
		event.Model = "unknown"
	}
	if event.Endpoint == "" {
		event.Endpoint = "-"
	}
	if event.Method == "" || event.Path == "" {
		if method, path := parseEndpoint(event.Endpoint); method != "" {
			if event.Method == "" {
				event.Method = method
			}
			if event.Path == "" {
				event.Path = path
			}
		}
	}
	if event.CacheTokens == 0 {
		event.CacheTokens = event.CacheReadTokens + event.CacheCreationTokens
	}
	if event.CacheTokens == 0 {
		event.CacheTokens = event.CachedTokens
	}
	if event.TotalTokens <= 0 {
		event.TotalTokens = event.InputTokens + event.OutputTokens + event.ReasoningTokens
	}
	if event.TotalTokens <= 0 {
		event.TotalTokens = event.InputTokens + event.OutputTokens + event.ReasoningTokens + maxInt64(event.CachedTokens, event.CacheTokens)
	}
	if event.CreatedAtMS <= 0 {
		event.CreatedAtMS = time.Now().UnixMilli()
	}
	if event.EventHash == "" {
		event.EventHash = buildEventHash(event)
	}
	return event
}

func (s *RequestStatistics) insertEvent(event Event) bool {
	event = normalizeEvent(event)
	if event.EventHash == "" {
		return false
	}

	timestamp := time.UnixMilli(event.TimestampMS)
	tokens := TokenStats{
		InputTokens:         event.InputTokens,
		OutputTokens:        event.OutputTokens,
		ReasoningTokens:     event.ReasoningTokens,
		CachedTokens:        event.CachedTokens,
		CacheReadTokens:     event.CacheReadTokens,
		CacheCreationTokens: event.CacheCreationTokens,
		CacheTokens:         event.CacheTokens,
		TotalTokens:         event.TotalTokens,
	}
	detail := RequestDetail{
		Timestamp:            timestamp,
		Source:               event.Source,
		AuthIndex:            event.AuthIndex,
		AccountSnapshot:      event.AccountSnapshot,
		AuthLabelSnapshot:    event.AuthLabelSnapshot,
		AuthFileSnapshot:     event.AuthFileSnapshot,
		AuthProviderSnapshot: event.AuthProviderSnapshot,
		AuthSnapshotAtMS:     event.AuthSnapshotAtMS,
		Tokens:               tokens,
		Failed:               event.Failed,
	}
	if event.LatencyMS != nil && *event.LatencyMS > 0 {
		detail.LatencyMs = *event.LatencyMS
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.eventHashes[event.EventHash]; exists {
		return false
	}
	s.eventHashes[event.EventHash] = struct{}{}

	s.totalRequests++
	if event.Failed {
		s.failureCount++
	} else {
		s.successCount++
	}
	s.totalTokens += event.TotalTokens

	stats := s.apis[event.Endpoint]
	if stats == nil {
		stats = &apiStats{Models: make(map[string]*modelStats)}
		s.apis[event.Endpoint] = stats
	}
	stats.TotalRequests++
	stats.TotalTokens += event.TotalTokens

	modelStatsValue := stats.Models[event.Model]
	if modelStatsValue == nil {
		modelStatsValue = &modelStats{}
		stats.Models[event.Model] = modelStatsValue
	}
	modelStatsValue.TotalRequests++
	modelStatsValue.TotalTokens += event.TotalTokens
	modelStatsValue.Details = append(modelStatsValue.Details, detail)

	dayKey := timestamp.Format("2006-01-02")
	hourKey := timestamp.Hour()
	s.requestsByDay[dayKey]++
	s.requestsByHour[hourKey]++
	s.tokensByDay[dayKey] += event.TotalTokens
	s.tokensByHour[hourKey] += event.TotalTokens
	return true
}

// Snapshot returns a copy of the aggregated metrics for external consumption.
func (s *RequestStatistics) Snapshot() StatisticsSnapshot {
	result := StatisticsSnapshot{APIs: map[string]APISnapshot{}}
	if s == nil {
		return result
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	result.TotalRequests = s.totalRequests
	result.SuccessCount = s.successCount
	result.FailureCount = s.failureCount
	result.TotalTokens = s.totalTokens

	result.APIs = make(map[string]APISnapshot, len(s.apis))
	for apiName, stats := range s.apis {
		apiSnapshot := APISnapshot{
			TotalRequests: stats.TotalRequests,
			TotalTokens:   stats.TotalTokens,
			Models:        make(map[string]ModelSnapshot, len(stats.Models)),
		}
		for modelName, modelStatsValue := range stats.Models {
			details := make([]RequestDetail, len(modelStatsValue.Details))
			copy(details, modelStatsValue.Details)
			apiSnapshot.Models[modelName] = ModelSnapshot{
				TotalRequests: modelStatsValue.TotalRequests,
				TotalTokens:   modelStatsValue.TotalTokens,
				Details:       details,
			}
		}
		result.APIs[apiName] = apiSnapshot
	}

	result.RequestsByDay = make(map[string]int64, len(s.requestsByDay))
	for k, v := range s.requestsByDay {
		result.RequestsByDay[k] = v
	}
	result.RequestsByHour = make(map[string]int64, len(s.requestsByHour))
	for hour, v := range s.requestsByHour {
		result.RequestsByHour[formatHour(hour)] = v
	}
	result.TokensByDay = make(map[string]int64, len(s.tokensByDay))
	for k, v := range s.tokensByDay {
		result.TokensByDay[k] = v
	}
	result.TokensByHour = make(map[string]int64, len(s.tokensByHour))
	for hour, v := range s.tokensByHour {
		result.TokensByHour[formatHour(hour)] = v
	}

	return result
}

// ServiceStatus returns SQLite-backed usage service health information.
func (s *RequestStatistics) ServiceStatus(ctx context.Context) (ServiceStatus, error) {
	collector := CollectorStatus{
		Collector: "running",
		Upstream:  "internal",
		Mode:      "embedded",
		Transport: "plugin",
		Queue:     "internal",
	}
	if !statisticsEnabled.Load() {
		collector.Collector = "stopped"
	}
	status := ServiceStatus{
		Service:   "cpa-manager",
		Collector: collector,
	}
	if s == nil {
		return status, nil
	}

	s.mu.RLock()
	status.DBPath = s.dbPath
	store := s.store
	status.Collector.TotalSkipped = s.skippedEvents
	s.mu.RUnlock()

	if store == nil {
		return status, nil
	}
	events, deadLetters, err := store.Counts(ctx)
	if err != nil {
		return status, err
	}
	status.Events = events
	status.DeadLetters = deadLetters
	status.Collector.DeadLetters = deadLetters
	status.Collector.TotalInserted = events
	lastConsumedAt, lastInsertedAt, err := store.LatestEventTimes(ctx)
	if err != nil {
		return status, err
	}
	status.Collector.LastConsumedAt = lastConsumedAt
	status.Collector.LastInsertedAt = lastInsertedAt
	return status, nil
}

// ImportEvents merges events into the current store and appends new events to disk.
func (s *RequestStatistics) ImportEvents(parsed ImportParseResult) MergeResult {
	result := MergeResult{
		Total:       len(parsed.Events),
		Failed:      parsed.Failed,
		Unsupported: parsed.Unsupported,
		Format:      parsed.Format,
		Warnings:    parsed.Warnings,
	}
	if s == nil {
		return result
	}
	for _, event := range parsed.Events {
		event = normalizeEvent(event)
		if s.insertEvent(event) {
			result.Added++
			_ = s.persistEvent(context.Background(), event)
		} else {
			result.Skipped++
			s.addSkipped(1)
		}
	}
	return result
}

func (s *RequestStatistics) addSkipped(count int64) {
	if s == nil || count <= 0 {
		return
	}
	s.mu.Lock()
	s.skippedEvents += count
	s.mu.Unlock()
}

func (s *RequestStatistics) migrateLegacyEvents(path string) error {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("usage storage open events: %w", err)
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var event Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			s.addDeadLetter(context.Background(), line, err)
			continue
		}
		event = normalizeEvent(event)
		if store := s.currentStore(); store != nil {
			if _, err := store.InsertEvents(context.Background(), []Event{event}); err != nil {
				return fmt.Errorf("usage sqlite migrate events: %w", err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("usage storage scan events: %w", err)
	}
	return nil
}

func (s *RequestStatistics) loadEventsFromStore(ctx context.Context) error {
	store := s.currentStore()
	if store == nil {
		return nil
	}
	events, err := store.Events(ctx, 0)
	if err != nil {
		return fmt.Errorf("usage sqlite load events: %w", err)
	}
	for _, event := range events {
		s.insertEvent(event)
	}
	return nil
}

func (s *RequestStatistics) currentStore() *sqliteUsageStore {
	s.mu.RLock()
	store := s.store
	s.mu.RUnlock()
	return store
}

func (s *RequestStatistics) persistEvent(ctx context.Context, event Event) error {
	store := s.currentStore()
	if store == nil {
		return nil
	}
	_, err := store.InsertEvents(ctx, []Event{event})
	return err
}

func (s *RequestStatistics) addDeadLetter(ctx context.Context, payload string, err error) {
	store := s.currentStore()
	if store == nil {
		return
	}
	_ = store.AddDeadLetter(ctx, payload, err)
}

// ExportJSONL returns persisted usage events as newline-delimited JSON.
func (s *RequestStatistics) ExportJSONL() ([]byte, error) {
	if s == nil {
		return nil, nil
	}
	if store := s.currentStore(); store != nil {
		return store.ExportJSONL(context.Background())
	}
	return eventsFromSnapshot(s.Snapshot())
}

func eventsFromSnapshot(snapshot StatisticsSnapshot) ([]byte, error) {
	var out []byte
	for endpoint, api := range snapshot.APIs {
		method, path := parseEndpoint(endpoint)
		for model, modelSnapshot := range api.Models {
			for _, detail := range modelSnapshot.Details {
				event := eventFromDetail(endpoint, method, path, model, detail)
				line, err := json.Marshal(event)
				if err != nil {
					return nil, err
				}
				out = append(out, line...)
				out = append(out, '\n')
			}
		}
	}
	return out, nil
}

func eventFromDetail(endpoint, method, path, model string, detail RequestDetail) Event {
	timestamp := detail.Timestamp
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	var latency *int64
	if detail.LatencyMs > 0 {
		latency = &detail.LatencyMs
	}
	event := Event{
		TimestampMS:          timestamp.UnixMilli(),
		Timestamp:            timestamp.UTC().Format(time.RFC3339Nano),
		Model:                nonEmpty(model, "unknown"),
		Endpoint:             nonEmpty(endpoint, "-"),
		Method:               method,
		Path:                 path,
		AuthIndex:            detail.AuthIndex,
		Source:               detail.Source,
		SourceHash:           hashString(detail.Source),
		AccountSnapshot:      detail.AccountSnapshot,
		AuthLabelSnapshot:    detail.AuthLabelSnapshot,
		AuthFileSnapshot:     detail.AuthFileSnapshot,
		AuthProviderSnapshot: detail.AuthProviderSnapshot,
		AuthSnapshotAtMS:     detail.AuthSnapshotAtMS,
		InputTokens:          detail.Tokens.InputTokens,
		OutputTokens:         detail.Tokens.OutputTokens,
		ReasoningTokens:      detail.Tokens.ReasoningTokens,
		CachedTokens:         detail.Tokens.CachedTokens,
		CacheReadTokens:      detail.Tokens.CacheReadTokens,
		CacheCreationTokens:  detail.Tokens.CacheCreationTokens,
		CacheTokens:          detail.Tokens.CacheTokens,
		TotalTokens:          detail.Tokens.TotalTokens,
		LatencyMS:            latency,
		Failed:               detail.Failed,
		CreatedAtMS:          time.Now().UnixMilli(),
	}
	event.EventHash = buildEventHash(event)
	return event
}

// ParseImportPayload accepts usage-service JSONL and legacy usage snapshots.
func ParseImportPayload(data []byte) (ImportParseResult, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return ImportParseResult{}, errors.New("empty usage import payload")
	}
	switch trimmed[0] {
	case '{':
		return parseJSONObjectImport(trimmed)
	case '[':
		return parseJSONArrayImport(trimmed)
	default:
		return parseJSONLImport(trimmed)
	}
}

func parseJSONObjectImport(data []byte) (ImportParseResult, error) {
	var record map[string]any
	if err := decodeJSON(data, &record); err != nil {
		return ImportParseResult{}, err
	}
	if usageRaw, ok := record["usage"]; ok {
		usageRecord, ok := usageRaw.(map[string]any)
		if !ok {
			return ImportParseResult{Format: ImportFormatLegacyExport, Unsupported: 1}, errors.New("legacy usage export does not contain request details")
		}
		return eventsFromLegacyUsage(usageRecord, ImportFormatLegacyExport)
	}
	if hasUsageAPIs(record) {
		return eventsFromLegacyUsage(record, ImportFormatLegacyPayload)
	}
	event, err := eventFromJSONRecord(data)
	if err != nil {
		return ImportParseResult{Format: ImportFormatJSONL, Failed: 1}, err
	}
	return ImportParseResult{Format: ImportFormatJSONL, Events: []Event{event}}, nil
}

func parseJSONArrayImport(data []byte) (ImportParseResult, error) {
	var items []json.RawMessage
	if err := decodeJSON(data, &items); err != nil {
		return ImportParseResult{}, err
	}
	result := ImportParseResult{Format: ImportFormatJSONL}
	for _, item := range items {
		event, err := eventFromJSONRecord(item)
		if err != nil {
			result.Failed++
			continue
		}
		result.Events = append(result.Events, event)
	}
	return result, nil
}

func parseJSONLImport(data []byte) (ImportParseResult, error) {
	result := ImportParseResult{Format: ImportFormatJSONL}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		event, err := eventFromJSONRecord([]byte(line))
		if err != nil {
			result.Failed++
			continue
		}
		result.Events = append(result.Events, event)
	}
	if err := scanner.Err(); err != nil {
		return result, err
	}
	return result, nil
}

func eventFromJSONRecord(data []byte) (Event, error) {
	var record map[string]any
	if err := decodeJSON(data, &record); err != nil {
		return Event{}, err
	}
	if eventHash := readString(record, "event_hash", "eventHash"); eventHash != "" {
		event := Event{
			RequestID:            readString(record, "request_id", "requestId"),
			EventHash:            eventHash,
			TimestampMS:          readInt(record, "timestamp_ms", "timestampMs"),
			Timestamp:            readString(record, "timestamp"),
			Provider:             readString(record, "provider"),
			Model:                readString(record, "model"),
			Endpoint:             readString(record, "endpoint"),
			Method:               readString(record, "method"),
			Path:                 readString(record, "path"),
			AuthType:             readString(record, "auth_type", "authType"),
			AuthIndex:            readString(record, "auth_index", "authIndex", "AuthIndex"),
			Source:               readString(record, "source"),
			SourceHash:           readString(record, "source_hash", "sourceHash"),
			APIKeyHash:           readString(record, "api_key_hash", "apiKeyHash"),
			AccountSnapshot:      readString(record, "account_snapshot", "accountSnapshot"),
			AuthLabelSnapshot:    readString(record, "auth_label_snapshot", "authLabelSnapshot"),
			AuthFileSnapshot:     readString(record, "auth_file_snapshot", "authFileSnapshot"),
			AuthProviderSnapshot: readString(record, "auth_provider_snapshot", "authProviderSnapshot"),
			AuthSnapshotAtMS:     readInt(record, "auth_snapshot_at_ms", "authSnapshotAtMs"),
			InputTokens:          readInt(record, "input_tokens", "inputTokens", "prompt_tokens", "promptTokens"),
			OutputTokens:         readInt(record, "output_tokens", "outputTokens", "completion_tokens", "completionTokens"),
			ReasoningTokens:      readInt(record, "reasoning_tokens", "reasoningTokens"),
			CachedTokens:         readInt(record, "cached_tokens", "cachedTokens"),
			CacheReadTokens:      readInt(record, "cache_read_tokens", "cacheReadTokens"),
			CacheCreationTokens:  readInt(record, "cache_creation_tokens", "cacheCreationTokens"),
			CacheTokens:          readInt(record, "cache_tokens", "cacheTokens"),
			TotalTokens:          readInt(record, "total_tokens", "totalTokens", "total"),
			LatencyMS:            readOptionalInt(record, "latency_ms", "latencyMs"),
			Failed:               readBool(record, "failed", "is_failed", "isFailed"),
			RawJSON:              readString(record, "raw_json", "rawJson"),
			CreatedAtMS:          readInt(record, "created_at_ms", "createdAtMs"),
		}
		if event.TimestampMS <= 0 || event.Timestamp == "" {
			event.TimestampMS, event.Timestamp = readTimestamp(record)
		}
		return event, nil
	}

	timestampMS, timestamp := readTimestamp(record)
	endpoint := readString(record, "endpoint", "api", "request", "operation")
	method := strings.ToUpper(readString(record, "method", "http_method", "httpMethod"))
	path := readString(record, "path", "url_path", "urlPath", "route")
	if endpoint == "" && method != "" && path != "" {
		endpoint = method + " " + path
	}
	if endpoint != "" {
		if m, p := parseEndpoint(endpoint); m != "" {
			method, path = m, p
		}
	}
	if endpoint == "" {
		endpoint = "-"
	}
	tokens := readTokenStats(record)
	sourceRaw := readString(record, "source", "api_key", "apiKey", "key", "account", "email")
	apiKey := readString(record, "api_key", "apiKey", "key")
	redacted := redactValue(record)
	rawJSON, _ := json.Marshal(redacted)

	event := Event{
		RequestID:            readString(record, "request_id", "requestId", "id"),
		TimestampMS:          timestampMS,
		Timestamp:            timestamp,
		Provider:             readString(record, "provider", "type", "auth_type", "authType"),
		Model:                nonEmpty(readString(record, "model", "model_name", "modelName"), "-"),
		Endpoint:             endpoint,
		Method:               method,
		Path:                 path,
		AuthType:             readString(record, "auth_type", "authType"),
		AuthIndex:            readString(record, "auth_index", "authIndex", "AuthIndex"),
		Source:               maskSource(sourceRaw),
		SourceHash:           hashString(sourceRaw),
		APIKeyHash:           hashString(apiKey),
		AccountSnapshot:      readString(record, "account_snapshot", "accountSnapshot"),
		AuthLabelSnapshot:    readString(record, "auth_label_snapshot", "authLabelSnapshot"),
		AuthFileSnapshot:     readString(record, "auth_file_snapshot", "authFileSnapshot"),
		AuthProviderSnapshot: readString(record, "auth_provider_snapshot", "authProviderSnapshot"),
		AuthSnapshotAtMS:     readInt(record, "auth_snapshot_at_ms", "authSnapshotAtMs"),
		InputTokens:          tokens.InputTokens,
		OutputTokens:         tokens.OutputTokens,
		ReasoningTokens:      tokens.ReasoningTokens,
		CachedTokens:         tokens.CachedTokens,
		CacheReadTokens:      tokens.CacheReadTokens,
		CacheCreationTokens:  tokens.CacheCreationTokens,
		CacheTokens:          tokens.CacheTokens,
		TotalTokens:          tokens.TotalTokens,
		LatencyMS:            readOptionalInt(record, "latency_ms", "latencyMs", "duration_ms", "durationMs", "elapsed_ms", "elapsedMs"),
		Failed:               readFailed(record),
		RawJSON:              string(rawJSON),
		CreatedAtMS:          time.Now().UnixMilli(),
	}
	event.EventHash = buildEventHash(event)
	return event, nil
}

func eventsFromLegacyUsage(usageRecord map[string]any, format string) (ImportParseResult, error) {
	apisRaw, ok := usageRecord["apis"].(map[string]any)
	if !ok {
		return ImportParseResult{Format: format, Unsupported: 1}, errors.New("legacy usage export does not contain request details")
	}
	result := ImportParseResult{
		Format: format,
		Warnings: []string{
			"legacy_usage_metadata_is_partial",
			"legacy_usage_source_matching_may_be_approximate",
		},
	}
	now := time.Now().UnixMilli()
	for _, endpoint := range sortedKeys(apisRaw) {
		apiEntry, ok := apisRaw[endpoint].(map[string]any)
		if !ok {
			result.Failed++
			continue
		}
		modelsRaw, ok := apiEntry["models"].(map[string]any)
		if !ok {
			result.Failed++
			continue
		}
		method, path := parseEndpoint(endpoint)
		for _, model := range sortedKeys(modelsRaw) {
			modelEntry, ok := modelsRaw[model].(map[string]any)
			if !ok {
				result.Failed++
				continue
			}
			detailsRaw, ok := modelEntry["details"].([]any)
			if !ok || len(detailsRaw) == 0 {
				result.Unsupported++
				continue
			}
			for _, detailRaw := range detailsRaw {
				detail, ok := detailRaw.(map[string]any)
				if !ok {
					result.Failed++
					continue
				}
				event, err := eventFromLegacyDetail(endpoint, method, path, model, detail, now)
				if err != nil {
					result.Failed++
					continue
				}
				result.Events = append(result.Events, event)
			}
		}
	}
	if len(result.Events) == 0 {
		return result, errors.New("legacy usage export does not contain request details")
	}
	return result, nil
}

func eventFromLegacyDetail(endpoint, method, path, model string, detail map[string]any, now int64) (Event, error) {
	if readString(detail, "timestamp", "time", "created_at", "createdAt") == "" {
		return Event{}, errors.New("legacy usage detail missing timestamp")
	}
	timestampMS, timestamp := readTimestamp(detail)
	tokens := readTokenStats(detail)
	sourceRaw := readString(detail, "source", "api_key", "apiKey", "key", "account", "email")
	apiKey := readString(detail, "api_key", "apiKey", "key")
	rawJSON, _ := json.Marshal(map[string]any{
		"format":   "legacy_usage_export",
		"endpoint": endpoint,
		"model":    model,
		"detail":   redactValue(detail),
	})
	event := Event{
		RequestID:            readString(detail, "request_id", "requestId", "id"),
		TimestampMS:          timestampMS,
		Timestamp:            timestamp,
		Provider:             readString(detail, "provider", "type", "auth_type", "authType"),
		Model:                nonEmpty(model, "-"),
		Endpoint:             nonEmpty(endpoint, "-"),
		Method:               method,
		Path:                 path,
		AuthType:             readString(detail, "auth_type", "authType"),
		AuthIndex:            readString(detail, "auth_index", "authIndex", "AuthIndex"),
		Source:               maskSource(sourceRaw),
		SourceHash:           hashString(sourceRaw),
		APIKeyHash:           hashString(apiKey),
		AccountSnapshot:      readString(detail, "account_snapshot", "accountSnapshot"),
		AuthLabelSnapshot:    readString(detail, "auth_label_snapshot", "authLabelSnapshot"),
		AuthFileSnapshot:     readString(detail, "auth_file_snapshot", "authFileSnapshot"),
		AuthProviderSnapshot: readString(detail, "auth_provider_snapshot", "authProviderSnapshot"),
		AuthSnapshotAtMS:     readInt(detail, "auth_snapshot_at_ms", "authSnapshotAtMs"),
		InputTokens:          tokens.InputTokens,
		OutputTokens:         tokens.OutputTokens,
		ReasoningTokens:      tokens.ReasoningTokens,
		CachedTokens:         tokens.CachedTokens,
		CacheReadTokens:      tokens.CacheReadTokens,
		CacheCreationTokens:  tokens.CacheCreationTokens,
		CacheTokens:          tokens.CacheTokens,
		TotalTokens:          tokens.TotalTokens,
		LatencyMS:            readOptionalInt(detail, "latency_ms", "latencyMs", "duration_ms", "durationMs", "elapsed_ms", "elapsedMs"),
		Failed:               readFailed(detail),
		RawJSON:              string(rawJSON),
		CreatedAtMS:          now,
	}
	event.EventHash = buildEventHash(event)
	return event, nil
}

// LoadModelPrices returns saved model prices.
func LoadModelPrices(ctx context.Context) (map[string]ModelPrice, error) {
	return defaultRequestStatistics.LoadModelPrices(ctx)
}

// SaveModelPrices replaces saved model prices.
func SaveModelPrices(ctx context.Context, prices map[string]ModelPrice) (map[string]ModelPrice, error) {
	return defaultRequestStatistics.SaveModelPrices(ctx, prices)
}

// SyncModelPrices imports selected LiteLLM model prices.
func SyncModelPrices(ctx context.Context, models []string) (ModelPriceSyncResult, error) {
	return defaultRequestStatistics.SyncModelPrices(ctx, models)
}

func (s *RequestStatistics) LoadModelPrices(_ context.Context) (map[string]ModelPrice, error) {
	if s == nil {
		return map[string]ModelPrice{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneModelPrices(s.modelPrices), nil
}

func (s *RequestStatistics) SaveModelPrices(ctx context.Context, prices map[string]ModelPrice) (map[string]ModelPrice, error) {
	if s == nil {
		return map[string]ModelPrice{}, nil
	}
	normalized := make(map[string]ModelPrice, len(prices))
	now := time.Now().UnixMilli()
	for model, price := range prices {
		model = strings.TrimSpace(model)
		if model == "" {
			return nil, errors.New("model is required")
		}
		if !validPriceValue(price.Prompt) || !validPriceValue(price.Completion) || !validPriceValue(price.Cache) {
			return nil, fmt.Errorf("invalid model price for %s", model)
		}
		price.UpdatedAtMS = now
		normalized[model] = price
	}

	s.mu.Lock()
	s.modelPrices = normalized
	store := s.store
	s.mu.Unlock()

	if store != nil {
		if err := store.SaveModelPrices(ctx, normalized); err != nil {
			return nil, err
		}
	}
	return cloneModelPrices(normalized), nil
}

func (s *RequestStatistics) SyncModelPrices(ctx context.Context, models []string) (ModelPriceSyncResult, error) {
	remotePrices, skipped, err := fetchLiteLLMModelPrices(ctx)
	if err != nil {
		return ModelPriceSyncResult{}, err
	}
	selected := selectModelPrices(remotePrices, models)
	now := time.Now().UnixMilli()

	s.mu.Lock()
	if s.modelPrices == nil {
		s.modelPrices = make(map[string]ModelPrice)
	}
	result := ModelPriceSyncResult{Source: modelPriceSyncSource, Skipped: skipped}
	for model, price := range selected {
		if err := validateModelPrice(model, price); err != nil {
			result.Skipped++
			continue
		}
		price.UpdatedAtMS = now
		price.SyncedAtMS = &now
		s.modelPrices[model] = price
		result.Imported++
	}
	result.Prices = cloneModelPrices(s.modelPrices)
	store := s.store
	s.mu.Unlock()

	if store != nil {
		if storeResult, err := store.UpsertModelPrices(ctx, selected); err != nil {
			return ModelPriceSyncResult{}, err
		} else if len(storeResult.Prices) > 0 {
			result.Prices = storeResult.Prices
		}
	}
	return result, nil
}

func (s *RequestStatistics) migrateLegacyModelPrices(path string) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("usage storage read model prices: %w", err)
	}
	var payload struct {
		Prices map[string]ModelPrice `json:"prices"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("usage storage decode model prices: %w", err)
	}
	if len(payload.Prices) == 0 {
		return nil
	}
	store := s.currentStore()
	if store == nil {
		return nil
	}
	existing, err := store.LoadModelPrices(context.Background())
	if err != nil {
		return err
	}
	if len(existing) > 0 {
		return nil
	}
	if err := store.SaveModelPrices(context.Background(), payload.Prices); err != nil {
		return fmt.Errorf("usage sqlite migrate model prices: %w", err)
	}
	return nil
}

func (s *RequestStatistics) loadModelPricesFromStore(ctx context.Context) error {
	store := s.currentStore()
	if store == nil {
		return nil
	}
	prices, err := store.LoadModelPrices(ctx)
	if err != nil {
		return fmt.Errorf("usage sqlite load model prices: %w", err)
	}
	s.mu.Lock()
	s.modelPrices = cloneModelPrices(prices)
	s.mu.Unlock()
	return nil
}

func fetchLiteLLMModelPrices(ctx context.Context) (map[string]ModelPrice, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, modelPriceSyncURL, nil)
	if err != nil {
		return nil, 0, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, 0, errors.New("model price sync failed: " + res.Status)
	}

	var payload map[string]json.RawMessage
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return nil, 0, err
	}
	prices := map[string]ModelPrice{}
	skipped := 0
	for model, raw := range payload {
		if model == "" || model == "sample_spec" {
			skipped++
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal(raw, &entry); err != nil {
			skipped++
			continue
		}
		prompt, hasPrompt := readFloat(entry, "input_cost_per_token")
		completion, hasCompletion := readFloat(entry, "output_cost_per_token")
		cache, hasCache := readFloat(entry, "cache_read_input_token_cost")
		if !hasCache {
			cache, hasCache = readFloat(entry, "cache_read_cost_per_token")
		}
		if !hasPrompt && !hasCompletion {
			skipped++
			continue
		}
		if !hasPrompt {
			prompt = 0
		}
		if !hasCompletion {
			completion = 0
		}
		if !hasCache {
			cache = prompt
		}
		prices[model] = ModelPrice{
			Prompt:        prompt * 1_000_000,
			Completion:    completion * 1_000_000,
			Cache:         cache * 1_000_000,
			Source:        modelPriceSyncSource,
			SourceModelID: model,
			RawJSON:       string(raw),
		}
	}
	return prices, skipped, nil
}

func selectModelPrices(prices map[string]ModelPrice, models []string) map[string]ModelPrice {
	wanted := make([]string, 0, len(models))
	seen := map[string]struct{}{}
	for _, model := range models {
		model = strings.TrimSpace(model)
		if model == "" {
			continue
		}
		if _, ok := seen[model]; ok {
			continue
		}
		seen[model] = struct{}{}
		wanted = append(wanted, model)
	}
	if len(wanted) == 0 {
		return prices
	}
	selected := map[string]ModelPrice{}
	for _, model := range wanted {
		if price, ok := prices[model]; ok {
			selected[model] = price
			continue
		}
		if price, ok := findSuffixModelPrice(prices, model); ok {
			selected[model] = price
		}
	}
	return selected
}

func findSuffixModelPrice(prices map[string]ModelPrice, model string) (ModelPrice, bool) {
	suffix := "/" + model
	var match ModelPrice
	matchedKey := ""
	for key, price := range prices {
		if !strings.HasSuffix(key, suffix) {
			continue
		}
		if matchedKey == "" || len(key) < len(matchedKey) {
			matchedKey = key
			match = price
		}
	}
	return match, matchedKey != ""
}

func cloneModelPrices(prices map[string]ModelPrice) map[string]ModelPrice {
	out := make(map[string]ModelPrice, len(prices))
	for model, price := range prices {
		out[model] = price
	}
	return out
}

func normaliseDetail(detail coreusage.Detail) TokenStats {
	tokens := TokenStats{
		InputTokens:         detail.InputTokens,
		OutputTokens:        detail.OutputTokens,
		ReasoningTokens:     detail.ReasoningTokens,
		CachedTokens:        detail.CachedTokens,
		CacheReadTokens:     detail.CacheReadTokens,
		CacheCreationTokens: detail.CacheCreationTokens,
		CacheTokens:         detail.CacheReadTokens + detail.CacheCreationTokens,
		TotalTokens:         detail.TotalTokens,
	}
	if tokens.CacheTokens == 0 {
		tokens.CacheTokens = tokens.CachedTokens
	}
	if tokens.TotalTokens == 0 {
		tokens.TotalTokens = tokens.InputTokens + tokens.OutputTokens + tokens.ReasoningTokens
	}
	if tokens.TotalTokens == 0 {
		tokens.TotalTokens = tokens.InputTokens + tokens.OutputTokens + tokens.ReasoningTokens + maxInt64(tokens.CachedTokens, tokens.CacheTokens)
	}
	return tokens
}

func normaliseTokenStats(tokens TokenStats) TokenStats {
	if tokens.CacheTokens == 0 {
		tokens.CacheTokens = tokens.CacheReadTokens + tokens.CacheCreationTokens
	}
	if tokens.CacheTokens == 0 {
		tokens.CacheTokens = tokens.CachedTokens
	}
	if tokens.TotalTokens == 0 {
		tokens.TotalTokens = tokens.InputTokens + tokens.OutputTokens + tokens.ReasoningTokens
	}
	if tokens.TotalTokens == 0 {
		tokens.TotalTokens = tokens.InputTokens + tokens.OutputTokens + tokens.ReasoningTokens + maxInt64(tokens.CachedTokens, tokens.CacheTokens)
	}
	return tokens
}

func normaliseLatency(latency time.Duration) int64 {
	if latency <= 0 {
		return 0
	}
	return latency.Milliseconds()
}

func resolveAPIIdentifier(ctx context.Context, record coreusage.Record) string {
	if ctx != nil {
		if ginCtx, ok := ctx.Value("gin").(*gin.Context); ok && ginCtx != nil {
			path := ginCtx.FullPath()
			if path == "" && ginCtx.Request != nil {
				path = ginCtx.Request.URL.Path
			}
			method := ""
			if ginCtx.Request != nil {
				method = ginCtx.Request.Method
			}
			if path != "" {
				if method != "" {
					return method + " " + path
				}
				return path
			}
		}
	}
	if record.Provider != "" {
		return record.Provider
	}
	return "unknown"
}

func resolveSuccess(ctx context.Context) bool {
	status := internallogging.GetResponseStatus(ctx)
	if status == 0 {
		return true
	}
	return status < http.StatusBadRequest
}

func parseEndpoint(endpoint string) (method string, path string) {
	if match := endpointPattern.FindStringSubmatch(endpoint); len(match) == 3 {
		return strings.ToUpper(match[1]), match[2]
	}
	return "", ""
}

func readTimestamp(record map[string]any) (int64, string) {
	raw := first(record, "timestamp", "time", "created_at", "createdAt", "created", "request_time", "requestTime")
	now := time.Now()
	if raw == nil {
		return now.UnixMilli(), now.UTC().Format(time.RFC3339Nano)
	}
	switch value := raw.(type) {
	case json.Number:
		number, _ := value.Int64()
		if number < 10_000_000_000 {
			number *= 1000
		}
		return number, time.UnixMilli(number).UTC().Format(time.RFC3339Nano)
	case float64:
		ms := int64(value)
		if ms < 10_000_000_000 {
			ms *= 1000
		}
		return ms, time.UnixMilli(ms).UTC().Format(time.RFC3339Nano)
	case string:
		trimmed := strings.TrimSpace(value)
		if number, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			if number < 10_000_000_000 {
				number *= 1000
			}
			return number, time.UnixMilli(number).UTC().Format(time.RFC3339Nano)
		}
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05"} {
			if parsed, err := time.Parse(layout, trimmed); err == nil {
				return parsed.UnixMilli(), parsed.UTC().Format(time.RFC3339Nano)
			}
		}
	}
	return now.UnixMilli(), now.UTC().Format(time.RFC3339Nano)
}

func readTokenStats(record map[string]any) TokenStats {
	tokens := map[string]any{}
	if nested, ok := first(record, "tokens", "usage").(map[string]any); ok {
		tokens = nested
	}
	out := TokenStats{
		InputTokens:         firstInt(tokens, record, "input_tokens", "inputTokens", "prompt_tokens", "promptTokens"),
		OutputTokens:        firstInt(tokens, record, "output_tokens", "outputTokens", "completion_tokens", "completionTokens"),
		ReasoningTokens:     firstInt(tokens, record, "reasoning_tokens", "reasoningTokens"),
		CachedTokens:        firstInt(tokens, record, "cached_tokens", "cachedTokens"),
		CacheReadTokens:     firstInt(tokens, record, "cache_read_tokens", "cacheReadTokens"),
		CacheCreationTokens: firstInt(tokens, record, "cache_creation_tokens", "cacheCreationTokens"),
		CacheTokens:         firstInt(tokens, record, "cache_tokens", "cacheTokens"),
		TotalTokens:         firstInt(tokens, record, "total_tokens", "totalTokens", "total"),
	}
	return normaliseTokenStats(out)
}

func firstInt(primary, fallback map[string]any, keys ...string) int64 {
	value := readInt(primary, keys...)
	if value != 0 {
		return value
	}
	return readInt(fallback, keys...)
}

func readFailed(record map[string]any) bool {
	if value, ok := first(record, "failed", "is_failed", "isFailed").(bool); ok {
		return value
	}
	if value, ok := first(record, "success", "ok").(bool); ok {
		return !value
	}
	status := readInt(record, "status", "status_code", "statusCode", "http_status", "httpStatus")
	if status >= 400 {
		return true
	}
	return first(record, "error", "error_message", "errorMessage") != nil
}

func readOptionalInt(record map[string]any, keys ...string) *int64 {
	value := readInt(record, keys...)
	if value == 0 && first(record, keys...) == nil {
		return nil
	}
	return &value
}

func readString(record map[string]any, keys ...string) string {
	raw := first(record, keys...)
	if raw == nil {
		return ""
	}
	switch value := raw.(type) {
	case string:
		return strings.TrimSpace(value)
	case json.Number:
		return value.String()
	case float64:
		if value == float64(int64(value)) {
			return strconv.FormatInt(int64(value), 10)
		}
		return strconv.FormatFloat(value, 'f', -1, 64)
	default:
		return strings.TrimSpace(fmt.Sprint(value))
	}
}

func readInt(record map[string]any, keys ...string) int64 {
	raw := first(record, keys...)
	switch value := raw.(type) {
	case json.Number:
		number, _ := value.Int64()
		return number
	case float64:
		return int64(value)
	case int64:
		return value
	case int:
		return int64(value)
	case string:
		parsed, _ := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		return parsed
	default:
		return 0
	}
}

func readBool(record map[string]any, keys ...string) bool {
	raw := first(record, keys...)
	switch value := raw.(type) {
	case bool:
		return value
	case json.Number:
		parsed, _ := value.Int64()
		return parsed != 0
	case float64:
		return value != 0
	case string:
		normalized := strings.ToLower(strings.TrimSpace(value))
		return normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on"
	default:
		return false
	}
}

func readFloat(entry map[string]any, key string) (float64, bool) {
	value, ok := entry[key]
	if !ok || value == nil {
		return 0, false
	}
	switch typed := value.(type) {
	case float64:
		return typed, true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func first(record map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := record[key]; ok {
			return value
		}
	}
	return nil
}

func hasUsageAPIs(record map[string]any) bool {
	apis, ok := record["apis"].(map[string]any)
	return ok && len(apis) > 0
}

func sortedKeys(record map[string]any) []string {
	keys := make([]string, 0, len(record))
	for key := range record {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func hashString(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(trimmed))
	return hex.EncodeToString(sum[:])
}

func buildEventHash(event Event) string {
	parts := []string{
		event.RequestID,
		event.Timestamp,
		event.Endpoint,
		event.Model,
		event.AuthIndex,
		event.SourceHash,
		strconv.FormatInt(event.InputTokens, 10),
		strconv.FormatInt(event.OutputTokens, 10),
		strconv.FormatInt(event.ReasoningTokens, 10),
		strconv.FormatInt(maxInt64(event.CachedTokens, event.CacheTokens), 10),
		strconv.FormatBool(event.Failed),
	}
	if event.LatencyMS != nil {
		parts = append(parts, strconv.FormatInt(*event.LatencyMS, 10))
	}
	return hashString(strings.Join(parts, "|"))
}

func maskSource(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if strings.Contains(trimmed, "@") {
		parts := strings.SplitN(trimmed, "@", 2)
		prefix := parts[0]
		if len(prefix) > 3 {
			prefix = prefix[:3]
		}
		return prefix + "***@" + parts[1]
	}
	if looksSecret(trimmed) {
		if len(trimmed) <= 8 {
			return "m:****"
		}
		return "m:" + trimmed[:4] + "..." + trimmed[len(trimmed)-4:]
	}
	return trimmed
}

func looksSecret(value string) bool {
	if strings.ContainsAny(value, " /\\") {
		return false
	}
	return strings.HasPrefix(value, "sk-") || strings.HasPrefix(value, "AIza") || len(value) >= 32
}

func redactValue(value any) any {
	switch item := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(item))
		for key, child := range item {
			if isSecretKey(key) {
				result[key] = "[redacted]"
				continue
			}
			result[key] = redactValue(child)
		}
		return result
	case []any:
		result := make([]any, 0, len(item))
		for _, child := range item {
			result = append(result, redactValue(child))
		}
		return result
	default:
		return value
	}
}

func isSecretKey(key string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
	return normalized == "api_key" ||
		normalized == "apikey" ||
		normalized == "authorization" ||
		normalized == "access_token" ||
		normalized == "refresh_token" ||
		normalized == "token" ||
		strings.Contains(normalized, "secret")
}

func decodeJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("usage import payload contains multiple JSON values")
	}
	return nil
}

func formatHour(hour int) string {
	if hour < 0 {
		hour = 0
	}
	hour %= 24
	return fmt.Sprintf("%02d", hour)
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func nonEmpty(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}
