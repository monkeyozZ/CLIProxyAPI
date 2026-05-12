package usage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type sqliteUsageStore struct {
	db *sql.DB
}

type sqliteInsertResult struct {
	Inserted int
	Skipped  int
}

func openSQLiteUsageStore(path string) (*sqliteUsageStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	store := &sqliteUsageStore{db: db}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *sqliteUsageStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *sqliteUsageStore) init() error {
	statements := []string{
		`pragma journal_mode = WAL`,
		`pragma synchronous = FULL`,
		`pragma busy_timeout = 5000`,
		`pragma foreign_keys = ON`,
		`create table if not exists usage_events (
			id integer primary key autoincrement,
			request_id text,
			event_hash text not null unique,
			timestamp_ms integer not null,
			timestamp text not null,
			provider text,
			model text not null,
			endpoint text,
			method text,
			path text,
			auth_type text,
			auth_index text,
			source text,
			source_hash text,
			api_key_hash text,
			account_snapshot text,
			auth_label_snapshot text,
			auth_file_snapshot text,
			auth_provider_snapshot text,
			auth_snapshot_at_ms integer,
			input_tokens integer not null default 0,
			output_tokens integer not null default 0,
			reasoning_tokens integer not null default 0,
			cached_tokens integer not null default 0,
			cache_read_tokens integer not null default 0,
			cache_creation_tokens integer not null default 0,
			cache_tokens integer not null default 0,
			total_tokens integer not null default 0,
			latency_ms integer,
			failed integer not null default 0,
			raw_json text,
			created_at_ms integer not null
		)`,
		`create index if not exists idx_usage_events_timestamp on usage_events(timestamp_ms)`,
		`create index if not exists idx_usage_events_request_id on usage_events(request_id)`,
		`create index if not exists idx_usage_events_model on usage_events(model)`,
		`create index if not exists idx_usage_events_auth_index on usage_events(auth_index)`,
		`create index if not exists idx_usage_events_endpoint on usage_events(endpoint)`,
		`create table if not exists dead_letter_events (
			id integer primary key autoincrement,
			payload text not null,
			error text not null,
			created_at_ms integer not null
		)`,
		`create table if not exists settings (
			key text primary key,
			value text not null,
			updated_at_ms integer not null
		)`,
		`create table if not exists model_prices (
			model text primary key,
			prompt_per_1m real not null,
			completion_per_1m real not null,
			cache_per_1m real not null,
			source text,
			source_model_id text,
			raw_json text,
			updated_at_ms integer not null,
			synced_at_ms integer
		)`,
	}
	for _, statement := range statements {
		if _, err := s.db.Exec(statement); err != nil {
			return err
		}
	}
	return s.ensureUsageEventColumns()
}

func (s *sqliteUsageStore) ensureUsageEventColumns() error {
	rows, err := s.db.Query(`pragma table_info(usage_events)`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	existing := map[string]struct{}{}
	for rows.Next() {
		var cid int
		var name string
		var columnType string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			return err
		}
		existing[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	columns := []struct {
		name       string
		definition string
	}{
		{name: "account_snapshot", definition: "text"},
		{name: "auth_label_snapshot", definition: "text"},
		{name: "auth_file_snapshot", definition: "text"},
		{name: "auth_provider_snapshot", definition: "text"},
		{name: "auth_snapshot_at_ms", definition: "integer"},
		{name: "cache_read_tokens", definition: "integer not null default 0"},
		{name: "cache_creation_tokens", definition: "integer not null default 0"},
	}
	for _, column := range columns {
		if _, ok := existing[column.name]; ok {
			continue
		}
		if _, err := s.db.Exec(fmt.Sprintf(
			`alter table usage_events add column %s %s`,
			column.name,
			column.definition,
		)); err != nil {
			return err
		}
	}
	return nil
}

func (s *sqliteUsageStore) InsertEvents(ctx context.Context, events []Event) (sqliteInsertResult, error) {
	if s == nil || s.db == nil || len(events) == 0 {
		return sqliteInsertResult{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return sqliteInsertResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, `insert or ignore into usage_events (
		request_id, event_hash, timestamp_ms, timestamp, provider, model, endpoint, method, path,
		auth_type, auth_index, source, source_hash, api_key_hash,
		account_snapshot, auth_label_snapshot, auth_file_snapshot, auth_provider_snapshot, auth_snapshot_at_ms,
		input_tokens, output_tokens, reasoning_tokens, cached_tokens, cache_read_tokens, cache_creation_tokens,
		cache_tokens, total_tokens, latency_ms, failed, raw_json, created_at_ms
	) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return sqliteInsertResult{}, err
	}
	defer func() { _ = stmt.Close() }()

	result := sqliteInsertResult{}
	for _, event := range events {
		event = normalizeEvent(event)
		res, err := stmt.ExecContext(
			ctx,
			sqliteNullString(event.RequestID),
			event.EventHash,
			event.TimestampMS,
			event.Timestamp,
			sqliteNullString(event.Provider),
			event.Model,
			sqliteNullString(event.Endpoint),
			sqliteNullString(event.Method),
			sqliteNullString(event.Path),
			sqliteNullString(event.AuthType),
			sqliteNullString(event.AuthIndex),
			sqliteNullString(event.Source),
			sqliteNullString(event.SourceHash),
			sqliteNullString(event.APIKeyHash),
			sqliteNullString(event.AccountSnapshot),
			sqliteNullString(event.AuthLabelSnapshot),
			sqliteNullString(event.AuthFileSnapshot),
			sqliteNullString(event.AuthProviderSnapshot),
			sqliteNullPositiveInt64(event.AuthSnapshotAtMS),
			event.InputTokens,
			event.OutputTokens,
			event.ReasoningTokens,
			event.CachedTokens,
			event.CacheReadTokens,
			event.CacheCreationTokens,
			event.CacheTokens,
			event.TotalTokens,
			sqliteNullInt(event.LatencyMS),
			sqliteBoolInt(event.Failed),
			sqliteNullString(event.RawJSON),
			event.CreatedAtMS,
		)
		if err != nil {
			return sqliteInsertResult{}, err
		}
		affected, _ := res.RowsAffected()
		if affected > 0 {
			result.Inserted++
		} else {
			result.Skipped++
		}
	}
	if err := tx.Commit(); err != nil {
		return sqliteInsertResult{}, err
	}
	return result, nil
}

func (s *sqliteUsageStore) Events(ctx context.Context, limit int) ([]Event, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	query := `select
		request_id, event_hash, timestamp_ms, timestamp, provider, model, endpoint, method, path,
		auth_type, auth_index, source, source_hash, api_key_hash,
		account_snapshot, auth_label_snapshot, auth_file_snapshot, auth_provider_snapshot, auth_snapshot_at_ms,
		input_tokens, output_tokens, reasoning_tokens, cached_tokens, cache_read_tokens, cache_creation_tokens,
		cache_tokens, total_tokens, latency_ms, failed, raw_json, created_at_ms
		from usage_events
		order by timestamp_ms asc, id asc`
	args := []any{}
	if limit > 0 {
		query += ` limit ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	events := make([]Event, 0)
	for rows.Next() {
		event, err := scanSQLiteEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func scanSQLiteEvent(rows *sql.Rows) (Event, error) {
	var event Event
	var requestID, provider, endpoint, method, path, authType, authIndex, source, sourceHash, apiKeyHash sql.NullString
	var accountSnapshot, authLabelSnapshot, authFileSnapshot, authProviderSnapshot, rawJSON sql.NullString
	var authSnapshotAt sql.NullInt64
	var latency sql.NullInt64
	var failed int
	if err := rows.Scan(
		&requestID,
		&event.EventHash,
		&event.TimestampMS,
		&event.Timestamp,
		&provider,
		&event.Model,
		&endpoint,
		&method,
		&path,
		&authType,
		&authIndex,
		&source,
		&sourceHash,
		&apiKeyHash,
		&accountSnapshot,
		&authLabelSnapshot,
		&authFileSnapshot,
		&authProviderSnapshot,
		&authSnapshotAt,
		&event.InputTokens,
		&event.OutputTokens,
		&event.ReasoningTokens,
		&event.CachedTokens,
		&event.CacheReadTokens,
		&event.CacheCreationTokens,
		&event.CacheTokens,
		&event.TotalTokens,
		&latency,
		&failed,
		&rawJSON,
		&event.CreatedAtMS,
	); err != nil {
		return Event{}, err
	}
	event.RequestID = requestID.String
	event.Provider = provider.String
	event.Endpoint = endpoint.String
	event.Method = method.String
	event.Path = path.String
	event.AuthType = authType.String
	event.AuthIndex = authIndex.String
	event.Source = source.String
	event.SourceHash = sourceHash.String
	event.APIKeyHash = apiKeyHash.String
	event.AccountSnapshot = accountSnapshot.String
	event.AuthLabelSnapshot = authLabelSnapshot.String
	event.AuthFileSnapshot = authFileSnapshot.String
	event.AuthProviderSnapshot = authProviderSnapshot.String
	if authSnapshotAt.Valid {
		event.AuthSnapshotAtMS = authSnapshotAt.Int64
	}
	event.RawJSON = rawJSON.String
	event.Failed = failed != 0
	if latency.Valid {
		value := latency.Int64
		event.LatencyMS = &value
	}
	return normalizeEvent(event), nil
}

func (s *sqliteUsageStore) Counts(ctx context.Context) (events int64, deadLetters int64, err error) {
	if s == nil || s.db == nil {
		return 0, 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err = s.db.QueryRowContext(ctx, `select count(*) from usage_events`).Scan(&events); err != nil {
		return 0, 0, err
	}
	if err = s.db.QueryRowContext(ctx, `select count(*) from dead_letter_events`).Scan(&deadLetters); err != nil {
		return 0, 0, err
	}
	return events, deadLetters, nil
}

func (s *sqliteUsageStore) LatestEventTimes(ctx context.Context) (lastConsumedAt int64, lastInsertedAt int64, err error) {
	if s == nil || s.db == nil {
		return 0, 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var consumed sql.NullInt64
	var inserted sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `select max(timestamp_ms), max(created_at_ms) from usage_events`).Scan(&consumed, &inserted); err != nil {
		return 0, 0, err
	}
	if consumed.Valid {
		lastConsumedAt = consumed.Int64
	}
	if inserted.Valid {
		lastInsertedAt = inserted.Int64
	}
	return lastConsumedAt, lastInsertedAt, nil
}

func (s *sqliteUsageStore) ExportJSONL(ctx context.Context) ([]byte, error) {
	events, err := s.Events(ctx, 0)
	if err != nil {
		return nil, err
	}
	output := make([]byte, 0)
	for _, event := range events {
		line, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		output = append(output, line...)
		output = append(output, '\n')
	}
	return output, nil
}

func (s *sqliteUsageStore) LoadModelPrices(ctx context.Context) (map[string]ModelPrice, error) {
	if s == nil || s.db == nil {
		return map[string]ModelPrice{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := s.db.QueryContext(ctx, `select
		model, prompt_per_1m, completion_per_1m, cache_per_1m, source, source_model_id, raw_json,
		updated_at_ms, synced_at_ms
		from model_prices order by model`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	prices := map[string]ModelPrice{}
	for rows.Next() {
		var model string
		var price ModelPrice
		var source, sourceModelID, rawJSON sql.NullString
		var syncedAt sql.NullInt64
		if err := rows.Scan(
			&model,
			&price.Prompt,
			&price.Completion,
			&price.Cache,
			&source,
			&sourceModelID,
			&rawJSON,
			&price.UpdatedAtMS,
			&syncedAt,
		); err != nil {
			return nil, err
		}
		price.Source = source.String
		price.SourceModelID = sourceModelID.String
		price.RawJSON = rawJSON.String
		if syncedAt.Valid {
			value := syncedAt.Int64
			price.SyncedAtMS = &value
		}
		prices[model] = price
	}
	return prices, rows.Err()
}

func (s *sqliteUsageStore) SaveModelPrices(ctx context.Context, prices map[string]ModelPrice) error {
	if s == nil || s.db == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `delete from model_prices`); err != nil {
		return err
	}
	if len(prices) == 0 {
		return tx.Commit()
	}

	stmt, err := tx.PrepareContext(ctx, `insert into model_prices (
		model, prompt_per_1m, completion_per_1m, cache_per_1m, source, source_model_id,
		raw_json, updated_at_ms, synced_at_ms
	) values (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	now := time.Now().UnixMilli()
	for model, price := range prices {
		if err := validateModelPrice(model, price); err != nil {
			return err
		}
		if price.UpdatedAtMS <= 0 {
			price.UpdatedAtMS = now
		}
		if _, err := stmt.ExecContext(
			ctx,
			model,
			price.Prompt,
			price.Completion,
			price.Cache,
			sqliteNullString(price.Source),
			sqliteNullString(price.SourceModelID),
			sqliteNullString(price.RawJSON),
			price.UpdatedAtMS,
			sqliteNullInt(price.SyncedAtMS),
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *sqliteUsageStore) UpsertModelPrices(ctx context.Context, prices map[string]ModelPrice) (ModelPriceSyncResult, error) {
	if s == nil || s.db == nil || len(prices) == 0 {
		return ModelPriceSyncResult{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ModelPriceSyncResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, `insert into model_prices (
		model, prompt_per_1m, completion_per_1m, cache_per_1m, source, source_model_id,
		raw_json, updated_at_ms, synced_at_ms
	) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
	on conflict(model) do update set
		prompt_per_1m = excluded.prompt_per_1m,
		completion_per_1m = excluded.completion_per_1m,
		cache_per_1m = excluded.cache_per_1m,
		source = excluded.source,
		source_model_id = excluded.source_model_id,
		raw_json = excluded.raw_json,
		updated_at_ms = excluded.updated_at_ms,
		synced_at_ms = excluded.synced_at_ms`)
	if err != nil {
		return ModelPriceSyncResult{}, err
	}
	defer func() { _ = stmt.Close() }()

	now := time.Now().UnixMilli()
	result := ModelPriceSyncResult{Source: modelPriceSyncSource}
	for model, price := range prices {
		if err := validateModelPrice(model, price); err != nil {
			result.Skipped++
			continue
		}
		if price.Source == "" {
			price.Source = modelPriceSyncSource
		}
		if price.SourceModelID == "" {
			price.SourceModelID = model
		}
		price.UpdatedAtMS = now
		price.SyncedAtMS = &now
		if _, err := stmt.ExecContext(
			ctx,
			model,
			price.Prompt,
			price.Completion,
			price.Cache,
			sqliteNullString(price.Source),
			sqliteNullString(price.SourceModelID),
			sqliteNullString(price.RawJSON),
			price.UpdatedAtMS,
			sqliteNullInt(price.SyncedAtMS),
		); err != nil {
			return ModelPriceSyncResult{}, err
		}
		result.Imported++
	}
	if err := tx.Commit(); err != nil {
		return ModelPriceSyncResult{}, err
	}
	result.Prices, err = s.LoadModelPrices(ctx)
	if err != nil {
		return ModelPriceSyncResult{}, err
	}
	return result, nil
}

func (s *sqliteUsageStore) AddDeadLetter(ctx context.Context, payload string, parseErr error) error {
	if s == nil || s.db == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if parseErr == nil {
		parseErr = errors.New("unknown parse error")
	}
	_, err := s.db.ExecContext(
		ctx,
		`insert into dead_letter_events(payload, error, created_at_ms) values(?, ?, ?)`,
		payload,
		parseErr.Error(),
		time.Now().UnixMilli(),
	)
	return err
}

func validateModelPrice(model string, price ModelPrice) error {
	if model == "" {
		return errors.New("model is required")
	}
	if !validPriceValue(price.Prompt) || !validPriceValue(price.Completion) || !validPriceValue(price.Cache) {
		return fmt.Errorf("invalid model price for %s", model)
	}
	return nil
}

func sqliteNullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func sqliteNullInt(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func sqliteNullPositiveInt64(value int64) any {
	if value <= 0 {
		return nil
	}
	return value
}

func sqliteBoolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func validPriceValue(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
