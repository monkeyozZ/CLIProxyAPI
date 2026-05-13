package usage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	postgresUsageEventsTable      = "usage_events"
	postgresDeadLetterEventsTable = "usage_dead_letter_events"
	postgresModelPricesTable      = "usage_model_prices"
)

type postgresUsageStore struct {
	db               *sql.DB
	usageEvents      string
	deadLetterEvents string
	modelPrices      string
}

func openConfiguredPostgresUsageStore(ctx context.Context) (*postgresUsageStore, error) {
	dsn := lookupUsageEnv("USAGE_POSTGRES_DSN", "usage_postgres_dsn")
	if dsn == "" {
		dsn = lookupUsageEnv("PGSTORE_DSN", "pgstore_dsn")
	}
	if dsn == "" {
		return nil, nil
	}
	schema := lookupUsageEnv("USAGE_POSTGRES_SCHEMA", "usage_postgres_schema")
	if schema == "" {
		schema = lookupUsageEnv("PGSTORE_SCHEMA", "pgstore_schema")
	}

	ctxOpen := ctx
	cancel := func() {}
	if ctxOpen == nil {
		ctxOpen = context.Background()
	}
	if _, ok := ctxOpen.Deadline(); !ok {
		ctxOpen, cancel = context.WithTimeout(ctxOpen, 10*time.Second)
	}
	defer cancel()
	return openPostgresUsageStore(ctxOpen, dsn, schema)
}

func openPostgresUsageStore(ctx context.Context, dsn, schema string) (*postgresUsageStore, error) {
	db, err := sql.Open("pgx", strings.TrimSpace(dsn))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(30 * time.Minute)

	store := &postgresUsageStore{
		db:               db,
		usageEvents:      postgresTableName(schema, postgresUsageEventsTable),
		deadLetterEvents: postgresTableName(schema, postgresDeadLetterEventsTable),
		modelPrices:      postgresTableName(schema, postgresModelPricesTable),
	}
	if err := store.init(ctx, schema); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *postgresUsageStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *postgresUsageStore) init(ctx context.Context, schema string) error {
	if s == nil || s.db == nil {
		return nil
	}
	if err := s.db.PingContext(ctx); err != nil {
		return err
	}
	if schema = strings.TrimSpace(schema); schema != "" {
		if _, err := s.db.ExecContext(ctx, "create schema if not exists "+quotePostgresIdentifier(schema)); err != nil {
			return err
		}
	}

	statements := []string{
		fmt.Sprintf(`create table if not exists %s (
			id bigserial primary key,
			request_id text,
			event_hash text not null unique,
			timestamp_ms bigint not null,
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
			auth_snapshot_at_ms bigint,
			input_tokens bigint not null default 0,
			output_tokens bigint not null default 0,
			reasoning_tokens bigint not null default 0,
			cached_tokens bigint not null default 0,
			cache_read_tokens bigint not null default 0,
			cache_creation_tokens bigint not null default 0,
			cache_tokens bigint not null default 0,
			total_tokens bigint not null default 0,
			latency_ms bigint,
			failed boolean not null default false,
			raw_json text,
			created_at_ms bigint not null
		)`, s.usageEvents),
		fmt.Sprintf(`create index if not exists %s on %s(timestamp_ms)`, postgresIndexName(schema, "idx_usage_events_timestamp"), s.usageEvents),
		fmt.Sprintf(`create index if not exists %s on %s(request_id)`, postgresIndexName(schema, "idx_usage_events_request_id"), s.usageEvents),
		fmt.Sprintf(`create index if not exists %s on %s(model)`, postgresIndexName(schema, "idx_usage_events_model"), s.usageEvents),
		fmt.Sprintf(`create index if not exists %s on %s(auth_index)`, postgresIndexName(schema, "idx_usage_events_auth_index"), s.usageEvents),
		fmt.Sprintf(`create index if not exists %s on %s(endpoint)`, postgresIndexName(schema, "idx_usage_events_endpoint"), s.usageEvents),
		fmt.Sprintf(`create table if not exists %s (
			id bigserial primary key,
			payload text not null,
			error text not null,
			created_at_ms bigint not null
		)`, s.deadLetterEvents),
		fmt.Sprintf(`create table if not exists %s (
			model text primary key,
			prompt_per_1m double precision not null,
			completion_per_1m double precision not null,
			cache_per_1m double precision not null,
			source text,
			source_model_id text,
			raw_json text,
			updated_at_ms bigint not null,
			synced_at_ms bigint
		)`, s.modelPrices),
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (s *postgresUsageStore) InsertEvents(ctx context.Context, events []Event) (sqliteInsertResult, error) {
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

	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`insert into %s (
		request_id, event_hash, timestamp_ms, timestamp, provider, model, endpoint, method, path,
		auth_type, auth_index, source, source_hash, api_key_hash,
		account_snapshot, auth_label_snapshot, auth_file_snapshot, auth_provider_snapshot, auth_snapshot_at_ms,
		input_tokens, output_tokens, reasoning_tokens, cached_tokens, cache_read_tokens, cache_creation_tokens,
		cache_tokens, total_tokens, latency_ms, failed, raw_json, created_at_ms
	) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
		$20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31)
	on conflict(event_hash) do nothing`, s.usageEvents))
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
			event.Failed,
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

func (s *postgresUsageStore) Events(ctx context.Context, limit int) ([]Event, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	query := fmt.Sprintf(`select
		request_id, event_hash, timestamp_ms, timestamp, provider, model, endpoint, method, path,
		auth_type, auth_index, source, source_hash, api_key_hash,
		account_snapshot, auth_label_snapshot, auth_file_snapshot, auth_provider_snapshot, auth_snapshot_at_ms,
		input_tokens, output_tokens, reasoning_tokens, cached_tokens, cache_read_tokens, cache_creation_tokens,
		cache_tokens, total_tokens, latency_ms, failed, raw_json, created_at_ms
		from %s
		order by timestamp_ms asc, id asc`, s.usageEvents)
	args := []any{}
	if limit > 0 {
		query += ` limit $1`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	events := make([]Event, 0)
	for rows.Next() {
		event, err := scanPostgresEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *postgresUsageStore) ClearEvents(ctx context.Context, startMS, endMS *int64) error {
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

	eventWhere, eventArgs := clearRangeWhereClause("timestamp_ms", startMS, endMS, postgresPlaceholder)
	deadLetterWhere, deadLetterArgs := clearRangeWhereClause("created_at_ms", startMS, endMS, postgresPlaceholder)
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`delete from %s%s`, s.usageEvents, eventWhere), eventArgs...); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`delete from %s%s`, s.deadLetterEvents, deadLetterWhere), deadLetterArgs...); err != nil {
		return err
	}
	return tx.Commit()
}

func scanPostgresEvent(rows *sql.Rows) (Event, error) {
	var event Event
	var requestID, provider, endpoint, method, path, authType, authIndex, source, sourceHash, apiKeyHash sql.NullString
	var accountSnapshot, authLabelSnapshot, authFileSnapshot, authProviderSnapshot, rawJSON sql.NullString
	var authSnapshotAt sql.NullInt64
	var latency sql.NullInt64
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
		&event.Failed,
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
	if latency.Valid {
		value := latency.Int64
		event.LatencyMS = &value
	}
	return normalizeEvent(event), nil
}

func (s *postgresUsageStore) LoadModelPrices(ctx context.Context) (map[string]ModelPrice, error) {
	if s == nil || s.db == nil {
		return map[string]ModelPrice{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`select
		model, prompt_per_1m, completion_per_1m, cache_per_1m, source, source_model_id, raw_json,
		updated_at_ms, synced_at_ms
		from %s order by model`, s.modelPrices))
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

func (s *postgresUsageStore) SaveModelPrices(ctx context.Context, prices map[string]ModelPrice) error {
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

	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`delete from %s`, s.modelPrices)); err != nil {
		return err
	}
	if len(prices) == 0 {
		return tx.Commit()
	}

	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`insert into %s (
		model, prompt_per_1m, completion_per_1m, cache_per_1m, source, source_model_id,
		raw_json, updated_at_ms, synced_at_ms
	) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)`, s.modelPrices))
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

func (s *postgresUsageStore) UpsertModelPrices(ctx context.Context, prices map[string]ModelPrice) (ModelPriceSyncResult, error) {
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

	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`insert into %s (
		model, prompt_per_1m, completion_per_1m, cache_per_1m, source, source_model_id,
		raw_json, updated_at_ms, synced_at_ms
	) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	on conflict(model) do update set
		prompt_per_1m = excluded.prompt_per_1m,
		completion_per_1m = excluded.completion_per_1m,
		cache_per_1m = excluded.cache_per_1m,
		source = excluded.source,
		source_model_id = excluded.source_model_id,
		raw_json = excluded.raw_json,
		updated_at_ms = excluded.updated_at_ms,
		synced_at_ms = excluded.synced_at_ms`, s.modelPrices))
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

func (s *postgresUsageStore) AddDeadLetter(ctx context.Context, payload string, parseErr error) error {
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
		fmt.Sprintf(`insert into %s(payload, error, created_at_ms) values($1, $2, $3)`, s.deadLetterEvents),
		payload,
		parseErr.Error(),
		time.Now().UnixMilli(),
	)
	return err
}

func lookupUsageEnv(keys ...string) string {
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok {
			if trimmed := strings.TrimSpace(value); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func postgresTableName(schema, table string) string {
	quotedTable := quotePostgresIdentifier(table)
	if schema = strings.TrimSpace(schema); schema != "" {
		return quotePostgresIdentifier(schema) + "." + quotedTable
	}
	return quotedTable
}

func postgresIndexName(schema, index string) string {
	if schema = strings.TrimSpace(schema); schema != "" {
		return quotePostgresIdentifier(schema) + "." + quotePostgresIdentifier(index)
	}
	return quotePostgresIdentifier(index)
}

func quotePostgresIdentifier(value string) string {
	return `"` + strings.ReplaceAll(strings.TrimSpace(value), `"`, `""`) + `"`
}
