#!/usr/bin/env bash

set -euo pipefail

GO_BIN="${GO_BIN:-go}"
if ! command -v "${GO_BIN}" >/dev/null 2>&1; then
  if [ -x /opt/homebrew/bin/go ]; then
    GO_BIN="/opt/homebrew/bin/go"
  else
    echo "[usage-check] go binary not found" >&2
    exit 127
  fi
fi

echo "[usage-check] validating embedded CPA-Manager SQLite usage storage"
"${GO_BIN}" test ./internal/usage ./internal/api/handlers/management -run 'Test(RequestStatisticsRecordBuildsEndpointSnapshot|RequestStatisticsPersistsEventsInSQLite|RequestStatisticsMigratesLegacyJSONLToSQLite|ParseImportPayloadAcceptsLegacyUsageExport|GetUsageQueue)'

echo "[usage-check] validating management usage routes and availability gates"
"${GO_BIN}" test ./internal/api -run 'Test(Healthz|ManagementUsageRequiresManagementAuthAndQueuePopsArray|HomeEnabledHidesManagementEndpointsAndControlPanel)'

echo "[usage-check] validating usage queue payload compatibility"
"${GO_BIN}" test ./internal/redisqueue
