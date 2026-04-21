#!/usr/bin/env bash

set -euo pipefail

GO_BIN="${GO_BIN:-go}"
if ! command -v "${GO_BIN}" >/dev/null 2>&1; then
  if [ -x /opt/homebrew/bin/go ]; then
    GO_BIN="/opt/homebrew/bin/go"
  else
    echo "[kiro-check] go binary not found" >&2
    exit 127
  fi
fi

echo "[kiro-check] validating Kiro executor request/response shaping"
"${GO_BIN}" test ./internal/runtime/executor -run 'Test(BuildKiroRequestBody|BuildClaudeResponsePayload|BuildClaudeStreamEventsPreserveIncrementalToolUseDeltas|BuildClaudeDataLinesTranslate|RewriteOpenAIResponsesCompactPayload|NormalizeKiroOpenAIResponsesInputPreservesArrayInput)'

echo "[kiro-check] validating Kiro auth/model helpers"
"${GO_BIN}" test ./internal/runtime/executor/helps -run 'Test(ResolveKiroProfileContextUsesFixedBuilderProfile|BuildKiroRegistryModelsFiltersUnsupportedModels|KiroMapModelSupportsDirectCatalogModels)'

echo "[kiro-check] validating Kiro shared translator paths"
"${GO_BIN}" test \
  ./internal/translator/openai/claude \
  ./internal/translator/claude/openai/chat-completions \
  ./internal/translator/claude/openai/responses \
  ./sdk/translator
