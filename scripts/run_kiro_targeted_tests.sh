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
"${GO_BIN}" test ./internal/runtime/executor -run 'Test(Kiro|BuildKiro|BuildClaude|NormalizeClaudeStreamEventIndices|ParseAnthropicAssistantMessage|RewriteOpenAIResponsesCompactPayload|NormalizeKiro)'

echo "[kiro-check] validating Kiro auth/model helpers"
"${GO_BIN}" test ./internal/runtime/executor/helps -run 'Test(Kiro|ResolveKiro|ApplyKiro|BuildKiro)'

echo "[kiro-check] validating Kiro management and SDK integration"
"${GO_BIN}" test ./internal/api/handlers/management ./sdk/cliproxy -run 'Test(Kiro|NormalizeOAuthProviderSupportsKiro)'

echo "[kiro-check] validating Kiro shared translator paths"
"${GO_BIN}" test \
  ./internal/translator/openai/claude \
  ./internal/translator/claude/openai/chat-completions \
  ./internal/translator/claude/openai/responses \
  ./sdk/translator
