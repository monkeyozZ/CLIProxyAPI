#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/dev-local.sh [--build-only] [--watch] [--help] [-- <extra server args...>]

Build CLIProxyAPI from local source and run it with local-friendly defaults.

Options:
  --build-only  Build the binary and exit without starting the server
  --watch       Rebuild and restart automatically when Go sources change
  --help        Show this help message
  --            Pass the remaining arguments to the server binary

Environment overrides:
  GO_BIN                Go executable path
  CLIPROXY_BIN          Output binary path (default: ./cli-proxy-api)
  CLIPROXY_CONFIG       Config file path (default: ./config.yaml)
  CLIPROXY_LOCAL_MODEL  Set to 0 to disable --local-model (default: 1)
  CLIPROXY_WATCH_INTERVAL
                        Poll interval in seconds for --watch (default: 1)
  GOPATH                Go workspace (default: /tmp/go)
  GOMODCACHE            Go module cache (default: /tmp/go/pkg/mod)
  GOCACHE               Go build cache (default: /tmp/go-build)
EOF
}

GO_BIN="${GO_BIN:-}"
if [[ -z "${GO_BIN}" ]]; then
  if command -v go >/dev/null 2>&1; then
    GO_BIN="$(command -v go)"
  elif [[ -x "/opt/homebrew/bin/go" ]]; then
    GO_BIN="/opt/homebrew/bin/go"
  else
    echo "Error: go executable not found. Set GO_BIN or install Go first." >&2
    exit 1
  fi
fi

CLIPROXY_BIN="${CLIPROXY_BIN:-./cli-proxy-api}"
CLIPROXY_CONFIG="${CLIPROXY_CONFIG:-./config.yaml}"
CLIPROXY_LOCAL_MODEL="${CLIPROXY_LOCAL_MODEL:-1}"
CLIPROXY_WATCH_INTERVAL="${CLIPROXY_WATCH_INTERVAL:-1}"

export GOPATH="${GOPATH:-/tmp/go}"
export GOMODCACHE="${GOMODCACHE:-/tmp/go/pkg/mod}"
export GOCACHE="${GOCACHE:-/tmp/go-build}"

BUILD_ONLY=0
WATCH_MODE=0
SERVER_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-only)
      BUILD_ONLY=1
      shift
      ;;
    --watch)
      WATCH_MODE=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      shift
      SERVER_ARGS+=("$@")
      break
      ;;
    *)
      SERVER_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ ! -f "${CLIPROXY_CONFIG}" ]]; then
  echo "Error: config file not found: ${CLIPROXY_CONFIG}" >&2
  exit 1
fi

mkdir -p "${GOPATH}" "${GOMODCACHE}" "${GOCACHE}"

now_ts() {
  date '+%Y-%m-%d %H:%M:%S'
}

log_info() {
  printf '[%s] %s\n' "$(now_ts)" "$*"
}

build_binary() {
  log_info "Building local binary: ${CLIPROXY_BIN}"
  "${GO_BIN}" build -o "${CLIPROXY_BIN}" ./cmd/server
}

capture_watch_state() {
  local paths=()
  local state

  if [[ -d "cmd" ]]; then
    paths+=("cmd")
  fi
  if [[ -d "internal" ]]; then
    paths+=("internal")
  fi
  if [[ -d "sdk" ]]; then
    paths+=("sdk")
  fi
  if [[ -f "go.mod" ]]; then
    paths+=("go.mod")
  fi
  if [[ -f "go.sum" ]]; then
    paths+=("go.sum")
  fi

  if [[ ${#paths[@]} -eq 0 ]]; then
    echo ""
    return
  fi

  state="$(
    /usr/bin/find "${paths[@]}" -type f \( -name '*.go' -o -name 'go.mod' -o -name 'go.sum' \) -print \
      | /usr/bin/sort \
      | /usr/bin/xargs /usr/bin/shasum 2>/dev/null \
      | /usr/bin/shasum \
      | /usr/bin/awk '{print $1}'
  )"
  echo "${state}"
}

start_server() {
  local action="${1:-Started}"
  local change_time="${2:-}"
  "${CLIPROXY_BIN}" "${RUN_ARGS[@]}" &
  SERVER_PID=$!
  if [[ -n "${change_time}" ]]; then
    log_info "${action} local source server. change_time=${change_time} pid=${SERVER_PID}"
  else
    log_info "${action} local source server. pid=${SERVER_PID}"
  fi
}

stop_server() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  SERVER_PID=""
}

cleanup() {
  stop_server
}

build_binary

if [[ "${BUILD_ONLY}" == "1" ]]; then
  log_info "Build finished."
  exit 0
fi

RUN_ARGS=(--config "${CLIPROXY_CONFIG}")
if [[ "${CLIPROXY_LOCAL_MODEL}" != "0" ]]; then
  RUN_ARGS+=(--local-model)
fi
if [[ ${#SERVER_ARGS[@]} -gt 0 ]]; then
  RUN_ARGS+=("${SERVER_ARGS[@]}")
fi

log_info "Starting local source server with config: ${CLIPROXY_CONFIG}"

unset ALL_PROXY HTTP_PROXY HTTPS_PROXY all_proxy http_proxy https_proxy

if [[ "${WATCH_MODE}" == "0" ]]; then
  log_info "Press Ctrl+C to stop."
  exec "${CLIPROXY_BIN}" "${RUN_ARGS[@]}"
fi

log_info "Watch mode enabled."
log_info "Go source changes trigger rebuild + restart."
log_info "config.yaml and auths keep using in-process hot reload."
log_info "Press Ctrl+C to stop."

trap cleanup EXIT INT TERM

LAST_STATE="$(capture_watch_state)"
SERVER_PID=""
start_server "Started"

while true; do
  sleep "${CLIPROXY_WATCH_INTERVAL}"
  CURRENT_STATE="$(capture_watch_state)"
  if [[ "${CURRENT_STATE}" != "${LAST_STATE}" ]]; then
    CHANGE_DETECTED_AT="$(now_ts)"
    log_info "Source change detected. change_time=${CHANGE_DETECTED_AT}"
    if build_binary; then
      stop_server
      start_server "Restarted" "${CHANGE_DETECTED_AT}"
      LAST_STATE="${CURRENT_STATE}"
    else
      log_info "Build failed. Keeping previous server process. pid=${SERVER_PID:-none}"
    fi
  fi
done
