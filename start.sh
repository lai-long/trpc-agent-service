#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

mkdir -p "$ROOT/bin" "$ROOT/data"
if [[ ! -x "$ROOT/bin/trpc-service" ]]; then
  "$ROOT/build.sh"
fi

PID_FILE="$ROOT/data/trpc-service.pid"
if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "already running: pid=$(cat "$PID_FILE")"
  exit 0
fi

# The Admin API refuses to start without a token; local dev opts out with the
# config.AdminTokenDevInsecure sentinel so the loopback-only listener stays
# reachable (deploy/k8s injects a real secret instead).
export TRPC_ADMIN_TOKEN="${TRPC_ADMIN_TOKEN:-dev-insecure}"

# The mock channel is an unauthenticated message injector: the binary keeps it
# off by default, so the local dev entrypoint opts in explicitly (the k8s
# ConfigMap sets TRPC_MOCK_CHANNEL=false). This is the only place that turns
# it on — a deployment that never reads start.sh stays closed.
export TRPC_MOCK_CHANNEL="${TRPC_MOCK_CHANNEL:-true}"

nohup "$ROOT/bin/trpc-service" serve >"$ROOT/data/trpc-service.log" 2>&1 &
echo $! >"$PID_FILE"
echo "started: pid=$(cat "$PID_FILE")"
