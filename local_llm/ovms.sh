#!/usr/bin/env bash
# Manage native OVMS (no Docker).
#
# Usage:
#   ./ovms.sh start gpu      start GPU instance (REST :8000, gRPC :9000)
#   ./ovms.sh stop gpu
#   ./ovms.sh status

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODEL_DIR="$SCRIPT_DIR/models/qwen2.5-7b-openvino"
RUNTIME_DIR="/tmp/ovms_runtime"
PID_FILE="$RUNTIME_DIR/ovms-gpu.pid"
LOG_FILE="$RUNTIME_DIR/ovms-gpu.log"
OVMS_BIN="${HOME}/.local/opt/ovms/bin/ovms"
OVMS_LIBS="${HOME}/.local/opt/ovms/lib"

# ── helpers ──────────────────────────────────────────────────────────────────

prepare_configs() {
  mkdir -p "$RUNTIME_DIR"

  # graph.pbtxt: replace /model with actual model dir (fixes: models_path: "/model/1")
  sed "s|/model|${MODEL_DIR}|g" \
    "$MODEL_DIR/graph.pbtxt" > "$RUNTIME_DIR/graph.pbtxt"

  # ovms_config.json: point graph_path to the runtime (patched) graph.pbtxt
  sed "s|/model/graph\.pbtxt|${RUNTIME_DIR}/graph.pbtxt|g" \
    "$MODEL_DIR/ovms_config.json" > "$RUNTIME_DIR/ovms_config.json"
}

start_gpu() {
  if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "ovms-gpu is already running (PID $(cat "$PID_FILE"))"
    return
  fi

  prepare_configs

  echo "Starting GPU instance on REST :8000 / gRPC :9000..."

  LD_LIBRARY_PATH="${OVMS_LIBS}:/usr/lib/wsl/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
  "$OVMS_BIN" \
    --config_path "$RUNTIME_DIR/ovms_config.json" \
    --port 9000 \
    --rest_port 8000 \
    >> "$LOG_FILE" 2>&1 &

  echo $! > "$PID_FILE"
  echo "ovms-gpu started (PID $(cat "$PID_FILE")) — waiting for model to load..."
  wait_ready
}

stop_gpu() {
  if [ ! -f "$PID_FILE" ]; then
    echo "ovms-gpu is not running (no PID file)"
    return
  fi
  local pid
  pid=$(cat "$PID_FILE")
  if kill -0 "$pid" 2>/dev/null; then
    echo "Stopping ovms-gpu (PID $pid)..."
    kill "$pid"
    rm -f "$PID_FILE"
  else
    echo "ovms-gpu PID $pid not running (stale PID file removed)"
    rm -f "$PID_FILE"
  fi
}

wait_ready() {
  local tries=0
  until grep -q "state changed to: AVAILABLE" "$LOG_FILE" 2>/dev/null; do
    sleep 3
    tries=$((tries + 1))
    printf '.'
    if [ $tries -ge 60 ]; then
      echo ""
      echo "Timed out waiting for OVMS. Check: $LOG_FILE"
      exit 1
    fi
  done
  echo ""
  echo "ovms-gpu is AVAILABLE — REST http://localhost:8000  gRPC localhost:9000"
}

show_status() {
  echo "=== OVMS instances ==="
  if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "  ovms-gpu  RUNNING  PID=$(cat "$PID_FILE")  → http://localhost:8000"
    echo "  log: $LOG_FILE"
  else
    echo "  ovms-gpu  stopped"
  fi

  echo ""
  echo "=== Available OpenVINO devices ==="
  python3 -c "
import openvino as ov
core = ov.Core()
for d in core.available_devices:
    try:
        name = core.get_property(d, 'FULL_DEVICE_NAME')
    except Exception:
        name = '(unknown)'
    print(f'  {d}: {name}')
" 2>/dev/null || echo "  (openvino not installed in host env)"
}

# ── dispatch ─────────────────────────────────────────────────────────────────

ACTION=${1:-}
TARGET=${2:-}

case "$ACTION" in
  start)
    case "$TARGET" in
      gpu) start_gpu ;;
      *)   echo "Usage: $0 start gpu"; exit 1 ;;
    esac
    ;;
  stop)
    case "$TARGET" in
      gpu) stop_gpu ;;
      *)   echo "Usage: $0 stop gpu"; exit 1 ;;
    esac
    ;;
  status) show_status ;;
  *)
    echo "Usage: $0 {start|stop} gpu"
    echo "       $0 status"
    exit 1
    ;;
esac
