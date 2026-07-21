#!/usr/bin/env bash
# Start a local broker (if needed) and run scripts/pika_smoke.py.
# Env:
#   QUEUEFORGE_SMOKE_START=1  (default) start broker in background
#   QUEUEFORGE_SMOKE_START=0  assume broker already on :5672 / :15672
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export QUEUEFORGE_ADMIN_USER="${QUEUEFORGE_ADMIN_USER:-admin}"
export QUEUEFORGE_ADMIN_PASSWORD="${QUEUEFORGE_ADMIN_PASSWORD:-devpassword12}"
MGMT_URL="${QUEUEFORGE_MGMT_URL:-http://127.0.0.1:15672}"
START="${QUEUEFORGE_SMOKE_START:-1}"
DATA_DIR="${QUEUEFORGE_SMOKE_DATA:-$(mktemp -d /tmp/queueforge-pika-XXXXXX)}"
BROKER_PID=""

cleanup() {
  if [[ -n "${BROKER_PID}" ]] && kill -0 "${BROKER_PID}" 2>/dev/null; then
    kill -TERM "${BROKER_PID}" 2>/dev/null || true
    wait "${BROKER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

if [[ "${START}" == "1" ]]; then
  if [[ ! -x target/debug/queueforge && ! -x target/release/queueforge ]]; then
    cargo build -p queueforge-broker
  fi
  BIN=target/debug/queueforge
  [[ -x target/release/queueforge ]] && BIN=target/release/queueforge
  [[ -x target/debug/queueforge ]] && BIN=target/debug/queueforge

  mkdir -p "${DATA_DIR}"
  # Prefer example config but override data dir + binds via env.
  export QUEUEFORGE_CONFIG="${QUEUEFORGE_CONFIG:-configs/queueforge.example.toml}"
  export QUEUEFORGE_DATA_DIR="${DATA_DIR}"
  export QUEUEFORGE_AMQP_ADDR="${QUEUEFORGE_AMQP_ADDR:-127.0.0.1:5672}"
  export QUEUEFORGE_MGMT_ADDR="${QUEUEFORGE_MGMT_ADDR:-127.0.0.1:15672}"

  echo "starting broker (${BIN}) data_dir=${DATA_DIR}"
  "${BIN}" --config "${QUEUEFORGE_CONFIG}" --dev-bootstrap &
  BROKER_PID=$!

  # Wait for readiness on management listener.
  for i in $(seq 1 60); do
    if curl -fsS "${MGMT_URL}/readyz" >/dev/null 2>&1; then
      echo "broker ready"
      break
    fi
    if ! kill -0 "${BROKER_PID}" 2>/dev/null; then
      echo "broker exited early" >&2
      exit 1
    fi
    sleep 0.5
    if [[ "$i" -eq 60 ]]; then
      echo "timeout waiting for ${MGMT_URL}/readyz" >&2
      exit 1
    fi
  done
fi

PYTHON=python3
if ! "${PYTHON}" -c 'import pika' 2>/dev/null; then
  if [[ -z "${VIRTUAL_ENV:-}" ]]; then
    VENV="${ROOT}/.venv-pika-smoke"
    if [[ ! -x "${VENV}/bin/python" ]]; then
      python3 -m venv "${VENV}"
      "${VENV}/bin/pip" install -q pika
    fi
    PYTHON="${VENV}/bin/python"
  else
    pip install -q pika
  fi
fi

"${PYTHON}" "${ROOT}/scripts/pika_smoke.py"
