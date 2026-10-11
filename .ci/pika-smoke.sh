#!/bin/sh
set -eu

repo_dir=$PWD
data_dir=$(mktemp -d)
export QUEUEFORGE_DATA_DIR="$data_dir"
export QUEUEFORGE_AMQP_ADDR=127.0.0.1:5672
export QUEUEFORGE_MGMT_ADDR=127.0.0.1:15672
cd rust
./target/debug/queueforge --config configs/queueforge.example.toml --dev-bootstrap &
broker_pid=$!

cleanup() {
  status=$?
  trap - EXIT
  kill -TERM "$broker_pid" 2>/dev/null || true
  wait "$broker_pid" 2>/dev/null || true
  rm -rf "$data_dir"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

attempt=0
until curl -fsS http://127.0.0.1:15672/readyz >/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ] || ! kill -0 "$broker_pid" 2>/dev/null; then
    echo "Broker did not become ready" >&2
    exit 1
  fi
  sleep 0.5
done

"$repo_dir/.ci-tools/pika/bin/python" scripts/pika_smoke.py
