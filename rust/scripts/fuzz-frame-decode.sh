#!/usr/bin/env bash
# Lightweight libFuzzer-free fuzz loop for AMQP frame decode.
# Usage: scripts/fuzz-frame-decode.sh [seconds]
# Full cargo-fuzz (nightly): cd crates/queueforge-amqp && cargo +nightly fuzz run frame_decode
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SECONDS_LIMIT="${1:-10}"
echo "Running queueforge-amqp fuzz_random_loop for ~${SECONDS_LIMIT}s (QUEUEFORGE_FUZZ_ITERS per pass)..."

export QUEUEFORGE_FUZZ_ITERS="${QUEUEFORGE_FUZZ_ITERS:-5000}"
end=$((SECONDS + SECONDS_LIMIT))
pass=0
while (( SECONDS < end )); do
  pass=$((pass + 1))
  cargo test -p queueforge-amqp --lib fuzz_skeleton::fuzz_random_loop -- --nocapture
done
echo "Completed ${pass} pass(es)."
