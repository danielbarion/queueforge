#!/bin/sh
# A private daemon keeps fixture ports and bind mounts inside this CI step.
set -eu

daemon_dir=$(mktemp -d)
export DOCKER_HOST="unix://$daemon_dir/docker.sock"
unset DOCKER_TLS_VERIFY DOCKER_CERT_PATH
dockerd --host="$DOCKER_HOST" --data-root="$daemon_dir/data" \
  --exec-root="$daemon_dir/exec" --pidfile="$daemon_dir/docker.pid" \
  --storage-driver=vfs > "$daemon_dir/docker.log" 2>&1 &
daemon_pid=$!

cleanup() {
  status=$?
  trap - EXIT
  if [ "$status" -ne 0 ]; then cat "$daemon_dir/docker.log" >&2; fi
  kill "$daemon_pid" 2>/dev/null || true
  wait "$daemon_pid" 2>/dev/null || true
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

attempt=0
until docker info >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ] || ! kill -0 "$daemon_pid" 2>/dev/null; then
    echo "Docker daemon did not become ready" >&2
    exit 1
  fi
  sleep 1
done

"$@"
