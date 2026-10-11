#!/bin/sh
set -eu

# The parity tests use RabbitMQ on fixed loopback ports.
docker run -d --name queueforge-ci-rabbit \
  -e RABBITMQ_DEFAULT_USER=admin \
  -e RABBITMQ_DEFAULT_PASS=devpassword12 \
  -e 'RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=-rabbitmq_stream advertised_host "127.0.0.1" advertised_port 5552' \
  -p 127.0.0.1:5672:5672 -p 127.0.0.1:15672:15672 \
  -p 127.0.0.1:15692:15692 -p 127.0.0.1:1883:1883 \
  -p 127.0.0.1:61613:61613 -p 127.0.0.1:5552:5552 \
  --entrypoint sh rabbitmq:4.3-management -c \
  'rabbitmq-plugins enable --offline rabbitmq_mqtt rabbitmq_stomp rabbitmq_stream rabbitmq_consistent_hash_exchange rabbitmq_event_exchange rabbitmq_shovel rabbitmq_federation rabbitmq_prometheus && exec docker-entrypoint.sh rabbitmq-server'

cleanup() {
  status=$?
  trap - EXIT
  if [ "$status" -ne 0 ]; then docker logs queueforge-ci-rabbit >&2 || true; fi
  docker rm -f queueforge-ci-rabbit >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

attempt=0
until curl -fsS -u admin:devpassword12 http://127.0.0.1:15672/api/overview >/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 120 ]; then
    echo "RabbitMQ did not become ready" >&2
    exit 1
  fi
  sleep 1
done

cd rust
cargo test --workspace -- --test-threads=1
