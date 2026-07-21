#!/usr/bin/env python3
"""Minimal pika smoke: declare → publish → consume → ack against a local broker."""

from __future__ import annotations

import os
import sys
import time

try:
    import pika
except ImportError:
    print("pika is required: pip install pika", file=sys.stderr)
    sys.exit(2)


def main() -> int:
    host = os.environ.get("QUEUEFORGE_AMQP_HOST", "127.0.0.1")
    port = int(os.environ.get("QUEUEFORGE_AMQP_PORT", "5672"))
    user = os.environ.get("QUEUEFORGE_ADMIN_USER", "admin")
    password = os.environ.get("QUEUEFORGE_ADMIN_PASSWORD", "devpassword12")
    vhost = os.environ.get("QUEUEFORGE_AMQP_VHOST", "/")
    queue = os.environ.get("QUEUEFORGE_SMOKE_QUEUE", "pika.smoke")
    body = os.environ.get("QUEUEFORGE_SMOKE_BODY", "queueforge-pika-smoke").encode()

    credentials = pika.PlainCredentials(user, password)
    params = pika.ConnectionParameters(
        host=host,
        port=port,
        virtual_host=vhost,
        credentials=credentials,
        connection_attempts=30,
        retry_delay=0.5,
        socket_timeout=5,
        blocked_connection_timeout=10,
    )

    print(f"connecting amqp://{user}:***@{host}:{port}{vhost}")
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=queue, durable=False, auto_delete=True)
    ch.basic_publish(exchange="", routing_key=queue, body=body)

    # Consume one message (poll with short timeout).
    deadline = time.time() + 10.0
    got = None
    while time.time() < deadline:
        method, _props, payload = ch.basic_get(queue=queue, auto_ack=False)
        if method is not None:
            got = payload
            ch.basic_ack(delivery_tag=method.delivery_tag)
            break
        time.sleep(0.05)

    ch.queue_delete(queue=queue)
    conn.close()

    if got is None:
        print("FAIL: no message received", file=sys.stderr)
        return 1
    if got != body:
        print(f"FAIL: body mismatch {got!r} != {body!r}", file=sys.stderr)
        return 1

    print("OK: pika declare/publish/get/ack")
    return 0


if __name__ == "__main__":
    sys.exit(main())
