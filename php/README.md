# QueueForge (PHP)

The PHP process is the same single-node classic broker for the AMQP paths below. It is not a full copy of the Rust and Bun processes.

Same as Rust and Bun:

- AMQP 0-9-1 login, channels, and the default exchange
- direct, fanout, and topic exchanges, plus queue bindings
- the built-in `amq.direct`, `amq.fanout`, `amq.topic`, and `amq.headers` exchanges
- durable classic queues, publisher confirms after the covering fsync, manual acks
- `basic.nack` with requeue
- `basic.return` when a publish is mandatory and matches nothing
- restart redelivery of a durable message that was not acked

Not the same:

- cluster protocol version 1, so a PHP process cannot sit in a Rust or Bun member list
- quorum queues
- header exchanges beyond declaring the type, alternate exchanges, TTL, dead-letter, max-length, and priority
- management UI, Prometheus, and TLS
- MQTT, STOMP, and streams

```bash
php bin/queueforge --config config.example.toml --dev-bootstrap
php test/roundtrip.php
php test/parity.php
```

AMQP listens on `127.0.0.1:5675` in the example config. `--dev-bootstrap` creates `admin` / `devpassword12` when the data directory has no users.
