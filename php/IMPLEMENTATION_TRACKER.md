# Implementation Tracker

Track completion status of each feature. Update checkboxes as you implement.

## Priority Items

### P0: Topic Permission Enforcement [DONE]
- [ ] Enforce topic write in `Broker::publish()`
- [ ] Enforce topic read in `Broker::getReady()`
- [ ] Enforce in `Broker::declareQueue()` binding checks
- [ ] Wire up HTTP endpoints in `Http.php`

### P0: Multi-Listener TLS [IN PROGRESS]
- [ ] Add TLS context builder to Broker
- [ ] Wire TLS to MQTT listener
- [ ] Wire TLS to STOMP listener  
- [ ] Wire TLS to Stream listener
- [ ] Config flags per-listener in config.example.toml

### P1: OAuth 2.0 JWT Validation [TODO]
- [ ] Create `OauthBackend.php`
- [ ] Add RS256 token validation
- [ ] Wire into login flow
- [ ] Add logout endpoint

### P1: MQTT QoS 1 + Reason Codes [TODO]
- [ ] Session persistence for in-flight messages
- [ ] Reason code handling in disconnect
- [ ] Properties in publish frames

### P1: Stream Protocol Fetch/Append [TODO]
- [ ] Implement stream status/metadata
- [ ] Implement fetch operation
- [ ] Implement append operation
- [ ] Wire to broker AMQP hooks

### P2: AMQP 1.0 Shim Completion [TODO]
- [ ] SASL PLAIN handshake
- [ ] Link attach/detach state machine
- [ ] Credit flow control
- [ ] Disposition handling
- [ ] Multi-frame transfer

---

## Test Files to Create/Update

- `php/test/topic-permissions.test.php`
- `php/test/tls-multi-listener.test.php`
- `php/test/oauth2.test.php`
- `php/test/mqtt5-qos1.test.php`
- `php/test/streams-protocol.test.php`
- `php/test/amqp10-completion.test.php`
