# QueueForge PHP Broker - Implementation Plan

**Goal:** Close remaining RabbitMQ parity gaps in the PHP broker to reach feature completeness.

---

## 1. Topic Permissions Enforcement

### Problem
Topic permissions data structure exists (`Broker::$topicPermissions`) but is never checked in routing code paths.

### Tasks

#### A. Enforce topic write permissions in `publish()`
**File:** `php/src/Broker.php` | **Line:** ~396-405  
**Change:** Add permission check before routing publishes to exchanges:
```php
// In publish() method, after $exchange parameter validation and before route():
if (!$this->topicWriteAllowed($user, '/', $exchange, $key)) {
    throw new RuntimeException(
        "ACCESS_REFUSED - topic permission denied for '$exchange' key '$key'", 
        403
    );
}
```

**File:** `php/src/Broker.php` | **Line:** ~764-805  
**Change:** Similar check in the routing loop, specifically around cluster forwarding:
```php
// In quorumPublish() and publish loops where remote nodes are addressed
if ($this->cluster !== null && !$this->topicWriteAllowed($user, '/', $exchange, $key)) {
    // Block forwarding to peer nodes for unauthorized topics
    throw new RuntimeException(
        "ACCESS_REFUSED - topic permission denied on cluster hop", 
        403
    );
}
```

#### B. Enforce topic read permissions in `getReady()` and message retrieval
**File:** `php/src/Broker.php` | **Line:** ~1697-1718  
**Change:** Check read permission during basic.get operations:
```php
public function getReady(string $queue): ?int
{
    ...
    $msg = $this->msgs[$id];
    
    // New: enforce read permission on retrieved message routing key
    if (!$this->topicWriteAllowed($user, '/', $msg['exchange'] ?? '', $msg['key'])) {
        continue; // Skip this message, try next in queue
    }
    
    ...
}
```

#### C. Enforce topic permissions in `declareQueue()` 
**File:** `php/src/Broker.php` | **Line:** ~343-400  
**Change:** When binding a new queue, validate the user's topic permissions for the target exchange:
```php
// After exchange validation near line 471-472
if (isset($this->exchanges[$exchange]) && !$this->topicWriteAllowed($user, '/', $exchange, $key)) {
    throw new RuntimeException(
        "ACCESS_REFUSED - cannot bind queue to exchange without topic write permission", 
        403
    );
}
```

#### D. Add topic management API endpoints
**File:** `php/src/Http.php`  
**New routes to implement:**
- `PUT /api/topic-permissions/{user}/{exchange}` (already exists, needs implementation)
- `DELETE /api/topic-permissions/{user}/{exchange}/{pattern}` (exists, wire it up)

**Current status:** Routes exist at lines ~624-639 but just store data without enforcement. The change above makes them effective.

---

## 2. AMQP 1.0 Shim Completion

### Problem
`Amqp10.php` only implements basic frame parsing and one transfer path. Missing: disposition, credit accounting, multi-frame transfers, full protocol state machine.

### Tasks

#### A. Add SASL PLAIN handshake support
**File:** `php/src/Amqp10.php` | **Line:** ~76-80  
**Add:** Challenge-response for SASL-PLAIN:
```php
private function saslPlain(string $body): string
{
    // Parse mechanism and response header
    if (str_contains($body, "\xc2")) {  // symbol PLAIN
        $username = "guest";  // Default fallback
        $password = "guest";
        
        // In real impl: parse username/password from SASL payload
        return "\x00\x53\x44" . chr(6) . chr(1) . "\x28\x00\u{0}"; // success
    }
    return "";  // Reject
}

// Call in onFrame() around line 79-80:
if (self::has($body, 0x40)) {  // sasl-mechanisms
    return $this->saslPlain("\xa0" . $body);
}
```

#### B. Implement link attach/detach state machine
**File:** `php/src/Amqp10.php` | **Line:** ~75-121  
**Extend:** Track per-link state including:
- Sender/receiver flags
- Credit window
- Max frame size
- Container ID validation

```php
private string $containerId = null;
private array $links = [];  // linkName => ['sender' => bool, 'credit' => int]

// In onFrame(), handle attach (0x10):
if (self::has($body, 0x10)) {
    // Parse link name from payload
    $linkName = $this->parseLinkName($body);
    $attachFlags = ($body[2] & 0x10) !== 0;  // sender flag
    
    $this->links[$linkName] = [
        'sender' => $attachFlags, 
        'credit' => 0,
        'maxFrameSize' => 131072
    ];
    
    return $this->frame(1, $body);  // Return attached performative
}
```

#### C. Implement credit-based flow control
**File:** `php/src/Amqp10.php` | **Line:** ~110-115  
**Add:** Credit accounting to delivery limit messages:

```php
if (str_contains($body, "\x10")) {  // attach or settle transfer
    $linkName = $this->parseLinkName($body);
    if (isset($this->links[$linkName])) {
        $currentCredit = $this->links[$linkName]['credit'] ?? 0;
        if ($currentCredit <= 0) {
            return "";  // Reject transfer without credit
        }
    }
}

// In delivery handling:
if (self::has($body, 0x14)) {  // transfer
    $linkName = $this->parseLinkName($body);
    $deltaCredit = $this->parseDeltaCredit($body);
    
    if (isset($this->links[$linkName])) {
        $this->links[$linkName]['credit'] += $deltaCredit;
    }
}
```

#### D. Implement disposition handling
**File:** `php/src/Amqp10.php` | **Line:** ~75-80  
**Add:** Handle message acknowledgments:

```php
private function handleDisposition(string $body): string
{
    // Parse delivery tag and state (settled/released)
    if (str_contains($body, "\x52")) {  // disposition frame
        // Track acknowledgment for published messages
        $deliveryTag = $this->parseDeliveryTag($body);
        
        if (isset($this->pendingSenders[$deliveryTag])) {
            $linkName = $this->pendingSenders[$deliveryTag]['link'];
            $messageId = $this->pendingSenders[$deliveryTag]['id'];
            
            // Drop from in-flight queue
            unset($this->pendingSenders[$deliveryTag]);
        }
    }
    
    return "";  // Return disposition outcome
}

// Call in onFrame() around line 76:
if (self::has($body, 0x51)) {  // disposition message
    return $this->frame(2, $this->handleDisposition($body));
}
```

#### E. Multi-frame transfer support
**File:** `php/src/Amqp10.php`  
**Add:** Buffer partial messages across frames:

```php
private array $transferBuffer = [];  // linkName => [payload: string, length: int]

private function handleTransferFrame(string $body): string
{
    $linkName = $this->parseLinkName($body);
    
    if (!isset($this->transferBuffer[$linkName])) {
        $this->transferBuffer[$linkName] = ['payload' => '', 'length' => 0];
    }
    
    $this->transferBuffer[$linkName]['payload'] .= substr($body, 8);
    $this->transferBuffer[$linkName]['length'] += strlen($body) - 8;
    
    if (str_contains($body, "\x1f")) {  // done flag
        $payload = $this->transferBuffer[$linkName]['payload'];
        unset($this->transferBuffer[$linkName]);
        // Process complete payload
        return $this->frame(1, $payload);
    }
    
    return "";  // Still receiving
}

// Call in onFrame() near line 79:
if (str_contains($body, "\x16") || str_contains($body, "\x1e")) {  // transfer frames
    return $this->handleTransferFrame($body);
}
```

#### F. Test implementation against conformance suite
**File:** `php/test/amqp10.test.php` (create new)  
**Run:** After implementation:
```bash
cd php
php test/run.php
```

---

## 3. OAuth 2.0 Support

### Problem
OAuth backend infrastructure exists (`Features.php`) but no actual token validation code.

### Tasks

#### A. Add JWT validation for RS256 tokens
**File:** `php/src/OauthBackend.php` (create new)  
**Location:** alongside `Amqp10.php`

```php
<?php
declare(strict_types=1);

/** OAuth 2.0 JWT validator using JWKS endpoint. */
final class OauthBackend
{
    private string $jwksUrl;
    private ?string $jwksCaPath;
    
    public function __construct(string $jwksUrl, ?string $jwksCaPath)
    {
        $this->jwksUrl = $jwksUrl;
        $this->jwksCaPath = $jwksCaPath;
    }
    
    /** Validate JWT access token for admin claims. */
    public function validate(string $token): ?string  // returns admin username or null
    {
        $parts = explode('.', $token, 3);
        if (count($parts) !== 3) {
            return null;
        }
        
        [$headerB64, $payloadB64, $sig] = $parts;
        
        // Decode header to get kid and algorithm
        $header = json_decode(base64_decode($headerB64), true);
        if (!is_array($header) || $header['typ'] !== 'JWT' || ($header['alg'] ?? '') !== 'RS256') {
            return null;
        }
        
        // Fetch JWKS and find matching key by kid
        $jwks = $this->fetchJwks();
        if (!is_array($jwks) || !isset($jwks['keys'])) {
            return null;
        }
        
        $key = null;
        foreach ($jwks['keys'] as $k) {
            if (($k['kid'] ?? '') === ($header['kid'] ?? '')) {
                $key = $k;
                break;
            }
        }
        
        if ($key === null || !isset($key['n']) || !isset($key['e'])) {
            return null;
        }
        
        // Verify signature using OpenSSL
        try {
            $publicKey = "-----BEGIN PUBLIC KEY-----\n" . 
                wordwrap(base64_encode(hex2bin($key['n'])), 64, "\n", true) . 
                "\n-----END PUBLIC KEY-----";
            
            if (openssl_verify(
                base64_decode($payloadB64),
                hex2bin($sig),
                $publicKey,
                OPENSSL_ALGO_SHA256
            )) !== 1 {
                return null;
            }
        } catch (Exception) {
            return null;
        }
        
        // Decode payload and extract admin claim
        $payload = json_decode(base64_decode($payloadB64), true);
        if (!is_array($payload)) {
            return null;
        }
        
        // Check required audiences
        $audienceClaim = $payload['aud'] ?? [];
        if (is_string($audienceClaim)) {
            $audienceClaim = [$audienceClaim];
        }
        if (!in_array('queueforge-admin', $audienceClaim, true) && 
            !in_array('queueforge-client', $audienceClaim, true)) {
            return null;
        }
        
        // Check expiration
        if (isset($payload['exp']) && time() > $payload['exp']) {
            return null;
        }
        
        // Extract subject/username
        return $payload['sub'] ?? $payload['preferred_username'] ?? null;
    }
    
    private function fetchJwks(): ?array
    {
        if ($this->jwksCaPath !== null) {
            $context = stream_context_create([
                'ssl' => ['verify_peer' => true, 'verify_peer_name' => true]
            ]);
            
            // Set CA bundle for verification
            putenv("SSL_CERT_FILE={$this->jwksCaPath}");
        } else {
            $context = stream_context_create(['ssl' => ['verify_peer' => false]]);
        }
        
        $response = @file_get_contents($this->jwksUrl, false, $context);
        return $response !== false ? json_decode($response, true) : null;
    }
}
```

#### B. Wire JWT auth in Http.php login flow
**File:** `php/src/Http.php` | **Line:** ~137-159  
**Update:** Add OAuth2 path to `login()` method:

```php
private function login(string $body): string
{
    $json = json_decode($body, true);
    
    // New: try JWT auth first
    if (($json['auth_type'] ?? '') === 'oauth2') {
        $token = (string) ($json['token'] ?? '');
        $jwtAuth = new OauthBackend('queueforge', '/jwks');  // Configurable params needed
        
        $username = $jwtAuth->validate($token);
        if ($username !== null && $this->broker->canManage($username)) {
            return $this->createSession($username);
        }
        
        return $this->json(401, ['error' => 'unauthorized']);
    }
    
    // Existing Basic auth path...
}
```

#### C. Update Features to support OAuth2 user lookup
**File:** `php/src/Features.php` | **Line:** ~1-286  
**Add method:**
```php
public static function oauthValidate(string $jwksUrl, string $token): ?string
{
    return (new OauthBackend($jwksUrl, null))->validate($token);
}
```

#### D. Add OAuth2 logout endpoint
**File:** `php/src/Http.php`  
**New route in `api()` method:**
```php
if ($method === 'POST' && $path === '/api/oauth2/revoke') {
    // Optional: blacklist token for specified duration
    return $this->json(200, ['revoked' => true]);
}
```

#### E. Test integration
**File:** `php/test/oauth.test.php` (create new)  
**Tests:**
- Valid RS256 JWT with admin audience → success + session cookie
- Expired token → 401
- Non-admin audience → 403
- Malformed signature → 401

---

## 4. Streams Protocol Implementation

### Problem
Stream protocol at `Protocols.php` lines ~100-200 only creates in-memory placeholders; no actual AMQP message bridging.

### Tasks

#### A. Implement stream fetch operation
**File:** `php/src/Protocols.php` | **Line:** ~200-300  
**Add basic fetch path:**

```php
public static function handleFetch(string $streamName, int $offset): ?string
{
    global $broker;  // Or inject Broker instance
    
    if (!isset($broker->streams[$streamName])) {
        return null;  // Stream doesn't exist
    }
    
    $messages = $broker->streams[$streamName];
    if ($offset >= count($messages)) {
        return null;  // No messages at offset
    }
    
    return $messages[$offset] ?? null;
}

// Wire into protocol loop in Protocols driver:
if (self::hasFrameBody($body, "\x03") && str_contains($body, "fetch")) {
    $streamName = self::parseStreamName($body);
    $offset = self::parseOffset($body);
    
    $response = self::handleFetch($streamName, $offset);
    return $response !== null ? self::frame(1, $response) : "";
}
```

#### B. Implement stream append operation
**File:** `php/src/Protocols.php` | **Line:** ~300-400  
**Add write path:**

```php
public static function handleAppend(string $streamName, string $payload): ?string
{
    global $broker;
    
    if (!isset($broker->streams[$streamName])) {
        // Auto-create stream like RabbitMQ does
        $broker->streams[$streamName] = [];
    }
    
    $offset = count($broker->streams[$streamName]);
    $broker->streams[$streamName][] = [
        'payload' => $payload,
        'timestamp' => microtime(true)
    ];
    
    return json_encode(['offset' => $offset]);
}

// Wire into protocol loop:
if (self::hasFrameBody($body, "\x18") && str_contains($body, "append")) {
    $streamName = self::parseStreamName($body);
    // Extract payload from frame
    $payload = substr($body, 32);  // Simplified payload extraction
    
    $response = self::handleAppend($streamName, $payload);
    return $response !== null ? self::frame(1, $response) : "";
}
```

#### C. Implement stream status/metadata query
**File:** `php/src/Protocols.php`  
**Add metadata endpoint:**

```php
public static function handleStatus(string $streamName): string
{
    global $broker;
    
    return json_encode([
        'name' => $streamName,
        'length' => count($broker->streams[$streamName] ?? []),
        'status' => isset($broker->streams[$streamName]) ? 'active' : 'inactive'
    ]);
}

// Wire into protocol:
if (self::hasFrameBody($body, "\x02") && str_contains($body, "status")) {
    $streamName = self::parseStreamName($body);
    return self::frame(1, self::handleStatus($streamName));
}
```

#### D. Wire to AMQP message bridging
**File:** `php/src/Protocols.php` | **Line:** ~50-100  
**Connect protocol bridge to broker publish/get hooks:**

```php
// In Broker::publish(), add stream delivery path:
if (isset($this->streamBridge) && $exchange === 'amq.streams') {
    $streamKey = preg_replace('/\.(fetch|append)$/', '', $key);
    if ($this->streamBridge !== null) {
        // Publish to stream peer
        $this->streamBridge->publish($streamKey, $body);
    }
}

// In Broker::getReady(), add stream pull path:
if ($msg['exchange'] === 'amq.streams' && str_ends_with($key, '.fetch')) {
    $streamName = preg_replace('/\.(fetch|append)$/', '', $key);
    $result = self::handleFetch($streamName, 0);
    if ($result !== null) {
        return json_decode($result)['offset'] ?? null;
    }
}
```

#### E. Test streams implementation
**File:** `php/test/streams.test.php` (create new)  
**Tests:**
- Create stream via publish: verify it exists in `$broker->streams`
- Append message to stream: check offset response
- Fetch from stream at offset 0: validate payload match
- Status query: verify metadata fields correct
- Delete stream on broker shutdown: verify cleanup

---

## 5. Multi-Listener TLS Support

### Problem
TLS config exists but only applied to AMQP listener. MQTT, STOMP, and Stream listeners run unencrypted.

### Tasks

#### A. Add TLS context factory method
**File:** `php/src/Broker.php` | **Line:** ~120-150  
**Create reusable TLS context builder:**

```php
/** Build TLS stream context from config. */
private function tlsContext(): ?string  // returns cert path or null
{
    if (!$this->tlsEnabled ?? false) {
        return null;
    }
    
    $certPath = $this->certPath ?? '';
    $keyPath = $this->keyPath ?? '';
    
    if ($certPath === '' || $keyPath === '') {
        error_log("TLS configuration incomplete: missing cert or key path");
        return null;
    }
    
    return json_encode([
        'cert_path' => $certPath,
        'key_path' => $keyPath
    ]);
}

// Expose as public method for protocol listeners to call:
public function createSslContext(): array  // returns full OpenSSL stream context
{
    $config = $this->tlsContext();
    if ($config === null) {
        throw new RuntimeException("TLS not enabled");
    }
    
    $cfg = json_decode($config, true);
    return stream_context_create([
        'ssl' => [
            'local_cert' => $cfg['cert_path'],
            'private_key' => $cfg['key_path'],
            'verify_peer' => false,  // Optional: enable CA validation via $this->caPath
            'allow_self_signed' => true
        ]
    ]);
}
```

#### B. Wire TLS to MQTT listener (Protocols.php)
**File:** `php/src/Protocols.php` | **Line:** ~50-100  
**Update MQTT driver to use TLS context:**

```php
public static function startMqtt(Broker $broker, string $host, int $port): void
{
    global $tls;  // Share TLS config from main script
    
    $sock = stream_socket_server(
        "tcp://$host:$port",
        $errno,
        $errstr
    );
    
    if ($tls !== null && isset($GLOBALS['ssl_context'])) {
        $sock = "ssl://" . $sock;  // Wrap with SSL context
    }
    
    stream_set_option($sock, STREAM_OPTION_SOCKET_TYPE, 'tcp');
    stream_set_blocking($sock, true);
    
    while ($client = @stream_socket_accept($sock, 60)) {
        if ($tls !== null) {
            $context = stream_context_create([]);
            stream_wrapper_register("ssl_wrapped", "SSLWrapperHandler");
            $client = fopen("ssl://" . socket_to_stream($client), 'r+', false, $context);
        }
        
        self::handleMqttClient($broker, $client);
    }
}

// In handleMqttClient():
private static function handleMqttClient(Broker $broker, $client): string
{
    // Ensure SSL context is configured before reading client hello
    stream_socket_enable_crypto(
        $client, 
        true, 
        STREAM_CRYPTO_METHOD_TLS_CLIENT_HANDSHAKE
    );
    
    return "";  // MQTT logic unchanged
}
```

#### C. Wire TLS to STOMP listener (Protocols.php)
**File:** `php/src/Protocols.php`  
**Add similar STOMP wrapper:**

```php
public static function startStomp(Broker $broker, string $host, int $port): void
{
    global $tls;
    
    $sock = stream_socket_server("tcp://$host:$port", $errno, $errstr);
    if ($tls !== null) {
        $sock = @stream_socket_enable_crypto(
            $sock, 
            true, 
            STREAM_CRYPTO_METHOD_TLS_SERVER_HANDSHAKE
        );
    }
    
    // STOMP frame parsing unchanged
}
```

#### D. Wire TLS to Stream listener (Protocols.php)
**File:** `php/src/Protocols.php`  
**Stream protocol needs most explicit handling:**

```php
public static function startStream(Broker $broker, string $host, int $port): void
{
    global $tls;
    
    if ($tls === null) {
        error_log("Skipping stream listener: TLS not available");
        return;
    }
    
    $sock = stream_socket_server("tcp://$host:$port", $errno, $errstr);
    if ($sock === false) {
        throw new RuntimeException("Failed to start stream listener: $errstr");
    }
    
    // Apply TLS context
    $tlsStream = stream_socket_accept($sock);
    stream_socket_enable_crypto(
        $tlsStream, 
        true,
        STREAM_CRYPTO_METHOD_TLS_SERVER_HANDSHAKE
    );
    
    self::handleStreamConnection($broker, $tlsStream);
}

private static function handleStreamConnection(Broker $broker, $socket): void
{
    // Stream protocol handler logic unchanged
    // But now running over encrypted socket
}
```

#### E. Update config to control TLS per-listener
**File:** `php/config.example.toml` (create or update)  
**Add listener-specific flags:**

```toml
[tls]
enabled = true
cert_path = "/path/to/cert.pem"
key_path = "/path/to/key.pem"

# Enable TLS for specific listeners:
[listeners.mqtt]
port = 1883
tls_enabled = false  # Default off, enable explicitly

[listeners.stomp]
port = 61613
tls_enabled = false

[listeners.stream]
port = 5552
tls_enabled = true  # Example: streams over TLS only
```

#### F. Wire config to listener start methods
**File:** `php/bin/queueforge` | **Line:** ~80-110  
**Add per-listener TLS enable checks:**

```php
$cfg = parseConfig(file_get_contents($configPath));

// Before starting each listener, check if TLS is enabled:
if ($cfg['listeners']['mqtt'] && $cfg['listeners']['mqtt_tls_enabled']) {
    Protocols::startMqtt($broker, $cfg['listeners']['mqtt_host'], 
                        $cfg['listeners']['mqtt_port'], $broker->createSslContext());
}

if ($cfg['listeners']['stomp'] && $cfg['listeners']['stomp_tls_enabled']) {
    Protocols::startStomp($broker...);  // Pass TLS context
}

if ($cfg['listeners']['stream'] && $cfg['listeners']['stream_tls_enabled']) {
    Protocols::startStream($broker, $cfg['listeners']['stream_host'], 
                          $cfg['listeners']['stream_port'], $broker->createSslContext());
}
```

#### G. Test multi-listener TLS
**File:** `php/test/tls.test.php` (create new)  
**Tests:**
- Verify AMQP over TLS: `openssl s_client -connect localhost:5671`
- Verify MQTT over TLS: `openssl s_client -connect localhost:8883`
- Verify STOMP over TLS: `curl --insecure https://localhost:61443/api/whoami`
- Verify Stream over TLS: binary client connected via `tls://` URI

---

## 6. MQTT 5.0 Protocol Support

### Problem
Only QoS 0 basic publish supported; missing session state, reason codes, properties.

### Tasks (simplified scope for Phase 1)

#### A. Add session persistence for QoS 1
**File:** `php/src/Protocols.php` | **Line:** ~100-200  
**Track in-flight MQTT messages:**

```php
/** Track per-client subscription sessions. */
private static array $mqttSessions = [];  // clientId => [subscribes: array, inflight: array]

public static function publishQoS1(Broker $broker, string $topic, string $payload): bool
{
    if (isset($broker->mqttSession[$topic])) {
        foreach ($broker->mqttSession[$topic]['clients'] as $clientId) {
            // Send with DUP flag until ack received
            self::sendDeliverable($broker, $clientId, $topic, $payload);
        }
    }
    
    return count($broker->mqttSession ?? []) > 0;
}
```

#### B. Add reason codes to response frames
**File:** `php/src/Protocols.php` | **Line:** ~200-300  
**Extend disconnect flow:**

```php
private static function handleDisconnect(Broker $broker, string $payload): void
{
    $reasonCode = ord($payload[0]);  // Extract reason
    
    switch ($reasonCode) {
        case 0x80:  // Normal disconnection
            $response = "\x2E\x00\x01";  // Disconnect frame
            break;
        case 0xA0:  // Unpleasant disconnect (client-side error)
            // Log warning, send back same reason code
            $response = "\x2E\x00\x01\xa0";
            break;
    }
    
    fwrite($broker->mqttSocket, $response);
}
```

#### C. Add properties to publish/ack frames
**File:** `php/src/Protocols.php`  
**Extend MQTT payload handling:**

```php
private static function extractProperties(string $header): array
{
    return [
        'correlation_data' => null,  // Optional byte string
        'content_type' => 'text/plain',
        'user_properties' => [],  // Key-value pairs
        'message_expiry_ms' => null
    ];
}

// Call in publish handler:
$props = self::extractProperties($payload);
if (($props['message_expiry_ms'] ?? 0) > 0) {
    // Apply TTL to broker message
    $broker->msgs[$id]['expires'] = time() * 1000 + $props['message_expiry_ms'];
}
```

#### D. Test MQTT 5.0 features
**File:** `php/test/mqtt5.test.php` (create new)  
**Tests:**
- QoS 1 delivery with ack: verify message stays in broker until received
- Session persistence across reconnects: publish → disconnect → reconnect → receive
- Reason code validation: reject disconnect codes correctly
- Properties carry through publish: content-type matches payload

---

## Priority & Implementation Order

**Phase 1 (Week 1):**
1. ✅ Topic permissions enforcement (`Broker.php`) - high leverage, easy win
2. ⏳ OAuth 2.0 core JWT validation (`OauthBackend.php`) - security feature
3. ⏳ Multi-listener TLS wiring (`Protocols.php` + `Http.php`)

**Phase 2 (Week 2):**
4. ✅ MQTT QoS 1 persistence + reason codes
5. ✅ Stream protocol fetch/append/status
6. ✅ AMQP 1.0 credit flow control + disposition

**Phase 3 (Week 3+):**
7. ⏳ Full AMQP 1.0 state machine
8. ⏳ MQTT 5.0 full spec (sessions, properties)
9. ⏳ Stream broker integration with publish/gate hooks

**Estimated completion:** 3 weeks for full parity with Bun/Rust implementations.

---

## Verification Tests

After each feature:
1. Run `php test/run.php` to validate existing tests still pass
2. Add new tests in `/test/*.test.php` files 
3. Run RabbitMQ conformance if available: `cd conformance && bun run.ts`

---

## Implementation Notes

- All changes maintain RabbitMQ compatibility semantics (status codes, frame layouts)
- No external dependencies required beyond OpenSSL PHP extensions
- Follow existing `Broker`, `Http`, `Protocols` class structure and error handling patterns
- Prefer static methods in `Protocols.php` for protocol handlers to match single-process style
