<?php
declare(strict_types=1);

/**
 * Management HTTP, the password hash, and log compaction.
 *
 * The password check pins the cross-language fixture from
 * bun/test/password-hash.test.ts, so a change to the hashing would break
 * logins against brokers of the other two implementations.
 */

require_once __DIR__ . '/lib/Harness.php';
$root = dirname(__DIR__);
require_once $root . '/src/Routing.php';
require_once $root . '/src/Features.php';
require_once $root . '/src/Policy.php';
require_once $root . '/src/Codec.php';
require_once $root . '/src/Auth.php';
require_once $root . '/src/Store.php';
require_once $root . '/src/Cluster.php';
require_once $root . '/src/Broker.php';

// The exact value Bun and Rust produce for this salt and password. The salt
// is the four bytes 0x10 0x22 0x33 0x44, per bun/test/password-hash.test.ts.
Harness::guard('password hash fixture', static function (): void {
    $salt = "\x10\x22\x33\x44";
    $expected = 'ECIzRClc/+1u2ev0HwDpqq+CC4ixd405UlwH0cCTvqGs8avG';
    Harness::eq('the cross-language fixture matches', $expected, Auth::hashWithSalt('devpassword12', $salt));
    Harness::eq('and it verifies', true, Auth::matches('devpassword12', $expected));
    Harness::eq('a wrong password does not', false, Auth::matches('devpassword13', $expected));

    // A random salt still round-trips.
    Harness::eq('a fresh hash verifies', true, Auth::matches('devpassword12', Auth::hash('devpassword12')));

    // Argon2 PHC strings must never verify. This is Bun's fixture.
    Harness::eq(
        'an argon2 string never verifies',
        false,
        Auth::matches(
            'devpassword12',
            '$argon2id$v=19$m=19456,t=2,p=1$WbkrTzkiA7IX96DqFx4oyqY4ycwq51Qc2ernvzYq2H8$E44EIl5HVbAgkCuygMc0dOWT79GeatS2lEh4vIWdbvQ',
        ),
    );
    Harness::eq('an empty hash never verifies', false, Auth::matches('x', ''));
    Harness::eq('a 4-byte salt is required', true, (static function (): bool {
        try {
            Auth::hashWithSalt('devpassword12', 'abc');
            return false;
        } catch (RuntimeException $e) {
            return true;
        }
    })());

    // The policy: 8 code points, 1024 bytes.
    $rejected = static function (string $password): bool {
        try {
            Auth::check($password);
            return false;
        } catch (RuntimeException $e) {
            return true;
        }
    };
    Harness::eq('an empty password is rejected', true, $rejected(''));
    Harness::eq('seven characters are rejected', true, $rejected('1234567'));
    Harness::eq('eight characters are accepted', false, $rejected('12345678'));
    Harness::eq('over 1024 bytes is rejected', true, $rejected(str_repeat('a', 1025)));
    // Eight astral characters are 32 bytes but 8 code points, so accepted.
    Harness::eq('code points are counted, not bytes', false, $rejected(str_repeat("\u{1F600}", 8)));
});

// Compaction rewrites the log and keeps only the live records.
Harness::guard('log compaction', static function (): void {
    $dir = sys_get_temp_dir() . '/qf-compact-' . bin2hex(random_bytes(6));
    mkdir($dir, 0777, true);
    $path = $dir . '/messages.log';
    $store = new Store($path);

    // 40 messages in, 39 acked, so almost all of the log is dead weight.
    for ($i = 1; $i <= 40; $i++) {
        $store->appendPublish($i, 'q', str_repeat('x', 512), 2, null);
    }
    for ($i = 1; $i <= 39; $i++) {
        $store->appendAck($i);
    }
    $store->sync();
    $before = $store->end;
    Harness::ok('the log grew', $before > 20000, "end=$before");

    $live = [['id' => 40, 'queue' => 'q', 'body' => str_repeat('x', 512), 'mode' => 2, 'propRaw' => null]];
    Harness::eq('compaction runs', true, $store->compact($live));
    Harness::ok('the log shrank', $store->end < $before, "before=$before after={$store->end}");
    Harness::eq('synced tracks the new end', $store->end, $store->synced);

    // Replaying the compacted log must give back exactly the live record.
    $reopened = new Store($path);
    $replayed = $reopened->replay();
    Harness::eq('one record survives', 1, count($replayed));
    Harness::eq('it is the unacked one', 40, $replayed[0]['id']);
    Harness::eq('its body is intact', str_repeat('x', 512), $replayed[0]['body']);

    // The size rule: a small log is left alone.
    Harness::eq('a small log is not compacted', false, $reopened->shouldCompact(100));
});

// Properties must survive a store round trip, including older records.
Harness::guard('store property round trip', static function (): void {
    $dir = sys_get_temp_dir() . '/qf-props-' . bin2hex(random_bytes(6));
    mkdir($dir, 0777, true);
    $store = new Store($dir . '/messages.log');
    $props = pack('n', 0x9000) . Codec::shortstr('application/json') . chr(2);
    $store->appendPublish(1, 'q', 'with-props', 2, $props);
    $store->appendPublish(2, 'q', 'no-props', 2, null);
    $store->sync();

    $replayed = (new Store($dir . '/messages.log'))->replay();
    Harness::eq('both records replay', 2, count($replayed));
    Harness::eq('the properties survive', $props, $replayed[0]['propRaw']);
    Harness::eq('an absent property block stays null', null, $replayed[1]['propRaw']);
});

// Management HTTP over a real socket.
$port = Harness::freePort();
$broker = Harness::broker(["management = \"127.0.0.1:$port\""]);

/** Issues one HTTP request and returns the status, headers and body. */
function http(int $port, string $method, string $path, ?array $json = null, string $cookie = ''): array
{
    $body = $json === null ? '' : (string) json_encode($json);
    $request = "$method $path HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nConnection: close\r\n";
    if ($cookie !== '') {
        $request .= "Cookie: $cookie\r\n";
    }
    if ($json !== null) {
        $request .= "Content-Type: application/json\r\nContent-Length: " . strlen($body) . "\r\n";
    }
    $request .= "\r\n" . $body;

    $fp = stream_socket_client("tcp://127.0.0.1:$port", $errno, $errstr, 5.0);
    if ($fp === false) {
        throw new RuntimeException("management connect failed: $errstr");
    }
    fwrite($fp, $request);
    $raw = '';
    stream_set_timeout($fp, 5);
    while (!feof($fp)) {
        $chunk = fread($fp, 8192);
        if ($chunk === false || $chunk === '') {
            break;
        }
        $raw .= $chunk;
    }
    fclose($fp);
    $split = strpos($raw, "\r\n\r\n");
    $head = $split === false ? $raw : substr($raw, 0, $split);
    $payload = $split === false ? '' : substr($raw, $split + 4);
    preg_match('#HTTP/1\.1 (\d+)#', $head, $m);
    return [
        'status' => (int) ($m[1] ?? 0),
        'head' => $head,
        'body' => $payload,
        'json' => json_decode($payload, true),
    ];
}

Harness::guard('management auth', static function () use ($port): void {
    Harness::eq('healthz is open', 200, http($port, 'GET', '/healthz')['status']);
    Harness::eq('readyz is open', 200, http($port, 'GET', '/readyz')['status']);
    Harness::eq('metrics is open', 200, http($port, 'GET', '/metrics')['status']);
    Harness::eq('the api needs a session', 401, http($port, 'GET', '/api/overview')['status']);
    Harness::eq('a bad password is refused', 401, http($port, 'POST', '/api/login', ['username' => 'admin', 'password' => 'wrong'])['status']);

    $login = http($port, 'POST', '/api/login', ['username' => 'admin', 'password' => 'devpassword12']);
    Harness::eq('login succeeds', 200, $login['status']);
    Harness::ok('login sets a cookie', str_contains($login['head'], 'Set-Cookie: queueforge_session'));
    Harness::ok('the cookie is HttpOnly', str_contains($login['head'], 'HttpOnly'));
    Harness::eq('login reports the tags', ['administrator'], $login['json']['tags'] ?? []);
});

/** Logs in and returns the session cookie. */
function session(int $port): string
{
    $login = http($port, 'POST', '/api/login', ['username' => 'admin', 'password' => 'devpassword12']);
    preg_match('#Set-Cookie: ([^;]+)#', $login['head'], $m);
    return $m[1] ?? '';
}

Harness::guard('management routes', static function () use ($port): void {
    $cookie = session($port);
    Harness::ok('a session was issued', $cookie !== '');

    // Queues: create, read, publish into, get back, purge, delete.
    Harness::eq('put a queue', 201, http($port, 'PUT', '/api/queues/%2F/mq', ['durable' => true], $cookie)['status']);
    $one = http($port, 'GET', '/api/queues/%2F/mq', null, $cookie);
    Harness::eq('read one queue', 200, $one['status']);
    Harness::eq('it is the right queue', 'mq', $one['json']['name'] ?? '');
    Harness::eq('a missing queue is 404', 404, http($port, 'GET', '/api/queues/%2F/nope', null, $cookie)['status']);

    $published = http($port, 'POST', '/api/exchanges/%2F/amq.default/publish', [
        'routing_key' => 'mq',
        'payload' => 'via-http',
        'payload_encoding' => 'string',
        'properties' => ['delivery_mode' => 2],
    ], $cookie);
    // amq.default is not a declared exchange here, so this must 404 rather
    // than silently drop the message.
    Harness::eq('publishing to an unknown exchange is 404', 404, $published['status']);

    Harness::eq('declare an exchange', 201, http($port, 'PUT', '/api/exchanges/%2F/mx', ['type' => 'direct'], $cookie)['status']);
    Harness::eq('bind it', 201, http($port, 'POST', '/api/bindings/%2F', [
        'source' => 'mx', 'destination' => 'mq', 'routing_key' => 'k',
    ], $cookie)['status']);
    $routed = http($port, 'POST', '/api/exchanges/%2F/mx/publish', [
        'routing_key' => 'k',
        'payload' => 'via-http',
        'properties' => ['delivery_mode' => 2],
    ], $cookie);
    Harness::eq('publish through the exchange', 200, $routed['status']);
    Harness::eq('it reports routed', true, $routed['json']['routed'] ?? false);

    $got = http($port, 'POST', '/api/queues/%2F/mq/get', ['count' => 1, 'requeue' => false], $cookie);
    Harness::eq('get returns the message', 200, $got['status']);
    Harness::eq('with the payload', 'via-http', $got['json'][0]['payload'] ?? '');

    $purged = http($port, 'POST', '/api/queues/%2F/mq/purge', null, $cookie);
    Harness::eq('purge', 200, $purged['status']);
    Harness::ok('purge reports the count', isset($purged['json']['message_count']));
    Harness::eq('delete the queue', 204, http($port, 'DELETE', '/api/queues/%2F/mq', null, $cookie)['status']);
    Harness::eq('deleting it twice is 404', 404, http($port, 'DELETE', '/api/queues/%2F/mq', null, $cookie)['status']);

    // A built-in exchange cannot be deleted.
    Harness::eq('deleting amq.direct is refused', 400, http($port, 'DELETE', '/api/exchanges/%2F/amq.direct', null, $cookie)['status']);
    Harness::eq('delete our own exchange', 204, http($port, 'DELETE', '/api/exchanges/%2F/mx', null, $cookie)['status']);
});

Harness::guard('users and permissions', static function () use ($port): void {
    $cookie = session($port);
    Harness::eq('create a user', 201, http($port, 'PUT', '/api/users/alice', [
        'password' => 'alicepassword', 'tags' => 'management',
    ], $cookie)['status']);
    $users = http($port, 'GET', '/api/users', null, $cookie);
    $names = array_column((array) $users['json'], 'name');
    Harness::ok('the user is listed', in_array('alice', $names, true));

    Harness::eq('a short password is refused', 400, http($port, 'PUT', '/api/users/bob', [
        'password' => 'short', 'tags' => 'management',
    ], $cookie)['status']);

    Harness::eq('set permissions', 201, http($port, 'PUT', '/api/permissions/alice/%2F', [
        'configure' => '.*', 'write' => '.*', 'read' => '.*',
    ], $cookie)['status']);
    $perms = http($port, 'GET', '/api/permissions', null, $cookie);
    $users = array_column((array) $perms['json'], 'user');
    Harness::ok('alice has permissions listed', in_array('alice', $users, true), 'saw ' . implode(',', $users));
    Harness::ok('admin keeps its bootstrap permissions', in_array('admin', $users, true));
    Harness::eq('permissions for an unknown user are 404', 404, http($port, 'PUT', '/api/permissions/ghost/%2F', [
        'configure' => '.*', 'write' => '.*', 'read' => '.*',
    ], $cookie)['status']);
    Harness::eq('clear permissions', 204, http($port, 'DELETE', '/api/permissions/alice/%2F', null, $cookie)['status']);

    Harness::eq('delete the user', 204, http($port, 'DELETE', '/api/users/alice', null, $cookie)['status']);
    Harness::eq('deleting the current user is refused', 400, http($port, 'DELETE', '/api/users/admin', null, $cookie)['status']);
});

Harness::guard('policies, limits and flags', static function () use ($port): void {
    $cookie = session($port);
    Harness::eq('a policy needs a definition', 400, http($port, 'PUT', '/api/policies/%2F/p1', [
        'pattern' => '.*',
    ], $cookie)['status']);
    Harness::eq('put a policy', 201, http($port, 'PUT', '/api/policies/%2F/p1', [
        'pattern' => '.*', 'definition' => ['max-length' => 100], 'priority' => 1,
    ], $cookie)['status']);
    $policies = http($port, 'GET', '/api/policies/%2F', null, $cookie);
    Harness::eq('the policy is listed', 'p1', $policies['json'][0]['name'] ?? '');
    Harness::eq('delete the policy', 204, http($port, 'DELETE', '/api/policies/%2F/p1', null, $cookie)['status']);
    Harness::eq('deleting it twice is 404', 404, http($port, 'DELETE', '/api/policies/%2F/p1', null, $cookie)['status']);

    Harness::eq('put an operator policy', 201, http($port, 'PUT', '/api/operator-policies/%2F/o1', [
        'pattern' => '.*', 'definition' => ['max-length' => 5],
    ], $cookie)['status']);
    Harness::eq('operator policies list', 200, http($port, 'GET', '/api/operator-policies', null, $cookie)['status']);

    Harness::eq('set a user limit', 204, http($port, 'PUT', '/api/user-limits/admin/max-connections', ['value' => 10], $cookie)['status']);
    Harness::eq('an unknown limit is refused', 400, http($port, 'PUT', '/api/user-limits/admin/max-nonsense', ['value' => 1], $cookie)['status']);
    $limits = http($port, 'GET', '/api/limits', null, $cookie);
    Harness::eq('the limit is reported', 10, $limits['json']['user_limits']['admin']['max-connections'] ?? 0);
    Harness::eq('clear the limit', 204, http($port, 'DELETE', '/api/user-limits/admin/max-connections', null, $cookie)['status']);

    $flags = http($port, 'GET', '/api/feature-flags', null, $cookie);
    Harness::ok('feature flags are listed', is_array($flags['json']) && $flags['json'] !== []);
    Harness::eq('disable a flag', 204, http($port, 'POST', '/api/feature-flags/classic_queue_type/disable', null, $cookie)['status']);
    Harness::eq('an unknown flag is 404', 404, http($port, 'POST', '/api/feature-flags/nope/enable', null, $cookie)['status']);

    Harness::eq('cluster name', 200, http($port, 'GET', '/api/cluster-name', null, $cookie)['status']);
    Harness::eq('topic permissions list', 200, http($port, 'GET', '/api/topic-permissions', null, $cookie)['status']);
    Harness::eq('deprecated features list', 200, http($port, 'GET', '/api/deprecated-features', null, $cookie)['status']);
    Harness::eq('consumers list', 200, http($port, 'GET', '/api/consumers/%2F', null, $cookie)['status']);
});

Harness::guard('definitions import', static function () use ($port): void {
    $cookie = session($port);
    Harness::eq('import definitions', 204, http($port, 'POST', '/api/definitions', [
        'queues' => [['name' => 'imported', 'arguments' => []]],
        'exchanges' => [['name' => 'imported-ex', 'type' => 'topic']],
        'bindings' => [['source' => 'imported-ex', 'destination' => 'imported', 'routing_key' => 'a.#']],
    ], $cookie)['status']);
    $queues = http($port, 'GET', '/api/queues/%2F', null, $cookie);
    $names = array_column((array) ($queues['json']['items'] ?? []), 'name');
    Harness::ok('the imported queue exists', in_array('imported', $names, true));
    $exported = http($port, 'GET', '/api/definitions', null, $cookie);
    Harness::eq('definitions export', 200, $exported['status']);
});

Harness::guard('parity additions', static function () use ($port): void {
    $cookie = session($port);

    // whoami reports the user's real tags rather than a fixed value.
    $who = http($port, 'GET', '/api/whoami', null, $cookie);
    Harness::eq('whoami names the user', 'admin', $who['json']['name'] ?? '');
    Harness::ok('and reports tags', is_array($who['json']['tags'] ?? null) && $who['json']['tags'] !== []);

    // A required feature flag cannot be turned off.
    Harness::eq(
        'disabling quorum_queues is refused',
        400,
        http($port, 'POST', '/api/feature-flags/quorum_queues/disable', null, $cookie)['status'],
    );
    $flags = http($port, 'GET', '/api/feature-flags', null, $cookie);
    Harness::ok('flags carry a stability field', isset($flags['json'][0]['stability']));

    // Policy validation happens at the API boundary.
    Harness::eq('a policy without a definition is refused', 400, http($port, 'PUT', '/api/policies/%2F/p1', [
        'pattern' => '.*',
    ], $cookie)['status']);
    Harness::eq('an unknown definition key is refused', 400, http($port, 'PUT', '/api/policies/%2F/p1', [
        'pattern' => '.*',
        'definition' => ['nonsense' => 1],
    ], $cookie)['status']);
    Harness::eq('a bad apply-to is refused', 400, http($port, 'PUT', '/api/policies/%2F/p1', [
        'pattern' => '.*',
        'apply-to' => 'wat',
        'definition' => [],
    ], $cookie)['status']);

    // A policy reaches a queue that already exists.
    Harness::eq('declare a queue to be policed', 201, http($port, 'PUT', '/api/queues/%2Fpolicy-target', [], $cookie)['status'] === 201 ? 201 : http($port, 'PUT', '/api/queues/%2F/policy-target', [], $cookie)['status']);
    Harness::eq('set a policy', 201, http($port, 'PUT', '/api/policies/%2F/ttl-all', [
        'pattern' => '^policy-target$',
        'definition' => ['message-ttl' => 1234],
        'priority' => 1,
        'apply-to' => 'queues',
    ], $cookie)['status']);
    $listed = http($port, 'GET', '/api/policies/%2F', null, $cookie);
    Harness::eq('the policy is listed', 200, $listed['status']);

    // The routes that were missing.
    Harness::eq('a shovel parameter is accepted', 201, http($port, 'PUT', '/api/parameters/shovel/%2F/s1', [
        'value' => ['src-queue' => 'a', 'dest-queue' => 'b'],
    ], $cookie)['status']);
    Harness::eq('a federation upstream is accepted', 201, http($port, 'PUT', '/api/parameters/federation-upstream/%2F/u1', [
        'value' => ['uri' => 'amqp://elsewhere'],
    ], $cookie)['status']);
    Harness::eq('a deprecated feature can be dismissed', 204, http($port, 'DELETE', '/api/deprecated-features/anything', null, $cookie)['status']);
    Harness::eq('closing an untracked connection is 404', 404, http($port, 'DELETE', '/api/connections/nope', null, $cookie)['status']);

    // Logout invalidates the token server side.
    Harness::eq('logout succeeds', 200, http($port, 'POST', '/api/logout', null, $cookie)['status']);
    Harness::eq('the old cookie no longer authenticates', 401, http($port, 'GET', '/api/overview', null, $cookie)['status']);
});

Harness::guard('prometheus series', static function () use ($port): void {
    $raw = http($port, 'GET', '/metrics');
    Harness::eq('metrics are served', 200, $raw['status']);
    $body = $raw['body'];
    foreach ([
        'rabbitmq_connections_opened_total',
        'rabbitmq_connections_closed_total',
        'rabbitmq_channels',
        'rabbitmq_channels_opened_total',
        'rabbitmq_queues_declared_total',
        'rabbitmq_queues_created_total',
        'rabbitmq_queues_deleted_total',
        'rabbitmq_consumers',
        'rabbitmq_global_messages_received_confirm_total',
        'rabbitmq_global_messages_routed_total',
        'rabbitmq_global_messages_unroutable_dropped_total',
        'rabbitmq_global_messages_unroutable_returned_total',
        'rabbitmq_global_messages_delivered_consume_manual_ack_total',
        'rabbitmq_global_messages_delivered_get_auto_ack_total',
        'rabbitmq_global_messages_get_empty_total',
        'rabbitmq_global_messages_redelivered_total',
        'rabbitmq_global_messages_dead_lettered_expired_total',
        'rabbitmq_global_messages_dead_lettered_maxlen_total',
        'rabbitmq_global_messages_dead_lettered_delivery_limit_total',
        'rabbitmq_disk_space_available_bytes',
        'rabbitmq_queue_messages_ready',
        'rabbitmq_queue_consumers',
        'queueforge_wal_fsync_seconds_count',
        'queueforge_full_flush_total',
        'rabbitmq_identity_info',
    ] as $series) {
        Harness::ok("$series is exposed", str_contains($body, $series));
    }
    // The per-queue gauges carry labels.
    Harness::ok('per-queue gauges are labelled', str_contains($body, 'rabbitmq_queue_messages_ready{vhost="/"'));
    // The durability invariant: no confirm may be released before its fsync.
    Harness::ok(
        'no confirm preceded its fsync',
        str_contains($body, 'queueforge_confirm_before_fsync_total 0'),
    );
});

Harness::stop($broker);
Harness::done();
