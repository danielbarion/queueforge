<?php
declare(strict_types=1);

/** Management HTTP and Prometheus text. Routes match the Rust management API the SPA calls. */
final class Http
{
    /** @var array<string, string> */
    public array $sessions = [];
    /**
     * Called after a membership change so the owner can persist and
     * broadcast it. Set by Extras, which owns members.json and the peers.
     *
     * @var ?callable():void
     */
    public $onMembers = null;
    /**
     * Called after a change to users, permissions or topology so the owner
     * can replicate it. Without this a user added through one node's
     * management API was invisible to its peers until they restarted.
     *
     * @var ?callable():void
     */
    public $onTopology = null;

    public function __construct(public Broker $broker, public string $uiRoot, public int $port, public bool $secure)
    {
    }

    public function handle(string $raw): string
    {
        $head = strstr($raw, "\r\n\r\n", true);
        if ($head === false) {
            return $this->status(400, 'bad request');
        }
        $lines = explode("\r\n", $head);
        $parts = explode(' ', $lines[0] ?? '');
        $method = $parts[0] ?? '';
        $target = $parts[1] ?? '/';
        $path = explode('?', $target, 2)[0];
        $body = substr($raw, strlen($head) + 4);
        $cookie = '';
        $basic = null;
        foreach ($lines as $line) {
            if (str_starts_with(strtolower($line), 'cookie:')) {
                $cookie = trim(substr($line, 7));
            }
            if (str_starts_with(strtolower($line), 'authorization:')) {
                $basic = $this->basicUser(trim(substr($line, 14)));
            }
        }
        // HTTP Basic, as RabbitMQ's management API and its CLI tools use it,
        // or the console's session cookie.
        $user = $basic ?? $this->userFromCookie($cookie);
        if ($method === 'GET' && ($path === '/healthz' || $path === '/readyz')) {
            $ok = $path === '/healthz' || $this->broker->ready;
            return $this->status($ok ? 200 : 503, $ok ? "ok\n" : "not ready\n", 'text/plain');
        }
        if ($method === 'GET' && $path === '/api/identity') {
            return $this->json(200, ['product_name' => 'QueueForge', 'kind' => 'php']);
        }
        if ($method === 'GET' && $path === '/metrics') {
            return $this->status(200, $this->metrics(), 'text/plain; version=0.0.4');
        }
        if ($method === 'POST' && $path === '/api/login') {
            return $this->login($body);
        }
        if ($method === 'POST' && $path === '/api/logout') {
            // The token is invalidated server side, not just cleared in the
            // browser, so a captured cookie stops working at logout.
            $token = $this->tokenFromCookie($cookie);
            if ($token !== null) {
                unset($this->sessions[$token]);
            }
            return $this->status(200, '', 'application/json', ['Set-Cookie: ' . $this->cookieName() . '=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0']);
        }
        if ($user === null && str_starts_with($path, '/api/')) {
            return $this->json(401, ['error' => 'unauthorized']);
        }
        if (str_starts_with($path, '/api/')) {
            // A broker error answers this request; it must not end the process.
            try {
                $response = $this->api($method, $path, $body, $user ?? '');
            } catch (RuntimeException $err) {
                $code = $err->getCode();
                $status = $code === 404 ? 404 : ($code === 403 ? 403 : 400);
                return $this->json($status, ['error' => $status === 404 ? 'not_found' : 'bad_request', 'reason' => $err->getMessage()]);
            }
            // Any mutation that succeeded is replicated, so a user or a
            // queue created through one node reaches its peers. Replication
            // sends a whole snapshot, so doing it once here rather than per
            // route is equivalent and far harder to forget.
            if ($method !== 'GET' && str_starts_with($response, 'HTTP/1.1 2')) {
                $this->onTopologyChanged();
            }
            return $response;
        }
        return $this->file($path);
    }

    /**
     * The alarm fields of a RabbitMQ node row. This broker has no memory
     * watermark, so mem_alarm stays false; the disk alarm uses RabbitMQ's
     * default 50 MB free limit on the data directory's filesystem.
     *
     * @return array<string, mixed>
     */
    private function alarms(): array
    {
        $free = @disk_free_space(dirname($this->broker->userFile));
        $free = $free === false ? null : (int) $free;
        return [
            'mem_used' => memory_get_usage(true),
            'mem_alarm' => false,
            'disk_free' => $free,
            'disk_free_limit' => 50_000_000,
            'disk_free_alarm' => $free !== null && $free < 50_000_000,
        ];
    }

    /** The user an `Authorization: Basic ...` value logs in, if the password and a management tag check out. */
    private function basicUser(string $value): ?string
    {
        if (stripos($value, 'basic ') !== 0) {
            return null;
        }
        $decoded = base64_decode(trim(substr($value, 6)), true);
        if ($decoded === false || !str_contains($decoded, ':')) {
            return null;
        }
        [$name, $pass] = explode(':', $decoded, 2);
        if (!$this->broker->verify($name, $pass) || !$this->broker->canManage($name)) {
            return null;
        }
        return $name;
    }

    private function login(string $body): string
    {
        $json = json_decode($body, true);
        $name = is_array($json) ? (string) ($json['username'] ?? '') : '';
        $pass = is_array($json) ? (string) ($json['password'] ?? '') : '';
        if (!$this->broker->verify($name, $pass)) {
            return $this->json(401, ['error' => 'unauthorized']);
        }
        // A user with no management tag can publish over AMQP but has no
        // business in the management API, which is how Bun gates login.
        if (!$this->broker->canManage($name)) {
            return $this->json(403, ['error' => 'forbidden']);
        }
        $token = bin2hex(random_bytes(16));
        $this->sessions[$token] = $name;
        $flags = 'HttpOnly; SameSite=Lax; Path=/';
        if ($this->secure) {
            $flags .= '; Secure';
        }
        return $this->json(200, ['name' => $name, 'tags' => $this->broker->tags[$name] ?? ['administrator']], [
            'Set-Cookie: ' . $this->cookieName() . '=' . $token . '; ' . $flags,
        ]);
    }

    private function cookieName(): string
    {
        return $this->port === 80 || $this->port === 443 ? 'queueforge_session' : 'queueforge_session_' . $this->port;
    }

    private function userFromCookie(string $header): ?string
    {
        $token = $this->tokenFromCookie($header);
        return $token === null ? null : ($this->sessions[$token] ?? null);
    }

    /** The session token carried by a Cookie header, if any. */
    private function tokenFromCookie(string $header): ?string
    {
        $name = $this->cookieName();
        foreach (explode(';', $header) as $part) {
            $part = trim($part);
            if (str_starts_with($part, $name . '=')) {
                return substr($part, strlen($name) + 1);
            }
        }
        return null;
    }

    private function api(string $method, string $path, string $body, string $user): string
    {
        if ($method === 'GET' && $path === '/api/whoami') {
            return $this->json(200, ['name' => $user, 'tags' => $this->broker->tags[$user] ?? ['administrator']]);
        }
        if ($method === 'GET' && $path === '/api/overview') {
            return $this->json(200, $this->overview());
        }
        if ($method === 'GET' && $path === '/api/vhosts') {
            $items = [];
            foreach ($this->broker->vhosts as $name) {
                $items[] = ['name' => $name];
            }
            return $this->json(200, ['items' => $items, 'total_count' => count($items)]);
        }
        if ($method === 'GET' && preg_match('#^/api/queues/([^/]+)$#', $path, $m) === 1) {
            return $this->json(200, ['items' => $this->queueItems(), 'total_count' => count($this->broker->queues)]);
        }
        if ($method === 'PUT' && preg_match('#^/api/queues/([^/]+)/([^/]+)$#', $path, $m) === 1) {
            $json = json_decode($body, true);
            $args = is_array($json) && is_array($json['arguments'] ?? null) ? $json['arguments'] : [];
            $this->broker->declareQueue(rawurldecode($m[2]), $args);
            return $this->json(201, ['name' => rawurldecode($m[2])]);
        }
        if ($method === 'GET' && preg_match('#^/api/exchanges/([^/]+)$#', $path, $m) === 1) {
            $items = [];
            foreach ($this->broker->exchanges as $name => $type) {
                $items[] = ['name' => $name, 'vhost' => '/', 'type' => $type, 'durable' => true, 'auto_delete' => false, 'internal' => false];
            }
            return $this->json(200, ['items' => $items, 'total_count' => count($items)]);
        }
        if ($method === 'PUT' && preg_match('#^/api/exchanges/([^/]+)/([^/]+)$#', $path, $m) === 1) {
            $json = json_decode($body, true);
            $type = is_array($json) ? (string) ($json['type'] ?? 'direct') : 'direct';
            $this->broker->declareExchange(rawurldecode($m[2]), $type);
            return $this->json(201, ['name' => rawurldecode($m[2])]);
        }
        // Parenthesised: mixing && and || without them meant a non-GET
        // request to a bindings path fell into the second clause and was
        // handled as a GET.
        if ($method === 'GET' && ($path === '/api/bindings/%2F' || preg_match('#^/api/bindings/([^/]+)$#', $path) === 1)) {
            $items = [];
            foreach ($this->broker->bindings as $row) {
                $items[] = [
                    'source' => $row['exchange'],
                    'destination' => $row['queue'],
                    'destination_type' => 'queue',
                    'routing_key' => $row['key'],
                    'vhost' => '/',
                    'properties_key' => $row['key'],
                ];
            }
            return $this->json(200, ['items' => $items, 'total_count' => count($items)]);
        }
        if ($method === 'POST' && preg_match('#^/api/bindings/([^/]+)$#', $path) === 1) {
            $json = json_decode($body, true);
            if (!is_array($json)) {
                return $this->json(400, ['error' => 'bad request']);
            }
            $args = [];
            if (is_array($json['arguments'] ?? null)) {
                foreach ($json['arguments'] as $k => $v) {
                    $args[] = [(string) $k, (string) $v];
                }
            }
            $this->broker->bind((string) $json['destination'], (string) $json['source'], (string) ($json['routing_key'] ?? ''), $args);
            return $this->json(201, ['routed' => true]);
        }
        if ($method === 'GET' && $path === '/api/users') {
            $items = [];
            foreach (array_keys($this->broker->users) as $name) {
                $items[] = ['name' => $name, 'tags' => $this->broker->tags[$name] ?? ['administrator']];
            }
            return $this->json(200, $items);
        }
        if ($method === 'GET' && $path === '/api/connections') {
            return $this->json(200, ['items' => [], 'total_count' => $this->broker->prom['connections']]);
        }
        if ($method === 'GET' && $path === '/api/definitions') {
            return $this->json(200, [
                'vhosts' => [['name' => '/']],
                'queues' => $this->queueItems(),
                'exchanges' => $this->broker->exchanges,
                'bindings' => $this->broker->bindings,
            ]);
        }
        // /api/channels has no backing state in this broker, so it stays an
        // empty list. permissions, policies and nodes are served by api2.
        if ($method === 'GET' && $path === '/api/channels') {
            return $this->json(200, []);
        }
        if ($method === 'GET' && $path === '/api/nodes') {
            $items = [];
            foreach ($this->broker->members as $member) {
                $items[] = [
                    'name' => $member['id'],
                    'addr' => $member['addr'],
                    'running' => true,
                    'type' => 'queueforge-php',
                ] + $this->alarms();
            }
            if ($items === []) {
                $items[] = [
                    'name' => $this->broker->nodeId,
                    'addr' => '',
                    'running' => true,
                    'type' => 'queueforge-php',
                ] + $this->alarms();
            }
            return $this->json(200, $items);
        }
        return $this->api2($method, $path, $body, $user);
    }

    /**
     * The routes added for Bun parity: deletes, publish and get, users,
     * permissions, policies, limits, feature flags and nodes. Split from
     * api() only to keep either method readable.
     *
     * Status codes follow Bun: 201 on create, 204 on update or delete, 403
     * when authenticated without the administrator tag, 404 when missing and
     * 400 on bad input.
     */
    private function api2(string $method, string $path, string $body, string $user): string
    {
        $json = json_decode($body, true);
        $json = is_array($json) ? $json : [];

        // Queues.
        if ($method === 'DELETE' && preg_match('#^/api/queues/([^/]+)/([^/]+)$#', $path, $m) === 1) {
            $name = rawurldecode($m[2]);
            if (!isset($this->broker->queues[$name])) {
                return $this->json(404, ['error' => 'not found']);
            }
            $this->broker->deleteQueue($name);
            return $this->status(204, '', 'application/json');
        }
        if ($method === 'POST' && preg_match('#^/api/queues/([^/]+)/([^/]+)/purge$#', $path, $m) === 1) {
            $name = rawurldecode($m[2]);
            if (!isset($this->broker->queues[$name])) {
                return $this->json(404, ['error' => 'not found']);
            }
            $n = $this->broker->purge($name);
            // RabbitMQ answers 200 with the count, which management clients
            // display; a bare 204 leaves them with nothing to show.
            return $this->json(200, ['message_count' => $n]);
        }
        if ($method === 'POST' && preg_match('#^/api/queues/([^/]+)/([^/]+)/get$#', $path, $m) === 1) {
            $name = rawurldecode($m[2]);
            if (!isset($this->broker->queues[$name])) {
                return $this->json(404, ['error' => 'not found']);
            }
            $count = max(1, (int) ($json['count'] ?? 1));
            $requeue = ($json['requeue'] ?? false) === true;
            $items = [];
            $taken = [];
            while (count($items) < $count) {
                $id = $this->broker->getReady($name);
                if ($id === null) {
                    break;
                }
                $msg = $this->broker->msgs[$id];
                $items[] = [
                    'payload' => $msg['body'],
                    'payload_bytes' => strlen($msg['body']),
                    'payload_encoding' => 'string',
                    'routing_key' => $msg['key'] ?? $name,
                    'exchange' => $msg['exchange'] ?? '',
                    'redelivered' => (bool) ($msg['redelivered'] ?? false),
                    'message_count' => $this->broker->readyCount($name),
                ];
                $taken[] = $id;
            }
            foreach ($taken as $id) {
                if ($requeue) {
                    $this->broker->requeue($id);
                } else {
                    $this->broker->ack($id);
                }
            }
            return $this->json(200, $items);
        }
        if ($method === 'GET' && preg_match('#^/api/queues/([^/]+)/([^/]+)$#', $path, $m) === 1) {
            $name = rawurldecode($m[2]);
            foreach ($this->queueItems() as $item) {
                if ($item['name'] === $name) {
                    return $this->json(200, $item);
                }
            }
            return $this->json(404, ['error' => 'not found']);
        }

        // Exchanges.
        if ($method === 'DELETE' && preg_match('#^/api/exchanges/([^/]+)/([^/]+)$#', $path, $m) === 1) {
            $name = rawurldecode($m[2]);
            if (!isset($this->broker->exchanges[$name])) {
                return $this->json(404, ['error' => 'not found']);
            }
            if (!$this->broker->deleteExchange($name)) {
                return $this->json(400, ['error' => 'a built-in exchange cannot be deleted']);
            }
            return $this->status(204, '', 'application/json');
        }
        if ($method === 'POST' && preg_match('#^/api/exchanges/([^/]+)/([^/]+)/publish$#', $path, $m) === 1) {
            $name = rawurldecode($m[2]);
            if (!isset($this->broker->exchanges[$name])) {
                return $this->json(404, ['error' => 'not found']);
            }
            $payload = (string) ($json['payload'] ?? '');
            if (($json['payload_encoding'] ?? 'string') === 'base64') {
                $decoded = base64_decode($payload, true);
                $payload = $decoded === false ? '' : $decoded;
            }
            $props = is_array($json['properties'] ?? null) ? $json['properties'] : [];
            $mode = (int) ($props['delivery_mode'] ?? 1);
            $result = $this->broker->publish(
                0,
                0,
                0,
                $name,
                (string) ($json['routing_key'] ?? ''),
                $payload,
                $mode === 2 ? 2 : 1,
            );
            return $this->json(200, ['routed' => $result === 'wait']);
        }
        if ($method === 'GET' && preg_match('#^/api/exchanges/([^/]+)/([^/]+)$#', $path, $m) === 1) {
            $name = rawurldecode($m[2]);
            if (!isset($this->broker->exchanges[$name])) {
                return $this->json(404, ['error' => 'not found']);
            }
            return $this->json(200, [
                'name' => $name,
                'vhost' => '/',
                'type' => $this->broker->exchanges[$name],
                'durable' => true,
                'auto_delete' => false,
                'internal' => false,
            ]);
        }

        // Bindings.
        if ($method === 'DELETE' && preg_match('#^/api/bindings/([^/]+)/([^/]+)/([^/]+)/([^/]+)$#', $path, $m) === 1) {
            $this->broker->unbind(rawurldecode($m[3]), rawurldecode($m[2]), rawurldecode($m[4]));
            return $this->status(204, '', 'application/json');
        }

        // Users.
        if ($method === 'PUT' && preg_match('#^/api/users/([^/]+)$#', $path, $m) === 1) {
            if (!$this->broker->isAdmin($user)) {
                return $this->json(403, ['error' => 'forbidden']);
            }
            $tags = [];
            $raw = $json['tags'] ?? '';
            foreach (is_array($raw) ? $raw : explode(',', (string) $raw) as $tag) {
                $tag = trim((string) $tag);
                if ($tag !== '') {
                    $tags[] = $tag;
                }
            }
            try {
                $this->broker->putUser(
                    rawurldecode($m[1]),
                    (string) ($json['password'] ?? ''),
                    $tags,
                    (string) ($json['password_hash'] ?? ''),
                );
            } catch (RuntimeException $err) {
                return $this->json(400, ['error' => $err->getMessage()]);
            }
            return $this->status(201, '', 'application/json');
        }
        if ($method === 'DELETE' && preg_match('#^/api/users/([^/]+)$#', $path, $m) === 1) {
            if (!$this->broker->isAdmin($user)) {
                return $this->json(403, ['error' => 'forbidden']);
            }
            $name = rawurldecode($m[1]);
            if ($name === $user) {
                return $this->json(400, ['error' => 'a user cannot delete itself']);
            }
            return $this->broker->deleteUser($name)
                ? $this->status(204, '', 'application/json')
                : $this->json(404, ['error' => 'not found']);
        }

        // Permissions.
        if ($method === 'PUT' && preg_match('#^/api/permissions/([^/]+)/([^/]+)$#', $path, $m) === 1) {
            if (!$this->broker->isAdmin($user)) {
                return $this->json(403, ['error' => 'forbidden']);
            }
            $name = rawurldecode($m[1]);
            if (!isset($this->broker->users[$name])) {
                return $this->json(404, ['error' => 'not found']);
            }
            $this->broker->setPermissions(
                $name,
                rawurldecode($m[2]),
                (string) ($json['configure'] ?? ''),
                (string) ($json['write'] ?? ''),
                (string) ($json['read'] ?? ''),
            );
            return $this->status(201, '', 'application/json');
        }
        if ($method === 'DELETE' && preg_match('#^/api/permissions/([^/]+)/([^/]+)$#', $path, $m) === 1) {
            if (!$this->broker->isAdmin($user)) {
                return $this->json(403, ['error' => 'forbidden']);
            }
            return $this->broker->clearPermissions(rawurldecode($m[1]), rawurldecode($m[2]))
                ? $this->status(204, '', 'application/json')
                : $this->json(404, ['error' => 'not found']);
        }
        if ($method === 'GET' && $path === '/api/permissions') {
            $items = [];
            foreach ($this->broker->permissions as $name => $byVhost) {
                foreach ($byVhost as $vhost => $row) {
                    $items[] = ['user' => $name, 'vhost' => $vhost] + $row;
                }
            }
            return $this->json(200, $items);
        }

        // Vhosts.
        if ($method === 'PUT' && preg_match('#^/api/vhosts/([^/]+)$#', $path, $m) === 1) {
            if (!$this->broker->isAdmin($user)) {
                return $this->json(403, ['error' => 'forbidden']);
            }
            $name = rawurldecode($m[1]);
            if (!in_array($name, $this->broker->vhosts, true)) {
                $this->broker->vhosts[] = $name;
            }
            return $this->status(201, '', 'application/json');
        }
        if ($method === 'DELETE' && preg_match('#^/api/vhosts/([^/]+)$#', $path, $m) === 1) {
            if (!$this->broker->isAdmin($user)) {
                return $this->json(403, ['error' => 'forbidden']);
            }
            $name = rawurldecode($m[1]);
            if ($name === '/') {
                return $this->json(400, ['error' => 'the default vhost cannot be deleted']);
            }
            $this->broker->vhosts = array_values(array_filter(
                $this->broker->vhosts,
                static fn (string $row): bool => $row !== $name,
            ));
            return $this->status(204, '', 'application/json');
        }

        // Policies and operator policies.
        foreach ([['policies', 'policies'], ['operator-policies', 'operatorPolicies']] as [$segment, $field]) {
            if ($method === 'PUT' && preg_match('#^/api/' . $segment . '/([^/]+)/([^/]+)$#', $path, $m) === 1) {
                if (!$this->broker->isAdmin($user)) {
                    return $this->json(403, ['error' => 'forbidden']);
                }
                $error = Policy::validate($json);
                if ($error !== null) {
                    return $this->json(400, ['reason' => $error]);
                }
                $definition = is_array($json['definition'] ?? null) ? $json['definition'] : [];
                $this->broker->{$field}[rawurldecode($m[1])][rawurldecode($m[2])] = [
                    'pattern' => (string) ($json['pattern'] ?? '.*'),
                    'definition' => $definition,
                    'priority' => (int) ($json['priority'] ?? 0),
                    'apply-to' => (string) ($json['apply-to'] ?? 'all'),
                ];
                // A policy edit reaches queues that already exist.
                $this->broker->applyPolicies();
                return $this->status(201, '', 'application/json');
            }
            if ($method === 'DELETE' && preg_match('#^/api/' . $segment . '/([^/]+)/([^/]+)$#', $path, $m) === 1) {
                if (!$this->broker->isAdmin($user)) {
                    return $this->json(403, ['error' => 'forbidden']);
                }
                $vhost = rawurldecode($m[1]);
                $name = rawurldecode($m[2]);
                if (!isset($this->broker->{$field}[$vhost][$name])) {
                    return $this->json(404, ['error' => 'not found']);
                }
                unset($this->broker->{$field}[$vhost][$name]);
                return $this->status(204, '', 'application/json');
            }
            if ($method === 'GET' && preg_match('#^/api/' . $segment . '/([^/]+)$#', $path, $m) === 1) {
                $vhost = rawurldecode($m[1]);
                $items = [];
                foreach ($this->broker->{$field}[$vhost] ?? [] as $name => $row) {
                    $items[] = ['vhost' => $vhost, 'name' => $name] + $row;
                }
                return $this->json(200, $items);
            }
            if ($method === 'GET' && $path === '/api/' . $segment) {
                $items = [];
                foreach ($this->broker->{$field} as $vhost => $rows) {
                    foreach ($rows as $name => $row) {
                        $items[] = ['vhost' => $vhost, 'name' => $name] + $row;
                    }
                }
                return $this->json(200, $items);
            }
        }

        // Limits.
        if ($method === 'PUT' && preg_match('#^/api/(user|vhost)-limits/([^/]+)/([^/]+)$#', $path, $m) === 1) {
            if (!$this->broker->isAdmin($user)) {
                return $this->json(403, ['error' => 'forbidden']);
            }
            $allowed = $m[1] === 'user'
                ? ['max-connections', 'max-channels']
                : ['max-connections', 'max-queues'];
            if (!in_array($m[3], $allowed, true)) {
                return $this->json(400, ['reason' => 'unknown limit ' . $m[3]]);
            }
            $field = $m[1] === 'user' ? 'userLimits' : 'vhostLimits';
            $this->broker->{$field}[rawurldecode($m[2])][$m[3]] = (int) ($json['value'] ?? 0);
            return $this->status(204, '', 'application/json');
        }
        if ($method === 'DELETE' && preg_match('#^/api/(user|vhost)-limits/([^/]+)/([^/]+)$#', $path, $m) === 1) {
            if (!$this->broker->isAdmin($user)) {
                return $this->json(403, ['error' => 'forbidden']);
            }
            $field = $m[1] === 'user' ? 'userLimits' : 'vhostLimits';
            unset($this->broker->{$field}[rawurldecode($m[2])][$m[3]]);
            return $this->status(204, '', 'application/json');
        }
        if ($method === 'GET' && $path === '/api/limits') {
            return $this->json(200, [
                'user_limits' => $this->broker->userLimits,
                'vhost_limits' => $this->broker->vhostLimits,
            ]);
        }

        // Topic permissions.
        if ($method === 'GET' && $path === '/api/topic-permissions') {
            $items = [];
            foreach ($this->broker->topicPermissions as $name => $byExchange) {
                foreach ($byExchange as $exchange => $row) {
                    $items[] = ['user' => $name, 'vhost' => '/', 'exchange' => $exchange] + $row;
                }
            }
            return $this->json(200, $items);
        }
        if ($method === 'PUT' && preg_match('#^/api/topic-permissions/([^/]+)/([^/]+)$#', $path, $m) === 1) {
            if (!$this->broker->isAdmin($user)) {
                return $this->json(403, ['error' => 'forbidden']);
            }
            $this->broker->topicPermissions[rawurldecode($m[1])][(string) ($json['exchange'] ?? '')] = [
                'write' => (string) ($json['write'] ?? ''),
                'read' => (string) ($json['read'] ?? ''),
            ];
            return $this->status(201, '', 'application/json');
        }
        if ($method === 'DELETE' && preg_match('#^/api/topic-permissions/([^/]+)/([^/]+)/([^/]+)$#', $path, $m) === 1) {
            if (!$this->broker->isAdmin($user)) {
                return $this->json(403, ['error' => 'forbidden']);
            }
            unset($this->broker->topicPermissions[rawurldecode($m[1])][rawurldecode($m[3])]);
            return $this->status(204, '', 'application/json');
        }

        // Feature flags.
        if ($method === 'GET' && $path === '/api/feature-flags') {
            $items = [];
            foreach ($this->broker->featureFlags as $name => $on) {
                $items[] = [
                    'name' => $name,
                    'state' => $on ? 'enabled' : 'disabled',
                    'stability' => 'stable',
                ];
            }
            return $this->json(200, $items);
        }
        if ($method === 'POST' && preg_match('#^/api/feature-flags/([^/]+)/(enable|disable)$#', $path, $m) === 1) {
            if (!$this->broker->isAdmin($user)) {
                return $this->json(403, ['error' => 'forbidden']);
            }
            $name = rawurldecode($m[1]);
            if (!isset($this->broker->featureFlags[$name])) {
                return $this->json(404, ['error' => 'not found']);
            }
            // A required flag cannot be turned off once it is on.
            if ($m[2] === 'disable' && $name === 'quorum_queues') {
                return $this->json(400, ['reason' => 'feature flag quorum_queues cannot be disabled']);
            }
            $this->broker->featureFlags[$name] = $m[2] === 'enable';
            return $this->status(204, '', 'application/json');
        }

        // Nodes and membership.
        if ($method === 'GET' && $path === '/api/cluster-name') {
            return $this->json(200, ['name' => $this->broker->nodeId]);
        }
        if ($method === 'POST' && $path === '/api/nodes') {
            $id = (string) ($json['id'] ?? '');
            $addr = (string) ($json['addr'] ?? '');
            if ($id === '' || $addr === '') {
                return $this->json(400, ['error' => 'a node needs an id and an addr']);
            }
            $members = array_values(array_filter(
                $this->broker->members,
                static fn (array $row): bool => $row['id'] !== $id,
            ));
            $members[] = ['id' => $id, 'addr' => $addr];
            $this->broker->members = $members;
            $this->broker->refreshRole();
            $this->onMembersChanged();
            return $this->json(201, $members);
        }
        if ($method === 'DELETE' && preg_match('#^/api/nodes/([^/]+)$#', $path, $m) === 1) {
            $id = rawurldecode($m[1]);
            if ($id === $this->broker->nodeId) {
                return $this->json(400, ['error' => 'a node cannot remove itself']);
            }
            // A node that is the stored home of a classic queue stays.
            foreach ($this->broker->queues as $name => $queue) {
                if ($this->broker->home($name) === $id) {
                    return $this->json(400, ['error' => "node $id is the home of queue $name"]);
                }
            }
            $members = array_values(array_filter(
                $this->broker->members,
                static fn (array $row): bool => $row['id'] !== $id,
            ));
            if ($members === []) {
                return $this->json(400, ['error' => 'the member list cannot become empty']);
            }
            $this->broker->members = $members;
            $this->broker->refreshRole();
            $this->onMembersChanged();
            return $this->status(204, '', 'application/json');
        }

        // Definitions import.
        if ($method === 'POST' && $path === '/api/definitions') {
            if (!$this->broker->isAdmin($user)) {
                return $this->json(403, ['error' => 'forbidden']);
            }
            foreach ((array) ($json['queues'] ?? []) as $row) {
                if (is_array($row) && isset($row['name'])) {
                    $args = is_array($row['arguments'] ?? null) ? $row['arguments'] : [];
                    $this->broker->declareQueue((string) $row['name'], $args);
                }
            }
            foreach ((array) ($json['exchanges'] ?? []) as $row) {
                if (is_array($row) && isset($row['name'])) {
                    $this->broker->declareExchange((string) $row['name'], (string) ($row['type'] ?? 'direct'));
                }
            }
            foreach ((array) ($json['bindings'] ?? []) as $row) {
                if (is_array($row) && isset($row['source'], $row['destination'])) {
                    if (($row['destination_type'] ?? 'queue') === 'exchange') {
                        $this->broker->bindExchange(
                            (string) $row['destination'],
                            (string) $row['source'],
                            (string) ($row['routing_key'] ?? ''),
                        );
                    } else {
                        $this->broker->bind(
                            (string) $row['destination'],
                            (string) $row['source'],
                            (string) ($row['routing_key'] ?? ''),
                        );
                    }
                }
            }
            return $this->status(204, '', 'application/json');
        }

        // Read-only views the SPA asks for.
        if ($method === 'GET' && preg_match('#^/api/consumers/([^/]+)$#', $path) === 1) {
            $items = [];
            foreach ($this->broker->queues as $name => $queue) {
                foreach ($queue['consumers'] as $consumer) {
                    $items[] = [
                        'queue' => ['name' => $name, 'vhost' => '/'],
                        'consumer_tag' => $consumer['tag'],
                        'ack_required' => ($consumer['noAck'] ?? false) !== true,
                        'prefetch_count' => (int) ($consumer['credit'] ?? 0),
                    ];
                }
            }
            return $this->json(200, $items);
        }
        if ($method === 'GET' && preg_match('#^/api/(connections|channels)/([^/]+)$#', $path) === 1) {
            return $this->json(404, ['error' => 'not found']);
        }
        if ($method === 'GET' && $path === '/api/deprecated-features') {
            return $this->json(200, []);
        }
        if ($method === 'DELETE' && preg_match('#^/api/deprecated-features/([^/]+)$#', $path) === 1) {
            if (!$this->broker->isAdmin($user)) {
                return $this->json(403, ['error' => 'forbidden']);
            }
            // Nothing is gated on a deprecated feature, so acknowledging one
            // has no further effect.
            return $this->status(204, '', 'application/json');
        }
        if ($method === 'DELETE' && preg_match('#^/api/connections/([^/]+)$#', $path) === 1) {
            if (!$this->broker->isAdmin($user)) {
                return $this->json(403, ['error' => 'forbidden']);
            }
            // Connections are not tracked by name in the management view, so
            // there is never a match to close.
            return $this->json(404, ['error' => 'not found']);
        }
        if ($method === 'PUT' && preg_match('#^/api/parameters/(shovel|federation-upstream)/([^/]+)/([^/]+)$#', $path, $m) === 1) {
            if (!$this->broker->isAdmin($user)) {
                return $this->json(403, ['error' => 'forbidden']);
            }
            $value = is_array($json['value'] ?? null) ? $json['value'] : [];
            $this->broker->parameters[$m[1]][rawurldecode($m[2])][rawurldecode($m[3])] = $value;
            if ($m[1] === 'federation-upstream') {
                $uri = (string) ($value['uri'] ?? '');
                if ($uri !== '') {
                    $this->broker->fedUpstreams[rawurldecode($m[3])] = $uri;
                }
            }
            return $this->status(201, '', 'application/json');
        }
        return $this->json(404, ['error' => 'not found']);
    }

    /** Persists and broadcasts a membership change. */
    private function onMembersChanged(): void
    {
        if ($this->onMembers !== null) {
            ($this->onMembers)();
        }
    }

    /** Replicates a change to users, permissions or topology. */
    private function onTopologyChanged(): void
    {
        if ($this->onTopology !== null) {
            ($this->onTopology)();
        }
    }

    /** @return list<array<string, mixed>> */
    private function queueItems(): array
    {
        $items = [];
        foreach ($this->broker->queues as $name => $queue) {
            $ready = count($queue['ready']);
            $items[] = [
                'name' => $name,
                'vhost' => '/',
                'durable' => true,
                'exclusive' => false,
                'auto_delete' => false,
                'state' => 'running',
                'messages' => $ready,
                'messages_ready' => $ready,
                'messages_unacknowledged' => 0,
                'consumers' => count($queue['consumers']),
                'type' => $queue['args']['queueType'] ?? 'classic',
            ];
        }
        return $items;
    }

    /** @return array<string, mixed> */
    private function overview(): array
    {
        $ready = 0;
        $consumers = 0;
        foreach ($this->broker->queues as $queue) {
            $ready += count($queue['ready']);
            $consumers += count($queue['consumers']);
        }
        return [
            'product_name' => 'QueueForge',
            'product_version' => '0.1.0',
            'management_version' => '0.1.0',
            'rabbitmq_version_compat' => '0.9.1',
            'object_totals' => [
                'connections' => $this->broker->prom['connections'],
                'channels' => 0,
                'queues' => count($this->broker->queues),
                'exchanges' => count($this->broker->exchanges),
                'consumers' => $consumers,
                'vhosts' => 1,
            ],
            'queue_totals' => [
                'messages' => $ready,
                'messages_ready' => $ready,
                'messages_unacknowledged' => 0,
            ],
            'message_stats' => [
                'publish' => $this->broker->prom['received'],
                'deliver' => $this->broker->prom['delivered'],
                'ack' => $this->broker->prom['acknowledged'],
            ],
        ];
    }

    /**
     * The Prometheus exposition. The series set matches Bun's so one
     * dashboard reads either broker, including the per-queue gauges.
     */
    private function metrics(): string
    {
        $p = $this->broker->prom;
        $gauge = static fn (string $name, int|float $value): array => ["# TYPE $name gauge", "$name $value"];
        $counter = static fn (string $name, int|float $value): array => ["# TYPE $name counter", "$name $value"];
        $lines = [
            ...$gauge('rabbitmq_up', 1),
            ...$gauge('rabbitmq_ready', $this->broker->ready ? 1 : 0),
            ...$gauge('rabbitmq_connections', $p['connections']),
            ...$counter('rabbitmq_connections_opened_total', $p['connectionsOpened']),
            ...$counter('rabbitmq_connections_closed_total', $p['connectionsClosed']),
            ...$gauge('rabbitmq_channels', $p['channels']),
            ...$counter('rabbitmq_channels_opened_total', $p['channelsOpened']),
            ...$counter('rabbitmq_channels_closed_total', $p['channelsClosed']),
            ...$gauge('rabbitmq_queues', count($this->broker->queues)),
            ...$counter('rabbitmq_queues_declared_total', $p['queuesDeclared']),
            ...$counter('rabbitmq_queues_created_total', $p['queuesCreated']),
            ...$counter('rabbitmq_queues_deleted_total', $p['queuesDeleted']),
            ...$gauge('rabbitmq_consumers', $p['consumers']),
            ...$gauge('rabbitmq_global_consumers', $p['consumers']),
            ...$gauge('rabbitmq_global_publishers', 0),
            ...$counter('rabbitmq_global_messages_received_total', $p['received']),
            ...$counter('rabbitmq_global_messages_received_confirm_total', $p['receivedConfirm']),
            ...$counter('rabbitmq_global_messages_confirmed_total', $p['confirmed']),
            ...$counter('rabbitmq_global_messages_routed_total', $p['routed']),
            ...$counter('rabbitmq_global_messages_unroutable_dropped_total', $p['unroutableDropped']),
            ...$counter('rabbitmq_global_messages_unroutable_returned_total', $p['unroutableReturned']),
            ...$counter('rabbitmq_global_messages_delivered_total', $p['delivered']),
            ...$counter('rabbitmq_global_messages_delivered_consume_manual_ack_total', $p['deliveredConsumeManual']),
            ...$counter('rabbitmq_global_messages_delivered_consume_auto_ack_total', $p['deliveredConsumeAuto']),
            ...$counter('rabbitmq_global_messages_delivered_get_manual_ack_total', $p['deliveredGetManual']),
            ...$counter('rabbitmq_global_messages_delivered_get_auto_ack_total', $p['deliveredGetAuto']),
            ...$counter('rabbitmq_global_messages_get_empty_total', $p['getEmpty']),
            ...$counter('rabbitmq_global_messages_acknowledged_total', $p['acknowledged']),
            ...$counter('rabbitmq_global_messages_redelivered_total', $p['redelivered']),
            ...$counter('rabbitmq_global_messages_dead_lettered_expired_total', $p['dlxExpired']),
            ...$counter('rabbitmq_global_messages_dead_lettered_rejected_total', $p['dlxRejected']),
            ...$counter('rabbitmq_global_messages_dead_lettered_maxlen_total', $p['dlxMaxlen']),
            ...$counter('rabbitmq_global_messages_dead_lettered_delivery_limit_total', $p['dlxDeliveryLimit']),
            ...$counter('rabbitmq_global_messages_dead_lettered_confirmed_total', 0),
            ...$gauge('rabbitmq_alarms_memory_used_watermark', 0),
            ...$gauge('rabbitmq_alarms_free_disk_space_watermark', 0),
            ...$gauge('rabbitmq_disk_space_available_bytes', $this->diskFree()),
            ...$gauge('rabbitmq_unreachable_cluster_peers_count', 0),
        ];
        // Per-queue gauges. Names are escaped because a queue name may
        // legitimately contain a quote or a backslash.
        $ready = ['# TYPE rabbitmq_queue_messages_ready gauge'];
        $unacked = ['# TYPE rabbitmq_queue_messages_unacked gauge'];
        $total = ['# TYPE rabbitmq_queue_messages gauge'];
        $consumers = ['# TYPE rabbitmq_queue_consumers gauge'];
        foreach ($this->broker->queues as $name => $queue) {
            $labels = '{vhost="/",queue="' . self::promLabel((string) $name) . '"}';
            $readyN = count($queue['ready']);
            $unackedN = $this->broker->depth((string) $name) - $readyN;
            $ready[] = 'rabbitmq_queue_messages_ready' . $labels . ' ' . $readyN;
            $unacked[] = 'rabbitmq_queue_messages_unacked' . $labels . ' ' . max(0, $unackedN);
            $total[] = 'rabbitmq_queue_messages' . $labels . ' ' . $this->broker->depth((string) $name);
            $consumers[] = 'rabbitmq_queue_consumers' . $labels . ' ' . count($queue['consumers']);
        }
        $lines = [
            ...$lines,
            ...$ready,
            ...$unacked,
            ...$total,
            ...$consumers,
            '# TYPE queueforge_wal_fsync_seconds histogram',
            'queueforge_wal_fsync_seconds_count ' . $this->broker->store->fsyncCount,
            'queueforge_wal_fsync_seconds_sum ' . $this->broker->store->fsyncSeconds,
            ...$counter('queueforge_confirm_before_fsync_total', $this->broker->store->confirmsBeforeFsync),
            ...$counter('queueforge_full_flush_total', $this->broker->store->fullFlushes),
            ...$gauge('rabbitmq_identity_info{rabbitmq_node="' . self::promLabel($this->broker->nodeId) . '",rabbitmq_cluster="queueforge"}', 1),
            ...$gauge('rabbitmq_build_info{rabbitmq_version="0.1.0"}', 1),
            '',
        ];
        return implode("\n", $lines);
    }

    /** Available bytes on the filesystem that holds the data directory. A failed probe is 0. */
    private function diskFree(): int
    {
        $free = disk_free_space(dirname($this->broker->store->path));
        return $free === false ? 0 : (int) $free;
    }

    /** Escapes a Prometheus label value. */
    private static function promLabel(string $value): string
    {
        return str_replace(['\\', "\n", '"'], ['\\\\', '\\n', '\\"'], $value);
    }

    /**
     * Serves the built SPA. A path that is not a real file falls back to
     * index.html so a client-side route survives a reload, except for the
     * asset extensions, where a miss should stay a 404 rather than return
     * HTML a script tag would choke on.
     */
    private function file(string $path): string
    {
        // The reserved paths must never reach the static handler. Without
        // this the history fallback below would answer a mistyped /healthz
        // with the SPA shell, which reads as a healthy broker.
        if (in_array($path, ['/healthz', '/readyz', '/metrics', '/api'], true)
            || str_starts_with($path, '/api/')) {
            return $this->status(404, "not found\n", 'text/plain');
        }
        $rel = $path === '/' ? '/index.html' : $path;
        $root = realpath($this->uiRoot);
        if ($root === false) {
            return $this->status(404, "not found\n", 'text/plain');
        }
        $file = realpath($this->uiRoot . $rel);
        if ($file === false || !str_starts_with($file, $root) || !is_file($file)) {
            $asset = preg_match('/\.(js|mjs|css|map|json|png|jpg|jpeg|gif|svg|ico|webp|woff2?|ttf|eot)$/i', $rel) === 1;
            if ($asset) {
                return $this->status(404, "not found\n", 'text/plain');
            }
            $index = realpath($root . '/index.html');
            if ($index === false || !is_file($index)) {
                return $this->status(404, "not found\n", 'text/plain');
            }
            return $this->status(200, (string) file_get_contents($index), 'text/html');
        }
        $type = match (true) {
            str_ends_with($file, '.js'), str_ends_with($file, '.mjs') => 'text/javascript',
            str_ends_with($file, '.css') => 'text/css',
            str_ends_with($file, '.json'), str_ends_with($file, '.map') => 'application/json',
            str_ends_with($file, '.svg') => 'image/svg+xml',
            str_ends_with($file, '.png') => 'image/png',
            str_ends_with($file, '.ico') => 'image/x-icon',
            str_ends_with($file, '.woff2') => 'font/woff2',
            default => 'text/html',
        };
        return $this->status(200, (string) file_get_contents($file), $type);
    }

    /** @param list<string> $extra */
    private function json(int $code, mixed $body, array $extra = []): string
    {
        $encoded = json_encode($body);
        return $this->status($code, is_string($encoded) ? $encoded : '{}', 'application/json', $extra);
    }

    /** @param list<string> $extra */
    private function status(int $code, string $body, string $type = 'text/plain', array $extra = []): string
    {
        $text = [200 => 'OK', 201 => 'Created', 400 => 'Bad Request', 401 => 'Unauthorized', 404 => 'Not Found', 503 => 'Service Unavailable'][$code] ?? 'OK';
        $headers = array_merge([
            "HTTP/1.1 $code $text",
            'Content-Type: ' . $type,
            'Content-Length: ' . strlen($body),
            'Connection: close',
        ], $extra);
        return implode("\r\n", $headers) . "\r\n\r\n" . $body;
    }
}
