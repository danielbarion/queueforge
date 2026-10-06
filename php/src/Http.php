<?php
declare(strict_types=1);

/** Management HTTP and Prometheus text. Routes match the Rust management API the SPA calls. */
final class Http
{
    /** @var array<string, string> */
    public array $sessions = [];

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
        foreach ($lines as $line) {
            if (str_starts_with(strtolower($line), 'cookie:')) {
                $cookie = trim(substr($line, 7));
            }
        }
        $user = $this->userFromCookie($cookie);
        if ($method === 'GET' && ($path === '/healthz' || $path === '/readyz')) {
            $ok = $path === '/healthz' || $this->broker->ready;
            return $this->status($ok ? 200 : 503, $ok ? "ok\n" : "not ready\n", 'text/plain');
        }
        if ($method === 'GET' && $path === '/metrics') {
            return $this->status(200, $this->metrics(), 'text/plain; version=0.0.4');
        }
        if ($method === 'POST' && $path === '/api/login') {
            return $this->login($body);
        }
        if ($method === 'POST' && $path === '/api/logout') {
            return $this->status(200, '', 'application/json', ['Set-Cookie: ' . $this->cookieName() . '=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0']);
        }
        if ($user === null && str_starts_with($path, '/api/')) {
            return $this->json(401, ['error' => 'unauthorized']);
        }
        if (str_starts_with($path, '/api/')) {
            return $this->api($method, $path, $body, $user ?? '');
        }
        return $this->file($path);
    }

    private function login(string $body): string
    {
        $json = json_decode($body, true);
        $name = is_array($json) ? (string) ($json['username'] ?? '') : '';
        $pass = is_array($json) ? (string) ($json['password'] ?? '') : '';
        if (!$this->broker->verify($name, $pass)) {
            return $this->json(401, ['error' => 'unauthorized']);
        }
        $token = bin2hex(random_bytes(16));
        $this->sessions[$token] = $name;
        $flags = 'HttpOnly; SameSite=Lax; Path=/';
        if ($this->secure) {
            $flags .= '; Secure';
        }
        return $this->json(200, ['name' => $name, 'tags' => ['administrator']], [
            'Set-Cookie: ' . $this->cookieName() . '=' . $token . '; ' . $flags,
        ]);
    }

    private function cookieName(): string
    {
        return $this->port === 80 || $this->port === 443 ? 'queueforge_session' : 'queueforge_session_' . $this->port;
    }

    private function userFromCookie(string $header): ?string
    {
        $name = $this->cookieName();
        foreach (explode(';', $header) as $part) {
            $part = trim($part);
            if (str_starts_with($part, $name . '=')) {
                return $this->sessions[substr($part, strlen($name) + 1)] ?? null;
            }
        }
        return null;
    }

    private function api(string $method, string $path, string $body, string $user): string
    {
        if ($method === 'GET' && $path === '/api/whoami') {
            return $this->json(200, ['name' => $user, 'tags' => ['administrator']]);
        }
        if ($method === 'GET' && $path === '/api/overview') {
            return $this->json(200, $this->overview());
        }
        if ($method === 'GET' && $path === '/api/vhosts') {
            return $this->json(200, ['items' => [['name' => '/']], 'total_count' => 1]);
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
        if ($method === 'GET' && $path === '/api/bindings/%2F' || ($method === 'GET' && preg_match('#^/api/bindings/([^/]+)$#', $path) === 1)) {
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
                $items[] = ['name' => $name, 'tags' => ['administrator']];
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
        if ($method === 'GET' && ($path === '/api/nodes' || $path === '/api/permissions' || $path === '/api/policies' || $path === '/api/channels')) {
            return $this->json(200, []);
        }
        return $this->json(404, ['error' => 'not found']);
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

    private function metrics(): string
    {
        $p = $this->broker->prom;
        $lines = [
            '# TYPE rabbitmq_up gauge',
            'rabbitmq_up 1',
            '# TYPE rabbitmq_ready gauge',
            'rabbitmq_ready ' . ($this->broker->ready ? 1 : 0),
            '# TYPE rabbitmq_connections gauge',
            'rabbitmq_connections ' . $p['connections'],
            '# TYPE rabbitmq_queues gauge',
            'rabbitmq_queues ' . count($this->broker->queues),
            '# TYPE rabbitmq_global_messages_received_total counter',
            'rabbitmq_global_messages_received_total ' . $p['received'],
            '# TYPE rabbitmq_global_messages_delivered_total counter',
            'rabbitmq_global_messages_delivered_total ' . $p['delivered'],
            '# TYPE rabbitmq_global_messages_acknowledged_total counter',
            'rabbitmq_global_messages_acknowledged_total ' . $p['acknowledged'],
            '# TYPE queueforge_confirm_before_fsync_total counter',
            'queueforge_confirm_before_fsync_total 0',
            '# TYPE rabbitmq_build_info gauge',
            'rabbitmq_build_info{rabbitmq_version="0.1.0"} 1',
            '',
        ];
        return implode("\n", $lines);
    }

    private function file(string $path): string
    {
        $rel = $path === '/' ? '/index.html' : $path;
        $full = $this->uiRoot . $rel;
        $root = realpath($this->uiRoot);
        $file = realpath($full);
        if ($root === false || $file === false || !str_starts_with($file, $root) || !is_file($file)) {
            return $this->status(404, "not found\n", 'text/plain');
        }
        $type = str_ends_with($file, '.js') ? 'text/javascript' : (str_ends_with($file, '.css') ? 'text/css' : 'text/html');
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
