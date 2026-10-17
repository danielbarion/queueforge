<?php
declare(strict_types=1);

/**
 * Peer discovery at startup: DNS, Kubernetes endpoints, Consul, etcd v3 and
 * AWS EC2 tags. Member ids are host:port addresses, as with DNS on Rust and
 * Bun; the member port is cluster.dns_port or the cluster listen port (a
 * backend that publishes a port, Consul and etcd, supplies its own).
 */
final class Discovery
{
    public const BACKENDS = ['dns', 'k8s', 'consul', 'etcd', 'aws'];
    /** Timeout for one discovery HTTP request, in seconds. */
    public const HTTP_TIMEOUT = 3.0;

    public static function resolve(array $config): array
    {
        $kind = (string) ($config['discovery'] ?? '');
        if (!in_array($kind, self::BACKENDS, true)) return $config;
        $listen = (string)($config['cluster'] ?? '');
        if ($listen === '') throw new RuntimeException("$kind discovery requires cluster.listen");
        $colon = strrpos($listen, ':');
        $port = (int)($config['dns_port'] ?? ($colon === false ? 0 : substr($listen, $colon + 1)));
        if ($port < 1 || $port > 65535) throw new RuntimeException('Invalid discovery port');
        $opts = is_array($config['discovery_opts'] ?? null) ? $config['discovery_opts'] : [];
        $id = (string)($config['node_id'] ?? '');
        if ($id === '') $id = $listen;
        // Registration happens once at startup; DiscoveryAgent re-lists without it.
        $register = ($config['discovery_register'] ?? true) !== false;
        $lease = null;
        $endpoints = match ($kind) {
            'dns' => self::dns($config, $port),
            'k8s' => self::k8s($opts, $port),
            'consul' => self::consul($opts, $listen, $id, $register),
            'etcd' => self::etcd($opts, $listen, $register, $lease),
            'aws' => self::aws($opts, $port),
        };
        if ($lease !== null) $config['discovery_lease'] = $lease;
        if ($kind === 'dns' && $endpoints === []) throw new RuntimeException('DNS discovery resolved no peer addresses');
        $members = [];
        foreach ($endpoints as $endpoint) $members[$endpoint] = ['id'=>$endpoint, 'addr'=>$endpoint];
        $members[$id] = ['id'=>$id, 'addr'=>$listen];
        // The listen address and this node's id name the same member.
        if ($id !== $listen) unset($members[$listen]);
        ksort($members, SORT_STRING);
        $config['node_id'] = $id; $config['members'] = array_values($members);
        return $config;
    }

    /** Joins a host (IPv6 bracketed) and port into a member endpoint. */
    public static function endpoint(string $host, int $port): string
    {
        $host = trim($host, '[]');
        return (str_contains($host, ':') ? '[' . $host . ']' : $host) . ':' . $port;
    }

    /** @return list<string> */
    private static function dns(array $config, int $port): array
    {
        $name = (string)($config['dns_name'] ?? '');
        if ($name === '') throw new RuntimeException('DNS discovery requires cluster.listen and cluster.dns_name');
        $addresses = [];
        if (function_exists('socket_addrinfo_lookup')) {
            foreach (@socket_addrinfo_lookup($name, (string)$port, ['ai_socktype'=>SOCK_STREAM]) ?: [] as $info) {
                $row = socket_addrinfo_explain($info)['ai_addr'];
                $addresses[] = $row['sin_addr'] ?? $row['sin6_addr'] ?? '';
            }
        } else {
            if (function_exists('gethostbynamelist')) $addresses = @gethostbynamelist($name) ?: [];
            foreach (@dns_get_record($name, DNS_A | DNS_AAAA) ?: [] as $row) {
                if (isset($row['ip'])) $addresses[] = $row['ip'];
                if (isset($row['ipv6'])) $addresses[] = $row['ipv6'];
            }
        }
        $out = [];
        foreach (array_unique($addresses) as $address) if (filter_var($address, FILTER_VALIDATE_IP) !== false) $out[] = self::endpoint($address, $port);
        return $out;
    }

    private static function opt(array $opts, string $key, mixed $default): mixed
    {
        return array_key_exists($key, $opts) && $opts[$key] !== '' ? $opts[$key] : $default;
    }
    private static function file(string $path): ?string
    {
        return is_file($path) && is_readable($path) ? trim((string) file_get_contents($path)) : null;
    }

    /**
     * One HTTP request. Returns [status, body]; a transport failure throws.
     *
     * @param array<string, string> $headers
     * @return array{0:int,1:string}
     */
    public static function http(string $method, string $url, array $headers = [], ?string $body = null, ?string $ca = null): array
    {
        // A 1.1 request needs Connection: close or the read waits for the keep-alive timeout.
        $headers += ['Connection' => 'close'];
        $lines = '';
        foreach ($headers as $name => $value) $lines .= "$name: $value\r\n";
        $ssl = ['verify_peer' => true, 'verify_peer_name' => true];
        if ($ca !== null && $ca !== '' && is_file($ca)) $ssl['cafile'] = $ca;
        $context = stream_context_create(['http' => ['method' => $method, 'header' => $lines, 'content' => $body ?? '', 'timeout' => self::HTTP_TIMEOUT, 'ignore_errors' => true, 'follow_location' => 0, 'protocol_version' => 1.1], 'ssl' => $ssl]);
        $response = @file_get_contents($url, false, $context);
        $meta = function_exists('http_get_last_response_headers') ? (http_get_last_response_headers() ?? []) : [];
        if ($response === false && $meta === []) throw new RuntimeException("discovery request to $url failed");
        $status = preg_match('#^HTTP/\S+\s+(\d{3})#', (string) ($meta[0] ?? ''), $m) === 1 ? (int) $m[1] : 0;
        return [$status, (string) $response];
    }
    private static function json(array $reply, string $what): mixed
    {
        [$status, $body] = $reply;
        if ($status < 200 || $status > 299) throw new RuntimeException("$what returned HTTP $status");
        // Consul answers a registration or a check pass with an empty 200.
        if (trim($body) === '') return null;
        $decoded = json_decode($body, true);
        if ($decoded === null && trim($body) !== 'null') throw new RuntimeException("$what returned invalid JSON");
        return $decoded;
    }

    /** @return list<string> Kubernetes endpoints of the service, ready or not. */
    private static function k8s(array $opts, int $port): array
    {
        $host = (string) self::opt($opts, 'k8s_host', getenv('KUBERNETES_SERVICE_HOST') ?: 'kubernetes.default.svc');
        $apiPort = (int) self::opt($opts, 'k8s_port', getenv('KUBERNETES_SERVICE_PORT') ?: 443);
        $scheme = (string) self::opt($opts, 'k8s_scheme', 'https');
        $sa = '/var/run/secrets/kubernetes.io/serviceaccount';
        $token = self::file((string) self::opt($opts, 'k8s_token_path', "$sa/token"));
        $namespace = self::file((string) self::opt($opts, 'k8s_namespace_path', "$sa/namespace")) ?: 'default';
        $service = (string) self::opt($opts, 'k8s_service_name', 'queueforge');
        $type = (string) self::opt($opts, 'k8s_address_type', 'ip');
        $suffix = (string) self::opt($opts, 'k8s_hostname_suffix', '');
        $url = sprintf('%s://%s/api/v1/namespaces/%s/endpoints/%s', $scheme, self::endpoint($host, $apiPort), rawurlencode($namespace), rawurlencode($service));
        $headers = ['Accept' => 'application/json'];
        if ($token !== null && $token !== '') $headers['Authorization'] = 'Bearer ' . $token;
        $doc = self::json(self::http('GET', $url, $headers, null, (string) self::opt($opts, 'k8s_ca_path', "$sa/ca.crt")), 'Kubernetes endpoints');
        $out = [];
        foreach ((array) ($doc['subsets'] ?? []) as $subset) foreach (['addresses', 'notReadyAddresses'] as $field) foreach ((array) ($subset[$field] ?? []) as $address) {
            $name = $type === 'hostname' ? (isset($address['hostname']) ? $address['hostname'] . $suffix : null) : ($address['ip'] ?? null);
            if (is_string($name) && $name !== '') $out[] = self::endpoint($name, $port);
        }
        return array_values(array_unique($out));
    }

    /** @return array{base:string,headers:array<string,string>,svc:string,id:string,addr:string,port:int,ttl:int} */
    public static function consulAgent(array $opts, string $listen, string $nodeId): array
    {
        $colon = strrpos($listen, ':');
        $listenHost = trim($colon === false ? $listen : substr($listen, 0, $colon), '[]');
        $port = $colon === false ? 0 : (int) substr($listen, $colon + 1);
        $svc = (string) self::opt($opts, 'consul_svc', 'queueforge');
        $addr = (string) self::opt($opts, 'consul_svc_addr', $listenHost);
        $headers = ['Content-Type' => 'application/json'];
        $token = (string) self::opt($opts, 'consul_acl_token', '');
        if ($token !== '') $headers['X-Consul-Token'] = $token;
        $base = (string) self::opt($opts, 'consul_scheme', 'http') . '://' . self::endpoint((string) self::opt($opts, 'consul_host', 'localhost'), (int) self::opt($opts, 'consul_port', 8500));
        return ['base' => $base, 'headers' => $headers, 'svc' => $svc, 'id' => $svc . ':' . $nodeId, 'addr' => $addr, 'port' => $port, 'ttl' => max(1, (int) self::opt($opts, 'consul_svc_ttl', 30))];
    }
    /** Registers this node with a TTL check, passes it, and lists the passing members. @return list<string> */
    public static function consul(array $opts, string $listen, string $nodeId, bool $register): array
    {
        $a = self::consulAgent($opts, $listen, $nodeId);
        if ($register) {
            $body = json_encode(['ID' => $a['id'], 'Name' => $a['svc'], 'Address' => $a['addr'], 'Port' => $a['port'], 'Check' => ['TTL' => $a['ttl'] . 's', 'DeregisterCriticalServiceAfter' => max(60, $a['ttl'] * 10) . 's']], JSON_THROW_ON_ERROR);
            self::json(self::http('PUT', $a['base'] . '/v1/agent/service/register', $a['headers'], $body), 'Consul service registration');
            self::consulPass($a);
        }
        $rows = self::json(self::http('GET', $a['base'] . '/v1/health/service/' . rawurlencode($a['svc']) . '?passing=true', $a['headers']), 'Consul health query');
        $out = [];
        foreach ((array) $rows as $row) {
            $address = (string) (($row['Service']['Address'] ?? '') !== '' ? $row['Service']['Address'] : ($row['Node']['Address'] ?? ''));
            $port = (int) ($row['Service']['Port'] ?? 0);
            if ($address !== '' && $port > 0) $out[] = self::endpoint($address, $port);
        }
        return array_values(array_unique($out));
    }
    public static function consulPass(array $a): void
    {
        self::http('PUT', $a['base'] . '/v1/agent/check/pass/' . rawurlencode('service:' . $a['id']), $a['headers'], '');
    }
    public static function consulDeregister(array $a): void
    {
        self::http('PUT', $a['base'] . '/v1/agent/service/deregister/' . rawurlencode($a['id']), $a['headers'], '');
    }

    /** @return array{endpoints:list<string>,dir:string,key:string,value:string,ttl:int,user:string,password:string} */
    public static function etcdAgent(array $opts, string $listen): array
    {
        $endpoints = self::opt($opts, 'etcd_endpoints', ['http://127.0.0.1:2379']);
        if (is_string($endpoints)) $endpoints = [$endpoints];
        $prefix = trim((string) self::opt($opts, 'etcd_key_prefix', 'queueforge'), '/');
        $dir = '/' . $prefix . '/discovery/' . (string) self::opt($opts, 'etcd_cluster_name', 'default') . '/nodes/';
        return ['endpoints' => array_values(array_map('strval', (array) $endpoints)), 'dir' => $dir, 'key' => $dir . $listen, 'value' => $listen,
            'ttl' => max(1, (int) self::opt($opts, 'etcd_node_ttl', 30)), 'user' => (string) self::opt($opts, 'etcd_username', ''), 'password' => (string) self::opt($opts, 'etcd_password', '')];
    }
    /** POSTs to the v3 JSON gateway, trying each endpoint in turn. */
    public static function etcdCall(array $a, string $path, array $body, ?string &$token = null): mixed
    {
        $error = 'no etcd endpoints';
        foreach ($a['endpoints'] as $endpoint) {
            try {
                $headers = ['Content-Type' => 'application/json'];
                if ($a['user'] !== '') {
                    if ($token === null) {
                        $auth = self::json(self::http('POST', rtrim($endpoint, '/') . '/v3/auth/authenticate', $headers, json_encode(['name' => $a['user'], 'password' => $a['password']], JSON_THROW_ON_ERROR)), 'etcd authenticate');
                        $token = (string) ($auth['token'] ?? '');
                    }
                    $headers['Authorization'] = (string) $token;
                }
                return self::json(self::http('POST', rtrim($endpoint, '/') . $path, $headers, json_encode($body, JSON_THROW_ON_ERROR)), "etcd $path");
            } catch (RuntimeException $e) { $error = $e->getMessage(); }
        }
        throw new RuntimeException($error);
    }
    /** The key after the last one with this prefix, for a v3 range request. */
    public static function rangeEnd(string $prefix): string
    {
        $last = strlen($prefix) - 1;
        return substr($prefix, 0, $last) . chr(ord($prefix[$last]) + 1);
    }
    /** Writes this node's key under a lease and lists the member keys. @return list<string> */
    public static function etcd(array $opts, string $listen, bool $register, ?string &$lease = null): array
    {
        $a = self::etcdAgent($opts, $listen); $token = null;
        if ($register) {
            $grant = self::etcdCall($a, '/v3/lease/grant', ['TTL' => $a['ttl']], $token);
            $lease = (string) ($grant['ID'] ?? '');
            if ($lease === '') throw new RuntimeException('etcd lease grant returned no ID');
            self::etcdCall($a, '/v3/kv/put', ['key' => base64_encode($a['key']), 'value' => base64_encode($a['value']), 'lease' => $lease], $token);
        }
        $range = self::etcdCall($a, '/v3/kv/range', ['key' => base64_encode($a['dir']), 'range_end' => base64_encode(self::rangeEnd($a['dir']))], $token);
        $out = [];
        foreach ((array) ($range['kvs'] ?? []) as $kv) {
            $key = base64_decode((string) ($kv['key'] ?? ''), true);
            if (!is_string($key) || !str_starts_with($key, $a['dir'])) continue;
            $member = substr($key, strlen($a['dir']));
            if (preg_match('/^(\[[^\]]+\]|[^:\/]+):\d+$/', $member) === 1) $out[] = $member;
        }
        return array_values(array_unique($out));
    }
    public static function etcdKeepalive(array $opts, string $listen, string $lease): void
    {
        self::etcdCall(self::etcdAgent($opts, $listen), '/v3/lease/keepalive', ['ID' => $lease]);
    }

    /**
     * AWS Signature Version 4 for a request. Returns the headers to send,
     * including Authorization; `$headers` must not already hold it.
     *
     * @param array<string, string> $headers
     * @return array<string, string>
     */
    public static function sigv4(string $method, string $url, array $headers, string $payload, string $region, string $service, string $accessKey, string $secretKey, ?string $sessionToken = null, ?string $amzDate = null): array
    {
        $parts = parse_url($url);
        $amzDate ??= gmdate('Ymd\THis\Z');
        $date = substr($amzDate, 0, 8);
        $host = (string) ($parts['host'] ?? '') . (isset($parts['port']) ? ':' . $parts['port'] : '');
        $headers = array_change_key_case($headers, CASE_LOWER);
        $headers['host'] ??= $host;
        $headers['x-amz-date'] = $amzDate;
        if ($sessionToken !== null && $sessionToken !== '') $headers['x-amz-security-token'] = $sessionToken;
        $path = (string) ($parts['path'] ?? '/');
        $canonicalPath = $path === '' ? '/' : implode('/', array_map(static fn ($s) => rawurlencode(rawurldecode($s)), explode('/', $path)));
        $query = [];
        foreach (explode('&', (string) ($parts['query'] ?? '')) as $pair) {
            if ($pair === '') continue;
            [$k, $v] = array_pad(explode('=', $pair, 2), 2, '');
            $query[] = [rawurlencode(rawurldecode($k)), rawurlencode(rawurldecode($v))];
        }
        usort($query, static fn ($a, $b) => [$a[0], $a[1]] <=> [$b[0], $b[1]]);
        $canonicalQuery = implode('&', array_map(static fn ($p) => $p[0] . '=' . $p[1], $query));
        ksort($headers, SORT_STRING);
        $canonicalHeaders = '';
        foreach ($headers as $name => $value) $canonicalHeaders .= $name . ':' . trim((string) preg_replace('/\s+/', ' ', (string) $value)) . "\n";
        $signed = implode(';', array_keys($headers));
        $canonical = "$method\n$canonicalPath\n$canonicalQuery\n$canonicalHeaders\n$signed\n" . hash('sha256', $payload);
        $scope = "$date/$region/$service/aws4_request";
        $toSign = "AWS4-HMAC-SHA256\n$amzDate\n$scope\n" . hash('sha256', $canonical);
        $key = hash_hmac('sha256', 'aws4_request', hash_hmac('sha256', $service, hash_hmac('sha256', $region, hash_hmac('sha256', $date, 'AWS4' . $secretKey, true), true), true), true);
        $headers['authorization'] = "AWS4-HMAC-SHA256 Credential=$accessKey/$scope, SignedHeaders=$signed, Signature=" . hash_hmac('sha256', $toSign, $key);
        return $headers;
    }

    /** Static keys, then the environment, then the instance role over IMDSv2. @return array{0:string,1:string,2:?string} */
    private static function awsCredentials(array $opts): array
    {
        $key = (string) self::opt($opts, 'aws_access_key_id', getenv('AWS_ACCESS_KEY_ID') ?: '');
        $secret = (string) self::opt($opts, 'aws_secret_access_key', getenv('AWS_SECRET_ACCESS_KEY') ?: '');
        if ($key !== '' && $secret !== '') return [$key, $secret, (getenv('AWS_SESSION_TOKEN') ?: null)];
        $imds = rtrim((string) self::opt($opts, 'aws_imds_endpoint', 'http://169.254.169.254'), '/');
        [$status, $token] = self::http('PUT', "$imds/latest/api/token", ['X-aws-ec2-metadata-token-ttl-seconds' => '21600'], '');
        if ($status !== 200) throw new RuntimeException('AWS IMDSv2 token request failed');
        $auth = ['X-aws-ec2-metadata-token' => trim($token)];
        [$status, $role] = self::http('GET', "$imds/latest/meta-data/iam/security-credentials/", $auth);
        $role = trim(explode("\n", trim($role))[0] ?? '');
        if ($status !== 200 || $role === '') throw new RuntimeException('AWS instance has no IAM role');
        $creds = self::json(self::http('GET', "$imds/latest/meta-data/iam/security-credentials/" . rawurlencode($role), $auth), 'AWS instance credentials');
        return [(string) ($creds['AccessKeyId'] ?? ''), (string) ($creds['SecretAccessKey'] ?? ''), isset($creds['Token']) ? (string) $creds['Token'] : null];
    }
    /** @return list<string> running instances carrying every configured tag */
    private static function aws(array $opts, int $port): array
    {
        $region = (string) self::opt($opts, 'aws_region', getenv('AWS_REGION') ?: (getenv('AWS_DEFAULT_REGION') ?: 'us-east-1'));
        [$key, $secret, $session] = self::awsCredentials($opts);
        $endpoint = rtrim((string) self::opt($opts, 'aws_endpoint', "https://ec2.$region.amazonaws.com"), '/');
        $params = ['Action' => 'DescribeInstances', 'Version' => '2016-11-15'];
        $n = 1;
        foreach ((array) self::opt($opts, 'aws_ec2_tags', []) as $tag => $value) { $params["Filter.$n.Name"] = "tag:$tag"; $params["Filter.$n.Value.1"] = (string) $value; $n++; }
        $params["Filter.$n.Name"] = 'instance-state-name'; $params["Filter.$n.Value.1"] = 'running';
        $url = $endpoint . '/?' . http_build_query($params, '', '&', PHP_QUERY_RFC3986);
        $headers = self::sigv4('GET', $url, [], '', $region, 'ec2', $key, $secret, $session);
        unset($headers['host']);
        [$status, $body] = self::http('GET', $url, $headers);
        if ($status !== 200) throw new RuntimeException("AWS DescribeInstances returned HTTP $status");
        $private = (bool) self::opt($opts, 'aws_use_private_ip', true);
        $xml = @simplexml_load_string($body);
        if ($xml === false) throw new RuntimeException('AWS DescribeInstances returned invalid XML');
        $out = [];
        foreach ($xml->reservationSet->item ?? [] as $reservation) foreach ($reservation->instancesSet->item ?? [] as $instance) {
            $address = $private ? (string) $instance->privateIpAddress : (string) $instance->ipAddress;
            if ($address === '') $address = (string) $instance->privateDnsName;
            if ($address !== '') $out[] = self::endpoint($address, $port);
        }
        return array_values(array_unique($out));
    }

    /**
     * Parses one TOML value as the [cluster] discovery keys use it: a
     * string, integer, boolean, array of scalars, or an inline table.
     */
    public static function tomlValue(string $raw): mixed
    {
        $raw = trim($raw);
        if ($raw === '') return '';
        if ($raw[0] === '"' || $raw[0] === "'") return $raw[0] === '"' ? stripcslashes(substr($raw, 1, -1)) : substr($raw, 1, -1);
        if ($raw === 'true' || $raw === 'false') return $raw === 'true';
        if (preg_match('/^-?\d+$/', $raw) === 1) return (int) $raw;
        if ($raw[0] === '[' || $raw[0] === '{') {
            $table = $raw[0] === '{';
            $inner = substr($raw, 1, -1);
            preg_match_all('/\s*(?:([A-Za-z0-9_.-]+|"[^"]*")\s*=\s*)?("(?:[^"\\\\]|\\\\.)*"|\'[^\']*\'|[^,]+?)\s*(?:,|$)/', $inner, $m, PREG_SET_ORDER);
            $out = [];
            foreach ($m as $row) {
                if (trim($row[0]) === '' || trim($row[2]) === '') continue;
                $value = self::tomlValue($row[2]);
                if ($table) $out[trim($row[1], '"')] = $value; else $out[] = $value;
            }
            return $out;
        }
        return $raw;
    }
}

/**
 * Runtime half of cloud discovery: keeps a Consul TTL check or an etcd lease
 * alive, re-lists the members periodically and hands new ones to the
 * membership path, and deregisters on shutdown. Each HTTP call is bounded by
 * Discovery::HTTP_TIMEOUT and runs on a coarse timer, never per message.
 */
final class DiscoveryAgent
{
    private float $nextBeat = 0.0;
    private float $nextList = 0.0;
    private ?string $lease = null;
    private ?array $consul = null;
    public string $error = '';
    /** @param callable(list<string>):void $onMembers */
    public function __construct(private array $config, private $onMembers)
    {
        $opts = $this->opts();
        $kind = (string) $config['discovery'];
        if ($kind === 'consul') $this->consul = Discovery::consulAgent($opts, (string) $config['cluster'], (string) $config['node_id']);
        $now = microtime(true);
        $this->nextBeat = $now + $this->ttl() / 2;
        $this->nextList = $now + $this->interval();
        $this->lease = isset($config['discovery_lease']) ? (string) $config['discovery_lease'] : null;
    }
    private function opts(): array { return is_array($this->config['discovery_opts'] ?? null) ? $this->config['discovery_opts'] : []; }
    private function ttl(): int
    {
        $opts = $this->opts();
        return max(1, (int) ($this->config['discovery'] === 'etcd' ? ($opts['etcd_node_ttl'] ?? 30) : ($opts['consul_svc_ttl'] ?? 30)));
    }
    private function interval(): float { return max(1.0, (float) ($this->opts()['discovery_interval'] ?? 30)); }
    public function tick(): void
    {
        $now = microtime(true);
        $kind = (string) $this->config['discovery'];
        try {
            if ($now >= $this->nextBeat && ($kind === 'consul' || $kind === 'etcd')) {
                $this->nextBeat = $now + $this->ttl() / 2;
                if ($kind === 'consul' && $this->consul !== null) Discovery::consulPass($this->consul);
                if ($kind === 'etcd') {
                    // The first keepalive also writes the key: a node that
                    // resolved before etcd was reachable registers late.
                    if ($this->lease === null) Discovery::etcd($this->opts(), (string) $this->config['cluster'], true, $this->lease);
                    else Discovery::etcdKeepalive($this->opts(), (string) $this->config['cluster'], $this->lease);
                }
            }
            if ($now >= $this->nextList) {
                $this->nextList = $now + $this->interval();
                $resolved = Discovery::resolve(['discovery_register' => false, 'discovery_opts' => $this->opts()] + $this->config + ['members' => []]);
                ($this->onMembers)($resolved['members']);
            }
            $this->error = '';
        } catch (Throwable $e) { $this->error = $e->getMessage(); }
    }
    public function shutdown(): void
    {
        try { if ($this->consul !== null) Discovery::consulDeregister($this->consul); } catch (Throwable) {}
    }
}
