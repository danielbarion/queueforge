<?php
declare(strict_types=1);

/** External authentication, after the internal user store (shared Bun/Rust semantics). */
final class Security
{
    private static array $jwks = [];

    public static function verify(Broker $broker, array $cfg, string $username, string $password): ?array
    {
        if (array_key_exists($username, $broker->users)) return null;
        try {
            $oauth = $cfg['oauth'] ?? $cfg['oauth2'] ?? $cfg['auth']['oauth2'] ?? null;
            if (is_array($oauth) && substr_count($password, '.') === 2) {
                try { $principal = self::oauth($broker, $oauth, $username, $password); } catch (Throwable) { $principal = null; }
                if ($principal !== null) return $principal;
            }
            $ldap = $cfg['ldap'] ?? $cfg['auth']['ldap'] ?? null;
            return is_array($ldap) ? self::ldap($broker, $ldap, $username, $password) : null;
        } catch (Throwable) { return null; }
    }

    public static function live(array $p): bool
    {
        return ($p['expiresAt'] ?? null) === null || microtime(true) * 1000 < $p['expiresAt'];
    }
    public static function hasVhost(array $p, string $vhost): bool
    {
        if (!self::live($p)) return false;
        if (array_key_exists('scopes', $p) && $p['scopes'] === null) return true;
        foreach ($p['scopes'] ?? [] as $s) if (self::match($s['vhost'], $vhost)) return true;
        return false;
    }
    public static function allows(array $p, string $vhost, string $kind, string $resource, ?string $routingKey = null): bool
    {
        if (!self::live($p)) return false;
        if (array_key_exists('scopes', $p) && $p['scopes'] === null) return true;
        foreach ($p['scopes'] ?? [] as $s) {
            if ($s['kind'] === $kind && self::match($s['vhost'], $vhost) && self::match($s['resource'], $resource)
                && ($routingKey === null || $s['routingKey'] === null || self::match($s['routingKey'], $routingKey))) return true;
        }
        return false;
    }
    private static function match(string $pattern, string $value): bool { return preg_match('~' . $pattern . '~D', $value) === 1; }
    private static function wildcard(string $value): string
    {
        return '^' . implode('.*', array_map(static fn ($s) => preg_quote($s, '~'), explode('*', rawurldecode($value)))) . '$';
    }
    private static function option(array $cfg, string $camel, string $snake, mixed $default = null): mixed { return $cfg[$camel] ?? $cfg[$snake] ?? $default; }
    private static function principal(Broker $broker, string $name, string $source, array $tags, ?array $scopes, ?float $expiry): ?array
    {
        if ($name === '' || array_key_exists($name, $broker->users)) return null;
        $p = ['name' => $name, 'source' => $source, 'tags' => array_values(array_unique($tags)), 'scopes' => $scopes,
            'expiresAt' => $expiry, 'permissions' => [], 'topicPermissions' => []];
        foreach ($broker->vhosts as $vhost) {
            $rules = ['configure' => '(?!)', 'write' => '(?!)', 'read' => '(?!)'];
            if ($scopes === null) $rules = array_fill_keys(array_keys($rules), '.*');
            else foreach (array_keys($rules) as $kind) {
                $resources = [];
                foreach ($scopes as $s) if ($s['kind'] === $kind && self::match($s['vhost'], $vhost)) $resources[] = '(?:' . $s['resource'] . ')';
                if ($resources !== []) $rules[$kind] = implode('|', $resources);
            }
            if ($scopes === null || self::hasVhost($p, $vhost)) $p['permissions'][$vhost] = $rules;
        }
        // The scoped list preserves the resource/routing-key pairing, unlike a flat map.
        foreach ($scopes ?? [] as $s) if ($s['routingKey'] !== null) $p['topicPermissions'][] = $s;
        return $p;
    }

    private static function oauth(Broker $broker, array $cfg, string $username, string $token): ?array
    {
        if (strlen($token) > 65536) return null;
        [$h, $c, $sig] = explode('.', $token);
        $header = json_decode(self::b64($h), true, 32, JSON_THROW_ON_ERROR);
        $claims = json_decode(self::b64($c), true, 32, JSON_THROW_ON_ERROR);
        if (!is_array($header) || !is_array($claims) || ($header['alg'] ?? null) !== 'RS256' || !is_string($header['kid'] ?? null) || $header['kid'] === '' || !empty($header['crit'])) return null;
        $key = self::key($cfg, $header['kid']);
        if ($key === null || openssl_verify($h . '.' . $c, self::b64($sig), $key, OPENSSL_ALGO_SHA256) !== 1) return null;
        $exp = $claims['exp'] ?? null; $nbf = $claims['nbf'] ?? null; $now = microtime(true);
        if ((!is_int($exp) && !is_float($exp)) || !is_finite((float) $exp) || $exp <= $now) return null;
        if (array_key_exists('nbf', $claims) && ((!is_int($nbf) && !is_float($nbf)) || !is_finite((float) $nbf) || $nbf > $now + 60)) return null;
        $resource = self::option($cfg, 'resourceServerId', 'resource_server_id', 'rabbitmq');
        if (!is_string($resource) || $resource === '') return null;
        $aud = $claims['aud'] ?? null;
        if (is_string($aud)) $aud = [$aud];
        if (!is_array($aud) || !in_array($resource, $aud, true)) return null;
        $raw = $claims['scope'] ?? [];
        if (is_string($raw)) $raw = preg_split('/\s+/', $raw) ?: [];
        if (!is_array($raw) || count($raw) > 1024) return null;
        $scopes = []; $tags = []; $prefix = $resource . '.';
        foreach ($raw as $scope) {
            if (!is_string($scope) || !str_starts_with($scope, $prefix)) continue;
            $scope = substr($scope, strlen($prefix));
            if (str_starts_with($scope, 'tag:')) { $tags[] = substr($scope, 4); continue; }
            if (!preg_match('#^(configure|write|read):([^/]+)/([^/]+)(?:/(.+))?$#D', $scope, $m)) continue;
            $scopes[] = ['kind' => $m[1], 'vhost' => self::wildcard($m[2]), 'resource' => self::wildcard($m[3]), 'routingKey' => isset($m[4]) ? self::wildcard($m[4]) : null];
        }
        $subject = $claims['sub'] ?? $claims['client_id'] ?? '';
        if ($username === '' && !is_string($subject)) return null;
        return self::principal($broker, $username !== '' ? $username : $subject, 'oauth', $tags, $scopes, (float) $exp * 1000);
    }
    private static function b64(string $value): string
    {
        if ($value === '' || !preg_match('/^[A-Za-z0-9_-]+={0,2}$/D', $value) || strlen(rtrim($value, '=')) % 4 === 1) throw new RuntimeException('invalid base64url');
        $decoded = base64_decode(strtr($value, '-_', '+/'), true);
        if ($decoded === false) throw new RuntimeException('invalid base64url');
        return $decoded;
    }
    private static function key(array $cfg, string $kid): mixed
    {
        $url = self::option($cfg, 'jwksUrl', 'jwks_url', '');
        $ca = self::option($cfg, 'jwksCaPath', 'jwks_ca_path');
        if (!is_string($url) || !in_array(parse_url($url, PHP_URL_SCHEME), ['http', 'https'], true)) return null;
        if ($ca !== null && (!is_string($ca) || !is_file($ca) || !is_readable($ca))) return null;
        $cacheId = $url . "\0" . ($ca ?? ''); $cached = self::$jwks[$cacheId] ?? null;
        if ($cached === null || microtime(true) - $cached['at'] >= 10 || !isset($cached['keys'][$kid])) {
            // No redirects or stale-key fallback; failed refresh denies authentication.
            unset(self::$jwks[$cacheId]);
            $ssl = ['verify_peer' => true, 'verify_peer_name' => true, 'allow_self_signed' => false];
            if ($ca !== null) $ssl['cafile'] = $ca;
            $context = stream_context_create(['http' => ['timeout' => 5, 'follow_location' => 0, 'ignore_errors' => true, 'header' => "Accept: application/json\r\n"], 'ssl' => $ssl]);
            $fp = @fopen($url, 'rb', false, $context);
            if ($fp === false) return null;
            try {
                $meta = stream_get_meta_data($fp); $status = $meta['wrapper_data'][0] ?? '';
                if (!preg_match('#^HTTP/\S+ 200(?: |$)#', $status)) return null;
                $bytes = stream_get_contents($fp, 1048577);
                if ($bytes === false || strlen($bytes) > 1048576 || !feof($fp)) return null;
            } finally { fclose($fp); }
            $doc = json_decode($bytes, true, 32, JSON_THROW_ON_ERROR);
            if (!is_array($doc) || !is_array($doc['keys'] ?? null) || count($doc['keys']) > 100) return null;
            $keys = [];
            foreach ($doc['keys'] as $jwk) {
                if (!is_array($jwk) || ($jwk['kty'] ?? null) !== 'RSA' || !is_string($jwk['kid'] ?? null) || !is_string($jwk['n'] ?? null) || !is_string($jwk['e'] ?? null)) continue;
                if (isset($jwk['alg']) && $jwk['alg'] !== 'RS256' || isset($jwk['use']) && $jwk['use'] !== 'sig' || isset($jwk['key_ops']) && (!is_array($jwk['key_ops']) || !in_array('verify', $jwk['key_ops'], true))) continue;
                try { $n = self::b64($jwk['n']); $e = self::b64($jwk['e']); } catch (Throwable) { continue; }
                if (strlen($n) < 256 || strlen($n) > 1024 || strlen($e) > 8) continue;
                $rsa = self::ber(0x30, self::rsaInt($n) . self::rsaInt($e));
                $der = self::ber(0x30, hex2bin('300d06092a864886f70d0101010500') . self::ber(0x03, "\0" . $rsa));
                $pem = "-----BEGIN PUBLIC KEY-----\n" . chunk_split(base64_encode($der), 64, "\n") . "-----END PUBLIC KEY-----\n";
                $pub = openssl_pkey_get_public($pem);
                if ($pub !== false) {
                    $details = openssl_pkey_get_details($pub);
                    if (($details['bits'] ?? 0) < 2048 || ($details['bits'] ?? 0) > 8192) continue;
                    if (isset($keys[$jwk['kid']])) return null;
                    $keys[$jwk['kid']] = $pub;
                }
            }
            self::$jwks[$cacheId] = ['at' => microtime(true), 'keys' => $keys];
        }
        return self::$jwks[$cacheId]['keys'][$kid] ?? null;
    }
    private static function rsaInt(string $bytes): string { $bytes = ltrim($bytes, "\0"); return self::ber(0x02, $bytes === '' ? "\0" : ((ord($bytes[0]) & 128) ? "\0" . $bytes : $bytes)); }

    private static function ldap(Broker $broker, array $cfg, string $username, string $password): ?array
    {
        if ($username === '' || $password === '' || str_contains($username, "\0")) return null;
        $host = $cfg['server'] ?? '127.0.0.1'; $port = $cfg['port'] ?? 389;
        $pattern = self::option($cfg, 'userDnPattern', 'user_dn_pattern', '');
        if (!is_string($host) || $host === '' || strpbrk($host, "/\r\n") !== false || !is_numeric($port) || (int) $port < 1 || (int) $port > 65535 || !is_string($pattern) || !str_contains($pattern, '${username}')) return null;
        $escaped = preg_replace_callback('/[,+"\\\\<>;=\x00]|^[ #]| $/', static fn ($m) => '\\' . (in_array($m[0], ["\0"], true) ? '00' : $m[0]), $username);
        $dn = str_replace('${username}', $escaped, $pattern);
        $address = str_contains($host, ':') ? '[' . $host . ']' : $host;
        $fp = @stream_socket_client('tcp://' . $address . ':' . (int) $port, $errno, $error, 5);
        if ($fp === false) return null;
        $deadline = microtime(true) + 5; $id = 0;
        try {
            if (!self::bind($fp, ++$id, $dn, $password, $deadline)) return null;
            $tags = ['management']; $group = self::option($cfg, 'adminGroup', 'admin_group');
            if (is_string($group) && $group !== '') {
                try {
                    $bindDn = self::option($cfg, 'bindDn', 'bind_dn');
                    $bound = $bindDn === null || self::bind($fp, ++$id, (string) $bindDn, (string) self::option($cfg, 'bindPassword', 'bind_password', ''), $deadline);
                    if ($bound) {
                        $search = self::ber(0x04, $group) . self::ber(0x0a, "\0") . self::ber(0x0a, "\0") . self::ber(0x02, "\1") . self::ber(0x02, "\5") . self::ber(0x01, "\0")
                            . self::ber(0xa3, self::ber(0x04, 'member') . self::ber(0x04, $dn)) . self::ber(0x30, self::ber(0x04, '1.1'));
                        self::sendLdap($fp, ++$id, self::ber(0x63, $search), $deadline); $found = false;
                        for ($messages = 0; $messages < 64; $messages++) {
                            [$tag, $body] = self::recvLdap($fp, $id, $deadline);
                            if ($tag === 0x64) $found = true;
                            elseif ($tag === 0x65) { if ($found && self::resultCode($body) === 0) array_unshift($tags, 'administrator'); break; }
                            elseif ($tag !== 0x73) throw new RuntimeException('unexpected LDAP search reply');
                        }
                    }
                } catch (Throwable) { /* A failed group query never elevates the authenticated user. */ }
            }
            try { self::sendLdap($fp, ++$id, self::ber(0x42, ''), $deadline); } catch (Throwable) {}
            return self::principal($broker, $username, 'ldap', $tags, null, null);
        } finally { fclose($fp); }
    }
    private static function bind($fp, int $id, string $dn, string $password, float $deadline): bool
    {
        self::sendLdap($fp, $id, self::ber(0x60, self::ber(0x02, "\3") . self::ber(0x04, $dn) . self::ber(0x80, $password)), $deadline);
        [$tag, $body] = self::recvLdap($fp, $id, $deadline);
        return $tag === 0x61 && self::resultCode($body) === 0;
    }
    private static function resultCode(string $body): int
    {
        [$tag, $code] = self::tlv($body);
        if ($tag !== 0x0a || strlen($code) !== 1) throw new RuntimeException('invalid LDAP result');
        return ord($code);
    }
    private static function ber(int $tag, string $body): string
    {
        $length = strlen($body);
        if ($length < 128) return chr($tag) . chr($length) . $body;
        $bytes = ''; do { $bytes = chr($length & 255) . $bytes; $length >>= 8; } while ($length);
        return chr($tag) . chr(128 | strlen($bytes)) . $bytes . $body;
    }
    private static function tlv(string $bytes): array
    {
        if (strlen($bytes) < 2) throw new RuntimeException('truncated BER');
        $tag = ord($bytes[0]); $first = ord($bytes[1]); $at = 2; $length = $first;
        if ($first & 128) {
            $n = $first & 127; if (!$n || $n > 4 || strlen($bytes) < $at + $n) throw new RuntimeException('invalid BER length');
            $length = 0; for ($i = 0; $i < $n; $i++) $length = ($length << 8) | ord($bytes[$at++]);
        }
        if ($length > 1048576 || strlen($bytes) < $at + $length) throw new RuntimeException('truncated BER');
        return [$tag, substr($bytes, $at, $length), substr($bytes, $at + $length)];
    }
    private static function timeout($fp, float $deadline): void
    {
        $left = $deadline - microtime(true); if ($left <= 0) throw new RuntimeException('LDAP timeout');
        $micros = max(1, (int) ($left * 1000000)); stream_set_timeout($fp, intdiv($micros, 1000000), $micros % 1000000);
    }
    private static function sendLdap($fp, int $id, string $op, float $deadline): void
    {
        $int = ''; $n = $id; do { $int = chr($n & 255) . $int; $n >>= 8; } while ($n); if (ord($int[0]) & 128) $int = "\0" . $int;
        $bytes = self::ber(0x30, self::ber(0x02, $int) . $op); $at = 0;
        while ($at < strlen($bytes)) { self::timeout($fp, $deadline); $n = fwrite($fp, substr($bytes, $at)); if (!$n) throw new RuntimeException('LDAP write failed'); $at += $n; }
    }
    private static function read($fp, int $length, float $deadline): string
    {
        $bytes = ''; while (strlen($bytes) < $length) { self::timeout($fp, $deadline); $chunk = fread($fp, $length - strlen($bytes)); if ($chunk === false || $chunk === '') throw new RuntimeException('LDAP read failed'); $bytes .= $chunk; } return $bytes;
    }
    private static function recvLdap($fp, int $id, float $deadline): array
    {
        $head = self::read($fp, 2, $deadline); if (ord($head[0]) !== 0x30) throw new RuntimeException('invalid LDAP envelope');
        $length = ord($head[1]); if ($length & 128) { $n = $length & 127; if (!$n || $n > 4) throw new RuntimeException('invalid LDAP length'); $raw = self::read($fp, $n, $deadline); $length = 0; for ($i = 0; $i < $n; $i++) $length = ($length << 8) | ord($raw[$i]); }
        if ($length > 1048576) throw new RuntimeException('LDAP reply too large');
        [$tag, $wireId, $rest] = self::tlv(self::read($fp, $length, $deadline)); $received = 0;
        if ($tag !== 0x02 || strlen($wireId) > 4) throw new RuntimeException('invalid LDAP message id');
        for ($i = 0; $i < strlen($wireId); $i++) $received = ($received << 8) | ord($wireId[$i]);
        if ($received !== $id) throw new RuntimeException('unexpected LDAP message id');
        [$op, $body] = self::tlv($rest); return [$op, $body];
    }
}
