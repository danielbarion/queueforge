<?php
declare(strict_types=1);

/** Typed values retain AMQP constructors where strings/maps alone are ambiguous. */
final class Amqp10Value
{
    public function __construct(public string $type, public mixed $value) {}
}
final class Amqp10Described
{
    public function __construct(public int|string $descriptor, public mixed $value) {}
}
final class Amqp10Refuse extends RuntimeException
{
    public function __construct(public string $condition, string $message) { parent::__construct($message); }
}

/** Bounded AMQP 1.0 type codec. Collection sizes and nesting are checked before allocation. */
final class Amqp10Codec
{
    public const MAX_VALUE = 32 * 1024 * 1024;
    public function __construct(public string $bytes, public int $at = 0) {}
    private function take(int $n): string
    {
        if ($n < 0 || $n > self::MAX_VALUE || $this->at + $n > strlen($this->bytes)) throw new RuntimeException('truncated AMQP value');
        $out = substr($this->bytes, $this->at, $n); $this->at += $n; return $out;
    }
    private function byte(): int { return ord($this->take(1)); }
    private function u32(): int { return unpack('N', $this->take(4))[1]; }
    public function value(int $depth = 0, ?int $constructor = null): mixed
    {
        if ($depth > 64) throw new RuntimeException('AMQP nesting limit');
        $c = $constructor ?? $this->byte();
        if ($c === 0) return new Amqp10Described(self::scalar($this->value($depth + 1)), $this->value($depth + 1));
        if ($c === 0x40) return null;
        if ($c === 0x41) return true;
        if ($c === 0x42) return false;
        if ($c === 0x43 || $c === 0x44) return 0;
        if ($c === 0x45) return [];
        if (in_array($c, [0x50, 0x52, 0x53], true)) return $this->byte();
        if (in_array($c, [0x51, 0x54, 0x55], true)) { $n = $this->byte(); return $n > 127 ? $n - 256 : $n; }
        if ($c === 0x56) { $n = $this->byte(); if ($n > 1) throw new RuntimeException('invalid boolean'); return $n === 1; }
        if ($c === 0x60 || $c === 0x61) { $n = unpack('n', $this->take(2))[1]; return $c === 0x61 && $n > 32767 ? $n - 65536 : $n; }
        if ($c === 0x70 || $c === 0x71) { $n = $this->u32(); return $c === 0x71 && $n > 0x7fffffff ? $n - 0x100000000 : $n; }
        if ($c === 0x72) return unpack('G', $this->take(4))[1];
        if ($c === 0x82) return unpack('E', $this->take(8))[1];
        if ($c === 0x80 || $c === 0x81 || $c === 0x83) {
            $raw = $this->take(8); $n = Codec::readU64($raw, 0);
            if ($c === 0x80 && $n < 0) return new Amqp10Value('ulong-raw', $raw);
            return $c === 0x83 ? new Amqp10Value('timestamp', $n) : $n;
        }
        if (in_array($c, [0x73, 0x74, 0x84, 0x94, 0x98], true)) {
            $width = [0x73 => 4, 0x74 => 4, 0x84 => 8, 0x94 => 16, 0x98 => 16][$c];
            return new Amqp10Value('raw-' . $c, $this->take($width));
        }
        if (in_array($c, [0xa0, 0xa1, 0xa3, 0xb0, 0xb1, 0xb3], true)) {
            $n = $c < 0xb0 ? $this->byte() : $this->u32(); $v = $this->take($n);
            if (in_array($c, [0xa0, 0xb0], true)) return new Amqp10Value('binary', $v);
            if (!preg_match('//u', $v)) throw new RuntimeException('invalid UTF-8 string');
            return in_array($c, [0xa3, 0xb3], true) ? new Amqp10Value('symbol', $v) : $v;
        }
        if (in_array($c, [0xc0, 0xc1, 0xd0, 0xd1, 0xe0, 0xf0], true)) {
            $wide = $c >= 0xd0 && $c !== 0xe0;
            $size = $wide ? $this->u32() : $this->byte(); $end = $this->at + $size;
            if ($size < ($wide ? 4 : 1) || $end > strlen($this->bytes) || $size > self::MAX_VALUE) throw new RuntimeException('invalid collection size');
            $count = $wide ? $this->u32() : $this->byte();
            if ($count > 1000000) throw new RuntimeException('collection count limit');
            $map = $c === 0xc1 || $c === 0xd1; $array = $c === 0xe0 || $c === 0xf0;
            if ($map && $count % 2) throw new RuntimeException('odd map count');
            $out = []; $ctor = $array ? $this->byte() : null; $descriptor = null;
            if ($array && $ctor === 0) { $descriptor = self::scalar($this->value($depth + 1)); $ctor = $this->byte(); }
            for ($i = 0; $i < $count; $i++) {
                if ($this->at >= $end && !in_array($ctor, [0x40, 0x41, 0x42, 0x43, 0x44, 0x45], true)) throw new RuntimeException('collection count exceeds bytes');
                $item = $this->value($depth + 1, $ctor);
                $out[] = $descriptor === null ? $item : new Amqp10Described($descriptor, $item);
                if ($this->at > $end) throw new RuntimeException('collection exceeds declared size');
            }
            if ($this->at !== $end) throw new RuntimeException('collection size mismatch');
            if ($map) { $pairs = []; for ($i = 0; $i < $count; $i += 2) $pairs[] = [$out[$i], $out[$i + 1]]; return new Amqp10Value('map', $pairs); }
            return $array ? new Amqp10Value('array', $out) : $out;
        }
        throw new RuntimeException(sprintf('unsupported AMQP constructor 0x%02x', $c));
    }
    public static function scalar(mixed $v): mixed { return $v instanceof Amqp10Value ? $v->value : $v; }
    public static function symbol(string $s): Amqp10Value { return new Amqp10Value('symbol', $s); }
    public static function binary(string $s): Amqp10Value { return new Amqp10Value('binary', $s); }
    public static function uint(int $n): Amqp10Value { return new Amqp10Value('uint', $n); }
    public static function encode(mixed $v, int $depth = 0): string
    {
        if ($depth > 64) throw new RuntimeException('AMQP nesting limit');
        if ($v instanceof Amqp10Described) return "\x00" . (is_int($v->descriptor) ? ($v->descriptor <= 255 ? "\x53" . chr($v->descriptor) : "\x80" . Codec::u64($v->descriptor)) : self::encode(self::symbol($v->descriptor))) . self::encode($v->value, $depth + 1);
        if ($v instanceof Amqp10Value) {
            if ($v->type === 'uint') { if ($v->value < 0 || $v->value > 0xffffffff) throw new RuntimeException('uint range'); return $v->value === 0 ? "\x43" : ($v->value <= 255 ? "\x52" . chr($v->value) : "\x70" . pack('N', $v->value)); }
            if ($v->type === 'ushort') { if (!is_int($v->value) || $v->value < 0 || $v->value > 65535) throw new RuntimeException('ushort range'); return "\x60" . pack('n', $v->value); }
            if ($v->type === 'ubyte') { if (!is_int($v->value) || $v->value < 0 || $v->value > 255) throw new RuntimeException('ubyte range'); return "\x50" . chr($v->value); }
            if ($v->type === 'timestamp') return "\x83" . Codec::u64($v->value);
            if ($v->type === 'ulong-raw') return "\x80" . $v->value;
            if (str_starts_with($v->type, 'raw-')) return chr((int) substr($v->type, 4)) . $v->value;
            if ($v->type === 'symbol' || $v->type === 'binary') { $n = strlen($v->value); if ($n > self::MAX_VALUE) throw new RuntimeException('value size limit'); $base = $v->type === 'symbol' ? 0xa3 : 0xa0; return ($n <= 255 ? chr($base) . chr($n) : chr($base + 16) . pack('N', $n)) . $v->value; }
            if ($v->type === 'map') { $items = ''; foreach ($v->value as [$key, $value]) $items .= self::encode($key, $depth + 1) . self::encode($value, $depth + 1); return "\xd1" . pack('NN', strlen($items) + 4, count($v->value) * 2) . $items; }
            if ($v->type === 'array') {
                $items = ''; foreach ($v->value as $item) { if (!$item instanceof Amqp10Value || $item->type !== 'symbol') throw new RuntimeException('only symbol array encoding supported'); $items .= pack('N', strlen($item->value)) . $item->value; }
                return "\xf0" . pack('NN', strlen($items) + 5, count($v->value)) . "\xb3" . $items;
            }
            throw new RuntimeException('unsupported typed value');
        }
        if ($v === null) return "\x40";
        if (is_bool($v)) return $v ? "\x41" : "\x42";
        if (is_int($v)) return "\x81" . Codec::u64($v);
        if (is_float($v)) return "\x82" . pack('E', $v);
        if (is_string($v)) { $n = strlen($v); if ($n > self::MAX_VALUE) throw new RuntimeException('value size limit'); return ($n <= 255 ? "\xa1" . chr($n) : "\xb1" . pack('N', $n)) . $v; }
        if (is_array($v)) { if ($v === []) return "\x45"; $items = ''; foreach ($v as $item) $items .= self::encode($item, $depth + 1); return "\xd0" . pack('NN', strlen($items) + 4, count($v)) . $items; }
        throw new RuntimeException('unsupported AMQP value');
    }
}

/** Socket-independent authenticated AMQP 1.0 session, sharing broker message storage. */
final class Amqp10
{
    private const FRAME_MAX = 131072;
    private const WINDOW = 0x7fffffff;
    private const CREDIT = 256;
    private const SECTIONS = 'message/vnd.rabbitmq.amqp';
    private static int $seq = 0;
    /** Server hooks for the management node: fn(string $op, Broker $broker, string $queue, array $args). */
    public ?Closure $topology = null;
    public function __construct(private Broker $broker) {}
    /**
     * Cores mode: fn(array $state, ?string $queue): ?string names the sibling
     * process a connection should move to before an attach on that queue.
     */
    public ?Closure $moveFor = null;
    private static int $gates = 0;
    /** @var array<int, true> management replies whose topology change is still being replicated */
    private array $unshared = [];
    /** The queue an attach frame names as its source (receiver) or target (sender), if any. */
    private function attachQueue(array $f): ?string
    {
        $receiver = $f[2] ?? null; $terminus = $receiver === true ? ($f[5] ?? null) : ($f[6] ?? null);
        if (!$terminus instanceof Amqp10Described) return null;
        $address = self::field($terminus->value, 0);
        $parsed = is_string($address) ? self::parseAddress($address) : null;
        return isset($parsed['queue']) ? (string) $parsed['queue'] : null;
    }
    /** Nothing in flight that the state cannot carry: no deliveries, confirms, held frames, consumers or partial transfers. */
    private function movable(array $state): bool
    {
        if (($state['noMove'] ?? false) || ($state['pending'] ?? []) !== [] || ($state['exclusive'] ?? []) !== []) return false;
        foreach ($state['sessions'] ?? [] as $session) {
            if ($session['unsettled'] !== [] || $session['held'] !== [] || ($session['replies'] ?? []) !== [] || ($session['ended'] ?? false)) return false;
            foreach ($session['links'] as $link) if ($link['dir'] === 'out' || ($link['partial'] ?? null) !== null) return false;
        }
        return true;
    }
    /** The connection state as bytes another process can adopt. */
    public function export(array $state): string
    {
        unset($state['move']);
        foreach ($state['sessions'] as $channel => $session) $state['sessions'][$channel]['broker'] = null;
        $state['principal'] = $this->broker->exportPrincipal((string) ($state['user'] ?? ''));
        return serialize($state);
    }
    /** Takes over a connection a sibling process moved here; null when the bytes are not a state. */
    public function adopt(string $bytes, int $conn): ?array
    {
        $state = @unserialize($bytes, ['allowed_classes' => [Amqp10Value::class, Amqp10Described::class]]);
        if (!is_array($state) || !isset($state['phase'], $state['vhost'], $state['sessions'])) return null;
        $state['conn'] = $conn; $state['settled'] = true;
        if (is_array($state['principal'] ?? null)) $this->broker->adoptPrincipal((string) ($state['user'] ?? ''), $state['principal']);
        unset($state['principal']);
        foreach ($state['sessions'] as $channel => $session) $state['sessions'][$channel]['broker'] = $this->broker->forVhost((string) $state['vhost']);
        if (($state['user'] ?? '') !== '') $this->broker->currentUsers[$state['conn']] = $state['user'];
        return $state;
    }
    private static function u(int $n): Amqp10Value { return Amqp10Codec::uint($n); }
    private static function sym(string $s): Amqp10Value { return Amqp10Codec::symbol($s); }
    private static function bin(string $s): Amqp10Value { return Amqp10Codec::binary($s); }
    private static function desc(int $code, mixed $value = []): Amqp10Described { return new Amqp10Described($code, $value); }
    private static function field(mixed $value, int $at, mixed $fallback = null): mixed { return is_array($value) ? ($value[$at] ?? $fallback) : $fallback; }
    private static function text(mixed $value): ?string { $v = Amqp10Codec::scalar($value); return is_string($v) ? $v : null; }
    private static function error(string $condition, string $message): Amqp10Described { return self::desc(0x1d, [self::sym($condition), $message]); }
    private static function frame(int $type, int $channel, string $body): string { return pack('NCCn', strlen($body) + 8, 2, $type, $channel) . $body; }
    private static function perf(int $code, array $fields = []): string { while ($fields !== [] && end($fields) === null) array_pop($fields); return Amqp10Codec::encode(self::desc($code, $fields)); }
    private static function send(int $channel, int $code, array $fields = [], int $type = 0): string { return self::frame($type, $channel, self::perf($code, $fields)); }

    public function drive(string &$buf, array &$state): string
    {
        if (!isset($state['phase'])) $state += ['phase' => 'header', 'conn' => -(++self::$seq), 'user' => '', 'vhost' => '/', 'sessions' => [], 'pending' => [], 'tag' => 1, 'remoteMax' => self::FRAME_MAX];
        $out = '';
        try {
            while (!($state['closing'] ?? false)) {
                if (in_array($state['phase'], ['header', 'header2'], true)) {
                    if (strlen($buf) < 8) break;
                    $h = substr($buf, 0, 8); $buf = substr($buf, 8);
                    if ($state['phase'] === 'header' && $h === "AMQP\x03\x01\x00\x00") {
                        $mechanisms = [self::sym('PLAIN')]; if (($state['peerCN'] ?? null) !== null) array_unshift($mechanisms, self::sym('EXTERNAL'));
                        $out .= $h . self::send(0, 0x40, [new Amqp10Value('array', $mechanisms)], 1); $state['phase'] = 'sasl'; continue;
                    }
                    if ($state['phase'] === 'header2' && $h === "AMQP\x00\x01\x00\x00") { $out .= $h; $state['phase'] = 'open'; continue; }
                    $out .= "AMQP\x03\x01\x00\x00"; $this->closed($state); break;
                }
                if (strlen($buf) < 8) break;
                $size = unpack('N', substr($buf, 0, 4))[1]; $doff = ord($buf[4]) * 4; $type = ord($buf[5]); $channel = unpack('n', substr($buf, 6, 2))[1];
                if ($size < 8 || $size > self::FRAME_MAX || $doff < 8 || $doff > $size) throw new Amqp10Refuse('amqp:connection:framing-error', 'invalid frame size or data offset');
                if (strlen($buf) < $size) break;
                $raw = substr($buf, 0, $size);
                $body = substr($buf, $doff, $size - $doff); $buf = substr($buf, $size); if ($body === '') continue;
                $decoder = new Amqp10Codec($body); $perf = $decoder->value();
                if (!$perf instanceof Amqp10Described || !is_int($perf->descriptor) || !is_array($perf->value)) throw new Amqp10Refuse('amqp:decode-error', 'expected performative');
                // An attach naming a queue that lives in a sibling process moves
                // the connection there, attach included, before it is answered.
                if ($perf->descriptor === 0x12 && $type === 0 && $this->moveFor !== null && !($state['settled'] ?? false) && ($state['phase'] ?? '') === 'open') {
                    $home = $this->movable($state) ? ($this->moveFor)($state, $this->attachQueue($perf->value)) : null;
                    if ($home !== null) { $buf = $raw . $buf; $state['move'] = $home; break; }
                    if ($this->attachQueue($perf->value) !== null) $state['settled'] = true;
                }
                $out .= $this->onFrame($type, $channel, $perf->descriptor, $perf->value, substr($body, $decoder->at), $state);
            }
            if (!($state['closing'] ?? false) && !isset($state['move'])) $out .= $this->tick($state);
        } catch (Throwable $error) {
            if (($state['phase'] ?? '') === 'open') $out .= self::send(0, 0x18, [self::error($error instanceof Amqp10Refuse ? $error->condition : 'amqp:decode-error', $error->getMessage())]);
            $this->closed($state);
        }
        return $out;
    }
    private function allowed(array $s, string $kind, string $resource): bool
    {
        return $this->broker->resourceAllowed($s['user'], $s['vhost'], $kind, $resource);
    }
    public static function parseAddress(string $address): ?array
    {
        $p = explode('/', $address);
        if ($p[0] !== '') return null;
        if (in_array($p[1] ?? '', ['queues', 'queue'], true) && count($p) === 3 && $p[2] !== '') return ['queue' => rawurldecode($p[2])];
        if (($p[1] ?? '') === 'amq' && ($p[2] ?? '') === 'queue' && count($p) === 4 && $p[3] !== '') return ['queue' => rawurldecode($p[3])];
        if (in_array($p[1] ?? '', ['exchanges', 'exchange'], true) && in_array(count($p), [3, 4], true)) return ['exchange' => rawurldecode($p[2]) === 'amq.default' ? '' : rawurldecode($p[2]), 'key' => isset($p[3]) ? rawurldecode($p[3]) : null];
        if (($p[1] ?? '') === 'topic' && count($p) >= 3) return ['exchange' => 'amq.topic', 'key' => rawurldecode(implode('/', array_slice($p, 2)))];
        return null;
    }
    private function target(array $s, array $target): array
    {
        $broker = $this->broker->forVhost($s['vhost']);
        if (isset($target['queue'])) {
            if (!isset($broker->queues[$target['queue']])) throw new Amqp10Refuse('amqp:not-found', 'queue does not exist');
            $target = ['exchange' => '', 'key' => $target['queue']];
        }
        if ($target['exchange'] !== '' && !isset($broker->exchanges[$target['exchange']])) throw new Amqp10Refuse('amqp:not-found', 'exchange does not exist');
        if (($broker->exchangeRows[$target['exchange']]['internal'] ?? false) || !$this->allowed($s, 'write', $target['exchange'] ?: 'amq.default')) throw new Amqp10Refuse('amqp:unauthorized-access', 'write access refused');
        return $target;
    }
    private function flow(int $channel, array $session, array $link): string
    {
        return self::send($channel, 0x13, [self::u($session['incoming']), self::u(self::WINDOW), self::u($session['outgoing']), self::u(self::WINDOW), self::u($link['handle']), self::u($link['count']), self::u($link['credit']), null, $link['drain'] ?? null]);
    }
    private function onFrame(int $type, int $channel, int $code, array $f, string $payload, array &$s): string
    {
        if ($s['phase'] === 'sasl') {
            if ($type !== 1 || $channel !== 0 || $code !== 0x41) throw new Amqp10Refuse('amqp:not-allowed', 'expected SASL init');
            $mechanism = $f[0] ?? null; $response = $f[1] ?? null; $ok = false;
            if ($mechanism instanceof Amqp10Value && $mechanism->type === 'symbol' && $mechanism->value === 'PLAIN' && $response instanceof Amqp10Value && $response->type === 'binary') {
                $parts = explode("\0", $response->value);
                $identity = null;
                if (count($parts) === 3 && ($parts[0] === '' || $parts[0] === $parts[1])) $identity = $this->broker->authenticate($parts[1], $parts[2]);
                $ok = $identity !== null;
                if ($ok) { $s['user'] = $identity; $s['login'] = $parts[1]; $this->broker->currentUsers[$s['conn']] = $identity; }
            }
            // SASL EXTERNAL: the verified TLS client certificate's CN, which must name an existing user.
            if ($mechanism instanceof Amqp10Value && $mechanism->type === 'symbol' && $mechanism->value === 'EXTERNAL') {
                $cn = $s['peerCN'] ?? null;
                $ok = is_string($cn) && array_key_exists($cn, $this->broker->users);
                if ($ok) { $s['user'] = $cn; $this->broker->currentUsers[$s['conn']] = $cn; }
            }
            $out = self::send(0, 0x44, [new Amqp10Value('ubyte', $ok ? 0 : 1)], 1);
            if (!$ok) $this->closed($s); else $s['phase'] = 'header2';
            return $out;
        }
        if ($type !== 0 || $s['phase'] !== 'open') throw new Amqp10Refuse('amqp:not-allowed', 'unexpected frame type');
        if (!($s['opened'] ?? false) && $code !== 0x10) throw new Amqp10Refuse('amqp:not-allowed', 'expected open');
        if ($code === 0x10) {
            if ($s['opened'] ?? false) throw new Amqp10Refuse('amqp:illegal-state', 'connection already open');
            $s['opened'] = true; $host = $f[1] ?? '';
            if (is_string($host) && str_starts_with($host, 'vhost:')) $s['vhost'] = substr($host, 6);
            $max = $f[2] ?? self::FRAME_MAX;
            if (!is_int($max) || $max < 512) throw new Amqp10Refuse('amqp:invalid-field', 'max-frame-size below minimum');
            $s['remoteMax'] = min($max, self::FRAME_MAX); $s['idle'] = is_int($f[4] ?? null) ? $f[4] : 0; $s['heartbeat'] = microtime(true);
            $out = self::send(0, 0x10, ['queueforge-php-' . abs($s['conn']), null, self::u(self::FRAME_MAX), new Amqp10Value('ushort', 65535), null, null, null, null, null, new Amqp10Value('map', [[self::sym('product'), 'QueueForge'], [self::sym('platform'), 'PHP']])]);
            if (!$this->broker->hasVhostAccess($s['user'], $s['vhost'])) { $out .= self::send(0, 0x18, [self::error('amqp:not-allowed', 'vhost access refused')]); $this->closed($s); }
            return $out;
        }
        if ($code === 0x18) { $this->closed($s); return self::send(0, 0x18); }
        if ($code === 0x11) {
            if (isset($s['sessions'][$channel])) throw new Amqp10Refuse('amqp:illegal-state', 'duplicate begin');
            if (!$this->broker->channelAllowed($s['user'], count($s['sessions']))) throw new Amqp10Refuse('amqp:resource-limit-exceeded', 'session limit');
            $s['sessions'][$channel] = ['broker' => $this->broker->forVhost($s['vhost']), 'links' => [], 'incoming' => $f[1] ?? 0, 'outgoing' => 0, 'limit' => $f[2] ?? self::WINDOW, 'delivery' => 0, 'unsettled' => [], 'held' => [], 'replies' => []];
            return self::send($channel, 0x11, [new Amqp10Value('ushort', $channel), self::u(0), self::u(self::WINDOW), self::u(self::WINDOW), self::u(255)]);
        }
        if (!isset($s['sessions'][$channel])) throw new Amqp10Refuse('amqp:not-found', 'no session on channel');
        $session =& $s['sessions'][$channel];
        // A session the management node ended waits only for the peer's end.
        if ($session['ended'] ?? false) { if ($code === 0x17) unset($s['sessions'][$channel]); return ''; }
        if ($code === 0x17) { $this->dropSession($session); unset($s['sessions'][$channel]); return self::send($channel, 0x17); }
        if ($code === 0x12) {
            $name = $f[0] ?? null; $handle = $f[1] ?? null; $receiver = $f[2] ?? null;
            if (!is_string($name) || !is_int($handle) || $handle < 0 || $handle > 255 || !is_bool($receiver) || isset($session['links'][$handle])) throw new Amqp10Refuse('amqp:invalid-field', 'invalid attach name/handle/role');
            $source = $f[5] ?? null; $target = $f[6] ?? null; $snd = $f[3] ?? 2; $rcv = $f[4] ?? 0;
            if (!in_array($snd, [0, 1, 2], true) || !in_array($rcv, [0, 1], true)) throw new Amqp10Refuse('amqp:invalid-field', 'invalid settlement mode');
            if (($source !== null && (!$source instanceof Amqp10Described || $source->descriptor !== 0x28)) || ($target !== null && (!$target instanceof Amqp10Described || $target->descriptor !== 0x29))) throw new Amqp10Refuse('amqp:invalid-field', 'invalid source or target terminus');
            $reply = fn ($src, $tgt) => self::send($channel, 0x12, [$name, self::u($handle), !$receiver, new Amqp10Value('ubyte', $snd), new Amqp10Value('ubyte', $rcv), $src, $tgt, null, null, $receiver ? self::u(0) : null]);
            // RabbitMQ 4's management node: a paired sender and receiver on /management.
            $terminus = $receiver ? $source : $target;
            if ($terminus instanceof Amqp10Described && self::text(self::field($terminus->value, 0)) === '/management') {
                $session['links'][$handle] = $receiver
                    ? ['handle' => $handle, 'name' => $name, 'dir' => 'out', 'mgmt' => true, 'presettled' => true, 'credit' => 0, 'count' => 0, 'drain' => false]
                    : ['handle' => $handle, 'name' => $name, 'dir' => 'in', 'mgmt' => true, 'target' => null, 'credit' => self::CREDIT, 'count' => $f[9] ?? 0, 'partial' => null];
                $out = self::send($channel, 0x12, [$name, self::u($handle), !$receiver, new Amqp10Value('ubyte', $snd), new Amqp10Value('ubyte', $rcv), $source, $target, null, null, $receiver ? self::u(0) : null, null, null, null, $f[13] ?? null]);
                return $receiver ? $out : $out . $this->flow($channel, $session, $session['links'][$handle]);
            }
            try {
                if ($receiver) {
                    $address = $source instanceof Amqp10Described ? self::field($source->value, 0) : null;
                    $parsed = is_string($address) ? self::parseAddress($address) : null;
                    if (!$parsed || !isset($parsed['queue'])) throw new Amqp10Refuse('amqp:invalid-field', 'source is not a queue');
                    if (!isset($session['broker']->queues[$parsed['queue']])) throw new Amqp10Refuse('amqp:not-found', 'queue does not exist');
                    if (!$this->allowed($s, 'read', $parsed['queue'])) throw new Amqp10Refuse('amqp:unauthorized-access', 'read access refused');
                    $q = $session['broker']->queues[$parsed['queue']];
                    if (isset($q['owner']) && $q['owner'] !== $s['conn']) throw new Amqp10Refuse('amqp:resource-locked', 'exclusive queue belongs to another connection');
                    $tag = 'amq.ctag-10-' . $s['conn'] . '-' . $channel . '-' . $handle;
                    try { $session['broker']->addConsumer($parsed['queue'], $s['conn'], $channel, $tag, $snd === 1); }
                    catch (RuntimeException $e) { throw new Amqp10Refuse('amqp:resource-locked', $e->getMessage()); }
                    $session['links'][$handle] = ['handle' => $handle, 'name' => $name, 'dir' => 'out', 'queue' => $parsed['queue'], 'tag' => $tag, 'presettled' => $snd === 1, 'credit' => 0, 'count' => 0, 'drain' => false];
                    if (($q['args']['queueType'] ?? null) === 'stream') {
                        $session['links'][$handle]['offset'] = $session['broker']->streamFirst($parsed['queue']);
                        $filters = self::field($source->value, 7);
                        if ($filters instanceof Amqp10Value && $filters->type === 'map') foreach ($filters->value as [$key, $value]) {
                            if (!str_contains(self::text($key) ?? '', 'stream-offset')) continue;
                            $offset = Amqp10Codec::scalar($value instanceof Amqp10Described ? $value->value : $value);
                            if (is_int($offset) && $offset >= 0) $session['links'][$handle]['offset'] = $offset;
                            elseif ($offset === 'first') $session['links'][$handle]['offset'] = $session['broker']->streamFirst($parsed['queue']);
                            elseif ($offset === 'last') $session['links'][$handle]['offset'] = max($session['broker']->streamFirst($parsed['queue']), $session['broker']->streamNext($parsed['queue']) - 1);
                            elseif ($offset === 'next') $session['links'][$handle]['offset'] = $session['broker']->streamNext($parsed['queue']);
                            else { $this->dropLink($session, $handle); throw new Amqp10Refuse('amqp:invalid-field', 'unsupported stream offset'); }
                        }
                    }
                    return $reply($source, $target);
                }
                $address = $target instanceof Amqp10Described ? self::field($target->value, 0) : null;
                $resolved = null;
                if ($address !== null) { $parsed = is_string($address) ? self::parseAddress($address) : null; if (!$parsed) throw new Amqp10Refuse('amqp:invalid-field', 'invalid target address'); $resolved = $this->target($s, $parsed); }
                $session['links'][$handle] = ['handle' => $handle, 'name' => $name, 'dir' => 'in', 'target' => $resolved, 'credit' => self::CREDIT, 'count' => $f[9] ?? 0, 'partial' => null];
                return $reply($source, $target) . $this->flow($channel, $session, $session['links'][$handle]);
            } catch (Amqp10Refuse $e) { return $reply($receiver ? null : $source, $receiver ? $target : null) . self::send($channel, 0x16, [self::u($handle), true, self::error($e->condition, $e->getMessage())]); }
        }
        if ($code === 0x13) {
            $session['limit'] = ($f[0] ?? 0) + ($f[1] ?? self::WINDOW);
            $handle = $f[4] ?? null; if ($handle === null) return '';
            if (!is_int($handle) || !isset($session['links'][$handle])) throw new Amqp10Refuse('amqp:session:unattached-handle', 'flow unknown handle');
            $link =& $session['links'][$handle];
            if ($link['dir'] === 'out') { $link['credit'] = max(0, ($f[5] ?? 0) + ($f[6] ?? 0) - $link['count']); $link['drain'] = ($f[8] ?? false) === true; $link['echo'] = ($f[9] ?? false) === true; }
            return ($f[9] ?? false) === true && $link['dir'] === 'in' ? $this->flow($channel, $session, $link) : '';
        }
        if ($code === 0x14) {
            $handle = $f[0] ?? null;
            if (!is_int($handle) || !isset($session['links'][$handle]) || $session['links'][$handle]['dir'] !== 'in') throw new Amqp10Refuse('amqp:session:unattached-handle', 'transfer unknown handle');
            $link =& $session['links'][$handle]; $session['incoming']++;
            if ($link['partial'] === null) {
                if ($link['credit'] <= 0 || !is_int($f[1] ?? null)) throw new Amqp10Refuse('amqp:transfer-limit-exceeded', 'missing delivery id or exhausted credit');
                if (($f[3] ?? 0) !== 0) throw new Amqp10Refuse('amqp:not-implemented', 'unsupported message format');
                $link['partial'] = ['id' => $f[1], 'settled' => ($f[4] ?? false) === true, 'body' => ''];
            }
            elseif (isset($f[1]) && $f[1] !== $link['partial']['id']) throw new Amqp10Refuse('amqp:invalid-field', 'continuation delivery id changed');
            if (($f[9] ?? false) === true) { $link['partial'] = null; return ''; }
            $link['partial']['body'] .= $payload;
            if (strlen($link['partial']['body']) > Amqp10Codec::MAX_VALUE) throw new Amqp10Refuse('amqp:link:message-size-exceeded', 'message size limit');
            if (($f[5] ?? false) === true) return '';
            $partial = $link['partial']; $link['partial'] = null; $link['credit']--; $link['count']++;
            if ($link['mgmt'] ?? false) { $out = $this->manage($channel, $session, $partial, $s); if ($session['ended'] ?? false) return $out; }
            else $out = $this->publish($channel, $link, $partial, $s);
            if ($link['credit'] < self::CREDIT / 2) { $link['credit'] = self::CREDIT; $out .= $this->flow($channel, $session, $link); }
            return $out;
        }
        if ($code === 0x15) {
            if (($f[0] ?? false) !== true) return '';
            $first = $f[1] ?? null; $last = $f[2] ?? $first;
            if (!is_int($first) || !is_int($last) || $first < 0 || $last < $first) throw new Amqp10Refuse('amqp:invalid-field', 'invalid disposition range');
            $outcome = $f[4] ?? null; $kind = $outcome instanceof Amqp10Described ? $outcome->descriptor : 0x26;
            foreach ($session['unsettled'] as $id => $held) {
                if ($id < $first || $id > $last) continue;
                unset($session['unsettled'][$id]);
                if ($held['stream'] ?? false) continue;
                if ($kind === 0x24) $session['broker']->ack($held['msg']);
                elseif ($kind === 0x25 || ($kind === 0x27 && self::field($outcome->value, 1) === true)) $session['broker']->deadLetter($held['msg'], 'rejected');
                else $session['broker']->requeue($held['msg']);
            }
            return ($f[3] ?? false) === true ? '' : self::send($channel, 0x15, [false, self::u($first), self::u($last), true, $outcome]);
        }
        if ($code === 0x16) {
            $handle = $f[0] ?? null; if (!is_int($handle)) throw new Amqp10Refuse('amqp:invalid-field', 'invalid detach handle');
            $this->dropLink($session, $handle); return self::send($channel, 0x16, [self::u($handle), true]);
        }
        throw new Amqp10Refuse('amqp:not-implemented', 'unknown performative');
    }
    private function publish(int $channel, array $link, array $partial, array &$state): string
    {
        try {
            $msg = self::inbound($partial['body']); $target = $link['target'];
            if ($target === null) { $parsed = isset($msg['to']) ? self::parseAddress($msg['to']) : null; if (!$parsed) throw new Amqp10Refuse('amqp:invalid-field', 'missing or invalid to address'); $target = $this->target($state, $parsed); }
            $key = $target['key'] ?? $msg['subject'] ?? '';
            if (!$this->allowed($state, 'write', $target['exchange'] ?: 'amq.default') || !$this->broker->topicWriteAllowed($state['user'], $state['vhost'], $target['exchange'], $key)) throw new Amqp10Refuse('amqp:unauthorized-access', 'write access refused');
            if (isset($msg['props']['userId']) && $msg['props']['userId'] !== $state['user']) throw new Amqp10Refuse('amqp:unauthorized-access', 'user-id differs from authenticated user');
            $tag = $state['tag']++;
            $result = $this->broker->forVhost($state['vhost'])->publish($state['conn'], $channel, $tag, $target['exchange'], $key, $msg['body'], $msg['mode'], $msg['priority'], $msg['headers'], isset($msg['props']['expiration']) ? (int) $msg['props']['expiration'] : null, self::writeProps($msg['props'], $msg['typedHeaders']));
            if ($result === 'wait') { $state['pending'][$tag] = ['channel' => $channel, 'id' => $partial['id'], 'settled' => $partial['settled']]; return ''; }
            $outcome = self::desc($result === 'return' ? 0x26 : 0x25);
        } catch (Throwable $e) { $outcome = self::desc(0x25, [self::error($e instanceof Amqp10Refuse ? $e->condition : ($e->getCode() === 403 ? 'amqp:unauthorized-access' : ($e->getCode() === 404 ? 'amqp:not-found' : 'amqp:decode-error')), $e->getMessage())]); }
        return $partial['settled'] ? '' : self::send($channel, 0x15, [true, self::u($partial['id']), null, true, $outcome]);
    }
    /**
     * One management request ("HTTP over AMQP", RabbitMQ 4 v2 addresses). The
     * reply is queued for the paired receiver link and leaves once it has
     * credit. A refused operation ends the session, as RabbitMQ does.
     */
    private function manage(int $channel, array &$session, array $partial, array &$s): string
    {
        $d = new Amqp10Codec($partial['body']); $id = null; $to = ''; $method = ''; $body = null;
        while ($d->at < strlen($partial['body'])) {
            $section = $d->value(); if (!$section instanceof Amqp10Described) throw new Amqp10Refuse('amqp:decode-error', 'message section must be described');
            $code = Amqp10Codec::scalar($section->descriptor);
            if ($code === 0x73) { $id = self::field($section->value, 0); $to = self::text(self::field($section->value, 2)) ?? ''; $method = strtoupper(self::text(self::field($section->value, 3)) ?? ''); }
            elseif ($code === 0x77 || $code === 0x75) $body = $section->value;
        }
        try { [$status, $result] = $this->request($session['broker'], $s, $method, $to, $body); }
        catch (Amqp10Refuse $e) {
            if ($e->condition !== 'amqp:unauthorized-access') throw $e;
            $this->dropSession($session); $session['links'] = []; $session['replies'] = []; $session['held'] = []; $session['ended'] = true;
            return self::send($channel, 0x17, [self::error($e->condition, $e->getMessage())]);
        }
        $gate = ++self::$gates; $this->unshared[$gate] = true;
        $session['broker']->whenShared(function () use ($gate): void { unset($this->unshared[$gate]); });
        $session['replies'][] = [Amqp10Codec::encode(self::desc(0x73, [null, null, null, $status, null, $id])) . Amqp10Codec::encode(self::desc(0x77, self::typed($result))), $gate];
        return $partial['settled'] ? '' : self::send($channel, 0x15, [true, self::u($partial['id']), null, true, self::desc(0x24)]);
    }
    /** @return array{0:string,1:mixed} status code and reply body */
    private function request(Broker $b, array &$s, string $method, string $to, mixed $body): array
    {
        [$path, $query] = array_pad(explode('?', $to, 2), 2, '');
        $raw = explode('/', $path); if (array_shift($raw) !== '' || $raw === []) return ['400', "invalid path '$to'"];
        $seg = array_map('rawurldecode', $raw); $n = count($seg);
        $plain = self::plain($body); $args = is_array($plain) ? $plain : [];
        $topology = $this->topology ?? static function (): void {};
        try {
            if ($seg[0] === 'auth' && ($seg[1] ?? '') === 'tokens' && $n === 2 && $method === 'PUT') {
                $token = is_string($plain) ? $plain : '';
                $identity = ($s['login'] ?? '') === '' || $token === '' ? null : $this->broker->authenticate($s['login'], $token);
                if ($identity === null || !$this->broker->hasVhostAccess($identity, $s['vhost'])) throw new Amqp10Refuse('amqp:not-allowed', 'update-secret failed');
                $s['user'] = $identity; $this->broker->currentUsers[$s['conn']] = $identity;
                return ['204', null];
            }
            if ($seg[0] === 'queues' && $n === 2 && $seg[1] !== '') {
                $q = $seg[1];
                if ($method === 'GET') {
                    $this->permit($s, 'configure', 'queue', $q);
                    return isset($b->queues[$q]) ? ['200', $this->queueInfo($b, $q)] : ['404', "queue '$q' in vhost '{$s['vhost']}' not found"];
                }
                if ($method === 'PUT') {
                    $this->permit($s, 'configure', 'queue', $q);
                    $isNew = !isset($b->queues[$q]);
                    $owner = $b->queues[$q]['owner'] ?? null;
                    if ($owner !== null && $owner !== $s['conn']) return ['409', "queue '$q' in vhost '{$s['vhost']}' is exclusive to another connection"];
                    if ($isNew && !$b->queueAllowed($s['vhost'])) return ['403', 'queue limit'];
                    $declared = is_array($args['arguments'] ?? null) ? $args['arguments'] : [];
                    $exclusive = (bool) ($args['exclusive'] ?? false);
                    $b->declareQueue($q, $declared, (bool) ($args['durable'] ?? true), $exclusive, false, (bool) ($args['auto_delete'] ?? false));
                    if ($isNew && $exclusive) { $b->queues[$q]['owner'] = $s['conn']; $s['exclusive'][$q] = $s['vhost']; }
                    $topology('declare', $b, $q, $declared);
                    return [$isNew ? '201' : '200', $this->queueInfo($b, $q)];
                }
                if ($method === 'DELETE') {
                    $this->permit($s, 'configure', 'queue', $q);
                    if (isset($b->queues[$q])) $topology('delete', $b, $q, []);
                    return ['200', ['message_count' => $b->deleteQueue($q)]];
                }
            }
            if ($seg[0] === 'queues' && $n === 3 && $seg[2] === 'messages' && $method === 'DELETE') {
                $q = $seg[1]; $this->permit($s, 'read', 'queue', $q);
                if (!isset($b->queues[$q])) return ['404', "queue '$q' in vhost '{$s['vhost']}' not found"];
                $topology('purge', $b, $q, []);
                return ['200', ['message_count' => $b->purge($q)]];
            }
            if ($seg[0] === 'exchanges' && $n === 2 && $seg[1] !== '') {
                $x = $seg[1]; $this->permit($s, 'configure', 'exchange', $x);
                if ($method === 'PUT') {
                    $type = is_string($args['type'] ?? null) ? $args['type'] : 'direct';
                    if (isset($b->exchanges[$x]) && $b->exchanges[$x] !== $type) return ['409', "inequivalent arg 'type' for exchange '$x' in vhost '{$s['vhost']}'"];
                    $arguments = is_array($args['arguments'] ?? null) ? $args['arguments'] : [];
                    $alternate = $arguments['alternate-exchange'] ?? null;
                    $b->declareExchange($x, $type, (bool) ($args['durable'] ?? true), (bool) ($args['auto_delete'] ?? false), (bool) ($args['internal'] ?? false), is_string($alternate) ? $alternate : null, false, $arguments);
                    return ['204', null];
                }
                if ($method === 'DELETE') { $b->deleteExchange($x); return ['204', null]; }
            }
            if ($seg[0] === 'bindings' && $n === 1 && $method === 'POST') {
                $source = (string) ($args['source'] ?? ''); $key = (string) ($args['binding_key'] ?? '');
                $pairs = []; foreach (is_array($args['arguments'] ?? null) ? $args['arguments'] : [] as $k => $v) $pairs[] = [(string) $k, $v];
                $this->bindingChange($b, $s, $source, $args['destination_queue'] ?? null, $args['destination_exchange'] ?? null, $key, $pairs, true);
                return ['204', null];
            }
            if ($seg[0] === 'bindings' && $n === 2 && $method === 'DELETE') {
                $parts = [];
                foreach (explode(';', $raw[1]) as $part) { [$k, $v] = array_pad(explode('=', $part, 2), 2, ''); $parts[$k] = rawurldecode($v); }
                $this->bindingChange($b, $s, $parts['src'] ?? '', $parts['dstq'] ?? null, $parts['dste'] ?? null, $parts['key'] ?? '', [], false);
                return ['204', null];
            }
            if ($seg[0] === 'bindings' && $n === 1 && $method === 'GET') {
                parse_str($query, $filter); $rows = [];
                foreach ($b->bindings as $row) {
                    if (isset($filter['src']) && $row['exchange'] !== $filter['src']) continue;
                    if (isset($filter['dstq']) && $row['queue'] !== $filter['dstq']) continue;
                    if (isset($filter['dste']) || (isset($filter['key']) && $row['key'] !== $filter['key'])) continue;
                    $rows[] = ['source' => $row['exchange'], 'vhost' => $s['vhost'], 'destination_queue' => $row['queue'], 'binding_key' => $row['key'], 'arguments' => self::pairsToMap($row['args'] ?? [])];
                }
                foreach ($b->e2e as $row) {
                    if (isset($filter['src']) && $row['source'] !== $filter['src']) continue;
                    if (isset($filter['dstq']) || (isset($filter['dste']) && $row['destination'] !== $filter['dste']) || (isset($filter['key']) && $row['key'] !== $filter['key'])) continue;
                    $rows[] = ['source' => $row['source'], 'vhost' => $s['vhost'], 'destination_exchange' => $row['destination'], 'binding_key' => $row['key'], 'arguments' => []];
                }
                return ['200', $rows];
            }
        } catch (Amqp10Refuse $e) { throw $e; }
        catch (RuntimeException $e) {
            $code = (int) $e->getCode();
            if ($code === 403) throw new Amqp10Refuse('amqp:unauthorized-access', $e->getMessage());
            return [$code === 404 ? '404' : (in_array($code, [405, 406], true) ? '409' : '400'), $e->getMessage()];
        }
        return ['400', "unsupported request $method $path"];
    }
    private function bindingChange(Broker $b, array $s, string $source, mixed $queue, mixed $exchange, string $key, array $pairs, bool $add): void
    {
        $user = $this->broker->identityName($s['user']);
        if (is_string($queue)) {
            $this->permit($s, 'write', 'queue', $queue); $this->permit($s, 'read', 'exchange', $source ?: 'amq.default');
            if ($add) $b->bind($queue, $source, $key, $pairs, $s['user'], $s['vhost']); else $b->unbind($queue, $source, $key);
            return;
        }
        if (!is_string($exchange)) throw new RuntimeException('binding needs destination_queue or destination_exchange', 400);
        $this->permit($s, 'write', 'exchange', $exchange); $this->permit($s, 'read', 'exchange', $source);
        if (!$add) { $b->unbindExchange($exchange, $source, $key); return; }
        foreach ([$source, $exchange] as $x) if (!isset($b->exchanges[$x])) throw new RuntimeException("NOT_FOUND - no exchange '$x' in vhost '{$s['vhost']}' for user '$user'", 404);
        $b->bindExchange($exchange, $source, $key);
    }
    private function permit(array $s, string $kind, string $type, string $name): void
    {
        if ($this->broker->resourceAllowed($s['user'], $s['vhost'], $kind, $name)) return;
        throw new Amqp10Refuse('amqp:unauthorized-access', "$kind access to $type '$name' in vhost '{$s['vhost']}' refused for user '" . $this->broker->identityName($s['user']) . "'");
    }
    private function queueInfo(Broker $b, string $q): array
    {
        $row = $b->queues[$q]; $type = $row['args']['queueType'] ?? 'classic';
        return ['name' => $q, 'vhost' => $b->vhost, 'durable' => (bool) ($row['durable'] ?? true), 'auto_delete' => (bool) ($row['autoDelete'] ?? false), 'exclusive' => (bool) ($row['exclusive'] ?? false),
            'type' => in_array($type, ['quorum', 'stream'], true) ? $type : 'classic', 'arguments' => is_array($row['declaredArgs'] ?? null) ? $row['declaredArgs'] : [],
            'message_count' => $b->readyCount($q), 'consumer_count' => $b->consumerCount($q), 'leader' => $this->broker->nodeId, 'replicas' => [$this->broker->nodeId]];
    }
    private static function pairsToMap(array $pairs): array { $out = []; foreach ($pairs as $pair) if (is_array($pair) && count($pair) === 2) $out[(string) $pair[0]] = $pair[1]; return $out; }
    /** AMQP 1.0 decoded values as plain PHP: maps become string-keyed arrays. */
    private static function plain(mixed $v): mixed
    {
        if ($v instanceof Amqp10Described) return self::plain($v->value);
        if ($v instanceof Amqp10Value) {
            if ($v->type === 'map') { $out = []; foreach ($v->value as [$k, $x]) $out[(string) Amqp10Codec::scalar($k)] = self::plain($x); return $out; }
            if ($v->type === 'array') return array_map(self::plain(...), $v->value);
            return $v->value;
        }
        return is_array($v) ? array_map(self::plain(...), $v) : $v;
    }
    /** Plain PHP back into AMQP values: string-keyed (and empty) arrays become maps. */
    private static function typed(mixed $v): mixed
    {
        if (!is_array($v)) return $v;
        if ($v !== [] && array_is_list($v)) return array_map(self::typed(...), $v);
        $pairs = []; foreach ($v as $k => $x) $pairs[] = [(string) $k, self::typed($x)];
        return new Amqp10Value('map', $pairs);
    }
    /** Server::commit routes the broker's actual durability confirmation here. */
    public function confirmed(array &$state, int $tag, bool $nack): string
    {
        $pending = $state['pending'][$tag] ?? null; unset($state['pending'][$tag]);
        if (!$pending || $pending['settled'] || ($state['closing'] ?? false)) return '';
        return self::send($pending['channel'], 0x15, [true, self::u($pending['id']), null, true, self::desc($nack ? 0x25 : 0x24)]);
    }
    public function tick(array &$state): string
    {
        if (($state['phase'] ?? '') !== 'open' || ($state['closing'] ?? false)) return '';
        $out = '';
        foreach ($state['sessions'] as $channel => &$session) {
            while ($session['held'] !== [] && $session['outgoing'] < $session['limit']) { $out .= array_shift($session['held'])['bytes']; $session['outgoing']++; }
            foreach ($session['links'] as &$link) {
                if ($link['dir'] !== 'out') continue;
                $sent = 0;
                if ($link['mgmt'] ?? false) {
                    // Replies wait for credit: RabbitMQ never sends on the management link without it.
                    // A reply also waits until its topology change reached every member.
                    while ($link['credit'] > 0 && $session['replies'] !== [] && $session['outgoing'] < $session['limit'] && !isset($this->unshared[$session['replies'][0][1]])) {
                        $delivery = $session['delivery']++; $link['credit']--; $link['count']++; $session['outgoing']++;
                        $out .= self::frame(0, $channel, self::perf(0x14, [self::u($link['handle']), self::u($delivery), self::bin(pack('N', $delivery)), self::u(0), true, false]) . array_shift($session['replies'])[0]);
                    }
                }
                else while ($link['credit'] > 0 && $session['held'] === [] && $session['outgoing'] < $session['limit'] && $sent++ < 256 && strlen($out) < 4 * self::FRAME_MAX) {
                    if (!isset($session['broker']->queues[$link['queue']])) break;
                    $q = $session['broker']->queues[$link['queue']];
                    if (($q['args']['singleActive'] ?? false) && ($q['consumers'][0]['tag'] ?? null) !== $link['tag']) break;
                    $stream = isset($link['offset']);
                    if ($stream) {
                        $record = $session['broker']->streamRead($link['queue'], $link['offset'], 1)[0] ?? null; if ($record === null) break;
                        $id = $record['offset']; $link['offset'] = $id + 1; $msg = [...$record, 'mode' => 2, 'exchange' => '', 'key' => $link['queue'], 'redelivered' => false];
                    } else { $id = $session['broker']->getReady($link['queue']); if ($id === null) break; $msg = $session['broker']->msgs[$id]; }
                    $delivery = $session['delivery']++; $link['credit']--; $link['count']++;
                    $session['broker']->prom['delivered']++;
                    $session['broker']->prom[$link['presettled'] ? 'deliveredConsumeAuto' : 'deliveredConsumeManual']++;
                    if ($link['presettled'] && !$stream) $session['broker']->ack($id); elseif (!$link['presettled']) $session['unsettled'][$delivery] = ['handle' => $link['handle'], 'msg' => $id, 'stream' => $stream];
                    $message = self::outbound($msg); $at = 0; $first = true;
                    do {
                        $head = self::perf(0x14, [self::u($link['handle']), $first ? self::u($delivery) : null, $first ? self::bin(pack('N', $delivery)) : null, $first ? self::u(0) : null, $link['presettled'], true]);
                        $room = $state['remoteMax'] - 8 - strlen($head); $chunk = substr($message, $at, $room); $at += strlen($chunk); $more = $at < strlen($message);
                        $head = self::perf(0x14, [self::u($link['handle']), $first ? self::u($delivery) : null, $first ? self::bin(pack('N', $delivery)) : null, $first ? self::u(0) : null, $link['presettled'], $more]);
                        $frame = self::frame(0, $channel, $head . $chunk); $first = false;
                        if ($session['outgoing'] < $session['limit'] && $session['held'] === []) { $out .= $frame; $session['outgoing']++; } else $session['held'][] = ['handle' => $link['handle'], 'bytes' => $frame];
                    } while ($more);
                }
                if ($link['drain'] && $session['held'] === []) { $link['count'] += $link['credit']; $link['credit'] = 0; $out .= $this->flow($channel, $session, $link); $link['drain'] = false; }
                elseif ($link['echo'] ?? false) $out .= $this->flow($channel, $session, $link);
                $link['echo'] = false;
            }
            unset($link);
        }
        unset($session);
        if (($state['idle'] ?? 0) > 0 && microtime(true) - ($state['heartbeat'] ?? 0) >= max(0.5, $state['idle'] / 2000)) { $state['heartbeat'] = microtime(true); $out .= self::frame(0, 0, ''); }
        return $out;
    }
    private function dropLink(array &$session, int $handle): void
    {
        foreach (array_reverse(array_keys($session['unsettled'])) as $id) { $held = $session['unsettled'][$id]; if ($held['handle'] === $handle) { if (!($held['stream'] ?? false)) $session['broker']->requeue($held['msg']); unset($session['unsettled'][$id]); } }
        $link = $session['links'][$handle] ?? null;
        if ($link && $link['dir'] === 'out' && isset($link['queue'], $session['broker']->queues[$link['queue']])) {
            $q =& $session['broker']->queues[$link['queue']]; $before = count($q['consumers']);
            $q['consumers'] = array_values(array_filter($q['consumers'], static fn (array $consumer): bool => $consumer['tag'] !== $link['tag']));
            $session['broker']->prom['consumers'] -= $before - count($q['consumers']);
            if (($q['autoDelete'] ?? false) && $q['consumers'] === []) $session['broker']->deleteQueue($link['queue']);
        }
        unset($session['links'][$handle]);
        $session['held'] = array_values(array_filter($session['held'], static fn (array $frame): bool => $frame['handle'] !== $handle));
    }
    private function dropSession(array &$session): void { foreach (array_keys($session['links']) as $handle) $this->dropLink($session, $handle); }
    public function closed(array &$state): void
    {
        foreach ($state['sessions'] ?? [] as &$session) $this->dropSession($session);
        unset($session);
        // Exclusive queues the management node declared go with their connection.
        if (isset($state['conn'])) foreach ($state['exclusive'] ?? [] as $queue => $vhost) {
            $broker = $this->broker->forVhost($vhost);
            if (($broker->queues[$queue]['owner'] ?? null) === $state['conn']) $broker->deleteQueue($queue);
        }
        $state['exclusive'] = [];
        $state['sessions'] = []; $state['pending'] = []; $state['closing'] = true;
        if (isset($state['conn'])) unset($this->broker->currentUsers[$state['conn']]);
    }

    /** Map 1.0 message sections into the shared 0-9-1 message representation. */
    public static function inbound(string $payload): array
    {
        $d = new Amqp10Codec($payload); $props = []; $headers = []; $mode = 1; $priority = 0; $typedHeaders = []; $data = []; $other = false; $bodyStart = null; $bodyEnd = 0; $to = null; $subject = null;
        while ($d->at < strlen($payload)) {
            $start = $d->at; $section = $d->value(); if (!$section instanceof Amqp10Described) throw new RuntimeException('message section must be described'); $code = $section->descriptor; $value = $section->value;
            if ($code >= 0x75 && $code <= 0x77) { $bodyStart ??= $start; $bodyEnd = $d->at; if ($code === 0x75 && $value instanceof Amqp10Value && $value->type === 'binary') $data[] = $value->value; else $other = true; }
            elseif ($code === 0x70) { $mode = self::field($value, 0) === true ? 2 : 1; $priority = self::field($value, 1, 0); $ttl = self::field($value, 2); if ($ttl !== null) $props['expiration'] = (string) $ttl; }
            elseif (($code === 0x72 || $code === 0x74) && $value instanceof Amqp10Value && $value->type === 'map') {
                foreach ($value->value as [$k, $v]) { $key = self::text($k); if ($key !== null && ($code === 0x74 || (str_starts_with($key, 'x-') && !in_array($key, ['x-exchange', 'x-routing-key'], true)))) { $headers[] = [$key, self::headerValue($v)]; $typedHeaders[] = [$key, $v]; } }
            } elseif ($code === 0x73) {
                foreach ([0 => 'messageId', 1 => 'userId', 4 => 'replyTo', 5 => 'correlationId', 6 => 'contentType', 7 => 'contentEncoding'] as $at => $key) { $v = self::field($value, $at); if ($v !== null) $props[$key] = self::idText($v); }
                $to = self::text(self::field($value, 2)); $subject = self::text(self::field($value, 3)); $created = self::field($value, 9); if ($created instanceof Amqp10Value && $created->type === 'timestamp') $props['timestamp'] = intdiv($created->value, 1000);
                $expires = self::field($value, 8); if ($expires instanceof Amqp10Value && $expires->type === 'timestamp' && !isset($props['expiration'])) $props['expiration'] = (string) max(0, $expires->value - (int) (microtime(true) * 1000));
            }
        }
        if ($other || count($data) > 1) { $body = substr($payload, $bodyStart ?? strlen($payload), $bodyEnd - ($bodyStart ?? $bodyEnd)); $props['contentType'] = self::SECTIONS; } else $body = $data[0] ?? '';
        $props['deliveryMode'] = $mode; if ($priority) $props['priority'] = $priority;
        return compact('body', 'props', 'headers', 'typedHeaders', 'mode', 'priority', 'to', 'subject');
    }
    private static function idText(mixed $v): string
    {
        if ($v instanceof Amqp10Value && $v->type === 'raw-152') { $h = bin2hex($v->value); return substr($h, 0, 8) . '-' . substr($h, 8, 4) . '-' . substr($h, 12, 4) . '-' . substr($h, 16, 4) . '-' . substr($h, 20); }
        if ($v instanceof Amqp10Value && $v->type === 'ulong-raw') {
            $decimal = '0';
            foreach (str_split($v->value) as $byte) {
                $carry = ord($byte);
                for ($i = strlen($decimal) - 1; $i >= 0; $i--) { $n = ((int) $decimal[$i]) * 256 + $carry; $decimal[$i] = (string) ($n % 10); $carry = intdiv($n, 10); }
                while ($carry > 0) { $decimal = (string) ($carry % 10) . $decimal; $carry = intdiv($carry, 10); }
            }
            return $decimal;
        }
        $raw = Amqp10Codec::scalar($v); if (!is_scalar($raw)) throw new RuntimeException('invalid message id'); return (string) $raw;
    }
    private static function headerValue(mixed $v): mixed
    {
        if ($v instanceof Amqp10Value) { if ($v->type === 'timestamp') return intdiv($v->value, 1000); if ($v->type === 'map') { $out = []; foreach ($v->value as [$key, $value]) $out[self::idText($key)] = self::headerValue($value); return $out; } if ($v->type === 'array') return array_map(self::headerValue(...), $v->value); return $v->value; }
        if (is_array($v)) return array_map(self::headerValue(...), $v);
        return $v;
    }
    private static function basicTable(array $pairs, int $depth = 0): string
    {
        if ($depth > 64) throw new RuntimeException('header nesting limit');
        $body = ''; foreach ($pairs as [$key, $value]) $body .= self::short((string) $key) . self::basicField($value, $depth + 1);
        return pack('N', strlen($body)) . $body;
    }
    private static function basicField(mixed $value, int $depth): string
    {
        if ($depth > 64) throw new RuntimeException('header nesting limit');
        if ($value === null) return 'V';
        if (is_bool($value)) return 't' . chr($value ? 1 : 0);
        if (is_int($value)) return 'l' . Codec::u64($value);
        if (is_float($value)) return 'd' . pack('E', $value);
        if ($value instanceof Amqp10Value) {
            if ($value->type === 'binary') return 'x' . Codec::longstr($value->value);
            if ($value->type === 'timestamp') return 'T' . Codec::u64(intdiv($value->value, 1000));
            if ($value->type === 'ulong-raw') return 'L' . $value->value;
            if ($value->type === 'map') return 'F' . self::basicTable($value->value, $depth + 1);
            if ($value->type === 'array') $value = $value->value;
            elseif ($value->type === 'symbol') $value = $value->value;
            else throw new RuntimeException('unsupported basic header type');
        }
        if (is_array($value)) {
            if (array_is_list($value)) { $body = ''; foreach ($value as $item) $body .= self::basicField($item, $depth + 1); return 'A' . pack('N', strlen($body)) . $body; }
            $pairs = []; foreach ($value as $key => $item) $pairs[] = [$key, $item]; return 'F' . self::basicTable($pairs, $depth + 1);
        }
        if (!is_string($value)) throw new RuntimeException('unsupported header value');
        return 'S' . Codec::longstr($value);
    }
    private static function basicBytes(string $raw, int &$at, int $n): string
    {
        if ($n < 0 || $at + $n > strlen($raw)) throw new RuntimeException('truncated basic table');
        $bytes = substr($raw, $at, $n); $at += $n; return $bytes;
    }
    private static function readBasicTable(string $raw, int &$at, int $depth = 0): array
    {
        if ($depth > 64) throw new RuntimeException('header nesting limit');
        $n = unpack('N', self::basicBytes($raw, $at, 4))[1]; $body = self::basicBytes($raw, $at, $n); $o = 0; $out = [];
        while ($o < strlen($body)) { $len = ord(self::basicBytes($body, $o, 1)); $key = self::basicBytes($body, $o, $len); $out[$key] = self::readBasicField($body, $o, $depth + 1); }
        return $out;
    }
    private static function readBasicField(string $raw, int &$at, int $depth): mixed
    {
        if ($depth > 64) throw new RuntimeException('header nesting limit');
        $type = self::basicBytes($raw, $at, 1);
        if ($type === 'V') return null;
        if ($type === 't') return ord(self::basicBytes($raw, $at, 1)) !== 0;
        if ($type === 'S' || $type === 'x') { $len = unpack('N', self::basicBytes($raw, $at, 4))[1]; $value = self::basicBytes($raw, $at, $len); return $type === 'x' ? self::bin($value) : $value; }
        if ($type === 's') { $n = unpack('n', self::basicBytes($raw, $at, 2))[1]; return $n > 32767 ? $n - 65536 : $n; }
        if ($type === 'b' || $type === 'B') { $n = ord(self::basicBytes($raw, $at, 1)); return $type === 'b' && $n > 127 ? $n - 256 : $n; }
        if ($type === 'U' || $type === 'u') { $n = unpack('n', self::basicBytes($raw, $at, 2))[1]; return $type === 'U' && $n > 32767 ? $n - 65536 : $n; }
        if ($type === 'I' || $type === 'i') { $n = unpack('N', self::basicBytes($raw, $at, 4))[1]; return $type === 'I' && $n > 0x7fffffff ? $n - 0x100000000 : $n; }
        if ($type === 'l' || $type === 'L' || $type === 'T') { $bytes = self::basicBytes($raw, $at, 8); $n = Codec::readU64($bytes, 0); return $type === 'T' ? new Amqp10Value('timestamp', $n * 1000) : ($type === 'L' && $n < 0 ? new Amqp10Value('ulong-raw', $bytes) : $n); }
        if ($type === 'd') return unpack('E', self::basicBytes($raw, $at, 8))[1];
        if ($type === 'f') return unpack('G', self::basicBytes($raw, $at, 4))[1];
        if ($type === 'D') { $scale = ord(self::basicBytes($raw, $at, 1)); $n = unpack('N', self::basicBytes($raw, $at, 4))[1]; if ($n > 0x7fffffff) $n -= 0x100000000; return $n / (10 ** $scale); }
        if ($type === 'F') { $map = self::readBasicTable($raw, $at, $depth + 1); $pairs = []; foreach ($map as $key => $v) $pairs[] = [$key, $v]; return new Amqp10Value('map', $pairs); }
        if ($type === 'A') { $n = unpack('N', self::basicBytes($raw, $at, 4))[1]; $body = self::basicBytes($raw, $at, $n); $o = 0; $out = []; while ($o < strlen($body)) $out[] = self::readBasicField($body, $o, $depth + 1); return $out; }
        throw new RuntimeException('unsupported basic field constructor');
    }
    private static function short(string $s): string { if (strlen($s) > 255) throw new RuntimeException('property exceeds short-string limit'); return Codec::shortstr($s); }
    public static function writeProps(array $props, array $headers): string
    {
        $flags = 0; $body = '';
        foreach (['contentType', 'contentEncoding', 'headers', 'deliveryMode', 'priority', 'correlationId', 'replyTo', 'expiration', 'messageId', 'timestamp', 'type', 'userId', 'appId'] as $bit => $key) {
            $value = $key === 'headers' ? ($headers === [] ? null : $headers) : ($props[$key] ?? null); if ($value === null) continue;
            $flags |= 1 << (15 - $bit);
            $body .= $key === 'headers' ? self::basicTable($value) : (in_array($key, ['deliveryMode', 'priority'], true) ? chr((int) $value) : ($key === 'timestamp' ? Codec::u64((int) $value) : self::short((string) $value)));
        }
        return pack('n', $flags) . $body;
    }
    public static function readProps(?string $raw): array
    {
        if ($raw === null || strlen($raw) < 2) return ['headers' => []];
        $flags = unpack('n', substr($raw, 0, 2))[1]; $o = 2; $props = ['headers' => []];
        foreach (['contentType', 'contentEncoding', 'headers', 'deliveryMode', 'priority', 'correlationId', 'replyTo', 'expiration', 'messageId', 'timestamp', 'type', 'userId', 'appId'] as $bit => $key) {
            if (!($flags & (1 << (15 - $bit)))) continue;
            if ($key === 'headers') $props['headers'] = self::readBasicTable($raw, $o);
            elseif (in_array($key, ['deliveryMode', 'priority'], true)) { if (!isset($raw[$o])) throw new RuntimeException('truncated basic property'); $props[$key] = ord($raw[$o++]); }
            elseif ($key === 'timestamp') { if ($o + 8 > strlen($raw)) throw new RuntimeException('truncated timestamp'); $props[$key] = Codec::readU64($raw, $o); $o += 8; }
            else { if (!isset($raw[$o]) || $o + 1 + ord($raw[$o]) > strlen($raw)) throw new RuntimeException('truncated short property'); $props[$key] = Codec::readShortstr($raw, $o); }
        }
        return $props;
    }
    public static function outbound(array $msg): string
    {
        $p = self::readProps($msg['propRaw'] ?? null); $out = Amqp10Codec::encode(self::desc(0x70, [($msg['mode'] ?? 1) === 2, new Amqp10Value('ubyte', $p['priority'] ?? 4), isset($p['expiration']) ? self::u((int) $p['expiration']) : null, !($msg['redelivered'] ?? false), self::u(0)]));
        $annotations = [[self::sym('x-exchange'), $msg['exchange'] ?? ''], [self::sym('x-routing-key'), $msg['key'] ?? '']]; $app = [];
        $headers = $p['headers']; if ($headers === []) foreach ($msg['headers'] ?? [] as [$key, $value]) $headers[$key] = $value;
        foreach ($headers as $key => $value) { if (str_starts_with($key, 'x-')) $annotations[] = [self::sym($key), $value]; else $app[] = [$key, $value]; }
        $out .= Amqp10Codec::encode(self::desc(0x72, new Amqp10Value('map', $annotations)));
        $props = [$p['messageId'] ?? null, isset($p['userId']) ? self::bin($p['userId']) : null, null, null, $p['replyTo'] ?? null, $p['correlationId'] ?? null, isset($p['contentType']) && $p['contentType'] !== self::SECTIONS ? self::sym($p['contentType']) : null, isset($p['contentEncoding']) ? self::sym($p['contentEncoding']) : null, null, isset($p['timestamp']) ? new Amqp10Value('timestamp', $p['timestamp'] * 1000) : null];
        while ($props !== [] && end($props) === null) array_pop($props); if ($props !== []) $out .= Amqp10Codec::encode(self::desc(0x73, $props));
        if ($app !== []) $out .= Amqp10Codec::encode(self::desc(0x74, new Amqp10Value('map', $app)));
        return $out . (($p['contentType'] ?? '') === self::SECTIONS ? $msg['body'] : Amqp10Codec::encode(self::desc(0x75, self::bin($msg['body']))));
    }
}
