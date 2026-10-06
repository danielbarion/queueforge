<?php
declare(strict_types=1);

/**
 * A small confirm-rate probe.
 *
 * This is NOT the paced `queueforge-compare` ladder and its numbers are not
 * comparable to the tables in BENCHMARK.md. Its only job is to measure the
 * same thing twice against two builds, so a throughput regression on the
 * publish path shows up.
 *
 * Usage: php probe.php <host> <port> <inflight> <seconds>
 */

$host = $argv[1] ?? '127.0.0.1';
$port = (int) ($argv[2] ?? 35675);
$inflight = (int) ($argv[3] ?? 1);
$seconds = (float) ($argv[4] ?? 8.0);
$body = str_repeat('x', 256);

$fp = stream_socket_client("tcp://$host:$port", $errno, $errstr, 5.0);
if ($fp === false) {
    fwrite(STDERR, "connect failed: $errstr\n");
    exit(1);
}
stream_set_blocking($fp, true);
stream_set_timeout($fp, 10);

function shortstr(string $s): string
{
    return chr(strlen($s)) . $s;
}
function longstr(string $s): string
{
    return pack('N', strlen($s)) . $s;
}
function u64(int $n): string
{
    return pack('J', $n);
}
function frame(int $type, int $ch, string $payload): string
{
    return chr($type) . pack('n', $ch) . pack('N', strlen($payload)) . $payload . "\xce";
}
function method(int $ch, int $class, int $m, string $args = ''): string
{
    return frame(1, $ch, pack('nn', $class, $m) . $args);
}

/** Reads one frame, returning [type, channel, payload]. */
function readFrame($fp): ?array
{
    $head = '';
    while (strlen($head) < 7) {
        $chunk = fread($fp, 7 - strlen($head));
        if ($chunk === false || $chunk === '') {
            return null;
        }
        $head .= $chunk;
    }
    $type = ord($head[0]);
    $ch = unpack('n', substr($head, 1, 2))[1];
    $len = unpack('N', substr($head, 3, 4))[1];
    $payload = '';
    while (strlen($payload) < $len) {
        $chunk = fread($fp, $len - strlen($payload));
        if ($chunk === false || $chunk === '') {
            return null;
        }
        $payload .= $chunk;
    }
    fread($fp, 1);
    return [$type, $ch, $payload];
}

/** Reads until a given class.method arrives. */
function expect($fp, int $class, int $m): string
{
    while (true) {
        $f = readFrame($fp);
        if ($f === null) {
            throw new RuntimeException("expected $class.$m, got nothing");
        }
        if ($f[0] !== 1 || strlen($f[2]) < 4) {
            continue;
        }
        $c = unpack('n', substr($f[2], 0, 2))[1];
        $mm = unpack('n', substr($f[2], 2, 2))[1];
        if ($c === $class && $mm === $m) {
            return substr($f[2], 4);
        }
        if ($c === 20 && $mm === 40) {
            throw new RuntimeException('channel closed: ' . bin2hex(substr($f[2], 4, 40)));
        }
        if ($c === 10 && $mm === 50) {
            throw new RuntimeException('connection closed: ' . bin2hex(substr($f[2], 4, 40)));
        }
    }
}

fwrite($fp, "AMQP\x00\x00\x09\x01");
expect($fp, 10, 10);
$response = "\0admin\0devpassword12";
fwrite($fp, method(0, 10, 11, pack('N', 0) . shortstr('PLAIN') . longstr($response) . shortstr('en_US')));
expect($fp, 10, 30);
fwrite($fp, method(0, 10, 31, pack('nNn', 0, 131072, 0)));
fwrite($fp, method(0, 10, 40, shortstr('/') . shortstr('') . chr(0)));
expect($fp, 10, 41);
fwrite($fp, method(1, 20, 10, shortstr('')));
expect($fp, 20, 11);

$queue = 'probe-' . bin2hex(random_bytes(4));
// The queue is bounded because this probe never consumes. An unbounded queue
// grows until log compaction exhausts PHP's memory_limit, which would measure
// the crash rather than the publish path.
$args = chr(14) . 'x-max-length' . 'I' . pack('N', 20000);
fwrite($fp, method(1, 50, 10, pack('n', 0) . shortstr($queue) . chr(2) . pack('N', strlen($args)) . $args));
expect($fp, 50, 11);
fwrite($fp, method(1, 85, 10, chr(0)));
expect($fp, 85, 11);

// A persistent publish: delivery-mode 2 in the property block.
$props = pack('n', 0x1000) . chr(2);
$publish = method(1, 60, 40, pack('n', 0) . shortstr('') . shortstr($queue) . chr(0))
    . frame(2, 1, pack('nn', 60, 0) . u64(strlen($body)) . $props)
    . frame(3, 1, $body);

$started = microtime(true);
$deadline = $started + $seconds;
$sent = 0;
$confirmed = 0;
$latencies = [];
$outstanding = [];

while (microtime(true) < $deadline) {
    while (count($outstanding) < $inflight && microtime(true) < $deadline) {
        fwrite($fp, $publish);
        $sent++;
        $outstanding[$sent] = microtime(true);
    }
    if ($outstanding === []) {
        break;
    }
    $f = readFrame($fp);
    if ($f === null) {
        break;
    }
    if ($f[0] !== 1 || strlen($f[2]) < 4) {
        continue;
    }
    $c = unpack('n', substr($f[2], 0, 2))[1];
    $m = unpack('n', substr($f[2], 2, 2))[1];
    if ($c !== 60 || ($m !== 80 && $m !== 120)) {
        continue;
    }
    $tag = unpack('J', substr($f[2], 4, 8))[1];
    $multiple = (ord($f[2][12]) & 1) === 1;
    $now = microtime(true);
    foreach ($outstanding as $have => $at) {
        if ($multiple ? $have <= $tag : $have === $tag) {
            $latencies[] = ($now - $at) * 1000;
            $confirmed++;
            unset($outstanding[$have]);
        }
    }
}
$elapsed = microtime(true) - $started;
sort($latencies);
$p = static function (array $xs, float $q): float {
    if ($xs === []) {
        return 0.0;
    }
    return $xs[min(count($xs) - 1, (int) floor($q * count($xs)))];
};

printf(
    "inflight=%d confirmed=%d elapsed=%.2fs rate=%.1f/s p50=%.2fms p99=%.2fms\n",
    $inflight,
    $confirmed,
    $elapsed,
    $confirmed / max(0.001, $elapsed),
    $p($latencies, 0.50),
    $p($latencies, 0.99),
);
fclose($fp);
