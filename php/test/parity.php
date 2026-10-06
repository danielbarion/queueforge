<?php
declare(strict_types=1);

$root = dirname(__DIR__);
$php = PHP_BINARY;
$dir = sys_get_temp_dir() . '/qf-php-parity-' . getmypid();
mkdir($dir);
$config = $dir . '/config.toml';
file_put_contents($config, <<<TOML
[listeners]
amqp = "127.0.0.1:5676"

[data]
dir = "$dir/data"
fsync_interval_ms = 10
TOML);
$proc = proc_open(
    [$php, $root . '/bin/queueforge', '--config', $config, '--dev-bootstrap'],
    [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
    $pipes,
    $root,
);
$conn = false;
$deadline = microtime(true) + 3;
while (microtime(true) < $deadline) {
    $conn = @stream_socket_client('tcp://127.0.0.1:5676', $e, $s, 0.1);
    if ($conn !== false) {
        break;
    }
    usleep(50000);
}
if ($conn === false) {
    fwrite(STDERR, stream_get_contents($pipes[2]));
    proc_terminate($proc, 9);
    exit(1);
}
stream_set_blocking($conn, false);

function frame($conn, int $type, int $channel, string $payload): void
{
    fwrite($conn, chr($type) . pack('n', $channel) . pack('N', strlen($payload)) . $payload . "\xce");
    fflush($conn);
}

function pull($conn, float $seconds): string
{
    $buf = '';
    $end = microtime(true) + $seconds;
    while (microtime(true) < $end) {
        $chunk = fread($conn, 65536);
        if (is_string($chunk) && $chunk !== '') {
            $buf .= $chunk;
        } else {
            usleep(1000);
        }
    }
    return $buf;
}

fwrite($conn, "AMQP\x00\x00\x09\x01");
fflush($conn);
$sasl = "\0admin\0devpassword12";
frame($conn, 1, 0, pack('nn', 10, 11) . pack('N', 0) . chr(5) . 'PLAIN' . pack('N', strlen($sasl)) . $sasl . chr(5) . 'en_US');
frame($conn, 1, 0, pack('nn', 10, 31) . pack('nNn', 2047, 131072, 0));
frame($conn, 1, 0, pack('nn', 10, 40) . chr(1) . '/' . chr(0) . chr(0));
frame($conn, 1, 1, pack('nn', 20, 10) . chr(0));
frame($conn, 1, 1, pack('nn', 40, 10) . pack('n', 0) . chr(2) . 'ex' . chr(6) . 'fanout' . chr(0) . pack('N', 0));
foreach (['a', 'b'] as $q) {
    frame($conn, 1, 1, pack('nn', 50, 10) . pack('n', 0) . chr(1) . $q . chr(2) . pack('N', 0));
    frame($conn, 1, 1, pack('nn', 50, 20) . pack('n', 0) . chr(1) . $q . chr(2) . 'ex' . chr(0) . chr(0) . pack('N', 0));
}
frame($conn, 1, 1, pack('nn', 60, 20) . pack('n', 0) . chr(1) . 'a' . chr(2) . 'ca' . chr(0) . pack('N', 0));
$body = 'fanout-body';
$pub = pack('nn', 60, 40) . pack('n', 0) . chr(2) . 'ex' . chr(3) . 'key' . chr(0);
$header = pack('nn', 60, 0) . pack('NN', 0, strlen($body)) . pack('n', 0x1000) . chr(1);
frame($conn, 1, 1, $pub);
frame($conn, 2, 1, $header);
frame($conn, 3, 1, $body);
$got = pull($conn, 0.4);
if (!str_contains($got, $body)) {
    $buf = $got;
    $methods = [];
    while (strlen($buf) >= 7) {
        $len = unpack('N', substr($buf, 3, 4))[1];
        if (strlen($buf) < 8 + $len) {
            break;
        }
        $payload = substr($buf, 7, $len);
        $buf = substr($buf, 8 + $len);
        if (strlen($payload) >= 4) {
            $methods[] = unpack('n', substr($payload, 0, 2))[1] . '.' . unpack('n', substr($payload, 2, 2))[1];
        }
    }
    fwrite(STDERR, "methods " . implode(' ', $methods) . "\n");
    proc_terminate($proc, 9);
    exit(1);
}
frame($conn, 1, 1, pack('nn', 60, 30) . chr(2) . 'ca' . chr(0));
frame($conn, 1, 1, pack('nn', 60, 20) . pack('n', 0) . chr(1) . 'b' . chr(2) . 'cb' . chr(0) . pack('N', 0));
$got = pull($conn, 0.4);
if (!str_contains($got, $body)) {
    fwrite(STDERR, "fanout consumer b missed the copy\n");
    proc_terminate($proc, 9);
    exit(1);
}
echo "ok fanout=2\n";
proc_terminate($proc, 9);
exit(0);
