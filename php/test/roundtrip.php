<?php
declare(strict_types=1);

$root = dirname(__DIR__);
$php = PHP_BINARY;
$dir = sys_get_temp_dir() . '/qf-php-' . getmypid();
mkdir($dir);
// A reserved free port rather than a fixed one, so a run does not collide
// with another broker or with a parallel test.
$probe = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
$name = (string) stream_socket_get_name($probe, false);
fclose($probe);
$port = (int) substr($name, (int) strrpos($name, ':') + 1);

// The temp directory goes away however this script exits.
register_shutdown_function(static function () use ($dir): void {
    $it = new RecursiveIteratorIterator(
        new RecursiveDirectoryIterator($dir, FilesystemIterator::SKIP_DOTS),
        RecursiveIteratorIterator::CHILD_FIRST,
    );
    foreach ($it as $entry) {
        $entry->isDir() ? @rmdir($entry->getPathname()) : @unlink($entry->getPathname());
    }
    @rmdir($dir);
});
$config = $dir . '/config.toml';
file_put_contents($config, <<<TOML
[listeners]
amqp = "127.0.0.1:$port"

[data]
dir = "$dir/data"
fsync_policy = "every_n_ms"
fsync_interval_ms = 10
TOML);

$proc = proc_open(
    [$php, $root . '/bin/queueforge', '--config', $config, '--dev-bootstrap'],
    [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
    $pipes,
    $root,
);
if (!is_resource($proc)) {
    fwrite(STDERR, "failed to start\n");
    exit(1);
}
$ready = false;
$deadline = microtime(true) + 3;
while (microtime(true) < $deadline) {
    $conn = @stream_socket_client("tcp://127.0.0.1:$port", $e, $s, 0.1);
    if ($conn !== false) {
        $ready = true;
        break;
    }
    usleep(50000);
}
if (!$ready) {
    fwrite(STDERR, stream_get_contents($pipes[2]));
    proc_terminate($proc, 9);
    exit(1);
}
stream_set_timeout($conn, 2);

function send_frame($conn, int $type, int $channel, string $payload): void
{
    fwrite($conn, chr($type) . pack('n', $channel) . pack('N', strlen($payload)) . $payload . "\xce");
}

function read_method($conn, int $wantClass, int $wantMethod): string
{
    $buf = '';
    $deadline = microtime(true) + 2;
    while (microtime(true) < $deadline) {
        $chunk = fread($conn, 65536);
        if ($chunk === false || $chunk === '') {
            usleep(1000);
            continue;
        }
        $buf .= $chunk;
        while (strlen($buf) >= 7) {
            $len = unpack('N', substr($buf, 3, 4))[1];
            if (strlen($buf) < 8 + $len) {
                break;
            }
            $type = ord($buf[0]);
            $payload = substr($buf, 7, $len);
            $buf = substr($buf, 8 + $len);
            if ($type !== 1 || strlen($payload) < 4) {
                continue;
            }
            $class = unpack('n', substr($payload, 0, 2))[1];
            $method = unpack('n', substr($payload, 2, 2))[1];
            if ($class === $wantClass && $method === $wantMethod) {
                return $payload;
            }
        }
    }
    throw new RuntimeException("timeout waiting $wantClass.$wantMethod");
}

try {
    fwrite($conn, "AMQP\x00\x00\x09\x01");
    read_method($conn, 10, 10);
    $sasl = "\0admin\0devpassword12";
    $start = pack('nn', 10, 11) . pack('N', 0) . chr(5) . 'PLAIN' . pack('N', strlen($sasl)) . $sasl . chr(5) . 'en_US';
    send_frame($conn, 1, 0, $start);
    read_method($conn, 10, 30);
    send_frame($conn, 1, 0, pack('nn', 10, 31) . pack('nNn', 2047, 131072, 0));
    send_frame($conn, 1, 0, pack('nn', 10, 40) . chr(1) . '/' . chr(0) . chr(0));
    read_method($conn, 10, 41);
    send_frame($conn, 1, 1, pack('nn', 20, 10) . chr(0));
    read_method($conn, 20, 11);
    $declare = pack('nn', 50, 10) . pack('n', 0) . chr(2) . 'q0' . chr(0x02) . pack('N', 0);
    send_frame($conn, 1, 1, $declare);
    read_method($conn, 50, 11);
    send_frame($conn, 1, 1, pack('nn', 85, 10) . chr(0));
    read_method($conn, 85, 11);
    send_frame($conn, 1, 1, pack('nn', 60, 10) . pack('N', 0) . pack('n', 10) . chr(0));
    read_method($conn, 60, 11);
    $consume = pack('nn', 60, 20) . pack('n', 0) . chr(2) . 'q0' . chr(2) . 'c0' . chr(0) . pack('N', 0);
    send_frame($conn, 1, 1, $consume);
    read_method($conn, 60, 21);

    $body = str_repeat('x', 11);
    $pub = pack('nn', 60, 40) . pack('n', 0) . chr(0) . chr(2) . 'q0' . chr(0);
    $header = pack('nn', 60, 0) . pack('NN', 0, strlen($body)) . pack('n', 0x1000) . chr(2);
    send_frame($conn, 1, 1, $pub);
    send_frame($conn, 2, 1, $header);
    send_frame($conn, 3, 1, $body);

    $sawAck = false;
    $sawBody = false;
    $buf = '';
    $deadline = microtime(true) + 2;
    while (microtime(true) < $deadline && (!$sawAck || !$sawBody)) {
        $chunk = fread($conn, 65536);
        if ($chunk === false || $chunk === '') {
            usleep(1000);
            continue;
        }
        $buf .= $chunk;
        while (strlen($buf) >= 7) {
            $len = unpack('N', substr($buf, 3, 4))[1];
            if (strlen($buf) < 8 + $len) {
                break;
            }
            $type = ord($buf[0]);
            $payload = substr($buf, 7, $len);
            $buf = substr($buf, 8 + $len);
            if ($type === 1 && strlen($payload) >= 4) {
                $class = unpack('n', substr($payload, 0, 2))[1];
                $method = unpack('n', substr($payload, 2, 2))[1];
                if ($class === 60 && $method === 80) {
                    $sawAck = true;
                }
            }
            if ($type === 3 && $payload === $body) {
                $sawBody = true;
            }
        }
    }
    if (!$sawAck || !$sawBody) {
        throw new RuntimeException('missing confirm or delivery');
    }
    echo "ok confirm=1 deliver=1 bytes=" . strlen($body) . "\n";
    proc_terminate($proc, 9);
    fclose($conn);
    $deadline = microtime(true) + 2;
    while (microtime(true) < $deadline) {
        $held = @stream_socket_client("tcp://127.0.0.1:$port", $e, $s, 0.05);
        if ($held === false) {
            break;
        }
        fclose($held);
        usleep(50000);
    }
    $proc = proc_open(
        [$php, $root . '/bin/queueforge', '--config', $config],
        [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
        $pipes,
        $root,
    );
    $conn = false;
    $deadline = microtime(true) + 3;
    while (microtime(true) < $deadline) {
        $conn = @stream_socket_client("tcp://127.0.0.1:$port", $e, $s, 0.1);
        if ($conn !== false) {
            break;
        }
        usleep(50000);
    }
    if ($conn === false) {
        throw new RuntimeException('restart failed');
    }
    stream_set_timeout($conn, 2);
    fwrite($conn, "AMQP\x00\x00\x09\x01");
    read_method($conn, 10, 10);
    send_frame($conn, 1, 0, $start);
    read_method($conn, 10, 30);
    send_frame($conn, 1, 0, pack('nn', 10, 31) . pack('nNn', 2047, 131072, 0));
    send_frame($conn, 1, 0, pack('nn', 10, 40) . chr(1) . '/' . chr(0) . chr(0));
    read_method($conn, 10, 41);
    send_frame($conn, 1, 1, pack('nn', 20, 10) . chr(0));
    read_method($conn, 20, 11);
    send_frame($conn, 1, 1, $consume);
    $again = false;
    $buf = '';
    $deadline = microtime(true) + 2;
    while (microtime(true) < $deadline && !$again) {
        $chunk = fread($conn, 65536);
        if ($chunk === false || $chunk === '') {
            usleep(1000);
            continue;
        }
        $buf .= $chunk;
        if (str_contains($buf, $body)) {
            $again = true;
        }
    }
    if (!$again) {
        throw new RuntimeException('restart did not redeliver');
    }
    echo "ok redeliver=1\n";
} catch (Throwable $err) {
    fwrite(STDERR, $err->getMessage() . "\n");
    fwrite(STDERR, stream_get_contents($pipes[1]));
    fwrite(STDERR, stream_get_contents($pipes[2]));
    proc_terminate($proc, 9);
    exit(1);
}
proc_terminate($proc, 9);
fclose($conn);
exit(0);
