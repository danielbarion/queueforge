<?php
declare(strict_types=1);

/**
 * MQTT, STOMP and the stream command set. The large MQTT payload is a
 * regression check: the remaining length used to be written as a single
 * byte, so anything from 128 bytes up declared a wrong length.
 */

require_once __DIR__ . '/lib/Harness.php';
$root = dirname(__DIR__);
require_once $root . '/src/Routing.php';
require_once $root . '/src/Features.php';

// MQTT topic filter matching.
Harness::guard('mqtt filters', static function (): void {
    $cases = [
        ['a/b/c', 'a/b/c', true],
        ['a/+/c', 'a/b/c', true],
        ['a/+/c', 'a/b/d', false],
        ['a/#', 'a/b/c', true],
        ['a/#', 'a', true],
        ['#', 'a/b', true],
        ['a/b', 'a/b/c', false],
        ['+/b', 'a/b', true],
        ['+', 'a/b', false],
    ];
    foreach ($cases as [$filter, $topic, $want]) {
        Harness::eq("mqtt \"$filter\" against \"$topic\"", $want, Features::mqttMatch($filter, $topic));
    }
});

$mqttPort = Harness::freePort();
$stompPort = Harness::freePort();
$streamPort = Harness::freePort();
$broker = Harness::broker([
    "mqtt = \"127.0.0.1:$mqttPort\"",
    "stomp = \"127.0.0.1:$stompPort\"",
    "stream = \"127.0.0.1:$streamPort\"",
]);

/** Decodes an MQTT remaining length, returning the value and header size. */
function mqttLen(string $frame): array
{
    $at = 1;
    $value = 0;
    $shift = 0;
    do {
        if (!isset($frame[$at])) {
            return ['value' => -1, 'header' => $at];
        }
        $byte = ord($frame[$at]);
        $value += ($byte & 0x7f) << $shift;
        $shift += 7;
        $at++;
    } while (($byte & 0x80) !== 0);
    return ['value' => $value, 'header' => $at];
}

function mqttString(string $text): string
{
    return pack('n', strlen($text)) . $text;
}

Harness::guard('mqtt connect and publish', static function () use ($mqttPort): void {
    $fp = stream_socket_client("tcp://127.0.0.1:$mqttPort", $errno, $errstr, 5.0);
    if ($fp === false) {
        throw new RuntimeException("mqtt connect failed: $errstr");
    }
    stream_set_timeout($fp, 3);

    // CONNECT, then expect CONNACK.
    $payload = mqttString('MQTT') . chr(4) . chr(2) . pack('n', 60) . mqttString('tester');
    fwrite($fp, chr(0x10) . chr(strlen($payload)) . $payload);
    $connack = fread($fp, 4);
    Harness::eq('connack arrives', "\x20\x02\x00\x00", $connack);

    // SUBSCRIBE to a topic, then expect SUBACK.
    $sub = pack('n', 1) . mqttString('news/#') . chr(0);
    fwrite($fp, chr(0x82) . chr(strlen($sub)) . $sub);
    $head = fread($fp, 2);
    Harness::eq('suback type', 0x90, ord($head[0] ?? chr(0)));
    $rest = fread($fp, ord($head[1] ?? chr(0)));
    Harness::ok('suback carries a return code', strlen($rest) >= 3);

    // PINGREQ gets PINGRESP.
    fwrite($fp, chr(0xc0) . chr(0));
    Harness::eq('pingresp arrives', "\xd0\x00", fread($fp, 2));
    fclose($fp);
});

// The regression: a payload of 128 bytes or more must declare a varint length.
Harness::guard('mqtt large payload', static function () use ($mqttPort): void {
    foreach ([64, 200, 5000, 100000] as $size) {
        // A distinct topic per size, so a replayed message from an earlier
        // iteration cannot be mistaken for this one's echo.
        $topic = "big/$size";
        $fp = stream_socket_client("tcp://127.0.0.1:$mqttPort", $errno, $errstr, 5.0);
        if ($fp === false) {
            throw new RuntimeException("mqtt connect failed at $size: $errstr");
        }
        stream_set_timeout($fp, 3);
        $payload = mqttString('MQTT') . chr(4) . chr(2) . pack('n', 60) . mqttString("c$size");
        fwrite($fp, chr(0x10) . chr(strlen($payload)) . $payload);
        fread($fp, 4);

        // Subscribe so the publish is echoed back on this connection.
        $sub = pack('n', 1) . mqttString($topic) . chr(0);
        fwrite($fp, chr(0x82) . chr(strlen($sub)) . $sub);
        $head = (string) fread($fp, 2);
        $remaining = strlen($head) > 1 ? ord($head[1]) : 0;
        if ($remaining > 0) {
            fread($fp, $remaining);
        }

        // PUBLISH with a body of the given size. The remaining length is a
        // varint on the way in as well.
        $body = str_repeat('x', $size);
        $pub = mqttString($topic) . $body;
        $len = '';
        $n = strlen($pub);
        do {
            $byte = $n % 128;
            $n = intdiv($n, 128);
            $len .= chr($n > 0 ? $byte | 0x80 : $byte);
        } while ($n > 0);
        fwrite($fp, chr(0x30) . $len . $pub);

        // Read the echoed PUBLISH and check the declared length matches.
        $raw = '';
        $expected = 2 + strlen($topic) + $size;
        $deadline = microtime(true) + 5.0;
        while (microtime(true) < $deadline) {
            $decoded = mqttLen($raw);
            if ($decoded['value'] >= 0 && strlen($raw) >= $decoded['header'] + $decoded['value']) {
                break;
            }
            $chunk = fread($fp, 65536);
            if ($chunk === false || $chunk === '') {
                usleep(20000);
                continue;
            }
            $raw .= $chunk;
        }
        $decoded = mqttLen($raw);
        Harness::eq("mqtt declares the right length at $size bytes", $expected, $decoded['value']);
        $echoed = substr($raw, $decoded['header'] + 2 + strlen($topic), $size);
        Harness::eq("the payload survives at $size bytes", $body, $echoed);
        fclose($fp);
    }
});

Harness::guard('stomp round trip', static function () use ($stompPort): void {
    $fp = stream_socket_client("tcp://127.0.0.1:$stompPort", $errno, $errstr, 5.0);
    if ($fp === false) {
        throw new RuntimeException("stomp connect failed: $errstr");
    }
    stream_set_timeout($fp, 3);
    fwrite($fp, "CONNECT\naccept-version:1.2\nhost:/\n\n\x00");
    $connected = fread($fp, 1024);
    Harness::ok('stomp connects', str_contains((string) $connected, 'CONNECTED'), 'got: ' . trim((string) $connected));

    fwrite($fp, "SUBSCRIBE\nid:0\ndestination:/queue/st\n\n\x00");
    usleep(150000);
    fwrite($fp, "SEND\ndestination:/queue/st\n\nhello-stomp\x00");
    $raw = '';
    $deadline = microtime(true) + 3.0;
    while (microtime(true) < $deadline && !str_contains($raw, 'hello-stomp')) {
        $chunk = fread($fp, 65536);
        if ($chunk === false || $chunk === '') {
            break;
        }
        $raw .= $chunk;
    }
    Harness::ok('the message comes back', str_contains($raw, 'hello-stomp'), 'got: ' . trim($raw));
    Harness::ok('as a MESSAGE frame', str_contains($raw, 'MESSAGE'));
    fclose($fp);
});

// The stream listener must answer rather than accept and go quiet, which is
// what it did while its dispatch branch was missing.
Harness::guard('stream commands', static function () use ($streamPort): void {
    $fp = stream_socket_client("tcp://127.0.0.1:$streamPort", $errno, $errstr, 5.0);
    if ($fp === false) {
        throw new RuntimeException("stream connect failed: $errstr");
    }
    stream_set_timeout($fp, 3);

    $send = static function ($fp, int $key, int $corr, string $extra = ''): void {
        $payload = pack('n', $key) . pack('n', 1) . pack('N', $corr) . $extra;
        fwrite($fp, pack('N', strlen($payload)) . $payload);
    };
    $recv = static function ($fp): array {
        $head = fread($fp, 4);
        if (strlen((string) $head) < 4) {
            return ['key' => 0, 'corr' => 0];
        }
        $body = (string) fread($fp, unpack('N', $head)[1]);
        return [
            'key' => unpack('n', substr($body, 0, 2))[1],
            'corr' => strlen($body) >= 8 ? unpack('N', substr($body, 4, 4))[1] : 0,
        ];
    };

    $send($fp, 0x0015, 11); // Open
    $reply = $recv($fp);
    Harness::eq('open is answered', 0x8015, $reply['key']);
    Harness::eq('the correlation id is echoed', 11, $reply['corr']);

    $send($fp, 0x000d, 12, pack('n', 6) . 'stream'); // Create
    $reply = $recv($fp);
    Harness::eq('create is answered', 0x800d, $reply['key']);

    $send($fp, 0x0011, 13); // PeerProperties
    $reply = $recv($fp);
    Harness::eq('an unhandled command still answers', 0x8011, $reply['key']);
    fclose($fp);
});

Harness::stop($broker);
Harness::done();
