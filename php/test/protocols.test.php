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
require_once $root . '/src/Policy.php';

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
            return ['key' => 0, 'corr' => 0, 'body' => ''];
        }
        $body = (string) fread($fp, unpack('N', $head)[1]);
        return [
            'key' => unpack('n', substr($body, 0, 2))[1],
            'corr' => strlen($body) >= 8 ? unpack('N', substr($body, 4, 4))[1] : 0,
            'body' => $body,
        ];
    };

    $send($fp, 0x0015, 11); // Open
    $reply = $recv($fp);
    Harness::eq('open is answered', 0x8015, $reply['key']);
    Harness::eq('the correlation id is echoed', 11, $reply['corr']);

    $send($fp, 0x000d, 12, pack('n', 6) . 'stream'); // Create
    $reply = $recv($fp);
    Harness::eq('create is answered', 0x800d, $reply['key']);

    // PeerProperties now carries a real properties map.
    $send($fp, 0x0011, 13);
    $reply = $recv($fp);
    Harness::eq('peer properties is answered', 0x8011, $reply['key']);
    Harness::ok('and names the product', str_contains($reply['body'], 'RabbitMQ'));

    // SaslHandshake lists PLAIN so a client can choose it.
    $send($fp, 0x0012, 14);
    $reply = $recv($fp);
    Harness::eq('sasl handshake is answered', 0x8012, $reply['key']);
    Harness::ok('PLAIN is offered', str_contains($reply['body'], 'PLAIN'));

    // SaslAuthenticate is followed by an unsolicited Tune.
    $send($fp, 0x0013, 15);
    $reply = $recv($fp);
    Harness::eq('sasl authenticate is answered', 0x8013, $reply['key']);
    $tune = $recv($fp);
    Harness::eq('a tune frame follows', 0x0014, $tune['key']);

    // DeclarePublisher, then Publish, which must come back as a 0x0003
    // PublishConfirm rather than a generic echo.
    $send($fp, 0x0001, 16, chr(7) . pack('n', 3) . 'ref' . pack('n', 6) . 'stream');
    $reply = $recv($fp);
    Harness::eq('declare publisher is answered', 0x8001, $reply['key']);

    $entry = pack('N', 0) . pack('N', 42) . pack('N', 5) . 'hello';
    $send($fp, 0x0002, 17, chr(7) . pack('N', 1) . $entry);
    $reply = $recv($fp);
    Harness::eq('publish is confirmed', 0x0003, $reply['key']);
    Harness::eq('the publisher id comes back', 7, ord($reply['body'][4]));
    Harness::eq('one id is confirmed', 1, unpack('N', substr($reply['body'], 5, 4))[1]);
    Harness::eq('and it is the one sent', 42, unpack('N', substr($reply['body'], 13, 4))[1]);

    // Subscribe replays the stored chunk as a 0x0008 Deliver.
    $send($fp, 0x0007, 18, chr(3) . pack('n', 6) . 'stream');
    $reply = $recv($fp);
    Harness::eq('subscribe is answered', 0x8007, $reply['key']);
    $deliver = $recv($fp);
    Harness::eq('a deliver frame follows', 0x0008, $deliver['key']);
    Harness::eq('for the right subscription', 3, ord($deliver['body'][4]));
    Harness::ok('and carries the payload', str_contains($deliver['body'], 'hello'));

    // A client Tune response and a heartbeat get no reply at all.
    $send($fp, 0x0014, 19, pack('N', 1048576) . pack('N', 60));
    $send($fp, 0x0016, 20); // Close, so there is something to read next.
    $reply = $recv($fp);
    Harness::eq('tune is not echoed and close is answered', 0x8016, $reply['key']);
    fclose($fp);
});

Harness::guard('mqtt unsubscribe and disconnect', static function () use ($mqttPort): void {
    $fp = stream_socket_client("tcp://127.0.0.1:$mqttPort", $errno, $errstr, 5.0);
    if ($fp === false) {
        throw new RuntimeException("mqtt connect failed: $errstr");
    }
    stream_set_timeout($fp, 3);
    $payload = mqttString('MQTT') . chr(4) . chr(2) . pack('n', 60) . mqttString('c1');
    fwrite($fp, chr(0x10) . chr(strlen($payload)) . $payload);
    fread($fp, 4);

    // Subscribe, then unsubscribe the same filter.
    $sub = pack('n', 1) . mqttString('uns/topic') . chr(0);
    fwrite($fp, "\x82" . chr(strlen($sub)) . $sub);
    $suback = (string) fread($fp, 5);
    Harness::eq('suback arrives', 0x90, ord($suback[0]));

    $uns = pack('n', 2) . mqttString('uns/topic');
    fwrite($fp, "\xa2" . chr(strlen($uns)) . $uns);
    $unsuback = (string) fread($fp, 4);
    Harness::eq('unsuback arrives', 0xb0, ord($unsuback[0]));
    Harness::eq('with the packet id echoed', 2, unpack('n', substr($unsuback, 2, 2))[1]);

    // A publish after unsubscribing must not come back.
    $pub = mqttString('uns/topic') . 'after';
    fwrite($fp, "\x30" . chr(strlen($pub)) . $pub);
    stream_set_timeout($fp, 1);
    $echo = fread($fp, 64);
    Harness::eq('nothing is delivered after unsubscribe', '', (string) $echo);

    // DISCONNECT closes the socket.
    fwrite($fp, "\xe0\x00");
    stream_set_timeout($fp, 2);
    $after = fread($fp, 16);
    Harness::ok('the socket is closed on disconnect', $after === '' || $after === false);
    fclose($fp);
});

Harness::guard('stomp unsubscribe, content-length and disconnect', static function () use ($stompPort): void {
    $fp = stream_socket_client("tcp://127.0.0.1:$stompPort", $errno, $errstr, 5.0);
    if ($fp === false) {
        throw new RuntimeException("stomp connect failed: $errstr");
    }
    stream_set_timeout($fp, 3);
    fwrite($fp, "CONNECT\naccept-version:1.2\n\n\0");
    $connected = (string) fread($fp, 64);
    Harness::ok('connected arrives', str_starts_with($connected, 'CONNECTED'));

    fwrite($fp, "SUBSCRIBE\nid:s1\ndestination:/queue/su\n\n\0");
    // content-length is honoured, so the trailing byte is not part of the body.
    $body = "five!";
    fwrite($fp, "SEND\ndestination:/queue/su\ncontent-length:5\n\n" . $body . "\0");
    $message = (string) fread($fp, 512);
    Harness::ok('the message is echoed', str_contains($message, 'MESSAGE'));
    Harness::ok('with the exact body', str_contains($message, "\n\nfive!"));

    fwrite($fp, "UNSUBSCRIBE\nid:s1\n\n\0");
    fwrite($fp, "SEND\ndestination:/queue/su\ncontent-length:3\n\nbye\0");
    stream_set_timeout($fp, 1);
    $after = (string) fread($fp, 256);
    Harness::ok('nothing is echoed after unsubscribe', !str_contains($after, 'MESSAGE'));

    fwrite($fp, "DISCONNECT\n\n\0");
    stream_set_timeout($fp, 2);
    $closed = fread($fp, 16);
    Harness::ok('the socket is closed on disconnect', $closed === '' || $closed === false);
    fclose($fp);
});

Harness::guard('amqp 1.0 shim', static function () use ($broker): void {
    $fp = stream_socket_client("tcp://127.0.0.1:{$broker['port']}", $errno, $errstr, 5.0);
    if ($fp === false) {
        throw new RuntimeException("amqp connect failed: $errstr");
    }
    stream_set_timeout($fp, 3);

    // The SASL header is answered with the matching header plus a
    // sasl-mechanisms frame, instead of the connection being dropped.
    fwrite($fp, "AMQP\x03\x01\x00\x00");
    $head = (string) fread($fp, 8);
    Harness::eq('the sasl header is echoed', "AMQP\x03\x01\x00\x00", $head);
    $frame = (string) fread($fp, 64);
    Harness::ok('mechanisms are offered', str_contains($frame, 'PLAIN'));

    // sasl-init draws a sasl-outcome.
    $init = "\x00\x53\x41\xc0\x04\x01\xa3\x00";
    fwrite($fp, pack('N', strlen($init) + 8) . chr(2) . chr(1) . "\x00\x00" . $init);
    $outcome = (string) fread($fp, 64);
    Harness::ok('a sasl outcome comes back', str_contains($outcome, "\x00\x53\x44"));

    // The plain header follows, then open and begin.
    fwrite($fp, "AMQP\x00\x01\x00\x00");
    $head = (string) fread($fp, 8);
    Harness::eq('the plain header is echoed', "AMQP\x00\x01\x00\x00", $head);

    $open = "\x00\x53\x10\x45";
    fwrite($fp, pack('N', strlen($open) + 8) . chr(2) . chr(0) . "\x00\x00" . $open);
    $reply = (string) fread($fp, 64);
    Harness::ok('open is answered', str_contains($reply, "\x00\x53\x10"));
    fclose($fp);
});

Harness::stop($broker);
Harness::done();
