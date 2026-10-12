<?php
declare(strict_types=1);

/**
 * Opens N AMQP connections and holds them until stdin closes.
 *
 * Each one sends the protocol header and waits for connection.start with a
 * blocking read: past 1,024 descriptors this process cannot use select either.
 * Prints "held <n>" once every connection got its start, or "failed <n> <why>".
 *
 * Usage: php hold.php <port> <count>
 */

[$_, $port, $count] = $argv + [null, '5672', '1'];
$held = [];
for ($i = 0; $i < (int) $count; $i++) {
    $fp = @stream_socket_client("tcp://127.0.0.1:$port", $errno, $errstr, 5.0);
    if ($fp === false) {
        fwrite(STDOUT, "failed $i connect: $errstr\n");
        exit(1);
    }
    stream_set_timeout($fp, 5);
    fwrite($fp, "AMQP\x00\x00\x09\x01");
    $head = readExactly($fp, 7);
    $size = $head === null ? 0 : unpack('N', substr($head, 3, 4))[1];
    $payload = $head === null ? null : readExactly($fp, $size + 1);
    if ($head === null || $payload === null || ord($head[0]) !== 1 || substr($payload, 0, 4) !== "\x00\x0a\x00\x0a") {
        fwrite(STDOUT, "failed $i no connection.start\n");
        exit(1);
    }
    $held[] = $fp;
}
fwrite(STDOUT, 'held ' . count($held) . "\n");
fread(STDIN, 1);

function readExactly($fp, int $n): ?string
{
    $buf = '';
    while (strlen($buf) < $n) {
        $chunk = fread($fp, $n - strlen($buf));
        if ($chunk === false || $chunk === '') {
            return null;
        }
        $buf .= $chunk;
    }
    return $buf;
}
