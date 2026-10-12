<?php
declare(strict_types=1);

/**
 * One process holds more connections than select() can watch (FD_SETSIZE,
 * 1,024): the server polls through the ev extension instead. A connection
 * opened after 1,100 others still completes a confirmed round trip.
 */

require_once __DIR__ . '/lib/Harness.php';

const HOLD = 1100;

if (!extension_loaded('ev')) {
    fwrite(STDOUT, "  skip connections past FD_SETSIZE: the ev extension is not loaded\n");
    Harness::done();
}
$limit = function_exists('posix_getrlimit') ? (int) (posix_getrlimit()['soft openfiles'] ?? 0) : 0;
if ($limit < 2 * HOLD + 200 && function_exists('posix_setrlimit')) {
    $hard = posix_getrlimit()['hard openfiles'] ?? 'unlimited';
    $want = 2 * HOLD + 400;
    if ($hard === 'unlimited' || (int) $hard >= $want) {
        @posix_setrlimit(POSIX_RLIMIT_NOFILE, $want, $hard === 'unlimited' ? POSIX_RLIMIT_INFINITY : (int) $hard);
        $limit = (int) (posix_getrlimit()['soft openfiles'] ?? 0);
    }
}
if ($limit < 2 * HOLD + 200) {
    fwrite(STDOUT, "  skip connections past FD_SETSIZE: open file limit $limit\n");
    Harness::done();
}

$broker = Harness::broker();
$pipes = [];
$holder = proc_open([PHP_BINARY, __DIR__ . '/lib/hold.php', (string) $broker['port'], (string) HOLD], [0 => ['pipe', 'r'], 1 => ['pipe', 'w']], $pipes);
$line = '';
$deadline = microtime(true) + 60.0;
stream_set_blocking($pipes[1], false);
while (!str_contains($line, "\n") && microtime(true) < $deadline) {
    $line .= (string) fread($pipes[1], 256);
    usleep(20000);
}
Harness::eq('holds ' . HOLD . ' connections', 'held ' . HOLD, trim($line) . (trim($line) === '' ? Harness::stderr($broker) : ''));

Harness::guard('round trip past FD_SETSIZE', static function () use ($broker): void {
    $c = new Amqp('127.0.0.1', $broker['port']);
    $c->channel();
    $c->declareQueue('fd-high');
    $c->confirmSelect();
    $c->publish('', 'fd-high', 'past-1024', ['deliveryMode' => 2]);
    $c->expect(60, 80, 5.0);
    $c->consume('fd-high', '');
    $got = null;
    $deadline = microtime(true) + 5.0;
    while ($got === null && microtime(true) < $deadline) {
        $frame = $c->read(max(0.05, $deadline - microtime(true)));
        if ($frame !== null && $frame['class'] === 60 && $frame['method'] === 60) $got = (string) $frame['body'];
    }
    Harness::eq('confirmed publish is delivered', 'past-1024', $got);
    $c->close();
});

fclose($pipes[0]);
fclose($pipes[1]);
proc_close($holder);
Harness::stop($broker, true);
Harness::done();
