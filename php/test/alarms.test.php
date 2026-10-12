<?php
declare(strict_types=1);

/**
 * Memory alarm, as RabbitMQ raises it: a 1,000 byte watermark is crossed at
 * once. A client that advertised `connection.blocked` is told so; a connection
 * that has published is no longer read; a consumer-only connection still is.
 */

require_once __DIR__ . '/lib/Harness.php';

$mgmt = Harness::freePort();
$broker = Harness::broker(["management = \"127.0.0.1:$mgmt\""], 10, '', [], ['QUEUEFORGE_MEM_HIGH_WATERMARK' => '1000']);
usleep(600000);

Harness::guard('blocked client is told why', static function () use ($broker): void {
    $c = new Amqp('127.0.0.1', $broker['port'], 'admin', 'devpassword12', true, 5.0, true);
    $frame = $c->read(2.0);
    Harness::eq('connection.blocked after open', [10, 60], $frame === null ? null : [$frame['class'], $frame['method']]);
    Harness::ok('reason names memory', str_contains((string) ($frame['args'] ?? ''), 'low on memory'));
    $c->close();
});

Harness::guard('a publisher is no longer read', static function () use ($broker): void {
    $setup = new Amqp('127.0.0.1', $broker['port']);
    $setup->channel();
    $setup->declareQueue('alarm-q');
    $p = new Amqp('127.0.0.1', $broker['port']);
    $p->channel();
    $p->confirmSelect();
    // The frame that marks the connection as a publisher is still handled.
    $p->publish('', 'alarm-q', 'first', ['deliveryMode' => 2]);
    $p->expect(60, 80, 3.0);
    usleep(400000);
    $p->publish('', 'alarm-q', 'second', ['deliveryMode' => 2]);
    $late = $p->read(1.5);
    Harness::eq('second publish waits for the alarm to clear', null, $late === null ? null : [$late['class'], $late['method']]);
    // A connection that never published still consumes.
    $setup->consume('alarm-q', '');
    $got = null;
    $deadline = microtime(true) + 3.0;
    while ($got === null && microtime(true) < $deadline) {
        $frame = $setup->read(max(0.05, $deadline - microtime(true)));
        if ($frame !== null && $frame['class'] === 60 && $frame['method'] === 60) $got = (string) $frame['body'];
    }
    Harness::eq('consumer receives the first message', 'first', $got);
    $setup->close();
});

Harness::guard('management reports the alarm', static function () use ($mgmt): void {
    $context = stream_context_create(['http' => ['header' => 'Authorization: Basic ' . base64_encode('admin:devpassword12'), 'timeout' => 3]]);
    $nodes = json_decode((string) @file_get_contents("http://127.0.0.1:$mgmt/api/nodes", false, $context), true);
    $node = is_array($nodes) ? ($nodes[0] ?? []) : [];
    Harness::eq('mem_alarm', true, $node['mem_alarm'] ?? null);
    Harness::eq('mem_limit', 1000, $node['mem_limit'] ?? null);
});

Harness::guard('a client without the capability is not sent the method', static function () use ($broker): void {
    $c = new Amqp('127.0.0.1', $broker['port']);
    $frame = $c->read(1.0);
    Harness::eq('no connection.blocked', null, $frame === null ? null : [$frame['class'], $frame['method']]);
    $c->close();
});

Harness::stop($broker, true);
Harness::done();
