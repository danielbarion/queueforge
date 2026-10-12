<?php
declare(strict_types=1);

/**
 * With one child per core, the queues of one fanout exchange are homed on
 * different children. A publish reaches each of them once, in order, as in
 * bun/test/fanout-cores.test.ts. Consumers connect to whichever child the
 * parent picks, so some subscribe across the cluster links.
 */

require_once __DIR__ . '/lib/Harness.php';

$broker = Harness::broker([], 10, '', [], ['QUEUEFORGE_CORES' => '3']);
$log = '';
$deadline = microtime(true) + 12.0;
while (microtime(true) < $deadline && !str_contains($log, 'parent cores=3')) {
    usleep(50000);
    $log = (string) @file_get_contents($broker['dir'] . '/out.log');
}
Harness::ok('three children', str_contains($log, 'parent cores=3'), trim($log . "\n" . Harness::stderr($broker)));

Harness::guard('a fanout across children delivers every copy in order', static function () use ($broker): void {
    $queues = 9;
    $n = 200;
    $setup = new Amqp('127.0.0.1', $broker['port']);
    $setup->channel();
    $setup->declareExchange('fan', 'fanout');
    for ($i = 0; $i < $queues; $i++) {
        $setup->declareQueue("fan-$i");
        $setup->bind("fan-$i", 'fan');
    }
    $setup->close();
    $consumers = [];
    for ($i = 0; $i < $queues; $i++) {
        $c = new Amqp('127.0.0.1', $broker['port']);
        $c->channel();
        $c->consume("fan-$i", '', true);
        $consumers[$i] = $c;
    }
    $p = new Amqp('127.0.0.1', $broker['port']);
    $p->channel();
    $p->confirmSelect();
    for ($m = 0; $m < $n; $m++) $p->publish('fan', '', (string) $m, ['deliveryMode' => 2]);
    $confirmed = 0;
    $until = microtime(true) + 10.0;
    while ($confirmed < $n && microtime(true) < $until) {
        $frame = $p->read(max(0.05, $until - microtime(true)));
        if ($frame === null || $frame['class'] !== 60 || $frame['method'] !== 80) continue;
        $tag = Codec::readU64($frame['args'], 0);
        $multiple = (ord($frame['args'][8] ?? "\0") & 1) === 1;
        $confirmed = $multiple ? max($confirmed, $tag) : $confirmed + 1;
    }
    Harness::eq('every publish is confirmed', $n, $confirmed);
    $inOrder = 0;
    foreach ($consumers as $i => $c) {
        $got = [];
        $until = microtime(true) + 10.0;
        while (count($got) < $n && microtime(true) < $until) {
            $frame = $c->read(max(0.05, $until - microtime(true)));
            if ($frame !== null && $frame['class'] === 60 && $frame['method'] === 60) $got[] = (int) $frame['body'];
        }
        if ($got === range(0, $n - 1)) $inOrder++;
        else fwrite(STDERR, "fan-$i got " . count($got) . " messages\n");
        $c->close();
    }
    Harness::eq('each queue has every message in order', $queues, $inOrder);
    $p->close();
});

Harness::stop($broker, true);
Harness::done();
