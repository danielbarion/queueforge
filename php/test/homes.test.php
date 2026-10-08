<?php
declare(strict_types=1);

/**
 * One classic queue lives on one child. The publisher and the consumer still
 * exchange every message when the parent handed them to different children.
 */

require_once __DIR__ . '/lib/Harness.php';

$broker = Harness::broker([], 10, '', [], ['QUEUEFORGE_CORES' => '2']);
$log = '';
$deadline = microtime(true) + 12.0;
while (microtime(true) < $deadline) {
    $log = (string) @file_get_contents($broker['dir'] . '/out.log');
    if (str_contains($log, 'parent cores=2')) {
        break;
    }
    usleep(50000);
}
Harness::ok('two children', str_contains($log, 'parent cores=2'), trim($log . "\n" . Harness::stderr($broker)));

/**
 * @param list<string> $bodies
 * @return list<string>
 */
function exchanged(int $port, string $queue, array $bodies, bool $consumerFirst): array
{
    $open = static function () use ($port): Amqp {
        $c = new Amqp('127.0.0.1', $port);
        $c->channel();
        return $c;
    };
    $consume = static function (Amqp $c, string $queue): void {
        $c->qos(10);
        $c->consume($queue, '');
    };
    $publish = static function (Amqp $p, string $queue, array $bodies): void {
        $p->confirmSelect();
        foreach ($bodies as $body) {
            $p->publish('', $queue, $body, ['deliveryMode' => 2]);
            $p->expect(60, 80, 3.0);
        }
    };
    if ($consumerFirst) {
        $c = $open();
        $c->declareQueue($queue);
        $consume($c, $queue);
        $p = $open();
        $publish($p, $queue, $bodies);
    } else {
        $p = $open();
        $p->declareQueue($queue);
        $publish($p, $queue, $bodies);
        $c = $open();
        $consume($c, $queue);
    }
    $got = [];
    $deadline = microtime(true) + 3.0;
    while (count($got) < count($bodies) && microtime(true) < $deadline) {
        $frame = $c->read(max(0.05, $deadline - microtime(true)));
        if ($frame !== null && $frame['class'] === 60 && $frame['method'] === 60) {
            $got[] = (string) $frame['body'];
        }
    }
    $c->close();
    $p->close();
    return $got;
}

if (str_contains($log, 'parent cores=2')) {
    Harness::guard('consumer first', static function () use ($broker): void {
        $got = exchanged($broker['port'], 'home-c', ['aaa', 'bbb'], true);
        Harness::eq('consumer first receives both', ['aaa', 'bbb'], $got);
    });
    Harness::guard('publisher first', static function () use ($broker): void {
        $got = exchanged($broker['port'], 'home-p', ['ccc', 'ddd'], false);
        Harness::eq('publisher first receives both', ['ccc', 'ddd'], $got);
    });
}

Harness::stop($broker, true);
Harness::done();
