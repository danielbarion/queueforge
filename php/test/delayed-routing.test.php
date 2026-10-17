<?php
declare(strict_types=1);

/**
 * x-delayed-message routes a delayed message when it is due, as RabbitMQ's
 * plugin does: a queue bound during the delay receives it, a queue unbound
 * during the delay does not. The pending message survives a restart.
 */

require_once __DIR__ . '/lib/Harness.php';

$broker = Harness::broker();
$port = $broker['port'];
$client = static function () use ($port): Amqp { $c = new Amqp('127.0.0.1', $port); $c->channel(); return $c; };

Harness::guard('bindings changed during the delay apply when it is due', static function () use ($client): void {
    $c = $client();
    $c->declareExchange('dr-x', 'x-delayed-message', 1, ['x-delayed-type' => 'direct']);
    $c->declareQueue('dr-old'); $c->declareQueue('dr-new');
    $c->bind('dr-old', 'dr-x', 'k');
    $c->confirmSelect();
    $c->publish('dr-x', 'k', 'late-routed', ['deliveryMode' => 2, 'headers' => ['x-delay' => 800]]);
    $c->expect(60, 80);
    // Rebind while the message waits.
    $c->bind('dr-new', 'dr-x', 'k');
    $c->unbind('dr-old', 'dr-x', 'k');
    Harness::eq('nothing delivered before the delay', false, $c->get('dr-new', true)['delivered']);
    $got = null;
    Harness::until(static function () use ($c, &$got): bool { $m = $c->get('dr-new', true); if ($m['delivered']) $got = $m; return $got !== null; }, 4.0);
    Harness::eq('a queue bound during the delay receives the message', 'late-routed', $got['body'] ?? null);
    Harness::eq('x-delay is kept on the delivered message', 800, Amqp::headersOf($got['propRaw'] ?? null)['x-delay'] ?? null);
    usleep(200000);
    Harness::eq('a queue unbound during the delay does not', false, $c->get('dr-old', true)['delivered']);
    // A zero delay routes at once.
    $c->publish('dr-x', 'k', 'no-delay', ['headers' => ['x-delay' => 0]]);
    $c->expect(60, 80);
    Harness::eq('x-delay 0 routes immediately', 'no-delay', $c->get('dr-new', true)['body']);
    $c->publish('dr-x', 'k', 'across-restart', ['deliveryMode' => 1, 'headers' => ['x-delay' => 2000]]);
    $c->expect(60, 80);
    $c->close();
});

usleep(200000);
$broker = Harness::restart($broker);
$port = $broker['port'];

Harness::guard('a pending delayed message is routed after a restart', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port); $c->channel();
    Harness::eq('still held right after the restart', false, $c->get('dr-new', true)['delivered']);
    // Bound only after the restart: routing happens at the due time.
    $c->declareQueue('dr-after');
    $c->bind('dr-after', 'dr-x', 'k');
    $got = [];
    Harness::until(static function () use ($c, &$got): bool {
        foreach (['dr-new', 'dr-after'] as $q) { $m = $c->get($q, true); if ($m['delivered']) $got[$q] = $m['body']; }
        return count($got) === 2;
    }, 5.0);
    ksort($got);
    Harness::eq('delivered to the queues bound at its due time', ['dr-after' => 'across-restart', 'dr-new' => 'across-restart'], $got);
    $c->close();
});

Harness::stop($broker);
Harness::done();
