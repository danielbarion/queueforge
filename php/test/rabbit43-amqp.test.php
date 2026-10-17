<?php
declare(strict_types=1);

/**
 * RabbitMQ 4.3 AMQP 0-9-1 behaviour: consumer acknowledgement timeouts on
 * quorum queues, stream filtering, consistent-hash hash-header and
 * hash-property, and connection.update-secret.
 */

require_once __DIR__ . '/lib/Harness.php';

$mgmt = Harness::freePort();
$broker = Harness::broker(["management = \"127.0.0.1:$mgmt\""]);
Harness::waitListening($mgmt, $broker['dir']);
$port = $broker['port'];

/** Waits for a frame of class.method, returning it or null. */
function waitFor(Amqp $c, int $class, int $method, float $seconds): ?array
{
    $deadline = microtime(true) + $seconds;
    while (microtime(true) < $deadline) {
        $frame = $c->read(max(0.05, $deadline - microtime(true)));
        if ($frame === null) return null;
        if ($frame['class'] === $class && $frame['method'] === $method) return $frame;
    }
    return null;
}

/** Collects message bodies delivered on the connection for `$seconds`, acking each. */
function deliveries(Amqp $c, float $seconds, bool $ack = true): array
{
    $got = [];
    $deadline = microtime(true) + $seconds;
    while (microtime(true) < $deadline) {
        $frame = $c->read(max(0.05, $deadline - microtime(true)));
        if ($frame === null) break;
        if ($frame['class'] === 60 && $frame['method'] === 60) {
            $got[] = $frame['body'];
            $o = 0; Codec::readShortstr($frame['args'], $o);
            if ($ack) $c->ack(Codec::readU64($frame['args'], $o));
        }
    }
    return $got;
}

Harness::guard('consumer timeout with consumer_cancel_notify', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port, capabilities: ['consumer_cancel_notify']);
    $c->channel();
    $c->declareQueue('ct-qq', true, ['x-queue-type' => 'quorum', 'x-consumer-timeout' => 1000]);
    $c->publish('', 'ct-qq', 'held', ['deliveryMode' => 2]);
    $c->consume('ct-qq', 'ct-tag');
    Harness::ok('the delivery arrives', waitFor($c, 60, 60, 3.0) !== null);
    $cancel = waitFor($c, 60, 30, 5.0);
    Harness::ok('a delivery held past x-consumer-timeout gets basic.cancel', $cancel !== null);
    $o = 0;
    Harness::eq('the cancel names the consumer', 'ct-tag', $cancel === null ? null : Codec::readShortstr($cancel['args'], $o));
    // The channel survives and the message is back, redelivered.
    $back = $c->get('ct-qq', true);
    Harness::eq('the message is requeued', 'held', $back['body']);
    $c->deleteQueue('ct-qq');
    $c->close();
});

Harness::guard('consumer timeout from basic.consume and acked in time', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port, capabilities: ['consumer_cancel_notify']);
    $c->channel();
    $c->declareQueue('ct-arg', true, ['x-queue-type' => 'quorum']);
    $c->publish('', 'ct-arg', 'one', ['deliveryMode' => 2]);
    $c->consume('ct-arg', 'arg-tag', extra: ['x-consumer-timeout' => 1000]);
    Harness::ok('the consume argument times out the consumer', waitFor($c, 60, 30, 5.0) !== null);
    $c->deleteQueue('ct-arg');

    $c->declareQueue('ct-ok', true, ['x-queue-type' => 'quorum', 'x-consumer-timeout' => 1000]);
    $c->consume('ct-ok', 'ok-tag');
    $c->publish('', 'ct-ok', 'quick', ['deliveryMode' => 2]);
    Harness::eq('first delivery acked', ['quick'], deliveries($c, 1.0));
    usleep(1500000);
    $c->publish('', 'ct-ok', 'again', ['deliveryMode' => 2]);
    $later = deliveries($c, 2.0);
    Harness::eq('an acked consumer keeps receiving', ['again'], $later);
    $c->deleteQueue('ct-ok');
    $c->close();
});

Harness::guard('consumer timeout without the capability closes the channel', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->declareQueue('ct-close', true, ['x-queue-type' => 'quorum', 'x-consumer-timeout' => 1000]);
    $c->publish('', 'ct-close', 'held', ['deliveryMode' => 2]);
    $c->consume('ct-close');
    $close = $c->expectClose(6.0);
    Harness::eq('channel closed with 406', 406, $close['code'] ?? null);
    $c->channel(2);
    Harness::eq('message requeued after the channel close', 'held', $c->get('ct-close', true, 2)['body']);
    $c->deleteQueue('ct-close', 2);
    $c->close();
});

Harness::guard('classic and stream queues refuse x-consumer-timeout', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    try { $c->declareQueue('ct-classic', true, ['x-consumer-timeout' => 1000]); $closed = null; }
    catch (RuntimeException $e) { $closed = $e->getMessage(); }
    Harness::ok('classic declare refused with 406', $closed !== null && str_contains($closed, 'PRECONDITION_FAILED') && str_contains($closed, 'rabbit_classic_queue'), (string) $closed);
    $c->channel(2);
    $c->declareQueue('ct-classic2', true, [], 2);
    try { $c->consume('ct-classic2', '', false, 2, extra: ['x-consumer-timeout' => 1000]); $closed = null; }
    catch (RuntimeException $e) { $closed = $e->getMessage(); }
    Harness::ok('classic consume argument refused with 406', $closed !== null && str_contains($closed, 'PRECONDITION_FAILED'), (string) $closed);
    $c->channel(3);
    try { $c->declareQueue('ct-stream', true, ['x-queue-type' => 'stream', 'x-consumer-timeout' => 1000], 3); $closed = null; }
    catch (RuntimeException $e) { $closed = $e->getMessage(); }
    Harness::ok('stream declare refused with 406', $closed !== null && str_contains($closed, 'rabbit_stream_queue'), (string) $closed);
    $c->channel(4);
    $c->deleteQueue('ct-classic2', 4);
    $c->close();
});

Harness::guard('consumer-timeout policy key', static function () use ($port, $mgmt): void {
    $put = Harness::http($mgmt, 'PUT', '/api/policies/%2f/ct-policy', ['pattern' => '^ct-pol', 'apply-to' => 'queues', 'definition' => ['consumer-timeout' => 1000]]);
    Harness::ok('consumer-timeout is a valid policy key', $put['status'] >= 200 && $put['status'] < 300, $put['body']);
    $bad = Harness::http($mgmt, 'PUT', '/api/policies/%2f/ct-bad', ['pattern' => '^x', 'definition' => ['consumer-timeout' => 'soon']]);
    Harness::eq('a non-integer consumer-timeout is refused', 400, $bad['status']);
    $c = new Amqp('127.0.0.1', $port, capabilities: ['consumer_cancel_notify']);
    $c->channel();
    // The queue argument (60 s) and the policy (1 s) combine as the lower.
    $c->declareQueue('ct-pol-q', true, ['x-queue-type' => 'quorum', 'x-consumer-timeout' => 60000]);
    $c->publish('', 'ct-pol-q', 'held', ['deliveryMode' => 2]);
    $c->consume('ct-pol-q');
    Harness::ok('the policy timeout applies', waitFor($c, 60, 30, 5.0) !== null);
    $c->deleteQueue('ct-pol-q');
    $c->close();
    Harness::http($mgmt, 'DELETE', '/api/policies/%2f/ct-policy');
});

Harness::guard('stream filtering', static function () use ($port): void {
    $p = new Amqp('127.0.0.1', $port);
    $p->channel();
    $p->confirmSelect();
    $p->declareQueue('sf', true, ['x-queue-type' => 'stream']);
    foreach ([['red', 3], [null, 2], ['blue', 3], ['red', 1]] as [$value, $n]) {
        for ($i = 0; $i < $n; $i++) $p->publish('', 'sf', ($value ?? 'none') . "-$i", $value === null ? [] : ['headers' => ['x-stream-filter-value' => $value]]);
    }
    usleep(300000);
    $read = static function (array $args) use ($port): array {
        $c = new Amqp('127.0.0.1', $port);
        $c->channel();
        $c->qos(100);
        $c->consume('sf', '', false, 1, extra: ['x-stream-offset' => 'first'] + $args);
        $got = deliveries($c, 1.0);
        $c->close();
        return $got;
    };
    $red = $read(['x-stream-filter' => 'red']);
    Harness::eq('a single filter value', ['red-0', 'red-1', 'red-2', 'red-0'], $red);
    Harness::eq('a list of filter values', 7, count($read(['x-stream-filter' => ['red', 'blue']])));
    $loose = $read(['x-stream-filter' => 'blue', 'x-stream-match-unfiltered' => true]);
    Harness::eq('match-unfiltered adds messages without a value', ['none-0', 'none-1', 'blue-0', 'blue-1', 'blue-2'], $loose);
    Harness::eq('no filter reads everything', 9, count($read([])));
    $p->deleteQueue('sf');
    $p->close();
});

Harness::guard('consistent-hash hash-header and hash-property', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    foreach (['header' => ['hash-header' => 'tenant'], 'property' => ['hash-property' => 'message_id']] as $kind => $args) {
        $x = "chx-$kind";
        $c->declareExchange($x, 'x-consistent-hash', 1, $args);
        foreach (['a', 'b'] as $q) { $c->declareQueue("$x-$q"); $c->purge("$x-$q"); $c->bind("$x-$q", $x, '1'); }
        for ($i = 0; $i < 100; $i++) {
            $c->publish($x, 'same-key', "m$i", $kind === 'header' ? ['headers' => ['tenant' => "t$i"]] : ['messageId' => "id-$i"]);
        }
        for ($i = 0; $i < 5; $i++) $c->publish($x, "key-$i", 'fixed', $kind === 'header' ? ['headers' => ['tenant' => 'fixed']] : ['messageId' => 'fixed']);
        // Missing header or property: unroutable, so dropped.
        $c->publish($x, 'same-key', 'missing', []);
        usleep(400000);
        $a = $c->declareQueue("$x-a", true, [], 1, true)['messages'];
        $b = $c->declareQueue("$x-b", true, [], 1, true)['messages'];
        Harness::eq("hash-$kind: every routable message lands once", 105, $a + $b);
        Harness::ok("hash-$kind: values spread over both queues", $a > 20 && $b > 20, "a=$a b=$b");
        $fixedA = 0;
        while (($m = $c->get("$x-a", true))['delivered']) if ($m['body'] === 'fixed') $fixedA++;
        Harness::ok("hash-$kind: one value always picks one queue", in_array($fixedA, [0, 5], true), "fixed on a: $fixedA");
    }
    $c->channel(2);
    try { $c->declareExchange('chx-both', 'x-consistent-hash', 2, ['hash-header' => 'h', 'hash-property' => 'message_id']); $refused = null; }
    catch (RuntimeException $e) { $refused = $e->getMessage(); }
    Harness::ok('both hash-header and hash-property: 406', $refused !== null && str_contains($refused, 'PRECONDITION_FAILED'), (string) $refused);
    $c->close();
});

Harness::guard('connection.update-secret', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->method(0, 10, 70, Codec::longstr('devpassword12') . Codec::shortstr('refresh'));
    Harness::ok('a valid secret is answered with update-secret-ok', waitFor($c, 10, 71, 3.0) !== null);
    $c->channel();
    $c->declareQueue('us-after');
    $c->deleteQueue('us-after');
    $c->method(0, 10, 70, Codec::longstr('not-the-password') . Codec::shortstr('refresh'));
    $close = $c->expectClose(3.0);
    Harness::eq('an invalid secret closes the connection with 530', [530, 10], [$close['code'] ?? null, $close['class'] ?? null]);
});

Harness::stop($broker);
Harness::done();
