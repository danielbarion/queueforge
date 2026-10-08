<?php
declare(strict_types=1);

/**
 * Covers the AMQP methods added for Bun parity: basic.cancel, basic.get,
 * basic.reject, basic.recover, queue.purge, queue.delete, queue.unbind,
 * exchange.delete, exchange.bind, exchange.unbind, tx.*, channel.flow, and
 * the NOT_IMPLEMENTED channel error for anything unsupported.
 */

require_once __DIR__ . '/lib/Harness.php';

$broker = Harness::broker();
$port = $broker['port'];

// basic.get on an empty queue, then on a queue holding one message.
Harness::guard('basic.get', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->confirmSelect();
    $c->declareQueue('g1');
    Harness::eq('basic.get empty returns get-empty', false, $c->get('g1')['delivered']);
    $c->publish('', 'g1', 'one');
    $c->expect(60, 80);
    $got = $c->get('g1');
    Harness::eq('basic.get returns the body', 'one', $got['body']);
    $c->ack(1);
    Harness::eq('basic.get drains the queue', false, $c->get('g1')['delivered']);
    $c->close();
});

// basic.cancel must stop deliveries to the cancelled tag.
Harness::guard('basic.cancel', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->confirmSelect();
    $c->declareQueue('c1');
    $tag = $c->consume('c1');
    Harness::eq('basic.cancel echoes the tag', $tag, $c->cancel($tag));
    $c->publish('', 'c1', 'after-cancel');
    $c->expect(60, 80);
    $frame = $c->read(1.0);
    $delivered = $frame !== null && $frame['class'] === 60 && $frame['method'] === 60;
    Harness::ok('no delivery after basic.cancel', !$delivered);
    // The message is still queued, so basic.get can take it.
    Harness::eq('cancelled consumer leaves the message queued', 'after-cancel', $c->get('c1')['body']);
    $c->close();
});

// queue.purge drops the ready messages and reports the count.
Harness::guard('queue.purge', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->confirmSelect();
    $c->declareQueue('p1');
    for ($i = 0; $i < 5; $i++) {
        $c->publish('', 'p1', "m$i");
    }
    for ($i = 0; $i < 5; $i++) {
        $c->expect(60, 80);
    }
    Harness::eq('queue.purge reports the count', 5, $c->purge('p1'));
    Harness::eq('queue.purge empties the queue', false, $c->get('p1')['delivered']);
    $c->close();
});

// basic.reject with requeue puts the message back; without it, the message goes.
Harness::guard('basic.reject', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->confirmSelect();
    $c->declareQueue('r1');
    $c->publish('', 'r1', 'rejected');
    $c->expect(60, 80);
    $first = $c->get('r1');
    Harness::eq('reject: message arrives', 'rejected', $first['body']);
    $c->reject(1, true);
    Harness::eq('reject requeue=true returns it', 'rejected', $c->get('r1')['body']);
    $c->reject(2, false);
    Harness::eq('reject requeue=false drops it', false, $c->get('r1')['delivered']);
    $c->close();
});

// basic.recover requeues everything unacked on the channel.
Harness::guard('basic.recover', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->confirmSelect();
    $c->declareQueue('rc');
    $c->publish('', 'rc', 'recovered');
    $c->expect(60, 80);
    Harness::eq('recover: message arrives', 'recovered', $c->get('rc')['body']);
    $c->recover(true);
    $c->expect(60, 111);
    Harness::eq('basic.recover requeues the unacked', 'recovered', $c->get('rc')['body']);
    $c->close();
});

// basic.recover with requeue=false is refused rather than silently dropping.
Harness::guard('basic.recover requeue=false', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->declareQueue('rc2');
    $c->recover(false);
    $close = $c->expectClose();
    Harness::eq('recover requeue=false is a 540', 540, $close['code'] ?? 0);
    $c->close();
});

// Exchange-to-exchange binding routes through the link.
Harness::guard('exchange.bind', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->confirmSelect();
    $c->declareExchange('ex-src', 'fanout');
    $c->declareExchange('ex-dst', 'fanout');
    $c->bindExchange('ex-dst', 'ex-src');
    $c->declareQueue('e2e');
    $c->bind('e2e', 'ex-dst');
    $c->publish('ex-src', '', 'via-e2e');
    $c->expect(60, 80);
    Harness::eq('exchange.bind routes through the link', 'via-e2e', $c->get('e2e')['body']);
    $c->ack(1);
    $c->unbindExchange('ex-dst', 'ex-src');
    $c->publish('ex-src', '', 'after-unbind');
    // With no destination left the publish is unroutable, so it nacks.
    $frame = $c->read(2.0);
    $unroutable = $frame !== null && $frame['class'] === 60 && in_array($frame['method'], [80, 120], true);
    Harness::ok('exchange.unbind removes the link', $unroutable);
    Harness::eq('nothing arrives after exchange.unbind', false, $c->get('e2e')['delivered']);
    $c->close();
});

// queue.unbind, queue.delete and exchange.delete.
Harness::guard('topology deletes', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->confirmSelect();
    $c->declareExchange('del-ex', 'fanout');
    $c->declareQueue('del-q');
    $c->bind('del-q', 'del-ex');
    $c->publish('del-ex', '', 'bound');
    $c->expect(60, 80);
    Harness::eq('bound publish arrives', 'bound', $c->get('del-q')['body']);
    $c->ack(1);
    $c->unbind('del-q', 'del-ex');
    $c->publish('del-ex', '', 'unbound');
    $c->read(1.0);
    Harness::eq('queue.unbind stops routing', false, $c->get('del-q')['delivered']);
    $c->publish('', 'del-q', 'still-here');
    $c->expect(60, 80);
    Harness::eq('queue.delete reports the message count', 1, $c->deleteQueue('del-q'));
    $c->deleteExchange('del-ex');
    Harness::ok('exchange.delete completes', true);
    $c->close();
});

// channel.flow is answered, and tx behaves as documented.
Harness::guard('channel.flow and tx', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    Harness::eq('channel.flow echoes active', true, $c->flow(true));
    $c->tx(10);
    $c->expect(90, 11);
    Harness::ok('tx.select is accepted', true);
    $c->tx(20);
    $c->expect(90, 21);
    Harness::ok('tx.commit is accepted', true);
    $c->close();

    // Rollback inside a transaction is accepted; outside one it is a 406,
    // as on RabbitMQ.
    $r = new Amqp('127.0.0.1', $port);
    $r->channel();
    $r->tx(10);
    $r->expect(90, 11);
    $r->tx(30);
    $r->expect(90, 31);
    Harness::ok('tx.rollback in a transaction is accepted', true);
    $r->close();
    $d = new Amqp('127.0.0.1', $port);
    $d->channel();
    $d->tx(30);
    $close = $d->expectClose();
    Harness::eq('tx.rollback outside a transaction is a 406', 406, $close['code'] ?? 0);
    $d->close();
});

// An unsupported method gets a channel error instead of silence.
Harness::guard('unsupported method', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    // access.request (30.10) is not implemented by this broker.
    $c->method(1, 30, 10, '');
    $close = $c->expectClose();
    Harness::eq('unknown method is a 540', 540, $close['code'] ?? 0);
    Harness::ok(
        'the 540 names the method',
        str_contains($close['text'] ?? '', '30.10'),
        'text was: ' . ($close['text'] ?? 'none'),
    );
    $c->close();
});

// basic.publish with immediate=true is refused, as it is in Bun.
Harness::guard('immediate publish', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->declareQueue('imm');
    $c->publish('', 'imm', 'now', [], false, true);
    $close = $c->expectClose();
    Harness::eq('immediate=true is a 540', 540, $close['code'] ?? 0);
    $c->close();
});

Harness::stop($broker);
Harness::done();
