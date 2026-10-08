<?php
declare(strict_types=1);

/**
 * Two publishes in one socket write must both be confirmed and delivered.
 * The broker used to copy the unread tail on every frame, and a short copy
 * dropped the second message.
 */

require_once __DIR__ . '/lib/Harness.php';

$broker = Harness::broker([], 10);
$port = $broker['port'];

Harness::guard('two publishes in one write', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->confirmSelect();
    $c->declareQueue('burst');
    $c->sendRaw($c->publishBytes('', 'burst', 'aaa') . $c->publishBytes('', 'burst', 'bbb'));
    $c->expect(60, 80);
    $c->expect(60, 80);
    $first = $c->get('burst');
    $c->ack((int) $first['tag']);
    $second = $c->get('burst');
    Harness::eq('first body', 'aaa', $first['body']);
    Harness::eq('second body', 'bbb', $second['body']);
    $c->ack((int) $second['tag']);
    Harness::eq('queue empty', false, $c->get('burst')['delivered']);
    $c->close();
});

Harness::stop($broker);
Harness::done();
