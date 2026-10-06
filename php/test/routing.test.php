<?php
declare(strict_types=1);

/**
 * Routing and queue argument behaviour: topic and headers matching, TTL,
 * dead-lettering, max-length overflow modes, and priority ordering.
 */

require_once __DIR__ . '/lib/Harness.php';
$root = dirname(__DIR__);
require_once $root . '/src/Routing.php';
require_once $root . '/src/Features.php';
require_once $root . '/src/Policy.php';
require_once $root . '/src/Codec.php';
require_once $root . '/src/Auth.php';
require_once $root . '/src/Store.php';
require_once $root . '/src/Cluster.php';
require_once $root . '/src/Broker.php';

// Topic pattern matching, including the awkward # cases.
Harness::guard('topic matching', static function (): void {
    $cases = [
        ['a.b.c', 'a.b.c', true],
        ['a.*.c', 'a.b.c', true],
        ['a.*.c', 'a.b.d', false],
        ['a.*', 'a.b.c', false],
        ['a.#', 'a.b.c', true],
        ['a.#', 'a', true],
        ['#', 'anything.at.all', true],
        ['#', '', true],
        ['#.c', 'a.b.c', true],
        ['a.#.c', 'a.b.x.c', true],
        ['a.#.c', 'a.c', true],
        ['a.#.c', 'a.b.c.d', false],
        ['*.*', 'a.b', true],
        ['*.*', 'a', false],
        ['a.b', 'a.b.c', false],
    ];
    foreach ($cases as [$pattern, $key, $want]) {
        Harness::eq("topic \"$pattern\" against \"$key\"", $want, Routing::topic($pattern, $key));
    }
});

// Headers matching with x-match all and any.
Harness::guard('headers matching', static function (): void {
    $all = [['x-match', 'all'], ['type', 'report'], ['format', 'pdf']];
    $any = [['x-match', 'any'], ['type', 'report'], ['format', 'pdf']];

    Harness::eq('all: both present matches', true, Features::headersMatch($all, [['type', 'report'], ['format', 'pdf']]));
    Harness::eq('all: one missing fails', false, Features::headersMatch($all, [['type', 'report']]));
    Harness::eq('all: a wrong value fails', false, Features::headersMatch($all, [['type', 'report'], ['format', 'csv']]));
    Harness::eq('any: one present matches', true, Features::headersMatch($any, [['type', 'report']]));
    Harness::eq('any: none present fails', false, Features::headersMatch($any, [['other', 'x']]));
    // No checks beyond x-match: all matches everything, any matches nothing.
    Harness::eq('all with no checks matches', true, Features::headersMatch([['x-match', 'all']], []));
    Harness::eq('any with no checks does not', false, Features::headersMatch([['x-match', 'any']], []));
    // A missing x-match behaves as all.
    Harness::eq('a missing x-match behaves as all', true, Features::headersMatch([['type', 'report']], [['type', 'report']]));
});

// Queue argument parsing.
Harness::guard('queue arguments', static function (): void {
    $args = Features::parseArgs([
        'x-message-ttl' => 5000,
        'x-max-length' => 10,
        'x-max-length-bytes' => 2048,
        'x-overflow' => 'reject-publish',
        'x-dead-letter-exchange' => 'dlx',
        'x-dead-letter-routing-key' => 'dead',
        'x-max-priority' => 5,
        'x-queue-type' => 'quorum',
    ]);
    Harness::eq('ttl is read', 5000, $args['messageTtl']);
    Harness::eq('max length is read', 10, $args['maxLength']);
    Harness::eq('max length bytes is read', 2048, $args['maxLengthBytes']);
    Harness::eq('overflow is read', 'reject-publish', $args['overflow']);
    Harness::eq('dlx is read', 'dlx', $args['dlx']);
    Harness::eq('dlx key is read', 'dead', $args['dlxKey']);
    Harness::eq('max priority is read', 5, $args['maxPriority']);
    Harness::eq('queue type is read', 'quorum', $args['queueType']);

    $defaults = Features::parseArgs([]);
    Harness::eq('overflow defaults to drop-head', 'drop-head', $defaults['overflow']);
    Harness::eq('type defaults to classic', 'classic', $defaults['queueType']);
    Harness::eq('ttl defaults to none', null, $defaults['messageTtl']);
    // An unknown overflow falls back rather than being taken literally.
    Harness::eq('an unknown overflow falls back', 'drop-head', Features::parseArgs(['x-overflow' => 'nonsense'])['overflow']);
    // A zero or negative max priority is treated as unset.
    Harness::eq('a zero max priority is unset', null, Features::parseArgs(['x-max-priority' => 0])['maxPriority']);
});

$broker = Harness::broker();
$port = $broker['port'];

// Topic and headers routing end to end.
Harness::guard('exchange routing', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->confirmSelect();

    $c->declareExchange('t-ex', 'topic');
    $c->declareQueue('t-all');
    $c->declareQueue('t-one');
    $c->bind('t-all', 't-ex', 'logs.#');
    $c->bind('t-one', 't-ex', 'logs.*.error');

    $c->publish('t-ex', 'logs.app.error', 'both');
    $c->expect(60, 80);
    Harness::eq('the # binding matched', 'both', $c->get('t-all')['body']);
    Harness::eq('the * binding matched', 'both', $c->get('t-one')['body']);

    $c->publish('t-ex', 'logs.app.info', 'only-hash');
    $c->expect(60, 80);
    Harness::eq('the # binding matched again', 'only-hash', $c->get('t-all')['body']);
    Harness::eq('the narrower binding did not', false, $c->get('t-one')['delivered']);

    $c->declareExchange('h-ex', 'headers');
    $c->declareQueue('h-q');
    $c->bind('h-q', 'h-ex', '', ['x-match' => 'all', 'type' => 'report']);
    $c->publish('h-ex', '', 'matched', ['headers' => ['type' => 'report']]);
    $c->expect(60, 80);
    Harness::eq('a headers binding matched', 'matched', $c->get('h-q')['body']);
    $c->publish('h-ex', '', 'unmatched', ['headers' => ['type' => 'other']], true);
    // An unroutable mandatory publish produces basic.return and then the
    // confirm, so both have to be drained before the next call.
    $c->expect(60, 50);
    $c->expect(60, 80);
    Harness::eq('a non-matching header did not route', false, $c->get('h-q')['delivered']);
    $c->close();
});

// A mandatory publish with no route comes back as basic.return.
Harness::guard('mandatory return', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->confirmSelect();
    $c->declareExchange('r-ex', 'direct');
    $c->publish('r-ex', 'nowhere', 'returned', ['contentType' => 'text/plain'], true);
    $frame = $c->read(3.0);
    Harness::eq('basic.return arrives', 50, $frame['method'] ?? 0);
    Harness::eq('it carries the body back', 'returned', $frame['body'] ?? '');
    $c->close();
});

// Per-message TTL expires the message instead of delivering it.
Harness::guard('message ttl', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->confirmSelect();
    $c->declareQueue('ttl-q');
    $c->publish('', 'ttl-q', 'short-lived', ['expiration' => '50']);
    $c->expect(60, 80);
    usleep(250000);
    Harness::eq('an expired message is not delivered', false, $c->get('ttl-q')['delivered']);

    // A queue-level TTL does the same.
    $c->declareQueue('ttl-q2', true, ['x-message-ttl' => 50]);
    $c->publish('', 'ttl-q2', 'also-short');
    $c->expect(60, 80);
    usleep(250000);
    Harness::eq('a queue ttl expires too', false, $c->get('ttl-q2')['delivered']);

    // Without a TTL the message stays.
    $c->declareQueue('ttl-q3');
    $c->publish('', 'ttl-q3', 'persists');
    $c->expect(60, 80);
    usleep(250000);
    Harness::eq('a message with no ttl survives', 'persists', $c->get('ttl-q3')['body']);
    $c->close();
});

// A rejected message lands on the dead-letter exchange.
Harness::guard('dead lettering', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->confirmSelect();
    $c->declareExchange('dlx-ex', 'fanout');
    $c->declareQueue('dead-letters');
    $c->bind('dead-letters', 'dlx-ex');
    $c->declareQueue('dl-src', true, ['x-dead-letter-exchange' => 'dlx-ex']);

    $c->publish('', 'dl-src', 'doomed');
    $c->expect(60, 80);
    $got = $c->get('dl-src');
    Harness::eq('the message arrives first', 'doomed', $got['body']);
    // Reject without requeue sends it to the dlx.
    $c->reject(1, false);
    usleep(200000);
    Harness::eq('it reappears on the dlx queue', 'doomed', $c->get('dead-letters')['body']);
    $c->close();
});

// Overflow modes.
Harness::guard('max length overflow', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->confirmSelect();

    // drop-head keeps the newest and drops the oldest.
    $c->declareQueue('ov-drop', true, ['x-max-length' => 2, 'x-overflow' => 'drop-head']);
    foreach (['one', 'two', 'three'] as $body) {
        $c->publish('', 'ov-drop', $body);
        $c->expect(60, 80);
    }
    $first = $c->get('ov-drop');
    Harness::eq('drop-head dropped the oldest', 'two', $first['body']);
    Harness::eq('and kept the newest', 'three', $c->get('ov-drop')['body']);
    Harness::eq('only two remain', false, $c->get('ov-drop')['delivered']);

    // reject-publish nacks once the queue is full.
    $c->declareQueue('ov-reject', true, ['x-max-length' => 1, 'x-overflow' => 'reject-publish']);
    $c->publish('', 'ov-reject', 'accepted');
    $frame = $c->read(3.0);
    Harness::eq('the first publish is acked', 80, $frame['method'] ?? 0);
    $c->publish('', 'ov-reject', 'refused');
    $frame = $c->read(3.0);
    Harness::eq('the overflowing publish is nacked', 120, $frame['method'] ?? 0);
    $c->close();
});

// Priority ordering: a higher priority message is delivered first.
Harness::guard('priority ordering', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->confirmSelect();
    $c->declareQueue('pri-q', true, ['x-max-priority' => 9]);
    foreach ([['low', 1], ['high', 9], ['mid', 5]] as [$body, $priority]) {
        $c->publish('', 'pri-q', $body, ['priority' => $priority]);
        $c->expect(60, 80);
    }
    Harness::eq('the highest priority comes first', 'high', $c->get('pri-q')['body']);
    Harness::eq('then the middle', 'mid', $c->get('pri-q')['body']);
    Harness::eq('then the lowest', 'low', $c->get('pri-q')['body']);
    $c->close();
});

// basic.nack with requeue puts the message back for redelivery.
Harness::guard('nack requeue', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->confirmSelect();
    $c->declareQueue('nack-q');
    $c->publish('', 'nack-q', 'nacked');
    $c->expect(60, 80);
    $got = $c->get('nack-q');
    Harness::eq('the message arrives', 'nacked', $got['body']);
    $c->nack(1, true);
    Harness::eq('a nack with requeue returns it', 'nacked', $c->get('nack-q')['body']);
    $c->nack(2, false);
    Harness::eq('a nack without requeue drops it', false, $c->get('nack-q')['delivered']);
    $c->close();
});

// Prefetch caps the number of unacked deliveries to a consumer.
Harness::guard('prefetch', static function () use ($port): void {
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->confirmSelect();
    $c->declareQueue('pf-q');
    for ($i = 0; $i < 5; $i++) {
        $c->publish('', 'pf-q', "m$i");
        $c->expect(60, 80);
    }
    $c->qos(2);
    $c->consume('pf-q');
    $delivered = 0;
    $deadline = microtime(true) + 2.0;
    while (microtime(true) < $deadline) {
        $frame = $c->read(0.4);
        if ($frame === null) {
            break;
        }
        if ($frame['class'] === 60 && $frame['method'] === 60) {
            $delivered++;
        }
    }
    Harness::eq('prefetch 2 stops after two unacked', 2, $delivered);
    $c->close();
});

Harness::stop($broker);
Harness::done();
