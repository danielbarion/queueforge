<?php
declare(strict_types=1);

/**
 * Parity checks for the behaviour added to match Bun and Rust: declare
 * rules, alternate exchanges, CC/BCC, x-death, delivery limits, consumer
 * priority, expiry, policies, and the permission refusals.
 */

$root = dirname(__DIR__);
require_once __DIR__ . '/lib/Harness.php';
require_once __DIR__ . '/lib/Amqp.php';
require_once $root . '/src/Routing.php';
require_once $root . '/src/Features.php';
require_once $root . '/src/Policy.php';
require_once $root . '/src/Codec.php';

Harness::guard('parity', static function (): void {
    // --- Unit: x-death header layout -------------------------------------
    $death = Features::deathHeaders([['keep', 'me']], 'q1', 'expired', 'ex', 'rk');
    Harness::eq('unrelated headers survive', 'me', $death[0][1]);
    Harness::eq('x-death is added', 'x-death', $death[1][0]);
    Harness::eq('it names the queue', 'q1', $death[1][1][0]['queue']);
    Harness::eq('and the reason', 'expired', $death[1][1][0]['reason']);
    Harness::eq('count is one', 1, $death[1][1][0]['count']);
    Harness::eq('the exchange is carried', 'ex', $death[1][1][0]['exchange']);
    Harness::eq('routing-keys is a list', ['rk'], $death[1][1][0]['routing-keys']);
    Harness::eq('first death reason', 'expired', $death[2][1]);
    Harness::eq('first death queue', 'q1', array_column($death,1,0)['x-first-death-queue']);

    // Different deaths retain history; repeated queue/reason pairs increment count.
    $again = Features::deathHeaders($death, 'q2', 'rejected', 'ex2', 'rk2');
    $names = array_map(static fn (array $p): string => $p[0], $again);
    Harness::eq('only one x-death remains', 1, count(array_keys($names, 'x-death', true)));
    Harness::eq('the latest death reason is recorded', 'rejected', array_column($again,1,0)['x-last-death-reason']);

    $history = array_column($again,1,0);
    Harness::eq('first death reason remains unchanged', 'expired', $history['x-first-death-reason']);
    Harness::eq('distinct death history is retained', 2, count($history['x-death']));
    $repeated = array_column(Features::deathHeaders($again, 'q1', 'expired', 'ex', 'rk'),1,0);
    Harness::eq('repeat death count increments', 2, $repeated['x-death'][0]['count']);
    Harness::eq('repeat keeps distinct history', 2, count($repeated['x-death']));

    // --- Unit: argument parsing ------------------------------------------
    $args = Features::parseArgs([
        'x-expires' => 5000,
        'x-single-active-consumer' => 'true',
        'x-dead-letter-strategy' => 'at-least-once',
    ]);
    Harness::eq('x-expires is parsed', 5000, $args['expiresMs']);
    Harness::ok('single active is parsed', $args['singleActive']);
    Harness::eq('the dlx strategy is parsed', 'at-least-once', $args['dlxStrategy']);
    Harness::ok('numeric single active works too', Features::parseArgs(['x-single-active-consumer' => 1])['singleActive']);
    Harness::eq('an unset strategy is at-most-once', 'at-most-once', Features::parseArgs([])['dlxStrategy']);
    Harness::ok('classic is a known type', Features::knownQueueType('classic'));
    Harness::ok('quorum is a known type', Features::knownQueueType('quorum'));
    Harness::ok('an empty type is known', Features::knownQueueType(''));
    Harness::ok('stream is a known type', Features::knownQueueType('stream'));

    // --- Unit: policy matching -------------------------------------------
    $table = [
        'low' => ['pattern' => '^p', 'priority' => 1, 'apply-to' => 'all', 'definition' => ['message-ttl' => 100]],
        'high' => ['pattern' => '^p', 'priority' => 5, 'apply-to' => 'all', 'definition' => ['message-ttl' => 900]],
        'other' => ['pattern' => '^z', 'priority' => 9, 'apply-to' => 'all', 'definition' => ['message-ttl' => 1]],
    ];
    $hit = Policy::match($table, 'pq', 'queues');
    Harness::eq('the highest priority policy wins', 900, $hit['definition']['message-ttl']);
    Harness::eq('a non-matching pattern is skipped', null, Policy::match($table, 'nope', 'queues'));

    // A tie goes to the lexicographically smaller name.
    $tie = Policy::match([
        'bbb' => ['pattern' => '.*', 'priority' => 2, 'definition' => ['max-length' => 2]],
        'aaa' => ['pattern' => '.*', 'priority' => 2, 'definition' => ['max-length' => 1]],
    ], 'x', 'queues');
    Harness::eq('a tie prefers the smaller name', 1, $tie['definition']['max-length']);

    // apply-to filters by entity kind.
    $onlyQueues = ['q' => ['pattern' => '.*', 'apply-to' => 'queues', 'definition' => ['max-length' => 7]]];
    Harness::ok('a queue policy matches a queue', Policy::match($onlyQueues, 'x', 'queues') !== null);
    Harness::eq('but not an exchange', null, Policy::match($onlyQueues, 'x', 'exchanges'));

    // Declared values override user defaults; numeric operator policies cap both.
    $resolved = Policy::resolve(
        ['x-message-ttl' => 11],
        ['definition' => ['message-ttl' => 22, 'max-length' => 33]],
        ['definition' => ['max-length' => 44]],
    );
    Harness::eq('a declared value is never overridden', 11, $resolved['x-message-ttl']);
    Harness::eq('the lower numeric user value remains under the operator cap', 33, $resolved['x-max-length']);
    $capped = Policy::resolve(['x-message-ttl' => 11], ['definition' => ['message-ttl' => 22]], ['definition' => ['message-ttl' => 5]]);
    Harness::eq('numeric operator cap constrains a declared value', 5, $capped['x-message-ttl']);

    // Validation.
    Harness::eq('a good policy validates', null, Policy::validate(['pattern' => '.*', 'definition' => ['max-length' => 1]]));
    Harness::ok('a missing pattern is refused', Policy::validate(['definition' => []]) !== null);
    Harness::ok('a missing definition is refused', Policy::validate(['pattern' => '.*']) !== null);
    Harness::ok('a bad apply-to is refused', Policy::validate(['pattern' => '.*', 'apply-to' => 'wat', 'definition' => []]) !== null);
    Harness::ok('an unknown key is refused', Policy::validate(['pattern' => '.*', 'definition' => ['nonsense' => 1]]) !== null);

    // --- Live broker ------------------------------------------------------
    $b = Harness::broker([], 10);
    $port = $b['port'];

    // Passive declare on a missing queue is a 404 channel close.
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->method(1, 50, 10, pack('n', 0) . Codec::shortstr('nope-passive') . chr(1) . Amqp::table([]));
    $closed = $c->expectClose();
    Harness::eq('a passive declare of a missing queue is 404', 404, $closed['code'] ?? 0);
    $c->close();

    // Passive declare of an existing queue reports its state.
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->declareQueue('live');
    $c->confirmSelect();
    $c->publish('', 'live', 'one');
    $c->expect(60, 80);
    $state = $c->declareQueue('live', true, [], 1, true);
    Harness::eq('a passive declare reports the depth', 1, $state['messages']);

    // A redeclare with a different durability is a 406.
    $c->method(2, 20, 10);
    $c->expect(20, 11);
    $c->method(2, 50, 10, pack('n', 0) . Codec::shortstr('live') . chr(0) . Amqp::table([]));
    $closed = $c->expectClose();
    Harness::eq('an inequivalent durable redeclare is 406', 406, $closed['code'] ?? 0);

    // An unknown x-queue-type is a 406 rather than being coerced to classic.
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->method(1, 50, 10, pack('n', 0) . Codec::shortstr('typed') . chr(2) . Amqp::table(['x-queue-type' => 'unsupported']));
    $closed = $c->expectClose();
    Harness::eq('an unsupported queue type is 406', 406, $closed['code'] ?? 0);
    $c->close();

    $c = new Amqp('127.0.0.1', $port); $c->channel();
    $c->method(1, 50, 10, pack('n', 0) . Codec::shortstr('stream-supported') . chr(2) . Amqp::table(['x-queue-type' => 'stream']));
    Harness::eq('a durable stream queue is supported', 11, $c->expect(50,11)['method']);
    $c->close();

    // A transient non-exclusive queue is the 541 deprecation, and it closes
    // the connection rather than the channel.
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->method(1, 50, 10, pack('n', 0) . Codec::shortstr('transient') . chr(0) . Amqp::table([]));
    $closed = $c->expectClose();
    Harness::eq('a transient non-exclusive queue is 541', 541, $closed['code'] ?? 0);
    Harness::eq('and it is a connection close', 10, $closed['class'] ?? 0);
    $c->close();

    // The amq. namespace is reserved on declare.
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->method(1, 40, 10, pack('n', 0) . Codec::shortstr('amq.mine') . Codec::shortstr('direct') . chr(2) . Amqp::table([]));
    $closed = $c->expectClose();
    Harness::eq('declaring amq.* is 403', 403, $closed['code'] ?? 0);
    $c->close();

    // Binding needs both ends to exist.
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->method(1, 50, 20, pack('n', 0) . Codec::shortstr('ghost-q') . Codec::shortstr('amq.direct') . Codec::shortstr('k') . chr(0) . Amqp::table([]));
    $closed = $c->expectClose();
    Harness::eq('binding a missing queue is 404', 404, $closed['code'] ?? 0);
    $c->close();

    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->declareQueue('real-q');
    $c->method(1, 50, 20, pack('n', 0) . Codec::shortstr('real-q') . Codec::shortstr('ghost-ex') . Codec::shortstr('k') . chr(0) . Amqp::table([]));
    $closed = $c->expectClose();
    Harness::eq('binding a missing exchange is 404', 404, $closed['code'] ?? 0);
    $c->close();

    // Publishing to the default exchange with no such queue is a 404, not a
    // silent drop.
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->publish('', 'no-such-queue-here', 'x');
    $closed = $c->expectClose();
    Harness::eq('a default-exchange publish to nothing is 404', 404, $closed['code'] ?? 0);
    $c->close();

    // An internal exchange refuses a client publish.
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->declareExchange('ex-internal', 'direct', 1, [], true);
    $c->declareQueue('internal-q');
    $c->bind('internal-q', 'ex-internal', 'k');
    $c->publish('ex-internal', 'k', 'x');
    $closed = $c->expectClose();
    Harness::eq('publishing to an internal exchange is 403', 403, $closed['code'] ?? 0);
    $c->close();

    // The alternate exchange takes an otherwise unroutable message.
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->declareExchange('ae-target', 'fanout');
    $c->declareQueue('ae-q');
    $c->bind('ae-q', 'ae-target', '');
    $c->declareExchange('ae-source', 'direct', 1, ['alternate-exchange' => 'ae-target']);
    $c->confirmSelect();
    $c->publish('ae-source', 'unmatched', 'rescued');
    $c->expect(60, 80);
    $got = $c->get('ae-q');
    Harness::ok('the alternate exchange caught it', $got['delivered']);
    Harness::eq('with the body intact', 'rescued', $got['body']);

    // CC adds a destination; BCC does too but is stripped from the headers.
    $c->declareQueue('cc-main');
    $c->declareQueue('cc-extra');
    $c->declareQueue('cc-hidden');
    $c->publish('', 'cc-main', 'fanned', [
        'headers' => ['CC' => ['cc-extra'], 'BCC' => ['cc-hidden']],
    ]);
    $c->expect(60, 80);
    Harness::ok('the primary queue got it', $c->get('cc-main')['delivered']);
    Harness::ok('the CC queue got it', $c->get('cc-extra')['delivered']);
    $hidden = $c->get('cc-hidden');
    Harness::ok('the BCC queue got it', $hidden['delivered']);
    $headers = Amqp::headersOf($hidden['propRaw'] ?? null);
    Harness::ok('BCC is stripped from the delivered headers', !isset($headers['BCC']));

    // A dead-lettered message arrives with x-death attached.
    $c->declareExchange('dl-ex', 'fanout');
    $c->declareQueue('dl-sink');
    $c->bind('dl-sink', 'dl-ex', '');
    $c->declareQueue('dl-src', true, ['x-dead-letter-exchange' => 'dl-ex']);
    $c->publish('', 'dl-src', 'doomed');
    $c->expect(60, 80);
    $delivery = $c->get('dl-src');
    $c->nack($delivery['tag'] ?? 1, false);
    $dead = $c->get('dl-sink');
    Harness::ok('the dead letter arrived', $dead['delivered']);
    $deathHeaders = Amqp::headersOf($dead['propRaw'] ?? null);
    Harness::ok('x-death is present', isset($deathHeaders['x-death']));
    Harness::eq('x-first-death-queue names the source', 'dl-src', $deathHeaders['x-first-death-queue'] ?? '');
    Harness::eq('x-first-death-reason is rejected', 'rejected', $deathHeaders['x-first-death-reason'] ?? '');

    // x-delivery-limit dead-letters instead of requeueing forever.
    $c->declareQueue('limited', true, [
        'x-delivery-limit' => 2,
        'x-dead-letter-exchange' => 'dl-ex',
    ]);
    $c->purge('dl-sink');
    $c->publish('', 'limited', 'poison');
    $c->expect(60, 80);
    for ($i = 0; $i < 3; $i++) {
        $one = $c->get('limited');
        if (!$one['delivered']) {
            break;
        }
        $c->nack($one['tag'], true);
    }
    Harness::ok('the poison message left the queue', !$c->get('limited')['delivered']);
    Harness::ok('and landed on the dead-letter queue', $c->get('dl-sink')['delivered']);

    // A partial rejection nacks the whole publish.
    $c->declareExchange('split-ex', 'fanout');
    $c->declareQueue('split-ok');
    $c->declareQueue('split-full', true, ['x-max-length' => 1, 'x-overflow' => 'reject-publish']);
    $c->bind('split-ok', 'split-ex', '');
    $c->bind('split-full', 'split-ex', '');
    $c->publish('split-ex', '', 'first');
    $c->expect(60, 80);
    $c->publish('split-ex', '', 'second');
    $second = $c->expect(60, 120);
    Harness::eq('one full destination nacks the publish', 120, $second['method']);
    $c->close();

    // An exclusive consumer excludes every other consumer.
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->declareQueue('excl');
    $c->consume('excl', 'first', false, 1, true);
    $other = new Amqp('127.0.0.1', $port);
    $other->channel();
    $other->method(1, 60, 20, pack('n', 0) . Codec::shortstr('excl') . Codec::shortstr('second') . chr(0) . Amqp::table([]));
    $closed = $other->expectClose();
    Harness::eq('a second consumer on an exclusive queue is 403', 403, $closed['code'] ?? 0);
    $other->close();
    $c->close();

    // The higher-priority consumer is served while it has credit.
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->declareQueue('prio');
    $c->qos(10);
    $c->consume('prio', 'low', false, 1, false, 1);
    $c->method(2, 20, 10);
    $c->expect(20, 11);
    $c->qos(10, 2);
    $c->consume('prio', 'high', false, 2, false, 9);
    $c->confirmSelect();
    $c->publish('', 'prio', 'to-the-top');
    $c->expect(60, 80);
    $delivered = $c->expect(60, 60);
    Harness::eq('the high-priority consumer is served', 2, $delivered['ch']);
    $c->close();

    // A message past its TTL stops counting toward the reported depth.
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->confirmSelect();
    $c->declareQueue('ttl-sweep', true, ['x-message-ttl' => 50]);
    $c->publish('', 'ttl-sweep', 'fades');
    $c->expect(60, 80);
    usleep(400000);
    $state = $c->declareQueue('ttl-sweep', true, [], 1, true);
    Harness::eq('an expired message is swept from the count', 0, $state['messages']);
    $c->close();

    // An idle queue with x-expires is deleted.
    $c = new Amqp('127.0.0.1', $port);
    $c->channel();
    $c->declareQueue('short-lived', true, ['x-expires' => 50]);
    usleep(500000);
    $c->declareQueue('short-lived-probe', true, [], 1, false);
    $c->method(1, 50, 10, pack('n', 0) . Codec::shortstr('short-lived') . chr(1) . Amqp::table([]));
    $closed = $c->expectClose();
    Harness::eq('an idle queue past x-expires is gone', 404, $closed['code'] ?? 0);
    $c->close();

    // connection.start advertises the capabilities clients gate on.
    $raw = new Amqp('127.0.0.1', $port, 'guest', 'guest', false);
    $start = $raw->rawStart();
    Harness::ok('publisher_confirms is advertised', str_contains($start, 'publisher_confirms'));
    Harness::ok('consumer_cancel_notify is advertised', str_contains($start, 'consumer_cancel_notify'));
    Harness::ok('basic.nack is advertised', str_contains($start, 'basic.nack'));
    $raw->close();

    Harness::stop($b);
});

Harness::done();
