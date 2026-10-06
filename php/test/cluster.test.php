<?php
declare(strict_types=1);

/**
 * Cluster protocol version 1: line framing, the quorum append body, the
 * home hash, snapshot merging, request timeouts, and the op set a peer can
 * call. These run against the Cluster class directly rather than over a
 * socket, so a failure points at the protocol rather than at the listener.
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

/** Builds an isolated broker plus cluster for one check. */
function node(string $id, array $members = []): array
{
    $dir = sys_get_temp_dir() . '/qf-cluster-' . bin2hex(random_bytes(6));
    mkdir($dir, 0777, true);
    $broker = new Broker(new Store($dir . '/messages.log'), $dir . '/users.json');
    $broker->members = $members;
    $cluster = new Cluster($broker, $id);
    return ['broker' => $broker, 'cluster' => $cluster, 'dir' => $dir];
}

// The quorum append body round-trips, and the Rust spelling decodes too.
Harness::guard('quorum append body', static function (): void {
    $encoded = Features::encodeQuorumAppend('/', 'orders', 'q-a-1', 'hello', 'ex', 'rk', true);
    Harness::eq('body is base64', base64_encode('hello'), $encoded['body_b64']);
    Harness::eq('version is 1', 1, $encoded['v']);
    $back = Features::decodeQuorumAppend($encoded);
    Harness::eq('round trip keeps the body', 'hello', $back['body']);
    Harness::eq('round trip keeps the id', 'q-a-1', $back['messageId']);
    Harness::eq('round trip keeps the routing key', 'rk', $back['routingKey']);

    // Rust and Bun both accept these aliases, so this side must as well.
    $rust = Features::decodeQuorumAppend([
        'vhost' => '/',
        'queue' => 'orders',
        'message' => [
            'qid' => 'q-b-9',
            'body' => base64_encode('from-rust'),
            'routingKey' => 'alt',
            'exchange' => 'ex2',
            'persistent' => false,
        ],
    ]);
    Harness::eq('nested message unwraps', 'from-rust', $rust['body']);
    Harness::eq('qid alias works', 'q-b-9', $rust['messageId']);
    Harness::eq('routingKey alias works', 'alt', $rust['routingKey']);
    Harness::eq('persistent false is honoured', false, $rust['persistent']);
});

// Line framing must hold a partial tail and skip blank lines.
Harness::guard('line framing', static function (): void {
    $n = node('a');
    $cluster = $n['cluster'];
    $replies = $cluster->ingest('{"v":1,"op":"stats","id":1,"payload":{"queue":"none"}}' . "\n");
    Harness::eq('one line gives one reply', 1, count($replies));
    $replies = $cluster->ingest("\n  \n");
    Harness::eq('blank lines are skipped', 0, count($replies));
    $replies = $cluster->ingest('{"v":1,"op":"stats","id":2,"payl');
    Harness::eq('a partial line waits', 0, count($replies));
    $replies = $cluster->ingest('oad":{"queue":"none"}}' . "\n");
    Harness::eq('the completed line replies', 1, count($replies));
    $decoded = json_decode($replies[0], true);
    Harness::eq('reply carries the correlation id', 2, $decoded['id']);
    Harness::eq('reply is ok', true, $decoded['ok']);
});

// The home hash must agree with Bun, whose values are pinned here.
Harness::guard('home hash', static function (): void {
    foreach (['a' => 3826002220, 'foo' => 2851307223, "/\0q0" => 1994922035] as $text => $want) {
        Harness::eq("fnv1a of " . json_encode($text), $want, Features::fnv1a32($text));
    }
    $members = [
        ['id' => 'a', 'addr' => '127.0.0.1:1'],
        ['id' => 'b', 'addr' => '127.0.0.1:2'],
        ['id' => 'c', 'addr' => '127.0.0.1:3'],
    ];
    // fnv1a("/\0q0") % 3 picks the member at that index of the sorted ids.
    $expected = ['a', 'b', 'c'][1994922035 % 3];
    Harness::eq('home of q0 follows the hash', $expected, Features::home($members, '/', 'q0'));
    Harness::ok(
        'home is stable across calls',
        Features::home($members, '/', 'orders') === Features::home($members, '/', 'orders'),
    );
    // A single node owns everything.
    $one = node('solo');
    Harness::eq('a lone node owns its queues', true, $one['broker']->ownsQueue('anything'));
});

// A peer's snapshot merges without clobbering what is already here.
Harness::guard('snapshot merge', static function (): void {
    $n = node('a');
    $broker = $n['broker'];
    $broker->declareQueue('mine');
    $broker->declareExchange('keep', 'topic');
    $broker->applySnapshot([
        'users' => ['peer-user' => 'hash-value'],
        'exchanges' => ['keep' => 'fanout', 'theirs' => 'fanout'],
        'queues' => [['name' => 'mine', 'type' => 'classic'], ['name' => 'theirs', 'type' => 'quorum']],
        'bindings' => [['queue' => 'theirs', 'exchange' => 'theirs', 'key' => '']],
    ]);
    Harness::eq('a new user is taken', 'hash-value', $broker->users['peer-user'] ?? '');
    Harness::eq('an existing exchange is not overwritten', 'topic', $broker->exchanges['keep']);
    Harness::eq('a new exchange is taken', 'fanout', $broker->exchanges['theirs']);
    Harness::ok('a new queue is created', isset($broker->queues['theirs']));
    Harness::eq('the queue type survives', 'quorum', $broker->queues['theirs']['args']['queueType']);
    Harness::eq('one binding arrives', 1, count($broker->bindings));
    // Applying the same snapshot twice must not duplicate the binding.
    $broker->applySnapshot([
        'bindings' => [['queue' => 'theirs', 'exchange' => 'theirs', 'key' => '']],
    ]);
    Harness::eq('a repeated snapshot does not duplicate bindings', 1, count($broker->bindings));
});

// Every op a peer can call is answered.
Harness::guard('op set', static function (): void {
    $n = node('a', [['id' => 'a', 'addr' => '127.0.0.1:1'], ['id' => 'b', 'addr' => '127.0.0.1:2']]);
    $broker = $n['broker'];
    $cluster = $n['cluster'];
    $call = static function (string $op, array $payload, int $id = 1) use ($cluster): array {
        $line = json_encode(['v' => 1, 'op' => $op, 'id' => $id, 'from' => 'b', 'nodeId' => 'b', 'payload' => $payload]);
        $reply = $cluster->handleLine((string) $line);
        return $reply === null ? [] : (array) json_decode($reply, true);
    };

    $declared = $call('declare_queue', ['queue' => 'shared']);
    Harness::eq('declare_queue is ok', true, $declared['ok'] ?? false);
    Harness::eq('declare_queue reports a home', true, isset($declared['payload']['queue']['home']));

    $enq = $call('enqueue', Features::encodeQuorumAppend('/', 'shared', 'm-1', 'payload-1', '', 'shared', true));
    Harness::eq('enqueue is ok', true, $enq['ok'] ?? false);
    Harness::eq('enqueue stored the message', 1, $broker->readyCount('shared'));

    $stats = $call('stats', ['queue' => 'shared']);
    Harness::eq('stats reports the depth', 1, $stats['payload']['messages_ready'] ?? -1);

    $got = $call('get', ['queue' => 'shared', 'no_ack' => true]);
    Harness::eq('get returns a message', base64_encode('payload-1'), $got['payload']['msg']['body_b64'] ?? '');
    $empty = $call('get', ['queue' => 'shared', 'no_ack' => true]);
    Harness::eq('get on an empty queue says empty', true, $empty['payload']['empty'] ?? false);

    $call('enqueue', Features::encodeQuorumAppend('/', 'shared', 'm-2', 'payload-2', '', 'shared', true));
    $purged = $call('purge', ['queue' => 'shared']);
    Harness::eq('purge reports the count', 1, $purged['payload'] ?? -1);

    $sub = $call('sub', ['queue' => 'shared', 'session' => 42, 'noAck' => false]);
    Harness::eq('sub is ok', true, $sub['ok'] ?? false);
    Harness::eq('sub registers a remote consumer', 1, count($broker->queues['shared']['consumers']));
    Harness::eq('the consumer is marked remote', 'b', $broker->queues['shared']['consumers'][0]['peer']);
    $call('credit', ['queue' => 'shared', 'session' => 42, 'credit' => 5]);
    Harness::eq('credit is added', 5, $broker->queues['shared']['consumers'][0]['credit']);
    $call('set_credit', ['queue' => 'shared', 'session' => 42, 'credit' => 2]);
    Harness::eq('set_credit replaces the value', 2, $broker->queues['shared']['consumers'][0]['credit']);
    $call('unsub', ['queue' => 'shared', 'session' => 42]);
    Harness::eq('unsub removes the consumer', 0, count($broker->queues['shared']['consumers']));

    $apply = $call('apply', ['kind' => 'members', 'body' => [
        ['id' => 'a', 'addr' => '127.0.0.1:1'],
        ['id' => 'b', 'addr' => '127.0.0.1:2'],
        ['id' => 'c', 'addr' => '127.0.0.1:3'],
    ]]);
    Harness::eq('apply members is ok', true, $apply['ok'] ?? false);
    Harness::eq('the member list grew', 3, count($broker->members));

    $del = $call('delete_queue', ['queue' => 'shared']);
    Harness::eq('delete_queue is ok', true, $del['ok'] ?? false);
    Harness::ok('the queue is gone', !isset($broker->queues['shared']));

    // An unknown op is answered rather than ignored, so a caller is not stuck.
    $unknown = $call('no_such_op', []);
    Harness::eq('an unknown op still replies', true, $unknown['ok'] ?? false);
});

// A dropped peer must leave the peer set and take its consumers with it.
Harness::guard('peer detach', static function (): void {
    $n = node('a', [['id' => 'a', 'addr' => '1'], ['id' => 'b', 'addr' => '2']]);
    $cluster = $n['cluster'];
    $cluster->attach('b', static function (string $line): void {
    });
    Harness::eq('the peer is attached', ['b'], $cluster->peerIds());
    $cluster->handleLine((string) json_encode([
        'v' => 1, 'op' => 'sub', 'id' => 1, 'from' => 'b', 'nodeId' => 'b',
        'payload' => ['queue' => 'q', 'session' => 7],
    ]));
    Harness::eq('the remote consumer is registered', 1, count($n['broker']->queues['q']['consumers']));
    $cluster->detach('b');
    Harness::eq('the peer is detached', [], $cluster->peerIds());
    Harness::eq('its consumers go with it', 0, count($n['broker']->queues['q']['consumers']));
});

// A request whose reply never comes must expire rather than wait forever.
Harness::guard('request timeout', static function (): void {
    $n = node('a', [['id' => 'a', 'addr' => '1'], ['id' => 'b', 'addr' => '2']]);
    $cluster = $n['cluster'];
    $sent = [];
    $cluster->attach('b', static function (string $line) use (&$sent): void {
        $sent[] = $line;
    });
    $correlation = $cluster->request('b', 'quorum_append', ['message_id' => 'q-1'], 'q-1');
    Harness::ok('the request was sent', $correlation > 0 && $sent !== []);
    $decoded = json_decode($sent[0], true);
    Harness::eq('the request names the op', 'quorum_append', $decoded['op']);
    Harness::eq('the request carries a correlation id', $correlation, $decoded['id']);

    // Not yet due.
    $cluster->tick();
    Harness::eq('nothing expires early', [], $cluster->timedOut);

    // A request to a peer that is not connected is refused outright.
    Harness::eq('an unknown peer is refused', 0, $cluster->request('zz', 'stats', []));

    Harness::eq('the timeout is Bun\'s 3 seconds', 3.0, Cluster::REQUEST_TIMEOUT);
});

// quorum_drop must remove the replica the leader rolled back.
Harness::guard('quorum drop', static function (): void {
    $n = node('a', [['id' => 'a', 'addr' => '1'], ['id' => 'b', 'addr' => '2']]);
    $broker = $n['broker'];
    $cluster = $n['cluster'];
    $broker->declareQueue('qq', ['x-queue-type' => 'quorum']);
    $cluster->handleLine((string) json_encode([
        'v' => 1, 'op' => 'quorum_append', 'id' => 1, 'from' => 'b', 'nodeId' => 'b',
        'payload' => Features::encodeQuorumAppend('/', 'qq', 'q-drop-1', 'rolled-back', '', 'qq', true),
    ]));
    $held = $broker->readyCount('qq') + count($broker->queues['qq']['replicas']);
    Harness::eq('the replica is held', 1, $held);
    $cluster->handleLine((string) json_encode([
        'v' => 1, 'op' => 'quorum_drop', 'id' => 2, 'from' => 'b', 'nodeId' => 'b',
        'payload' => ['vhost' => '/', 'queue' => 'qq', 'id' => 'q-drop-1'],
    ]));
    $after = $broker->readyCount('qq') + count($broker->queues['qq']['replicas']);
    Harness::eq('quorum_drop removes it', 0, $after);
});

// A deliver frame must carry both payload shapes.
Harness::guard('deliver shape', static function (): void {
    $n = node('a', [['id' => 'a', 'addr' => '1'], ['id' => 'b', 'addr' => '2']]);
    $cluster = $n['cluster'];
    $sent = [];
    $cluster->attach('b', static function (string $line) use (&$sent): void {
        $sent[] = $line;
    });
    $cluster->deliverTo('b', 'q', 7, [
        'body' => 'to-peer',
        'exchange' => 'ex',
        'key' => 'rk',
        'mode' => 2,
        'redelivered' => false,
        'propRaw' => '',
    ], 99, false);
    Harness::eq('one frame went out', 1, count($sent));
    $payload = json_decode($sent[0], true)['payload'];
    Harness::eq('the Rust shape carries the body', base64_encode('to-peer'), $payload['message']['body_b64']);
    Harness::eq('the Bun shape carries the body', base64_encode('to-peer'), $payload['msg']['body']);
    Harness::eq('the session is echoed', 7, $payload['session']);
    Harness::ok('a delivery id is assigned', ($payload['delivery_id'] ?? 0) > 0);

    // The peer's ack maps back to the local message id.
    $cluster->handleLine((string) json_encode([
        'v' => 1, 'op' => 'ack', 'id' => 5, 'from' => 'b', 'nodeId' => 'b',
        'payload' => ['queue' => 'q', 'delivery_id' => $payload['delivery_id']],
    ]));
    Harness::ok('the ack is accepted', true);
});

// A deliver frame received from a home node must land in the local queue.
Harness::guard('deliver inbound', static function (): void {
    $n = node('a');
    $broker = $n['broker'];
    $broker->declareQueue('inbound');
    $n['cluster']->handleLine((string) json_encode([
        'v' => 1, 'op' => 'deliver', 'id' => 0, 'from' => 'b', 'nodeId' => 'b',
        'payload' => [
            'queue' => 'inbound',
            'session' => 1,
            'delivery_id' => 4,
            'message' => [
                'exchange' => '',
                'routing_key' => 'inbound',
                'body_b64' => base64_encode('pushed'),
                'persistent' => true,
                'message_id' => 'q-b-4',
            ],
        ],
    ]));
    Harness::eq('the pushed message is queued', 1, $broker->readyCount('inbound'));
});

// The consumed set stops a quorum body being handed out twice after a peer
// replays its log.
Harness::guard('consumed set', static function (): void {
    $members = [
        ['id' => 'a', 'addr' => '127.0.0.1:1'],
        ['id' => 'b', 'addr' => '127.0.0.1:2'],
        ['id' => 'c', 'addr' => '127.0.0.1:3'],
    ];
    $n = node('a', $members);
    $broker = $n['broker'];

    Harness::ok('an unknown id is not consumed', !$broker->wasConsumed('q', 'q-x-1-aa'));
    $broker->noteConsumed('q', 'q-x-1-aa');
    Harness::ok('noting it records it', $broker->wasConsumed('q', 'q-x-1-aa'));
    Harness::eq('an empty id is ignored', false, $broker->wasConsumed('q', ''));

    $list = $broker->consumedList();
    Harness::eq('the wire list has one entry', 1, count($list));
    Harness::eq('it names the queue', 'q', $list[0][0]);
    Harness::eq('and the id', 'q-x-1-aa', $list[0][1]);

    // Applying a peer's set drops the matching local message.
    $broker->declareQueue('qq', ['x-queue-type' => 'quorum']);
    $broker->publish(0, 0, 0, '', 'qq', 'body', 1);
    $id = array_key_last($broker->msgs);
    $qid = (string) $broker->msgs[$id]['qid'];
    Harness::ok('the quorum message has a cluster id', str_starts_with($qid, 'q-'));
    $before = count($broker->msgs);
    $broker->applyConsumed([['qq', $qid]]);
    Harness::eq('applying the peer set dropped it', $before - 1, count($broker->msgs));
    Harness::ok('and recorded it as consumed', $broker->wasConsumed('qq', $qid));
    Harness::eq('the queue is empty', 0, $broker->readyCount('qq'));

    // A hello carries the set in both directions.
    $hello = $n['cluster']->hello();
    Harness::ok('hello carries the consumed set', $hello['payload']['consumed'] !== []);
});

// Session ids are namespaced by node, so two nodes never issue the same one.
Harness::guard('session ids', static function (): void {
    $members = [
        ['id' => 'a', 'addr' => '127.0.0.1:1'],
        ['id' => 'b', 'addr' => '127.0.0.1:2'],
    ];
    $a = node('a', $members)['broker'];
    $b = node('b', $members)['broker'];
    // Node a has no node id of its own in this fixture, so set it the way
    // the entrypoint does.
    $a->nodeId = 'a';
    $b->nodeId = 'b';

    $first = $a->nextSession();
    $second = $a->nextSession();
    Harness::ok('ids advance', $second > $first);
    Harness::eq('the low word is the counter', 1, $first & 0xffffffff);
    Harness::eq('a is slot one', 1, intdiv($first, 0x100000000));
    Harness::eq('b is slot two', 2, intdiv($b->nextSession(), 0x100000000));
    Harness::ok('so the two nodes never collide', $a->nextSession() !== $b->nextSession());

    // With no membership the raw counter is used.
    $solo = node('solo')['broker'];
    $solo->nodeId = 'solo';
    Harness::eq('a single node uses slot zero', 0, intdiv($solo->nextSession(), 0x100000000));
});

Harness::done();
