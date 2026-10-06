<?php
declare(strict_types=1);

/**
 * Quorum queues: declare rules, the durable-majority confirm gate, the
 * rollback that follows a failed replication, the default delivery limit,
 * and the readiness hold.
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

function qnode(string $id, int $members): array
{
    $dir = sys_get_temp_dir() . '/qf-quorum-' . bin2hex(random_bytes(6));
    mkdir($dir, 0777, true);
    $broker = new Broker(new Store($dir . '/messages.log'), $dir . '/users.json');
    $list = [];
    foreach (range(0, $members - 1) as $i) {
        $list[] = ['id' => chr(ord('a') + $i), 'addr' => '127.0.0.1:' . (9000 + $i)];
    }
    $broker->members = $list;
    $cluster = new Cluster($broker, $id);
    return ['broker' => $broker, 'cluster' => $cluster];
}

// The majority rule and the durable count it is measured against.
Harness::guard('majority arithmetic', static function (): void {
    Harness::eq('one member needs one', 1, Features::majority(1));
    Harness::eq('two members need two', 2, Features::majority(2));
    Harness::eq('three members need two', 2, Features::majority(3));
    Harness::eq('four members need three', 3, Features::majority(4));
    Harness::eq('five members need three', 3, Features::majority(5));

    Harness::eq('one durable of three is short', false, Features::durableMajority(3, ['durable']));
    Harness::eq('two durable of three is a majority', true, Features::durableMajority(3, ['durable', 'durable']));
    Harness::eq(
        'memory copies do not count',
        false,
        Features::durableMajority(3, ['durable', 'memory', 'memory']),
    );
    Harness::eq('the extra append cap is 32', 32, Cluster::EXTRA_APPEND_CAP);
});

// A quorum queue must be durable and non-exclusive.
Harness::guard('declare rules', static function (): void {
    $n = qnode('a', 3);
    $broker = $n['broker'];
    $broker->declareQueue('qq', ['x-queue-type' => 'quorum'], true, false);
    Harness::eq('a durable quorum queue is accepted', 'quorum', $broker->queues['qq']['args']['queueType']);
    Harness::eq('it is homed where it was declared', 'a', $broker->home('qq'));
    Harness::eq('the delivery limit defaults to 20', 20, $broker->queues['qq']['args']['deliveryLimit']);

    $threw = false;
    try {
        $broker->declareQueue('bad1', ['x-queue-type' => 'quorum'], false, false);
    } catch (RuntimeException $e) {
        $threw = true;
    }
    Harness::ok('a transient quorum queue is refused', $threw);

    $threw = false;
    try {
        $broker->declareQueue('bad2', ['x-queue-type' => 'quorum'], true, true);
    } catch (RuntimeException $e) {
        $threw = true;
    }
    Harness::ok('an exclusive quorum queue is refused', $threw);

    // A classic queue is unaffected and follows the hash instead.
    $broker->declareQueue('classic');
    Harness::eq('a classic queue has no pinned home', '', $broker->queues['classic']['home']);
});

// The confirm waits for a durable majority, not just for the local fsync.
Harness::guard('confirm gate', static function (): void {
    $n = qnode('a', 3);
    $broker = $n['broker'];
    $cluster = $n['cluster'];
    $sent = [];
    // Two peers, so a majority of three needs one of them plus the local copy.
    foreach (['b', 'c'] as $peer) {
        $cluster->attach($peer, static function (string $line) use (&$sent): void {
            $sent[] = $line;
        });
    }
    $broker->declareQueue('qq', ['x-queue-type' => 'quorum'], true, false);
    $result = $broker->publish(1, 1, 1, '', 'qq', 'quorum-body', 2);
    Harness::eq('the publish waits', 'wait', $result);
    Harness::ok('the append went to the peers', $sent !== []);

    $first = json_decode($sent[0], true);
    Harness::eq('the op is quorum_append', 'quorum_append', $first['op']);
    Harness::eq('the body is version 1', 1, $first['payload']['v']);
    $qid = (string) $first['payload']['message_id'];
    Harness::ok('the id uses the quorum shape', str_starts_with($qid, 'q-a-'), "id was $qid");

    // Only the local copy so far, so no confirm.
    Harness::eq('no confirm before a majority', 0, count($broker->flush()));

    // One peer stores it, which together with the local copy is a majority.
    $broker->noteCopy($qid);
    $confirms = $broker->flush();
    Harness::eq('the confirm is released on a majority', 1, count($confirms));
    Harness::eq('the confirm is an ack, not a nack', false, $confirms[0]['nack']);
    Harness::eq('the confirm carries the publish tag', 1, $confirms[0]['tag']);
});

// Replication that never reaches a majority is rolled back and nacked.
Harness::guard('rollback on failure', static function (): void {
    $n = qnode('a', 3);
    $broker = $n['broker'];
    $cluster = $n['cluster'];
    $sent = [];
    foreach (['b', 'c'] as $peer) {
        $cluster->attach($peer, static function (string $line) use (&$sent): void {
            $sent[] = $line;
        });
    }
    $broker->declareQueue('qq', ['x-queue-type' => 'quorum'], true, false);
    $broker->publish(1, 1, 7, '', 'qq', 'doomed', 2);
    $qid = (string) json_decode($sent[0], true)['payload']['message_id'];
    $sent = [];

    $broker->failQuorum($qid);
    $drops = 0;
    foreach ($sent as $line) {
        $decoded = json_decode($line, true);
        if (($decoded['op'] ?? '') === 'quorum_drop' && ($decoded['payload']['id'] ?? '') === $qid) {
            $drops++;
        }
    }
    Harness::eq('a quorum_drop goes to both peers', 2, $drops);

    $confirms = $broker->flush();
    Harness::eq('the publisher is answered', 1, count($confirms));
    Harness::eq('the answer is a nack', true, $confirms[0]['nack']);
    Harness::eq('the nack carries the publish tag', 7, $confirms[0]['tag']);
    Harness::eq('nothing is left queued', 0, $broker->readyCount('qq'));
});

// A single node does not wait for anyone.
Harness::guard('single node', static function (): void {
    $n = qnode('solo', 1);
    $broker = $n['broker'];
    $broker->declareQueue('qq', ['x-queue-type' => 'quorum'], true, false);
    $broker->publish(1, 1, 1, '', 'qq', 'alone', 2);
    $confirms = $broker->flush();
    Harness::eq('a lone node confirms straight away', 1, count($confirms));
    Harness::eq('and it is an ack', false, $confirms[0]['nack']);
});

// The whole path end to end, over a socket, on one node.
$broker = Harness::broker([], 10, 'a');
Harness::guard('quorum over the wire', static function () use ($broker): void {
    $c = new Amqp('127.0.0.1', $broker['port']);
    $c->channel();
    $c->confirmSelect();
    $declared = $c->declareQueue('wire-qq', true, ['x-queue-type' => 'quorum']);
    Harness::eq('the quorum queue declares', 'wire-qq', $declared['queue']);
    $c->publish('', 'wire-qq', 'over-the-wire');
    $frame = $c->read(3.0);
    Harness::eq('the publish is confirmed', 80, $frame['method'] ?? 0);
    $got = $c->get('wire-qq');
    Harness::eq('the message is readable', 'over-the-wire', $got['body']);
    $c->close();
});

// A transient quorum declare is refused over the wire too.
Harness::guard('transient quorum over the wire', static function () use ($broker): void {
    $c = new Amqp('127.0.0.1', $broker['port']);
    $c->channel();
    // durable=false with x-queue-type=quorum.
    $c->method(1, 50, 10, pack('n', 0) . Codec::shortstr('bad-qq') . chr(0) . Amqp::table(['x-queue-type' => 'quorum']));
    $close = $c->expectClose();
    Harness::eq('a transient quorum declare is a 406', 406, $close['code'] ?? 0);
    $c->close();
});

Harness::stop($broker);
Harness::done();
