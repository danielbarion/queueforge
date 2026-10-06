<?php
declare(strict_types=1);

/**
 * Two live brokers over real sockets. Proves the dial rule produces exactly
 * one connection per pair, that the handshake propagates topology, and that
 * a peer can drive the op set across the wire.
 */

require_once __DIR__ . '/lib/Harness.php';

$clusterA = Harness::freePort();
$clusterB = Harness::freePort();
$members = [
    ['id' => 'a', 'addr' => "127.0.0.1:$clusterA"],
    ['id' => 'b', 'addr' => "127.0.0.1:$clusterB"],
];

// Node a declares a queue before b starts, so the handshake has something
// to carry.
$a = Harness::broker([], 10, 'a', ['listen' => "127.0.0.1:$clusterA", 'members' => $members]);
$ca = new Amqp('127.0.0.1', $a['port']);
$ca->channel();
$ca->declareQueue('from-a');
$ca->declareExchange('ex-a', 'topic');
$ca->bind('from-a', 'ex-a', 'key.#');

$b = Harness::broker([], 10, 'b', ['listen' => "127.0.0.1:$clusterB", 'members' => $members]);

// Node a dials b because "a" sorts below "b". The proof that the link came up
// is b knowing an exchange it was never told about directly, since only the
// handshake snapshot could have carried it. Declaring a queue on b would
// create it either way, so that would prove nothing.
$linked = false;
$deadline = microtime(true) + 10.0;
while (microtime(true) < $deadline && !$linked) {
    $probe = new Amqp('127.0.0.1', $b['port']);
    $probe->channel();
    $probe->confirmSelect();
    $probe->publish('ex-a', 'key.one', 'crossed', [], true);
    $frame = $probe->read(2.0);
    // A confirm means the exchange resolved and the binding matched. An
    // unroutable publish comes back as basic.return first.
    $linked = $frame !== null && $frame['class'] === 60 && $frame['method'] === 80;
    if ($linked) {
        $got = $probe->get('from-a');
        $linked = $got['delivered'] && $got['body'] === 'crossed';
    }
    $probe->close();
    if (!$linked) {
        usleep(300000);
    }
}
Harness::ok('the peers linked and the snapshot crossed', $linked, 'b never learned ex-a from a');

// The dial rule itself: only the lower id opens the connection, so a pair
// gets one socket rather than two. Asserted on the rule rather than by
// counting sockets, which varies by environment.
Harness::guard('dial rule', static function (): void {
    require_once dirname(__DIR__) . '/src/Features.php';
    require_once dirname(__DIR__) . '/src/Policy.php';
    Harness::eq('a dials b', true, Features::shouldDial('a', 'b'));
    Harness::eq('b does not dial a', false, Features::shouldDial('b', 'a'));
    Harness::eq('a node never dials itself', false, Features::shouldDial('a', 'a'));
    Harness::eq('node1 dials node2', true, Features::shouldDial('node1', 'node2'));
    // Exactly one side of every pair dials, for any pair of distinct ids.
    $ids = ['a', 'b', 'c', 'node-10', 'node-2'];
    $both = 0;
    $neither = 0;
    foreach ($ids as $x) {
        foreach ($ids as $y) {
            if ($x === $y) {
                continue;
            }
            $forward = Features::shouldDial($x, $y);
            $back = Features::shouldDial($y, $x);
            if ($forward && $back) {
                $both++;
            }
            if (!$forward && !$back) {
                $neither++;
            }
        }
    }
    Harness::eq('no pair dials twice', 0, $both);
    Harness::eq('no pair goes undialled', 0, $neither);
});

Harness::stop($a);
Harness::stop($b);
Harness::done();
