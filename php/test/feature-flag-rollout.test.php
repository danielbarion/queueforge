<?php
declare(strict_types=1);

/**
 * Feature flag rollout and rolling upgrades in a PHP Raft cluster.
 *
 * Node c starts as an "older build" that lacks the stream_filtering flag
 * (QUEUEFORGE_UNSUPPORTED_FLAGS), so enabling the flag is refused while c
 * is a voter. c is then upgraded in place (restarted on the same data
 * without the override) and the flag is enabled: every node reports it,
 * a restarted node keeps it, and a node that joins afterwards gets it from
 * the meta log. Around that, every node is restarted one at a time and the
 * quorum queue, a durable classic queue and a replicated stream keep their
 * messages.
 */

require_once __DIR__ . '/lib/Harness.php';

$ids = ['a', 'b', 'c', 'd'];
$cluster = []; $mgmt = [];
foreach ($ids as $id) { $cluster[$id] = Harness::freePort(); $mgmt[$id] = Harness::freePort(); }
$member = static fn (string $id): array => ['id' => $id, 'addr' => '127.0.0.1:' . $cluster[$id]];
$three = array_map($member, ['a', 'b', 'c']);
$nodes = [];
foreach (['a', 'b', 'c'] as $id) {
    $nodes[$id] = Harness::broker(["management = \"127.0.0.1:{$mgmt[$id]}\""], 10, $id, ['listen' => '127.0.0.1:' . $cluster[$id], 'members' => $three], $id === 'c' ? ['QUEUEFORGE_UNSUPPORTED_FLAGS' => 'stream_filtering'] : []);
}
foreach ($nodes as $id => $node) Harness::waitListening($mgmt[$id], $node['dir']);

$flagOn = static function (string $id, string $flag = 'stream_filtering') use ($mgmt): ?bool {
    foreach ((array) (Harness::http($mgmt[$id], 'GET', '/api/feature-flags')['json'] ?? []) as $row) if (($row['name'] ?? '') === $flag) return $row['state'] === 'enabled';
    return null;
};
$ready = static fn (string $id): bool => Harness::until(static fn () => str_contains(Harness::http($mgmt[$id], 'GET', '/readyz', null, '')['body'], 'ready'), 20.0);
$raftOn = static fn (string $id): bool => Harness::until(static fn () => $flagOn($id, 'raft') === true, 20.0);
$publishConfirmed = static function (int $port, string $queue, array $bodies): bool {
    try {
        $p = new Amqp('127.0.0.1', $port); $p->channel(); $p->confirmSelect();
        foreach ($bodies as $b) $p->publish('', $queue, $b, ['deliveryMode' => 2]);
        $confirmed = 0; $until = microtime(true) + 8;
        while ($confirmed < count($bodies) && microtime(true) < $until) {
            $f = $p->read(0.5);
            if ($f === null || $f['class'] !== 60) continue;
            if ($f['method'] === 120) return false;
            if ($f['method'] !== 80) continue;
            $tag = Codec::readU64($f['args'], 0);
            $confirmed = (ord($f['args'][8] ?? "\0") & 1) ? max($confirmed, $tag) : $confirmed + 1;
        }
        $p->close();
        return $confirmed === count($bodies);
    } catch (Throwable) { return false; }
};
/** Retries a confirmed publish through each node until one accepts it. */
$publishAnywhere = static function (string $queue, array $bodies) use (&$nodes, $publishConfirmed): bool {
    $until = microtime(true) + 20;
    while (microtime(true) < $until) {
        foreach ($nodes as $node) if ($publishConfirmed($node['port'], $queue, $bodies)) return true;
        usleep(300000);
    }
    return false;
};
/** The depth the queue reports; a classic queue counts only at its home, so the highest wins. */
$messages = static function (string $queue) use (&$nodes): ?int {
    $best = null;
    foreach ($nodes as $node) {
        try { $c = new Amqp('127.0.0.1', $node['port']); $c->channel(); $n = $c->declareQueue($queue, true, [], 1, true)['messages']; $c->close(); $best = max($best ?? 0, $n); } catch (Throwable) {}
    }
    return $best;
};

Harness::guard('cluster forms with Raft', static function () use ($raftOn): void {
    foreach (['a', 'b', 'c'] as $id) Harness::ok("raft enabled on $id", $raftOn($id));
});

Harness::guard('seed data', static function () use ($nodes, $publishAnywhere, $messages): void {
    $c = new Amqp('127.0.0.1', $nodes['a']['port']); $c->channel();
    $c->declareQueue('roll-qq', true, ['x-queue-type' => 'quorum']);
    $c->declareQueue('roll-cq', true);
    $c->declareQueue('roll-st', true, ['x-queue-type' => 'stream']);
    $c->close();
    Harness::ok('quorum messages confirmed', $publishAnywhere('roll-qq', ['q1', 'q2', 'q3']));
    Harness::ok('classic messages confirmed', $publishAnywhere('roll-cq', ['c1', 'c2']));
    Harness::ok('stream messages confirmed', $publishAnywhere('roll-st', ['s1', 's2', 's3', 's4']));
    Harness::ok('quorum depth is visible', Harness::until(static fn () => $messages('roll-qq') === 3, 10.0));
});

Harness::guard('a flag an older node lacks cannot be enabled', static function () use ($mgmt, $flagOn): void {
    Harness::eq('the older node does not list it', null, $flagOn('c'));
    $off = Harness::http($mgmt['a'], 'PUT', '/api/feature-flags/stream_filtering/disable');
    Harness::eq('the flag is disabled cluster-wide', 204, $off['status']);
    Harness::ok('every supporting node reports it disabled', Harness::until(static fn () => $flagOn('a') === false && $flagOn('b') === false, 10.0));
    $on = Harness::http($mgmt['a'], 'PUT', '/api/feature-flags/stream_filtering/enable');
    Harness::ok('enabling is refused while c lacks it', $on['status'] === 400 && str_contains($on['body'], 'not supported'), $on['body']);
    Harness::ok('and it stays disabled', $flagOn('a') === false && $flagOn('b') === false);
});

$restart = static function (string $id) use (&$nodes, $mgmt, $ready, $raftOn): bool {
    $nodes[$id] = Harness::restart($nodes[$id]);
    Harness::waitListening($mgmt[$id], $nodes[$id]['dir']);
    return $ready($id) && $raftOn($id);
};

Harness::guard('rolling upgrade of the older node, then the flag rolls out', static function () use ($restart, $mgmt, $flagOn): void {
    Harness::ok('c restarts on the newer build and rejoins', $restart('c'));
    $enabled = Harness::until(static fn () => Harness::http($mgmt['b'], 'PUT', '/api/feature-flags/stream_filtering/enable')['status'] === 204, 15.0);
    Harness::ok('enabling succeeds once every voter supports it', $enabled);
    Harness::ok('every node reports it enabled', Harness::until(static fn () => $flagOn('a') && $flagOn('b') && $flagOn('c'), 10.0));
});

Harness::guard('a restarted node keeps an enabled flag', static function () use ($restart, $flagOn): void {
    Harness::ok('b restarts', $restart('b'));
    Harness::eq('b still reports it enabled', true, $flagOn('b'));
});

Harness::guard('rolling restart of every node keeps queues and data', static function () use ($restart, $publishAnywhere, $messages, &$nodes): void {
    $n = 0;
    foreach (['a', 'b', 'c'] as $id) {
        Harness::ok("$id restarts and rejoins", $restart($id));
        $n++;
        Harness::ok("quorum publish confirmed after $id restarted", $publishAnywhere('roll-qq', ["after-$id"]));
        Harness::ok("quorum depth after $id restarted", Harness::until(static fn () => $messages('roll-qq') === 3 + $n, 15.0), (string) $messages('roll-qq'));
        Harness::ok("classic queue keeps its messages after $id restarted", Harness::until(static fn () => $messages('roll-cq') === 2, 15.0), (string) $messages('roll-cq'));
    }
    $got = [];
    Harness::until(static function () use (&$nodes, &$got): bool {
        foreach ($nodes as $node) {
            try { $c = new Amqp('127.0.0.1', $node['port']); $c->channel(); while (($m = $c->get('roll-qq'))['delivered']) { $got[] = $m['body']; $c->ack($m['tag']); } $c->close(); } catch (Throwable) {}
        }
        return count($got) >= 6;
    }, 15.0);
    sort($got);
    Harness::eq('every quorum message is delivered once', ['after-a', 'after-b', 'after-c', 'q1', 'q2', 'q3'], $got);
    $classic = [];
    Harness::until(static function () use (&$nodes, &$classic): bool {
        foreach ($nodes as $node) {
            try { $c = new Amqp('127.0.0.1', $node['port']); $c->channel(); while (($m = $c->get('roll-cq'))['delivered']) { $classic[] = $m['body']; $c->ack($m['tag']); } $c->close(); } catch (Throwable) {}
        }
        return count($classic) >= 2;
    }, 15.0);
    Harness::eq('the durable classic queue delivers its messages', ['c1', 'c2'], $classic);
    $stream = [];
    Harness::until(static function () use (&$nodes, &$stream): bool {
        foreach ($nodes as $node) {
            $stream = [];
            try {
                $c = new Amqp('127.0.0.1', $node['port']); $c->channel(); $c->qos(100);
                $c->consume('roll-st', '', false, 1, extra: ['x-stream-offset' => 'first']);
                $until = microtime(true) + 1.5;
                while (microtime(true) < $until && ($f = $c->read(0.3)) !== null) if ($f['class'] === 60 && $f['method'] === 60) $stream[] = $f['body'];
                $c->close();
                if (count($stream) === 4) return true;
            } catch (Throwable) {}
        }
        return false;
    }, 15.0);
    Harness::eq('the stream keeps its records in order', ['s1', 's2', 's3', 's4'], $stream);
});

Harness::guard('a node that joins later gets the enabled flag', static function () use (&$nodes, $member, $three, $cluster, $mgmt, $flagOn, $ready, $raftOn): void {
    $four = [...$three, $member('d')];
    $added = Harness::http($mgmt['a'], 'POST', '/api/nodes', ['id' => 'd', 'addr' => '127.0.0.1:' . $cluster['d']]);
    Harness::ok('d is added to the membership', in_array($added['status'], [200, 201], true), $added['body']);
    $nodes['d'] = Harness::broker(["management = \"127.0.0.1:{$mgmt['d']}\""], 10, 'd', ['listen' => '127.0.0.1:' . $cluster['d'], 'members' => $four]);
    Harness::waitListening($mgmt['d'], $nodes['d']['dir']);
    Harness::ok('d enables Raft', $raftOn('d'));
    Harness::ok('d reports the flag enabled', Harness::until(static fn () => $flagOn('d') === true, 20.0));
});

foreach ($nodes as $node) Harness::stop($node);
Harness::done();
