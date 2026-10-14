<?php
declare(strict_types=1);

/**
 * Three live PHP nodes with Raft. Confirmed quorum messages survive a
 * kill -9 of the queue's Raft leader, the two left keep taking
 * confirmed publishes, and every body is delivered once, as in
 * bun/test/quorum-failover.test.ts.
 */

require_once __DIR__ . '/lib/Harness.php';

$ids = ['a', 'b', 'c'];
$ports = [];
foreach ($ids as $id) $ports[$id] = Harness::freePort();
$members = array_map(static fn (string $id): array => ['id' => $id, 'addr' => '127.0.0.1:' . $ports[$id]], $ids);
$nodes = [];
foreach ($ids as $id) $nodes[$id] = Harness::broker([], 10, $id, ['listen' => '127.0.0.1:' . $ports[$id], 'members' => $members]);

/** A confirmed publish of each body to `qq` through `$port`; false if any is not confirmed in time. */
$publish = static function (int $port, array $bodies): bool {
    try {
        $p = new Amqp('127.0.0.1', $port);
        $p->channel();
        $p->confirmSelect();
        foreach ($bodies as $body) $p->publish('', 'qq', $body, ['deliveryMode' => 2]);
        $confirmed = 0;
        $nacked = false;
        $until = microtime(true) + 8.0;
        while ($confirmed < count($bodies) && microtime(true) < $until) {
            $frame = $p->read(max(0.05, $until - microtime(true)));
            if ($frame === null || $frame['class'] !== 60 || !in_array($frame['method'], [80, 120], true)) continue;
            if ($frame['method'] === 120) $nacked = true;
            $tag = Codec::readU64($frame['args'], 0);
            $multiple = (ord($frame['args'][8] ?? "\0") & 1) === 1;
            $confirmed = $multiple ? max($confirmed, $tag) : $confirmed + 1;
        }
        $p->close();
        return !$nacked && $confirmed === count($bodies);
    } catch (Throwable) {
        return false;
    }
};

/**
 * The node that won the newest term of the queue's Raft group: a majority
 * persisted a vote for it in that term. Null while no term has a winner.
 */
$leaderOf = static function (array $nodes): ?string {
    $votes = [];
    foreach ($nodes as $id => $node) {
        $dirs = glob($node['dir'] . '/data/raft/q-*', GLOB_ONLYDIR) ?: [];
        $file = ($dirs[0] ?? $node['dir'] . '/data/raft/quorum') . '/state.json';
        $state = json_decode((string) @file_get_contents($file), true);
        if (is_array($state) && is_string($state['vote'] ?? null)) $votes[(int) $state['term']][$state['vote']][] = $id;
    }
    krsort($votes);
    foreach ($votes as $byCandidate) foreach ($byCandidate as $candidate => $voters) if (count($voters) >= 2) return (string) $candidate;
    return null;
};

/** Every body `qq` hands out through any of `$ports` within `$seconds`, acked. */
$drain = static function (array $ports, float $seconds): array {
    $got = [];
    $until = microtime(true) + $seconds;
    $idle = 0;
    while (microtime(true) < $until && $idle < 3) {
        $any = false;
        foreach ($ports as $port) {
            try {
                $c = new Amqp('127.0.0.1', $port);
                $c->channel();
                while (true) {
                    $m = $c->get('qq');
                    if (!$m['delivered']) break;
                    $got[] = $m['body'];
                    $c->ack($m['tag']);
                    $any = true;
                }
                $c->close();
            } catch (Throwable) {
                // A node that does not lead the queue yet, or is still electing.
            }
        }
        if ($any) $idle = 0; else { $idle++; usleep(300000); }
    }
    return $got;
};

Harness::guard('confirmed quorum messages survive a kill -9 of the leader', static function () use (&$nodes, $publish, $drain, $leaderOf): void {
    // Raft turns on once the members have seen each other; until then a
    // quorum declare can be refused, so retry.
    $declared = false;
    $until = microtime(true) + 15.0;
    while (!$declared && microtime(true) < $until) {
        try {
            $s = new Amqp('127.0.0.1', $nodes['a']['port']);
            $s->channel();
            $s->declareQueue('qq', true, ['x-queue-type' => 'quorum']);
            $s->close();
            $declared = true;
        } catch (Throwable) {
            usleep(300000);
        }
    }
    Harness::ok('quorum queue declared', $declared, Harness::stderr($nodes['a']));

    $first = array_map(static fn (int $i): string => "first-$i", range(0, 99));
    $ok = false;
    for ($try = 0; $try < 20 && !$ok; $try++) {
        $ok = $publish($nodes['a']['port'], $first);
        if (!$ok) usleep(300000);
    }
    Harness::ok('100 publishes confirmed on a', $ok, Harness::stderr($nodes['a']));

    $leader = $leaderOf($nodes);
    Harness::ok('the queue has a leader', $leader !== null);
    Harness::stop($nodes[$leader], true);
    $left = array_values(array_diff(array_keys($nodes), [$leader]));

    // The two left elect a new leader for the queue and take publishes again.
    $second = array_map(static fn (int $i): string => "second-$i", range(0, 49));
    $ok = false;
    $until = microtime(true) + 20.0;
    while (!$ok && microtime(true) < $until) {
        foreach ($left as $id) {
            if ($publish($nodes[$id]['port'], $second)) { $ok = true; break; }
        }
        if (!$ok) usleep(300000);
    }
    Harness::ok("after killing leader $leader, the others confirm 50 more", $ok, Harness::stderr($nodes[$left[0]]) . "\n" . Harness::stderr($nodes[$left[1]]));

    // A retried batch may have been stored before its confirm was lost, so
    // the second round may arrive more than once; the first must not.
    $got = $drain([$nodes[$left[0]]['port'], $nodes[$left[1]]['port']], 20.0);
    $firstGot = array_values(array_filter($got, static fn (string $b): bool => str_starts_with($b, 'first-')));
    sort($firstGot);
    $want = $first;
    sort($want);
    Harness::eq('every first-round body delivered once', $want, $firstGot);
    $secondGot = array_values(array_unique(array_filter($got, static fn (string $b): bool => str_starts_with($b, 'second-'))));
    Harness::eq('every second-round body delivered', 50, count($secondGot));
});

foreach ($nodes as $node) Harness::stop($node, true);
Harness::done();
