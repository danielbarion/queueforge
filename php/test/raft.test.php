<?php
declare(strict_types=1);

/** The same in-memory cluster the Rust and Bun Raft cores are tested with. */

require_once __DIR__ . '/lib/Harness.php';
require_once dirname(__DIR__) . '/src/Raft.php';

final class RaftSim
{
    /** @var array<string, Raft> */
    public array $nodes = [];
    /** @var array<string, true> */
    public array $down = [];
    public int $now = 0;
    /** @var array<string, list<mixed>> */
    public array $applied = [];

    /** @param list<string> $ids */
    public function __construct(array $ids)
    {
        foreach ($ids as $i => $id) {
            $this->nodes[$id] = new Raft($id, 'meta', $ids, 7 + $i * 1000);
        }
    }

    public function run(int $ms): void
    {
        $end = $this->now + $ms;
        while ($this->now < $end) {
            $this->now += 10;
            foreach ($this->nodes as $id => $node) {
                if (!isset($this->down[$id])) {
                    $node->tick($this->now);
                }
            }
            $this->flush();
        }
    }

    public function flush(): void
    {
        do {
            $moved = false;
            foreach ($this->nodes as $id => $node) {
                $node->takeDirty();
                $out = $node->takeOutbox();
                foreach ($node->takeCommitted() as $entry) {
                    if ($entry['kind'] !== 'noop' && $entry['kind'] !== 'config') {
                        $this->applied[$id][] = $entry['data'];
                    }
                }
                foreach ($out as [$to, $msg]) {
                    if (isset($this->down[$id]) || isset($this->down[$to]) || !isset($this->nodes[$to])) {
                        continue;
                    }
                    $this->nodes[$to]->step($id, $msg, $this->now);
                    $moved = true;
                }
            }
        } while ($moved);
    }

    /** @return list<string> */
    public function leaders(): array
    {
        $rows = [];
        foreach ($this->nodes as $node) {
            if ($node->role === 'leader' && !isset($this->down[$node->id])) {
                $rows[] = [$node->term, $node->id];
            }
        }
        usort($rows, static fn (array $a, array $b): int => $a[0] <=> $b[0] ?: $a[1] <=> $b[1]);
        return array_map(static fn (array $row): string => $row[1], $rows);
    }

    public function leader(): string
    {
        $live = $this->leaders();
        Harness::eq('one live leader', 1, count($live));
        return $live[0];
    }

    public function propose(mixed $data): void
    {
        $id = $this->leader();
        $index = $this->nodes[$id]->propose('x', $data, $this->now);
        Harness::ok('the leader accepted the proposal', $index !== null);
        $this->flush();
    }

    /** @return list<mixed> */
    public function values(string $id): array
    {
        return $this->applied[$id] ?? [];
    }
}

Harness::guard('elects one leader and replicates', static function (): void {
    $sim = new RaftSim(['a', 'b', 'c']);
    $sim->run(3000);
    $sim->propose(1);
    $sim->propose(2);
    $sim->run(500);
    foreach (['a', 'b', 'c'] as $id) {
        Harness::eq("replicated on $id", [1, 2], $sim->values($id));
    }
});

Harness::guard('a new leader keeps committed entries', static function (): void {
    $sim = new RaftSim(['a', 'b', 'c']);
    $sim->run(3000);
    $sim->propose('before');
    $sim->run(300);
    $old = $sim->leader();
    $sim->down[$old] = true;
    $sim->run(4000);
    $next = $sim->leader();
    Harness::ok('the new leader is someone else', $next !== $old);
    $sim->propose('after');
    $sim->run(300);
    $sim->down = [];
    $sim->run(1000);
    foreach (['a', 'b', 'c'] as $id) {
        Harness::eq("log on $id", ['before', 'after'], $sim->values($id));
    }
});

Harness::guard('a minority cannot commit', static function (): void {
    $sim = new RaftSim(['a', 'b', 'c']);
    $sim->run(3000);
    $leader = $sim->leader();
    foreach (['a', 'b', 'c'] as $id) {
        if ($id !== $leader) {
            $sim->down[$id] = true;
        }
    }
    $sim->nodes[$leader]->propose('x', 'lost', $sim->now);
    $sim->run(500);
    Harness::eq('the isolated leader committed nothing', [], $sim->values($leader));
    $sim->down = [$leader => true];
    $sim->run(4000);
    $sim->propose('kept');
    $sim->down = [];
    $sim->run(2000);
    foreach (['a', 'b', 'c'] as $id) {
        Harness::eq("kept on $id", ['kept'], $sim->values($id));
    }
});

Harness::guard('a partitioned node does not bump the term', static function (): void {
    $sim = new RaftSim(['a', 'b', 'c']);
    $sim->run(3000);
    $leader = $sim->leader();
    $term = $sim->nodes[$leader]->term;
    foreach (['a', 'b', 'c'] as $id) {
        if ($id !== $leader) {
            $sim->down[$id] = true;
            break;
        }
    }
    $sim->run(6000);
    $sim->down = [];
    $sim->run(1000);
    Harness::eq('the same leader', $leader, $sim->leader());
    Harness::eq('the same term', $term, $sim->nodes[$leader]->term);
});

Harness::guard('a lagging follower catches up from a snapshot', static function (): void {
    $sim = new RaftSim(['a', 'b', 'c']);
    $sim->run(3000);
    $leader = $sim->leader();
    $lag = '';
    foreach (['a', 'b', 'c'] as $id) {
        if ($id !== $leader) {
            $lag = $id;
            break;
        }
    }
    $sim->down[$lag] = true;
    for ($i = 0; $i < 20; $i++) {
        $sim->propose($i);
    }
    $sim->run(300);
    $node = $sim->nodes[$leader];
    $snap = $node->compact(['count' => 20]);
    Harness::ok('the leader compacted', $snap !== null);
    $node->snapshotSource = static fn (): array => ['count' => 20];
    Harness::ok('the snapshot covers the proposals', $snap['index'] >= 20);
    $sim->down = [];
    $sim->run(1000);
    Harness::ok('the follower installed the snapshot', $sim->nodes[$lag]->takeInstalled() !== null);
    Harness::ok('the follower commit caught up', $sim->nodes[$lag]->commit >= $snap['index']);
    $sim->propose('next');
    $sim->run(300);
    $values = $sim->values($lag);
    Harness::eq('the follower applied the next entry', 'next', $values[count($values) - 1] ?? null);
});

Harness::guard('membership changes one voter at a time', static function (): void {
    $sim = new RaftSim(['a', 'b', 'c']);
    $sim->nodes['d'] = new Raft('d', 'meta', ['a', 'b', 'c'], 99);
    $sim->run(3000);
    $leader = $sim->leader();
    $wanted = ['a', 'b', 'c', 'd'];
    for ($i = 0; $i < 4; $i++) {
        $sim->nodes[$leader]->reconfigure($wanted, $sim->now);
        $sim->run(300);
    }
    foreach ($wanted as $id) {
        Harness::eq("voters on $id", $wanted, $sim->nodes[$id]->voters());
    }
    $sim->propose('four');
    $sim->run(300);
    Harness::eq('d applied the entry', ['four'], $sim->values('d'));
});

Harness::guard('a single voter commits alone', static function (): void {
    $sim = new RaftSim(['solo']);
    $sim->run(2500);
    $sim->propose(1);
    Harness::eq('solo applied it', [1], $sim->values('solo'));
});

Harness::done();
