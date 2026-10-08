<?php
declare(strict_types=1);

/**
 * A Raft group as a deterministic state machine, as docs/raft.md specifies.
 *
 * Raft does no I/O. The driver feeds it tick, step and propose, persists
 * what takeDirty reports before sending takeOutbox, and applies takeCommitted
 * in order. Messages are the JSON payloads of section 4, the same shape the
 * Rust and Bun cores send.
 */
final class Raft
{
    public const HEARTBEAT_MS = 150;
    public const ELECTION_MIN_MS = 1000;
    public const ELECTION_MAX_MS = 2000;
    private const MAX_BATCH = 256;

    public int $term = 0;
    public ?string $votedFor = null;
    public string $role = 'follower';
    public ?string $leader = null;
    public int $commit = 0;
    public int $applied = 0;
    /** @var list<array{i:int,term:int,kind:string,data:mixed}> */
    private array $log = [];
    private int $snapIndex = 0;
    private int $snapTerm = 0;
    /** @var list<string> */
    private array $snapVoters = [];
    /** @var list<string> */
    private array $initialVoters;
    /** @var array<string, true> */
    private array $votes = [];
    /** @var array<string, int> */
    private array $nextIndex = [];
    /** @var array<string, int> */
    private array $matchIndex = [];
    private int $heardLeaderAt = 0;
    private int $electionDeadline = 0;
    private int $heartbeatDue = 0;
    private int $rng;
    /** @var list<array{0:string,1:array<string,mixed>}> */
    private array $outbox = [];
    /** @var array{hardState:bool,truncatedFrom:?int,appended:list<array<string,mixed>>,snapshot:?array<string,mixed>} */
    private array $dirty;
    /** @var ?array<string,mixed> */
    private ?array $installed = null;
    /** @var ?callable():mixed */
    public $snapshotSource = null;

    /** @param list<string> $voters */
    public function __construct(public string $id, public string $group, array $voters, int $seed)
    {
        $this->initialVoters = self::sorted($voters);
        $this->rng = $seed | 1;
        $this->dirty = ['hardState' => false, 'truncatedFrom' => null, 'appended' => [], 'snapshot' => null];
        $this->resetElection(0);
    }

    private function rand(): int
    {
        // xorshift64 on native 64-bit ints. `<<` drops the high bits; the
        // right shifts are made logical by masking off the sign extension.
        $x = $this->rng;
        $x ^= ($x >> 12) & (PHP_INT_MAX >> 11);
        $x ^= $x << 25;
        $x ^= ($x >> 27) & (PHP_INT_MAX >> 26);
        $this->rng = $x;
        return (($x >> 1) & PHP_INT_MAX) % 1000000007;
    }

    private function resetElection(int $now): void
    {
        $this->electionDeadline = $now + self::ELECTION_MIN_MS + ($this->rand() % (self::ELECTION_MAX_MS - self::ELECTION_MIN_MS));
    }

    public function lastIndex(): int
    {
        return $this->log === [] ? $this->snapIndex : $this->log[count($this->log) - 1]['i'];
    }

    public function lastTerm(): int
    {
        return $this->log === [] ? $this->snapTerm : $this->log[count($this->log) - 1]['term'];
    }

    public function termAt(int $index): ?int
    {
        if ($index === 0) {
            return 0;
        }
        if ($index === $this->snapIndex) {
            return $this->snapTerm;
        }
        if ($index < $this->snapIndex) {
            return null;
        }
        return $this->log[$index - $this->snapIndex - 1]['term'] ?? null;
    }

    /** @return ?array{i:int,term:int,kind:string,data:mixed} */
    private function entry(int $index): ?array
    {
        if ($index <= $this->snapIndex) {
            return null;
        }
        return $this->log[$index - $this->snapIndex - 1] ?? null;
    }

    /** @return list<string> */
    public function voters(): array
    {
        for ($i = count($this->log) - 1; $i >= 0; $i--) {
            $entry = $this->log[$i];
            if ($entry['kind'] === 'config' && is_array($entry['data']) && isset($entry['data']['voters']) && is_array($entry['data']['voters'])) {
                return self::sorted(array_map('strval', $entry['data']['voters']));
            }
        }
        return $this->snapVoters !== [] ? $this->snapVoters : $this->initialVoters;
    }

    private function quorum(): int
    {
        return intdiv(count($this->voters()), 2) + 1;
    }

    /** @return list<string> */
    private function peers(): array
    {
        return array_values(array_filter($this->voters(), fn (string $id): bool => $id !== $this->id));
    }

    /** @param array<string, mixed> $msg */
    private function send(string $to, array $msg): void
    {
        $this->outbox[] = [$to, $msg];
    }

    /** @return list<array{0:string,1:array<string,mixed>}> */
    public function takeOutbox(): array
    {
        $out = $this->outbox;
        $this->outbox = [];
        return $out;
    }

    /** @return array{hardState:bool,truncatedFrom:?int,appended:list<array<string,mixed>>,snapshot:?array<string,mixed>} */
    public function takeDirty(): array
    {
        $dirty = $this->dirty;
        $this->dirty = ['hardState' => false, 'truncatedFrom' => null, 'appended' => [], 'snapshot' => null];
        return $dirty;
    }

    /** @return ?array<string,mixed> */
    public function takeInstalled(): ?array
    {
        $installed = $this->installed;
        $this->installed = null;
        return $installed;
    }

    /** @return list<array{i:int,term:int,kind:string,data:mixed}> */
    public function takeCommitted(): array
    {
        $out = [];
        while ($this->applied < $this->commit) {
            $entry = $this->entry($this->applied + 1);
            if ($entry === null) {
                break;
            }
            $out[] = $entry;
            $this->applied++;
        }
        return $out;
    }

    private function becomeFollower(int $term, ?string $leader, int $now): void
    {
        if ($term > $this->term) {
            $this->term = $term;
            $this->votedFor = null;
            $this->dirty['hardState'] = true;
        }
        $this->role = 'follower';
        $this->leader = $leader;
        $this->votes = [];
        $this->resetElection($now);
    }

    private function appendLocal(string $kind, mixed $data): int
    {
        $entry = ['i' => $this->lastIndex() + 1, 'term' => $this->term, 'kind' => $kind, 'data' => $data];
        $this->dirty['appended'][] = $entry;
        $this->log[] = $entry;
        return $entry['i'];
    }

    public function propose(string $kind, mixed $data, int $now): ?int
    {
        if ($this->role !== 'leader') {
            return null;
        }
        $index = $this->appendLocal($kind, $data);
        $this->maybeCommit();
        $this->broadcastAppend($now);
        return $index;
    }

    /** @param list<string> $wanted */
    public function reconfigure(array $wanted, int $now): void
    {
        if ($this->role !== 'leader') {
            return;
        }
        foreach ($this->log as $entry) {
            if ($entry['kind'] === 'config' && $entry['i'] > $this->commit) {
                return;
            }
        }
        $current = $this->voters();
        $want = self::sorted($wanted);
        if ($want === [] || $current === $want) {
            return;
        }
        $next = $current;
        $add = null;
        foreach ($want as $id) {
            if (!in_array($id, $current, true)) {
                $add = $id;
                break;
            }
        }
        if ($add !== null) {
            $next[] = $add;
        } else {
            foreach ($current as $id) {
                if (!in_array($id, $want, true)) {
                    $next = array_values(array_filter($next, static fn (string $voter): bool => $voter !== $id));
                    break;
                }
            }
        }
        $this->appendLocal('config', ['voters' => self::sorted($next)]);
        $last = $this->lastIndex();
        foreach ($this->peers() as $peer) {
            $this->nextIndex[$peer] ??= $last;
            $this->matchIndex[$peer] ??= 0;
        }
        $this->maybeCommit();
        $this->broadcastAppend($now);
    }

    public function tick(int $now): void
    {
        if ($this->role === 'leader') {
            if ($now >= $this->heartbeatDue) {
                $this->broadcastAppend($now);
            }
        } elseif ($now >= $this->electionDeadline && in_array($this->id, $this->voters(), true)) {
            $this->startPreVote($now);
        }
    }

    private function startPreVote(int $now): void
    {
        $this->role = 'precandidate';
        $this->leader = null;
        $this->votes = [$this->id => true];
        $this->resetElection($now);
        if (count($this->votes) >= $this->quorum()) {
            $this->startElection($now);
            return;
        }
        $msg = ['t' => 'vote', 'g' => $this->group, 'term' => $this->term + 1, 'cand' => $this->id, 'lli' => $this->lastIndex(), 'llt' => $this->lastTerm(), 'pre' => true];
        foreach ($this->peers() as $peer) {
            $this->send($peer, $msg);
        }
    }

    private function startElection(int $now): void
    {
        $this->role = 'candidate';
        $this->term++;
        $this->votedFor = $this->id;
        $this->dirty['hardState'] = true;
        $this->leader = null;
        $this->votes = [$this->id => true];
        $this->resetElection($now);
        if (count($this->votes) >= $this->quorum()) {
            $this->becomeLeader($now);
            return;
        }
        $msg = ['t' => 'vote', 'g' => $this->group, 'term' => $this->term, 'cand' => $this->id, 'lli' => $this->lastIndex(), 'llt' => $this->lastTerm(), 'pre' => false];
        foreach ($this->peers() as $peer) {
            $this->send($peer, $msg);
        }
    }

    private function becomeLeader(int $now): void
    {
        $this->role = 'leader';
        $this->leader = $this->id;
        $this->nextIndex = [];
        $this->matchIndex = [];
        $next = $this->lastIndex() + 1;
        foreach ($this->peers() as $peer) {
            $this->nextIndex[$peer] = $next;
            $this->matchIndex[$peer] = 0;
        }
        $this->appendLocal('noop', null);
        $this->maybeCommit();
        $this->broadcastAppend($now);
    }

    private function broadcastAppend(int $now): void
    {
        $this->heartbeatDue = $now + self::HEARTBEAT_MS;
        foreach ($this->peers() as $peer) {
            $this->sendAppend($peer);
        }
    }

    private function sendAppend(string $peer): void
    {
        $next = $this->nextIndex[$peer] ?? ($this->lastIndex() + 1);
        if ($next <= $this->snapIndex) {
            $state = $this->snapshotSource !== null ? ($this->snapshotSource)() : null;
            $voters = $this->snapVoters !== [] ? $this->snapVoters : $this->voters();
            $this->send($peer, ['t' => 'snap', 'g' => $this->group, 'term' => $this->term, 'leader' => $this->id, 'lii' => $this->snapIndex, 'lit' => $this->snapTerm, 'voters' => $voters, 'state' => $state]);
            return;
        }
        $pli = $next - 1;
        $entries = [];
        for ($i = $next; $i <= $this->lastIndex() && count($entries) < self::MAX_BATCH; $i++) {
            $entry = $this->entry($i);
            if ($entry !== null) {
                $entries[] = $entry;
            }
        }
        $this->send($peer, ['t' => 'append', 'g' => $this->group, 'term' => $this->term, 'leader' => $this->id, 'pli' => $pli, 'plt' => $this->termAt($pli) ?? 0, 'entries' => $entries, 'commit' => $this->commit]);
    }

    private function maybeCommit(): void
    {
        if ($this->role !== 'leader') {
            return;
        }
        $matched = [];
        foreach ($this->voters() as $voter) {
            $matched[] = $voter === $this->id ? $this->lastIndex() : ($this->matchIndex[$voter] ?? 0);
        }
        rsort($matched);
        $n = $matched[$this->quorum() - 1] ?? null;
        if ($n !== null && $n > $this->commit && $this->termAt($n) === $this->term) {
            $this->commit = $n;
        }
    }

    /** @param array<string, mixed> $msg */
    public function step(string $from, array $msg, int $now): void
    {
        $type = (string) ($msg['t'] ?? '');
        $term = (int) ($msg['term'] ?? 0);
        $pre = ($msg['pre'] ?? false) === true;
        $preVote = ($type === 'vote' || $type === 'vote_r') && $pre;
        if (!$preVote && $term > $this->term && in_array($type, ['vote', 'vote_r', 'append', 'append_r', 'snap', 'snap_r'], true)) {
            $this->becomeFollower($term, $type === 'append' || $type === 'snap' ? $from : null, $now);
        }
        if ($type === 'vote') {
            $this->onVote($from, $msg, $term, $pre, $now);
        } elseif ($type === 'vote_r') {
            $this->onVoteReply($from, $msg, $term, $pre, $now);
        } elseif ($type === 'append') {
            $this->onAppend($from, $msg, $term, $now);
        } elseif ($type === 'append_r') {
            $this->onAppendReply($from, $msg, $term);
        } elseif ($type === 'snap') {
            $this->onSnap($from, $msg, $term, $now);
        } elseif ($type === 'snap_r') {
            $this->onSnapReply($from, $msg, $term);
        }
    }

    private function logOk(int $lli, int $llt): bool
    {
        return $llt > $this->lastTerm() || ($llt === $this->lastTerm() && $lli >= $this->lastIndex());
    }

    /** @param array<string, mixed> $msg */
    private function onVote(string $from, array $msg, int $term, bool $pre, int $now): void
    {
        $cand = is_string($msg['cand'] ?? null) ? $msg['cand'] : $from;
        $lli = (int) ($msg['lli'] ?? 0);
        $llt = (int) ($msg['llt'] ?? 0);
        if ($pre) {
            $leaderLive = $this->role === 'leader' || ($this->leader !== null && $now < $this->heardLeaderAt + self::ELECTION_MIN_MS);
            $granted = $term > $this->term && $this->logOk($lli, $llt) && !$leaderLive;
        } else {
            $granted = $term === $this->term && ($this->votedFor === null || $this->votedFor === $cand) && $this->logOk($lli, $llt);
        }
        if ($granted && !$pre) {
            $this->votedFor = $cand;
            $this->dirty['hardState'] = true;
            $this->resetElection($now);
        }
        $replyTerm = $pre ? ($granted ? $term : max($term, $this->term)) : $this->term;
        $this->send($from, ['t' => 'vote_r', 'g' => $this->group, 'term' => $replyTerm, 'granted' => $granted, 'pre' => $pre]);
    }

    /** @param array<string, mixed> $msg */
    private function onVoteReply(string $from, array $msg, int $term, bool $pre, int $now): void
    {
        if (($msg['granted'] ?? false) !== true) {
            return;
        }
        if ($pre) {
            if ($this->role !== 'precandidate' || $term !== $this->term + 1) {
                return;
            }
            $this->votes[$from] = true;
            if (count($this->votes) >= $this->quorum()) {
                $this->startElection($now);
            }
        } else {
            if ($this->role !== 'candidate' || $term !== $this->term) {
                return;
            }
            $this->votes[$from] = true;
            if (count($this->votes) >= $this->quorum()) {
                $this->becomeLeader($now);
            }
        }
    }

    /** @param array<string, mixed> $msg */
    private function onAppend(string $from, array $msg, int $term, int $now): void
    {
        if ($term < $this->term) {
            $this->send($from, ['t' => 'append_r', 'g' => $this->group, 'term' => $this->term, 'ok' => false, 'hint' => $this->lastIndex() + 1]);
            return;
        }
        if ($this->role !== 'follower' || $this->leader !== $from) {
            $this->role = 'follower';
            $this->leader = $from;
            $this->votes = [];
        }
        $this->heardLeaderAt = $now;
        $this->resetElection($now);
        $pli = (int) ($msg['pli'] ?? 0);
        $plt = (int) ($msg['plt'] ?? 0);
        $leaderCommit = (int) ($msg['commit'] ?? 0);
        if ($pli > $this->lastIndex()) {
            $this->send($from, ['t' => 'append_r', 'g' => $this->group, 'term' => $this->term, 'ok' => false, 'hint' => $this->lastIndex() + 1]);
            return;
        }
        if ($pli >= $this->snapIndex && $this->termAt($pli) !== $plt) {
            $bad = $this->termAt($pli) ?? 0;
            $hint = $pli;
            while ($hint > $this->snapIndex + 1 && $this->termAt($hint - 1) === $bad) {
                $hint--;
            }
            $this->send($from, ['t' => 'append_r', 'g' => $this->group, 'term' => $this->term, 'ok' => false, 'hint' => max($hint, $this->snapIndex + 1)]);
            return;
        }
        $entries = is_array($msg['entries'] ?? null) ? $msg['entries'] : [];
        $lastNew = $pli;
        foreach ($entries as $entry) {
            if (!is_array($entry)) {
                continue;
            }
            $lastNew = (int) $entry['i'];
            if ($lastNew <= $this->snapIndex) {
                continue;
            }
            $have = $this->termAt($lastNew);
            if ($have === (int) $entry['term']) {
                continue;
            }
            if ($have !== null) {
                $this->log = array_values(array_slice($this->log, 0, $lastNew - $this->snapIndex - 1));
                $this->dirty['appended'] = array_values(array_filter(
                    $this->dirty['appended'],
                    static fn (array $row): bool => $row['i'] < $entry['i'],
                ));
                $this->dirty['truncatedFrom'] = $this->dirty['truncatedFrom'] === null ? $lastNew : min($this->dirty['truncatedFrom'], $lastNew);
            }
            $copy = ['i' => $lastNew, 'term' => (int) $entry['term'], 'kind' => (string) $entry['kind'], 'data' => $entry['data'] ?? null];
            $this->dirty['appended'][] = $copy;
            $this->log[] = $copy;
        }
        if ($leaderCommit > $this->commit) {
            $this->commit = max($this->commit, min($leaderCommit, $lastNew));
        }
        $this->send($from, ['t' => 'append_r', 'g' => $this->group, 'term' => $this->term, 'ok' => true, 'match' => $lastNew]);
    }

    /** @param array<string, mixed> $msg */
    private function onAppendReply(string $from, array $msg, int $term): void
    {
        if ($this->role !== 'leader' || $term !== $this->term) {
            return;
        }
        if (($msg['ok'] ?? false) === true) {
            $match = (int) ($msg['match'] ?? 0);
            $prev = $this->matchIndex[$from] ?? 0;
            if ($match > $prev) {
                $this->matchIndex[$from] = $match;
            }
            $this->nextIndex[$from] = max($match, $prev) + 1;
            $this->maybeCommit();
            if (($this->nextIndex[$from] ?? 0) <= $this->lastIndex()) {
                $this->sendAppend($from);
            }
        } else {
            $hint = max(1, (int) ($msg['hint'] ?? 1));
            $this->nextIndex[$from] = min($hint, $this->lastIndex() + 1);
            $this->sendAppend($from);
        }
    }

    /** @param array<string, mixed> $msg */
    private function onSnap(string $from, array $msg, int $term, int $now): void
    {
        if ($term < $this->term) {
            $this->send($from, ['t' => 'snap_r', 'g' => $this->group, 'term' => $this->term, 'lii' => 0]);
            return;
        }
        $this->role = 'follower';
        $this->leader = $from;
        $this->heardLeaderAt = $now;
        $this->resetElection($now);
        $lii = (int) ($msg['lii'] ?? 0);
        $lit = (int) ($msg['lit'] ?? 0);
        if ($lii > $this->commit) {
            $voters = is_array($msg['voters'] ?? null) ? array_map('strval', $msg['voters']) : [];
            $snap = ['index' => $lii, 'term' => $lit, 'voters' => $voters, 'state' => $msg['state'] ?? null];
            if ($this->termAt($lii) === $lit) {
                $this->log = array_values(array_filter($this->log, static fn (array $entry): bool => $entry['i'] > $lii));
            } else {
                $this->log = [];
            }
            $this->snapIndex = $lii;
            $this->snapTerm = $lit;
            $this->snapVoters = self::sorted($voters);
            $this->commit = $lii;
            $this->applied = $lii;
            $this->dirty['snapshot'] = $snap;
            $this->installed = $snap;
        }
        $this->send($from, ['t' => 'snap_r', 'g' => $this->group, 'term' => $this->term, 'lii' => $lii]);
    }

    /** @param array<string, mixed> $msg */
    private function onSnapReply(string $from, array $msg, int $term): void
    {
        if ($this->role !== 'leader' || $term !== $this->term) {
            return;
        }
        $lii = (int) ($msg['lii'] ?? 0);
        if ($lii > ($this->matchIndex[$from] ?? 0)) {
            $this->matchIndex[$from] = $lii;
        }
        $this->nextIndex[$from] = $lii + 1;
        $this->maybeCommit();
    }

    /** @return ?array{index:int,term:int,voters:list<string>,state:mixed} */
    public function compact(mixed $state): ?array
    {
        if ($this->applied <= $this->snapIndex) {
            return null;
        }
        $index = $this->applied;
        $term = $this->termAt($index);
        if ($term === null) {
            return null;
        }
        $voters = $this->votersAt($index);
        $this->log = array_values(array_filter($this->log, static fn (array $entry): bool => $entry['i'] > $index));
        $this->snapIndex = $index;
        $this->snapTerm = $term;
        $this->snapVoters = $voters;
        return ['index' => $index, 'term' => $term, 'voters' => $voters, 'state' => $state];
    }

    /** @return list<string> */
    private function votersAt(int $index): array
    {
        for ($i = count($this->log) - 1; $i >= 0; $i--) {
            $entry = $this->log[$i];
            if ($entry['i'] > $index || $entry['kind'] !== 'config') {
                continue;
            }
            if (is_array($entry['data']) && isset($entry['data']['voters']) && is_array($entry['data']['voters'])) {
                return self::sorted(array_map('strval', $entry['data']['voters']));
            }
        }
        return $this->snapVoters !== [] ? $this->snapVoters : $this->initialVoters;
    }

    /** @param list<string> $ids */
    private static function sorted(array $ids): array
    {
        $ids = array_values(array_unique($ids));
        sort($ids);
        return $ids;
    }
}
