<?php
declare(strict_types=1);

require_once __DIR__ . '/Raft.php';

/** Durable runtime for the shared Raft JSON protocol. All I/O precedes replies. */
final class RaftNode
{
    public const PROPOSE_TIMEOUT_MS = 5000;
    /** @var array<string,array<string,mixed>> */
    private array $groups = [];
    private array $pending = [];
    private array $wanted;
    private int $sequence = 0;
    private bool $ticking = false;

    public function __construct(
        public string $id,
        public string $dir,
        array $voters,
        private $send,
        private $apply,
        private $install,
        private $state,
        private $leaderChanged = null,
        private $wantGroup = null,
    ) {
        $this->wanted = $this->sorted($voters === [] ? [$id] : $voters);
        $this->addGroup('meta');
        $this->addGroup('quorum');
        foreach (glob($dir . '/q-*/group.json') ?: [] as $file) {
            $row = $this->readJson($file, false);
            if (!is_array($row) || !is_string($row['group'] ?? null) || !str_starts_with($row['group'], 'q:')) throw new RuntimeException('invalid persisted Raft group');
            $this->addGroup($row['group']);
        }
    }

    public static function queueGroup(string $vhost, string $name): string
    {
        return 'q:v2:' . bin2hex($vhost) . ':' . bin2hex($name);
    }

    public static function groupDir(string $group): string
    {
        if ($group === 'meta' || $group === 'quorum') return $group;
        $hi = 0xcbf29ce4;
        $lo = 0x84222325;
        for ($i = 0, $n = strlen($group); $i < $n; $i++) {
            $lo ^= ord($group[$i]);
            $low = $lo * 0x1b3;
            $hi = ($hi * 0x1b3 + ($low >> 32) + ($lo << 8)) & 0xffffffff;
            $lo = $low & 0xffffffff;
        }
        return 'q-' . sprintf('%08x%08x', $hi, $lo);
    }

    private function now(): int { return (int) (hrtime(true) / 1000000); }
    private function sorted(array $ids): array { $ids = array_values(array_unique(array_map('strval', $ids))); sort($ids, SORT_STRING); return $ids; }
    public function groupNames(): array { return array_keys($this->groups); }
    public function leader(string $group): ?string { return $this->groups[$group]['announced'] ?? null; }
    public function setVoters(array $voters): void { if ($voters !== []) $this->wanted = $this->sorted($voters); }

    public function addGroup(string $name, bool $lead = false): void
    {
        if (isset($this->groups[$name])) return;
        if ($name !== 'meta' && $name !== 'quorum' && !str_starts_with($name, 'q:')) throw new RuntimeException('invalid Raft group');
        $path = $this->dir . '/' . self::groupDir($name);
        self::directory($path);
        if ($name !== 'meta' && $name !== 'quorum') {
            $named = $this->readJson($path . '/group.json', false);
            if ($named !== null && (!is_array($named) || ($named['group'] ?? null) !== $name)) throw new RuntimeException('Raft group directory collision');
            if ($named === null) self::atomic($path, 'group.json', ['group' => $name]);
        }
        $hard = $this->readJson($path . '/state.json', false) ?? [];
        if (!is_array($hard) || !is_int($hard['term'] ?? 0) || !is_null($hard['vote'] ?? null) && !is_string($hard['vote'])) throw new RuntimeException('invalid persisted Raft hard state');
        $snapshot = $this->readJson($path . '/snapshot.json', false);
        if ($snapshot !== null && (!is_array($snapshot) || !is_int($snapshot['index'] ?? null) || !is_int($snapshot['term'] ?? null) || !is_array($snapshot['voters'] ?? null))) throw new RuntimeException('invalid persisted Raft snapshot');
        $entries = $this->loadLog($path . '/log.jsonl', (int) ($snapshot['index'] ?? 0));
        $core = new Raft($this->id, $name, $this->wanted, random_int(1, PHP_INT_MAX));
        $core->restore((int) ($hard['term'] ?? 0), is_string($hard['vote'] ?? null) ? $hard['vote'] : null, $snapshot, $entries, (int) ($snapshot['index'] ?? 0));
        // A sole configured voter has no peer to wait for or compete with.
        $singleton = $this->wanted === [$this->id] && $core->voters() === [$this->id];
        $core->startTimer($this->now() - ($singleton ? Raft::ELECTION_MAX_MS : 0));
        $core->snapshotSource = fn () => ($this->state)($name);
        $this->groups[$name] = ['core' => $core, 'path' => $path, 'persist' => null, 'outbox' => [],
            'install' => $snapshot, 'apply' => [], 'waiters' => [], 'retryAt' => 0, 'retryDelay' => 100,
            'sinceSnapshot' => 0, 'announced' => null, 'leadFrom' => null, 'applied' => (int) ($snapshot['index'] ?? 0)];
        // A local queue leader locator is a preference, not a forced election.
        if ($lead) $core->startTimer($this->now() - Raft::ELECTION_MAX_MS);
    }

    public function dropGroup(string $name): void
    {
        if ($name === 'meta' || $name === 'quorum' || !isset($this->groups[$name])) return;
        foreach ($this->groups[$name]['waiters'] as $waiter) $this->complete($waiter['done'], false, 'Raft group deleted');
        unset($this->groups[$name]);
        $path = $this->dir . '/' . self::groupDir($name);
        foreach (glob($path . '/*') ?: [] as $file) {
            if (is_file($file) && !unlink($file)) throw new RuntimeException('could not remove Raft group file');
        }
        if (is_dir($path) && !rmdir($path)) throw new RuntimeException('could not remove Raft group');
    }

    /** Acceptance is not confirmation: done runs only after durable commit/application. */
    public function propose(string $group, string $kind, mixed $data, callable $done): bool
    {
        if (!isset($this->groups[$group])) { $this->complete($done, false, 'unknown Raft group'); return false; }
        $rid = $this->id . ':' . ++$this->sequence;
        $this->pending[$rid] = ['group' => $group, 'kind' => $kind, 'data' => $data, 'done' => $done,
            'deadline' => $this->now() + self::PROPOSE_TIMEOUT_MS, 'sentTo' => null, 'leaderCommitted' => false, 'localApplied' => false];
        return true;
    }

    public function step(string $from, array $message): void
    {
        $group = (string) ($message['g'] ?? '');
        if (!isset($this->groups[$group]) && str_starts_with($group, 'q:') && $this->wantGroup !== null && ($this->wantGroup)($group)) $this->addGroup($group);
        if (!isset($this->groups[$group])) return;
        $g =& $this->groups[$group];
        if (!in_array($from, array_merge($this->wanted, $g['core']->voters()), true)) return;
        $type = (string) ($message['t'] ?? '');
        if ($type === 'propose_r') {
            $rid = (string) ($message['rid'] ?? '');
            $p = $this->pending[$rid] ?? null;
            if ($p === null || $p['sentTo'] !== $from || $p['group'] !== $group) return;
            if (($message['ok'] ?? false) === true) {
                $this->pending[$rid]['leaderCommitted'] = true;
                if ($p['localApplied']) { unset($this->pending[$rid]); $this->complete($p['done'], true, null); }
            } else $this->pending[$rid]['sentTo'] = null;
            return;
        }
        if ($this->blocked($g)) return; // Peer retries; never pass an unpersisted term/log write.
        if ($type === 'propose') {
            $rid = (string) ($message['rid'] ?? '');
            $done = function (bool $ok, ?string $error) use ($from, $group, $rid): void {
                ($this->send)($from, ['t' => 'propose_r', 'g' => $group, 'rid' => $rid, 'ok' => $ok, 'error' => $error]);
            };
            $index = $g['core']->propose((string) ($message['kind'] ?? ''), $message['data'] ?? null, $this->now());
            if ($index === null) { $done(false, 'not leader'); return; }
            $g['waiters'][$index] = ['term' => $g['core']->term, 'done' => $done, 'deadline' => $this->now() + self::PROPOSE_TIMEOUT_MS];
            return;
        }
        $g['core']->step($from, $message, $this->now());
    }

    private function blocked(array $g): bool { return $g['persist'] !== null || $g['install'] !== null || $g['apply'] !== [] || $g['retryAt'] > 0; }

    public function tick(): void
    {
        if ($this->ticking) return;
        $this->ticking = true;
        try {
            $now = $this->now();
            foreach ($this->groups as &$g) {
                if (!$this->blocked($g)) {
                    $g['core']->tick($now);
                    if ($g['core']->role === 'leader') $g['core']->reconfigure($this->wanted, $now);
                }
            }
            unset($g);
            foreach ($this->pending as $rid => $p) {
                if ($now >= $p['deadline']) { unset($this->pending[$rid]); $this->complete($p['done'], false, 'Raft proposal timed out'); continue; }
                if (!isset($this->groups[$p['group']])) { unset($this->pending[$rid]); $this->complete($p['done'], false, 'Raft group deleted'); continue; }
                $g =& $this->groups[$p['group']];
                if ($this->blocked($g) || $p['leaderCommitted']) continue;
                if ($g['core']->role === 'leader') {
                    $index = $g['core']->propose($p['kind'], $p['data'], $now);
                    if ($index !== null) { unset($this->pending[$rid]); $g['waiters'][$index] = ['term' => $g['core']->term, 'done' => $p['done'], 'deadline' => $p['deadline']]; }
                } else {
                    $leader = $g['core']->leader;
                    if ($leader !== null && $leader !== $this->id && $p['sentTo'] !== $leader) {
                        ($this->send)($leader, ['t' => 'propose', 'g' => $p['group'], 'rid' => $rid, 'kind' => $p['kind'], 'data' => $p['data']]);
                        $this->pending[$rid]['sentTo'] = $leader;
                    }
                }
                unset($g);
            }
            foreach (array_keys($this->groups) as $group) $this->flush($group, $now);
        } finally { $this->ticking = false; }
    }

    private function flush(string $name, int $now): void
    {
        if (!isset($this->groups[$name])) return;
        $g =& $this->groups[$name];
        foreach ($g['waiters'] as $index => $waiter) {
            if ($now >= $waiter['deadline']) { unset($g['waiters'][$index]); $this->complete($waiter['done'], false, 'Raft proposal timed out'); }
        }
        if ($g['retryAt'] > $now) return;
        try {
            $core = $g['core'];
            if ($g['persist'] === null) {
                $dirty = $core->takeDirty();
                $g['outbox'] = array_merge($g['outbox'], $core->takeOutbox());
                $snapshot = $dirty['snapshot'];
                $rewrite = $snapshot !== null || $dirty['truncatedFrom'] !== null ? $core->logEntries() : null;
                if ($dirty['hardState'] || $snapshot !== null || $rewrite !== null || $dirty['appended'] !== []) {
                    $g['persist'] = ['hard' => $dirty['hardState'] ? ['term' => $core->term, 'vote' => $core->votedFor] : null,
                        'snapshot' => $snapshot, 'rewrite' => $rewrite, 'append' => $rewrite === null ? $dirty['appended'] : [], 'appended' => false];
                }
            }
            if ($g['persist'] !== null) $this->persist($g);
            if ($g['install'] === null) $g['install'] = $core->takeInstalled();
            if ($g['install'] !== null) {
                ($this->install)($name, $g['install']['state'] ?? null);
                $g['applied'] = (int) $g['install']['index'];
                $g['install'] = null;
            }
            if ($g['apply'] === []) $g['apply'] = $core->takeCommitted();
            while ($g['apply'] !== []) {
                $entry = $g['apply'][0];
                if (!in_array($entry['kind'], ['noop', 'config'], true)) ($this->apply)($name, $entry);
                array_shift($g['apply']);
                $g['applied'] = $entry['i'];
                foreach ($this->pending as $rid => $p) {
                    if ($p['group'] !== $name || $p['kind'] !== $entry['kind'] || $p['data'] != $entry['data']) continue;
                    $this->pending[$rid]['localApplied'] = true;
                    if ($p['leaderCommitted']) { unset($this->pending[$rid]); $this->complete($p['done'], true, null); }
                }
                $waiter = $g['waiters'][$entry['i']] ?? null;
                unset($g['waiters'][$entry['i']]);
                if ($waiter !== null) $this->complete($waiter['done'], $waiter['term'] === $entry['term'], $waiter['term'] === $entry['term'] ? null : 'entry replaced');
                $g['sinceSnapshot']++;
            }
            // A reply is never emitted before its hard state/log and apply succeed.
            foreach ($g['outbox'] as [$peer, $message]) ($this->send)($peer, $message);
            $g['outbox'] = [];
            if ($core->role === 'leader') {
                if ($g['leadFrom'] === null) $g['leadFrom'] = $core->lastIndex();
                $leader = $core->commit >= $g['leadFrom'] && $g['applied'] >= $g['leadFrom'] ? $this->id : null;
            } else { $g['leadFrom'] = null; $leader = $core->leader; }
            if ($leader !== $g['announced']) {
                $g['announced'] = $leader;
                if ($this->leaderChanged !== null) ($this->leaderChanged)($name, $leader);
            }
            $g['retryAt'] = 0; $g['retryDelay'] = 100;
            if ($g['sinceSnapshot'] >= ($name === 'meta' ? 50000 : 10000)) {
                $snapshot = $core->compact(($this->state)($name));
                if ($snapshot !== null) {
                    $g['persist'] = ['hard' => null, 'snapshot' => $snapshot, 'rewrite' => $core->logEntries(), 'append' => [], 'appended' => false];
                    $this->persist($g);
                }
                $g['sinceSnapshot'] = 0;
            }
        } catch (Throwable $error) {
            $g['retryAt'] = $now + $g['retryDelay']; $g['retryDelay'] = min(1000, $g['retryDelay'] * 2);
            // Held entries, dirty records and replies stay in order for a later tick.
            error_log('queueforge-php raft ' . $name . ': ' . $error->getMessage());
        }
    }

    private function persist(array &$g): void
    {
        $p =& $g['persist']; $path = $g['path'];
        // A crash between writes may leave a newer log, never an older term.
        if ($p['hard'] !== null) { self::atomic($path, 'state.json', $p['hard']); $p['hard'] = null; }
        if ($p['snapshot'] !== null) { self::atomic($path, 'snapshot.json', $p['snapshot']); $p['snapshot'] = null; }
        if ($p['rewrite'] !== null) {
            self::atomicBytes($path, 'log.jsonl', $this->logBytes($p['rewrite'])); $p['rewrite'] = null;
        }
        if ($p['append'] !== [] && !$p['appended']) {
            $fp = fopen($path . '/log.jsonl', 'c+b');
            if ($fp === false) throw new RuntimeException('cannot open Raft log');
            try {
                if (isset($p['appendStart'])) {
                    if (!ftruncate($fp, $p['appendStart'])) throw new RuntimeException('cannot roll back partial Raft append');
                    $start = $p['appendStart'];
                } else { $start = (int) fstat($fp)['size']; $p['appendStart'] = $start; }
                if (fseek($fp, $start) !== 0) throw new RuntimeException('cannot seek Raft log');
                self::writeAll($fp, $this->logBytes($p['append']));
                if (!fflush($fp) || !fsync($fp)) throw new RuntimeException('Raft log fsync failed');
                self::syncDirectory($path);
                $p['appended'] = true;
            } finally { fclose($fp); }
        }
        $g['persist'] = null;
    }

    private function logBytes(array $entries): string
    {
        $bytes = '';
        foreach ($entries as $entry) $bytes .= json_encode($entry, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . "\n";
        return $bytes;
    }
    private function readJson(string $file, bool $required): mixed
    {
        if (!is_file($file)) { if ($required) throw new RuntimeException('missing Raft file ' . $file); return null; }
        $bytes = file_get_contents($file);
        if ($bytes === false) throw new RuntimeException('could not read Raft file');
        try { $value = json_decode($bytes, true, 512, JSON_THROW_ON_ERROR);
            if (!is_array($value)) throw new RuntimeException('invalid Raft file ' . $file);
            return $value; }
        catch (JsonException $error) { throw new RuntimeException('corrupt Raft file ' . $file, 0, $error); }
    }
    private function loadLog(string $file, int $snapshotIndex): array
    {
        if (!is_file($file)) return [];
        $bytes = file_get_contents($file);
        if ($bytes === false) throw new RuntimeException('could not read Raft log');
        $end = strrpos($bytes, "\n");
        $complete = $end === false ? '' : substr($bytes, 0, $end + 1);
        if (strlen($complete) !== strlen($bytes)) {
            $fp = fopen($file, 'c+b');
            if ($fp === false) throw new RuntimeException('cannot repair torn Raft log');
            try { if (!ftruncate($fp, strlen($complete)) || !fflush($fp) || !fsync($fp)) throw new RuntimeException('cannot sync repaired Raft log'); }
            finally { fclose($fp); }
        }
        $entries = []; $next = $snapshotIndex + 1;
        foreach (explode("\n", $complete) as $line) {
            if ($line === '') continue;
            $entry = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
            if (!is_array($entry) || !is_int($entry['i'] ?? null) || !is_int($entry['term'] ?? null) || !is_string($entry['kind'] ?? null)) throw new RuntimeException('invalid Raft entry');
            if ($entry['i'] <= $snapshotIndex) continue;
            if ($entry['i'] !== $next++) throw new RuntimeException('noncontiguous Raft log');
            $entries[] = $entry;
        }
        return $entries;
    }
    private function complete(callable $done, bool $ok, ?string $error): void
    {
        try { $done($ok, $error); } catch (Throwable $failure) { error_log('queueforge-php raft callback: ' . $failure->getMessage()); }
    }
    private static function directory(string $dir): void
    {
        if (!is_dir($dir) && !mkdir($dir, 0777, true) && !is_dir($dir)) throw new RuntimeException('cannot create Raft directory');
    }
    public static function atomic(string $dir, string $name, mixed $value): void
    {
        self::atomicBytes($dir, $name, json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . "\n");
    }
    private static function atomicBytes(string $dir, string $name, string $bytes): void
    {
        self::directory($dir);
        $tmp = $dir . '/' . $name . '.tmp';
        $fp = fopen($tmp, 'wb');
        if ($fp === false) throw new RuntimeException('cannot create Raft temporary file');
        try { self::writeAll($fp, $bytes); if (!fflush($fp) || !fsync($fp)) throw new RuntimeException('Raft file fsync failed'); }
        finally { fclose($fp); }
        if (!rename($tmp, $dir . '/' . $name)) throw new RuntimeException('cannot replace Raft file');
        // Directory synchronization keeps rename/new file durability explicit.
        self::syncDirectory($dir);
    }
    private static function syncDirectory(string $dir): void
    {
        $folder = fopen($dir, 'r');
        if ($folder === false) throw new RuntimeException('cannot open Raft directory');
        try { if (!fsync($folder)) throw new RuntimeException('Raft directory fsync failed'); }
        finally { fclose($folder); }
    }
    private static function writeAll($fp, string $bytes): void
    {
        $offset = 0;
        while ($offset < strlen($bytes)) {
            $written = fwrite($fp, substr($bytes, $offset));
            if ($written === false || $written === 0) throw new RuntimeException('Raft write failed');
            $offset += $written;
        }
    }
}
