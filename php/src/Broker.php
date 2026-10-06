<?php
declare(strict_types=1);

/**
 * Single-node and clustered queues. A classic confirm is not released until the
 * covering fsync. A quorum confirm also waits for a durable majority.
 */
final class Broker
{
    /** @var array<string, array{ready: list<int>, consumers: list<array{conn:int,ch:int,tag:string,credit:int}>}> */
    public array $queues = [];
    /** @var array<int, array{queue:string,body:string,mode:int}> */
    public array $msgs = [];
    /** @var list<array{conn:int,ch:int,tag:int,end:int}> */
    public array $waiting = [];
    /** @var array<string, string> */
    public array $users = [];
    /** @var array<string, string> */
    public array $exchanges = [
        '' => 'direct',
        'amq.direct' => 'direct',
        'amq.fanout' => 'fanout',
        'amq.topic' => 'topic',
        'amq.headers' => 'headers',
    ];
    /** @var list<array{exchange:string,queue:string,key:string,args:list<array{0:string,1:string}>}> */
    public array $bindings = [];
    public int $nextId = 1;
    public int $cursor = 0;
    public string $nodeId = 'queueforge';
    /** @var list<array{id:string,addr:string}> */
    public array $members = [];
    public ?Cluster $cluster = null;
    public bool $ready = true;
    /** @var array<string, int> */
    public array $prom = [
        'received' => 0,
        'delivered' => 0,
        'acknowledged' => 0,
        'confirmed' => 0,
        'connections' => 0,
    ];

    public function __construct(public Store $store, public string $userFile)
    {
        if (is_file($userFile)) {
            $decoded = json_decode((string) file_get_contents($userFile), true);
            if (is_array($decoded)) {
                foreach ($decoded as $name => $hash) {
                    if (is_string($name) && is_string($hash)) {
                        $this->users[$name] = $hash;
                    }
                }
            }
        }
        foreach ($store->replay() as $msg) {
            $this->msgs[$msg['id']] = [
                'queue' => $msg['queue'],
                'body' => $msg['body'],
                'mode' => $msg['mode'],
                'redelivered' => false,
                'exchange' => '',
                'key' => $msg['queue'],
            ];
            $this->declareQueue($msg['queue']);
            $this->queues[$msg['queue']]['ready'][] = $msg['id'];
            if ($msg['id'] >= $this->nextId) {
                $this->nextId = $msg['id'] + 1;
            }
        }
    }

    public function bootstrap(string $password): void
    {
        if ($this->users !== []) {
            return;
        }
        $this->users['admin'] = Auth::hash($password);
        file_put_contents($this->userFile, json_encode($this->users));
    }

    public function verify(string $user, string $pass): bool
    {
        return isset($this->users[$user]) && Auth::matches($pass, $this->users[$user]);
    }

    public function declareQueue(string $name, array $rawArgs = []): void
    {
        if (!isset($this->queues[$name])) {
            $this->queues[$name] = [
                'ready' => [],
                'consumers' => [],
                'replicas' => [],
                'args' => Features::parseArgs($rawArgs),
            ];
            return;
        }
        if (!isset($this->queues[$name]['args'])) {
            $this->queues[$name]['args'] = Features::parseArgs([]);
        }
        if (!isset($this->queues[$name]['replicas'])) {
            $this->queues[$name]['replicas'] = [];
        }
        if ($rawArgs !== []) {
            $this->queues[$name]['args'] = Features::parseArgs($rawArgs);
        }
    }

    public function declareExchange(string $name, string $kind): void
    {
        if ($name === '') {
            return;
        }
        $this->exchanges[$name] = $kind === '' ? 'direct' : $kind;
    }

    /** @param list<array{0:string,1:string}> $args */
    public function bind(string $queue, string $exchange, string $key, array $args = []): void
    {
        $this->declareQueue($queue);
        foreach ($this->bindings as $row) {
            if ($row['queue'] === $queue && $row['exchange'] === $exchange && $row['key'] === $key && $row['args'] === $args) {
                return;
            }
        }
        $this->bindings[] = ['exchange' => $exchange, 'queue' => $queue, 'key' => $key, 'args' => $args];
    }

    /** @param list<array{0:string,1:string}> $headers
     *  @return list<string> */
    public function route(string $exchange, string $key, array $headers = []): array
    {
        if ($exchange === '') {
            return isset($this->queues[$key]) ? [$key] : [];
        }
        if (!isset($this->exchanges[$exchange])) {
            return [];
        }
        $rows = array_values(array_filter(
            $this->bindings,
            static fn (array $row): bool => $row['exchange'] === $exchange,
        ));
        $kind = $this->exchanges[$exchange];
        if ($kind === 'fanout') {
            return array_values(array_unique(array_column($rows, 'queue')));
        }
        if ($kind === 'topic') {
            $out = [];
            foreach ($rows as $row) {
                if (Routing::topic($row['key'], $key)) {
                    $out[] = $row['queue'];
                }
            }
            return array_values(array_unique($out));
        }
        if ($kind === 'headers') {
            $out = [];
            foreach ($rows as $row) {
                if (Features::headersMatch($row['args'] ?? [], $headers)) {
                    $out[] = $row['queue'];
                }
            }
            return array_values(array_unique($out));
        }
        $out = [];
        foreach ($rows as $row) {
            if ($row['key'] === $key) {
                $out[] = $row['queue'];
            }
        }
        return array_values(array_unique($out));
    }

    /** @param list<array{0:string,1:string}> $headers
     *  @return 'wait'|'return'|'nack' */
    public function publish(int $conn, int $ch, int $tag, string $exchange, string $key, string $body, int $mode, int $priority = 0, array $headers = [], ?int $expirationMs = null): string
    {
        $dests = $this->route($exchange, $key, $headers);
        if ($dests === []) {
            return 'return';
        }
        $ids = [];
        $qids = [];
        $end = 0;
        $rejected = 0;
        foreach ($dests as $queue) {
            $this->declareQueue($queue);
            $args = $this->queues[$queue]['args'];
            $depth = $this->depth($queue);
            $over = ($args['maxLength'] !== null && $depth >= $args['maxLength'])
                || ($args['maxLengthBytes'] !== null && $this->bytes($queue) + strlen($body) > $args['maxLengthBytes']);
            if ($over && ($args['overflow'] === 'reject-publish' || $args['overflow'] === 'reject-publish-dlx')) {
                if ($args['overflow'] === 'reject-publish-dlx') {
                    $this->deadLetterBody($queue, $exchange, $key, $body);
                }
                $rejected++;
                continue;
            }
            if ($over) {
                while ($this->queues[$queue]['ready'] !== [] && (
                    ($args['maxLength'] !== null && $this->depth($queue) >= $args['maxLength'])
                    || ($args['maxLengthBytes'] !== null && $this->bytes($queue) + strlen($body) > $args['maxLengthBytes'])
                )) {
                    $dropped = array_shift($this->queues[$queue]['ready']);
                    if (is_int($dropped)) {
                        $this->deadLetter($dropped);
                    }
                }
            }
            $id = $this->nextId++;
            $expires = null;
            $now = (int) (microtime(true) * 1000);
            if ($args['messageTtl'] !== null) {
                $expires = $now + $args['messageTtl'];
            }
            if ($expirationMs !== null) {
                $at = $now + $expirationMs;
                $expires = $expires === null ? $at : min($expires, $at);
            }
            $this->msgs[$id] = [
                'queue' => $queue,
                'body' => $body,
                'mode' => $mode,
                'redelivered' => false,
                'exchange' => $exchange,
                'key' => $key,
                'priority' => $priority,
                'expires' => $expires,
                'headers' => $headers,
            ];
            $ids[] = $id;
            $qids[] = (string) $id;
            $this->prom['received']++;
            if ($mode === 2) {
                $end = $this->store->appendPublish($id, $queue, $body, $mode);
            } else {
                $this->hold($queue, $id);
            }
        }
        if ($ids === []) {
            return $rejected > 0 ? 'nack' : 'return';
        }
        $need = 0;
        if ($this->quorumPublish($dests)) {
            $need = Features::majority(max(1, count($this->members)));
            if ($this->cluster !== null) {
                foreach ($qids as $i => $qid) {
                    $queue = $this->msgs[$ids[$i]]['queue'];
                    $this->cluster->replicate(Features::encodeQuorumAppend('/', $queue, $qid, $body, $exchange, $key, $mode === 2));
                }
            }
        }
        $this->waiting[] = [
            'conn' => $conn,
            'ch' => $ch,
            'tag' => $tag,
            'end' => $end,
            'ids' => $ids,
            'qids' => $qids,
            'quorumNeed' => $need,
            'quorumHave' => 1,
        ];
        return 'wait';
    }

    /** @param list<string> $dests */
    private function quorumPublish(array $dests): bool
    {
        if (count($this->members) < 2) {
            return false;
        }
        foreach ($dests as $queue) {
            if (($this->queues[$queue]['args']['queueType'] ?? 'classic') === 'quorum') {
                return true;
            }
        }
        return false;
    }

    private function depth(string $queue): int
    {
        $n = count($this->queues[$queue]['ready']) + count($this->queues[$queue]['replicas']);
        return $n;
    }

    private function bytes(string $queue): int
    {
        $n = 0;
        foreach (array_merge($this->queues[$queue]['ready'], $this->queues[$queue]['replicas']) as $id) {
            $n += isset($this->msgs[$id]) ? strlen($this->msgs[$id]['body']) : 0;
        }
        return $n;
    }

    private function hold(string $queue, int $id): void
    {
        $type = $this->queues[$queue]['args']['queueType'] ?? 'classic';
        if ($type === 'quorum' && !$this->isLeader()) {
            $this->queues[$queue]['replicas'][] = $id;
            return;
        }
        $this->pushReady($queue, $id);
    }

    private function pushReady(string $queue, int $id): void
    {
        if (!isset($this->msgs[$id])) {
            return;
        }
        $cap = $this->queues[$queue]['args']['maxPriority'] ?? null;
        $pri = $this->msgs[$id]['priority'] ?? 0;
        if ($cap !== null) {
            $pri = min($pri, $cap);
            $this->msgs[$id]['priority'] = $pri;
            $at = 0;
            $ready = $this->queues[$queue]['ready'];
            while ($at < count($ready) && ($this->msgs[$ready[$at]]['priority'] ?? 0) >= $pri) {
                $at++;
            }
            array_splice($this->queues[$queue]['ready'], $at, 0, [$id]);
            return;
        }
        $this->queues[$queue]['ready'][] = $id;
    }

    public function isLeader(): bool
    {
        $ids = [$this->nodeId];
        if ($this->cluster !== null) {
            foreach ($this->cluster->peerIds() as $id) {
                $ids[] = $id;
            }
        }
        if ($this->members === []) {
            return true;
        }
        return Features::leader($ids) === $this->nodeId;
    }

    public function refreshRole(): void
    {
        foreach ($this->queues as $name => $queue) {
            if (($queue['args']['queueType'] ?? 'classic') !== 'quorum') {
                continue;
            }
            if ($this->isLeader()) {
                foreach ($queue['replicas'] as $id) {
                    $this->pushReady($name, $id);
                }
                $this->queues[$name]['replicas'] = [];
            }
        }
    }

    public function noteCopy(string $messageId): void
    {
        foreach ($this->waiting as $i => $w) {
            if (in_array($messageId, $w['qids'] ?? [], true)) {
                $this->waiting[$i]['quorumHave']++;
            }
        }
    }

    /** Store a peer's quorum append. The body is durable before the reply. */
    public function enqueueLocal(string $queue, string $messageId, string $body, string $exchange, string $key, bool $persistent): bool
    {
        if (!isset($this->queues[$queue])) {
            return false;
        }
        $id = $this->nextId++;
        $this->msgs[$id] = [
            'queue' => $queue,
            'body' => $body,
            'mode' => $persistent ? 2 : 1,
            'redelivered' => false,
            'exchange' => $exchange,
            'key' => $key,
            'priority' => 0,
            'expires' => null,
            'headers' => [],
            'qid' => $messageId,
        ];
        if ($persistent) {
            $this->store->appendPublish($id, $queue, $body, 2);
            $this->store->sync();
        }
        $this->hold($queue, $id);
        return true;
    }

    public function pullBody(string $queue): ?string
    {
        if (!isset($this->queues[$queue]) || $this->queues[$queue]['ready'] === []) {
            return null;
        }
        $id = array_shift($this->queues[$queue]['ready']);
        if (!is_int($id) || !isset($this->msgs[$id])) {
            return null;
        }
        $body = $this->msgs[$id]['body'];
        $this->ack($id);
        return $body;
    }

    /** @return list<array{conn:int,ch:int,tag:int}> */
    public function flush(): array
    {
        $this->store->sync();
        $ready = [];
        $still = [];
        foreach ($this->waiting as $w) {
            if ($w['end'] > $this->store->synced) {
                $still[] = $w;
                continue;
            }
            if (($w['quorumNeed'] ?? 0) > ($w['quorumHave'] ?? 1)) {
                $still[] = $w;
                continue;
            }
            if ($w['end'] > 0) {
                foreach ($w['ids'] as $id) {
                    if (isset($this->msgs[$id])) {
                        $this->hold($this->msgs[$id]['queue'], $id);
                    }
                }
            }
            $ready[] = ['conn' => $w['conn'], 'ch' => $w['ch'], 'tag' => $w['tag']];
        }
        $this->waiting = $still;
        return $ready;
    }

    public function addConsumer(string $queue, int $conn, int $ch, string $tag): void
    {
        $this->declareQueue($queue);
        $this->queues[$queue]['consumers'][] = ['conn' => $conn, 'ch' => $ch, 'tag' => $tag, 'credit' => 0];
    }

    public function requeue(int $id): void
    {
        if (!isset($this->msgs[$id])) {
            return;
        }
        $this->msgs[$id]['redelivered'] = true;
        $queue = $this->msgs[$id]['queue'];
        array_unshift($this->queues[$queue]['ready'], $id);
    }

    public function ack(int $id): void
    {
        if (!isset($this->msgs[$id])) {
            return;
        }
        $this->store->appendAck($id);
        unset($this->msgs[$id]);
        $this->prom['acknowledged']++;
    }

    public function deadLetter(int $id): void
    {
        if (!isset($this->msgs[$id])) {
            return;
        }
        $msg = $this->msgs[$id];
        $args = $this->queues[$msg['queue']]['args'] ?? Features::parseArgs([]);
        $exchange = $args['dlx'] ?? null;
        $key = $args['dlxKey'] ?? $msg['key'];
        $body = $msg['body'];
        $this->ack($id);
        if (is_string($exchange) && $exchange !== '' && $this->dlxDepth < 2) {
            $this->dlxDepth++;
            $this->publish(0, 0, 0, $exchange, $key, $body, 1);
            $this->dlxDepth--;
        }
    }

    private int $dlxDepth = 0;

    private function deadLetterBody(string $queue, string $exchange, string $key, string $body): void
    {
        $args = $this->queues[$queue]['args'] ?? Features::parseArgs([]);
        $dlx = $args['dlx'] ?? null;
        if (!is_string($dlx) || $dlx === '' || $this->dlxDepth >= 2) {
            return;
        }
        $this->dlxDepth++;
        $this->publish(0, 0, 0, $dlx, $args['dlxKey'] ?? $key, $body, 1);
        $this->dlxDepth--;
    }

    public function expired(int $id): bool
    {
        if (!isset($this->msgs[$id])) {
            return false;
        }
        $at = $this->msgs[$id]['expires'] ?? null;
        return is_int($at) && $at <= (int) (microtime(true) * 1000);
    }
}
