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
    /**
     * Exchange-to-exchange links. In memory only, so they do not survive a
     * restart. Bun keeps them the same way (bun/src/broker/topology.ts:255).
     *
     * @var list<array{source:string,destination:string,key:string}>
     */
    public array $e2e = [];
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
                'propRaw' => $msg['propRaw'] ?? null,
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

    /**
     * Declares a queue. A quorum queue must be durable and non-exclusive, as
     * in Bun and RabbitMQ; asking for one any other way is a channel error.
     *
     * @param array<string, string|int> $rawArgs
     * @throws RuntimeException when a quorum queue is asked for transiently
     */
    public function declareQueue(string $name, array $rawArgs = [], bool $durable = true, bool $exclusive = false): void
    {
        if (($rawArgs['x-queue-type'] ?? '') === 'quorum') {
            if (!$durable) {
                throw new RuntimeException('PRECONDITION_FAILED - a quorum queue must be durable');
            }
            if ($exclusive) {
                throw new RuntimeException('PRECONDITION_FAILED - a quorum queue cannot be exclusive');
            }
        }
        if (!isset($this->queues[$name])) {
            $this->queues[$name] = [
                'ready' => [],
                'consumers' => [],
                'replicas' => [],
                'args' => Features::parseArgs($rawArgs),
                // A quorum queue is homed where it was declared rather than
                // by the classic hash, matching Bun.
                'home' => ($rawArgs['x-queue-type'] ?? '') === 'quorum' ? $this->nodeId : '',
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
        return $this->routeFrom($exchange, $key, $headers, []);
    }

    /**
     * Routes through an exchange, following exchange-to-exchange links. The
     * seen set means a link cycle is visited once instead of looping.
     *
     * @param list<array{0:string,1:string}> $headers
     * @param array<string, bool> $seen
     * @return list<string>
     */
    private function routeFrom(string $exchange, string $key, array $headers, array $seen): array
    {
        if (isset($seen[$exchange])) {
            return [];
        }
        $seen[$exchange] = true;
        $out = $this->routeDirect($exchange, $key, $headers);
        foreach ($this->e2e as $link) {
            if ($link['source'] !== $exchange) {
                continue;
            }
            $kind = $this->exchanges[$exchange] ?? 'direct';
            $matches = match ($kind) {
                'fanout' => true,
                'topic' => Routing::topic($link['key'], $key),
                default => $link['key'] === $key,
            };
            if ($matches) {
                $out = array_merge($out, $this->routeFrom($link['destination'], $key, $headers, $seen));
            }
        }
        return array_values(array_unique($out));
    }

    /** @param list<array{0:string,1:string}> $headers
     *  @return list<string> */
    private function routeDirect(string $exchange, string $key, array $headers = []): array
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
     *  @param ?string $propRaw raw publisher property bytes, replayed to consumers verbatim
     *  @return 'wait'|'return'|'nack' */
    public function publish(int $conn, int $ch, int $tag, string $exchange, string $key, string $body, int $mode, int $priority = 0, array $headers = [], ?int $expirationMs = null, ?string $propRaw = null): string
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
            // A quorum message carries the id its replicas are keyed by, in
            // the q-<node>-<millis>-<random> shape Bun uses, so a later
            // quorum_drop can name it. Classic messages keep the local id.
            $isQuorum = ($args['queueType'] ?? 'classic') === 'quorum';
            $qid = $isQuorum
                ? 'q-' . $this->nodeId . '-' . $now . '-' . bin2hex(random_bytes(4))
                : (string) $id;
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
                'propRaw' => $propRaw,
                'qid' => $qid,
            ];
            $ids[] = $id;
            $qids[] = $qid;
            $this->prom['received']++;
            if ($mode === 2) {
                $end = $this->store->appendPublish($id, $queue, $body, $mode, $propRaw);
            } else {
                $this->hold($queue, $id);
            }
        }
        if ($ids === []) {
            return $rejected > 0 ? 'nack' : 'return';
        }
        $need = 0;
        $copies = ['durable'];
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
            // One entry per durable copy. The local append is already fsynced
            // by the time the confirm is considered, so it counts as durable.
            'copies' => $copies,
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
                // A peer only replies ok once its own append is fsynced, so
                // an ok reply counts as a durable copy.
                $this->waiting[$i]['copies'][] = 'durable';
            }
        }
    }

    /**
     * Abandons a quorum publish whose replication did not reach a majority.
     * The replicas that did land are dropped on their peers and the local
     * copy goes too, so a nacked publish leaves nothing behind.
     */
    public function failQuorum(string $messageId): void
    {
        foreach ($this->waiting as $i => $w) {
            if (!in_array($messageId, $w['qids'] ?? [], true)) {
                continue;
            }
            if (($w['quorumNeed'] ?? 0) === 0) {
                return;
            }
            if ($this->cluster !== null) {
                foreach ($w['ids'] as $slot => $id) {
                    $queue = $this->msgs[$id]['queue'] ?? '';
                    $qid = $w['qids'][$slot] ?? '';
                    foreach ($this->cluster->peerIds() as $peer) {
                        $this->cluster->request($peer, 'quorum_drop', [
                            'vhost' => '/',
                            'queue' => $queue,
                            'id' => $qid,
                        ]);
                    }
                }
            }
            foreach ($w['ids'] as $id) {
                $queue = $this->msgs[$id]['queue'] ?? '';
                if ($queue !== '' && isset($this->queues[$queue])) {
                    foreach (['ready', 'replicas'] as $list) {
                        $this->queues[$queue][$list] = array_values(array_filter(
                            $this->queues[$queue][$list],
                            static fn (int $held): bool => $held !== $id,
                        ));
                    }
                }
                $this->ack($id);
            }
            $this->waiting[$i]['failed'] = true;
            return;
        }
    }

    /**
     * Drops a replica a leader rolled back with quorum_drop, by the message
     * id the leader assigned rather than the local id.
     */
    public function dropReplica(string $queue, string $messageId): void
    {
        if ($messageId === '') {
            return;
        }
        foreach ($this->msgs as $id => $msg) {
            if (($msg['qid'] ?? '') !== $messageId) {
                continue;
            }
            if ($queue !== '' && $msg['queue'] !== $queue) {
                continue;
            }
            $name = $msg['queue'];
            if (isset($this->queues[$name])) {
                foreach (['ready', 'replicas'] as $list) {
                    $this->queues[$name][$list] = array_values(array_filter(
                        $this->queues[$name][$list],
                        static fn (int $held): bool => $held !== $id,
                    ));
                }
            }
            $this->ack($id);
            return;
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

    /** @return list<array{conn:int,ch:int,tag:int,nack:bool}> */
    public function flush(): array
    {
        $this->store->sync();
        $ready = [];
        $still = [];
        foreach ($this->waiting as $w) {
            // A quorum publish that could not reach a majority is nacked, so
            // the publisher learns the message was not accepted.
            if (($w['failed'] ?? false) === true) {
                $ready[] = ['conn' => $w['conn'], 'ch' => $w['ch'], 'tag' => $w['tag'], 'nack' => true];
                continue;
            }
            if ($w['end'] > $this->store->synced) {
                $still[] = $w;
                continue;
            }
            // A quorum confirm needs a durable majority, not just any replies.
            if (($w['quorumNeed'] ?? 0) > 0
                && !Features::durableMajority(max(1, count($this->members)), $w['copies'] ?? ['durable'])) {
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
            $ready[] = ['conn' => $w['conn'], 'ch' => $w['ch'], 'tag' => $w['tag'], 'nack' => false];
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

    /**
     * Takes the next ready message without involving a consumer, for
     * basic.get. Expired messages are dead-lettered and skipped.
     */
    public function getReady(string $queue): ?int
    {
        if (!isset($this->queues[$queue])) {
            return null;
        }
        while ($this->queues[$queue]['ready'] !== []) {
            $id = array_shift($this->queues[$queue]['ready']);
            if (!is_int($id) || !isset($this->msgs[$id])) {
                continue;
            }
            if ($this->expired($id)) {
                $this->deadLetter($id);
                continue;
            }
            return $id;
        }
        return null;
    }

    /** Ready message count, for basic.get-ok and queue.declare-ok. */
    public function readyCount(string $queue): int
    {
        return isset($this->queues[$queue]) ? count($this->queues[$queue]['ready']) : 0;
    }

    /** Consumer count, for queue.declare-ok. */
    public function consumerCount(string $queue): int
    {
        return isset($this->queues[$queue]) ? count($this->queues[$queue]['consumers']) : 0;
    }

    /** Drops every ready message and returns how many went. */
    public function purge(string $queue): int
    {
        if (!isset($this->queues[$queue])) {
            return 0;
        }
        $ids = $this->queues[$queue]['ready'];
        $this->queues[$queue]['ready'] = [];
        $n = 0;
        foreach ($ids as $id) {
            if (!is_int($id) || !isset($this->msgs[$id])) {
                continue;
            }
            $this->ack($id);
            $n++;
        }
        return $n;
    }

    /**
     * Removes a queue, its ready messages, and every binding that names it.
     * Returns the message count that went with it.
     */
    public function deleteQueue(string $name): int
    {
        if (!isset($this->queues[$name])) {
            return 0;
        }
        $n = $this->purge($name);
        unset($this->queues[$name]);
        $this->bindings = array_values(array_filter(
            $this->bindings,
            static fn (array $row): bool => $row['queue'] !== $name,
        ));
        return $n;
    }

    /** @param list<array{0:string,1:string}> $args */
    public function unbind(string $queue, string $exchange, string $key, array $args = []): void
    {
        $this->bindings = array_values(array_filter(
            $this->bindings,
            static fn (array $row): bool => !(
                $row['queue'] === $queue
                && $row['exchange'] === $exchange
                && $row['key'] === $key
                && ($args === [] || $row['args'] === $args)
            ),
        ));
    }

    /**
     * Removes an exchange along with its bindings and exchange-to-exchange
     * links. The default exchange and the amq.* built-ins stay.
     */
    public function deleteExchange(string $name): bool
    {
        if ($name === '' || str_starts_with($name, 'amq.') || !isset($this->exchanges[$name])) {
            return false;
        }
        unset($this->exchanges[$name]);
        $this->bindings = array_values(array_filter(
            $this->bindings,
            static fn (array $row): bool => $row['exchange'] !== $name,
        ));
        $this->e2e = array_values(array_filter(
            $this->e2e,
            static fn (array $row): bool => $row['source'] !== $name && $row['destination'] !== $name,
        ));
        return true;
    }

    /** Links one exchange to another. Held in memory only, as Bun does. */
    public function bindExchange(string $destination, string $source, string $key): void
    {
        foreach ($this->e2e as $row) {
            if ($row['source'] === $source && $row['destination'] === $destination && $row['key'] === $key) {
                return;
            }
        }
        $this->e2e[] = ['source' => $source, 'destination' => $destination, 'key' => $key];
    }

    public function unbindExchange(string $destination, string $source, string $key): void
    {
        $this->e2e = array_values(array_filter(
            $this->e2e,
            static fn (array $row): bool => !(
                $row['source'] === $source
                && $row['destination'] === $destination
                && $row['key'] === $key
            ),
        ));
    }

    /**
     * The node that owns a classic queue. An empty member list means this is
     * a single node and everything is local.
     */
    public function home(string $queue): string
    {
        // A quorum queue keeps the node it was declared on.
        $stored = $this->queues[$queue]['home'] ?? '';
        if (is_string($stored) && $stored !== '') {
            return $stored;
        }
        if (count($this->members) < 2) {
            return $this->nodeId;
        }
        return Features::home($this->members, '/', $queue);
    }

    /** True when this node owns the queue, so no forwarding is needed. */
    public function ownsQueue(string $queue): bool
    {
        $home = $this->home($queue);
        return $home === '' || $home === $this->nodeId;
    }

    /**
     * Merges a peer's topology. Additive on purpose: an existing user,
     * exchange or queue is never overwritten, which is how Bun's
     * applySnapshot behaves. Bindings are matched before insert so repeated
     * handshakes do not pile up duplicates, which Bun does not guard against
     * (bun/src/broker/snapshot.ts:192-195).
     *
     * @param array<string, mixed> $snapshot
     */
    public function applySnapshot(array $snapshot): void
    {
        foreach ((array) ($snapshot['users'] ?? []) as $name => $hash) {
            // Accepts both a name list and a name to hash map.
            if (is_int($name) && is_string($hash)) {
                continue;
            }
            if (is_string($name) && is_string($hash) && !isset($this->users[$name])) {
                $this->users[$name] = $hash;
            }
        }
        foreach ((array) ($snapshot['exchanges'] ?? []) as $name => $kind) {
            if (is_string($name) && is_string($kind) && !isset($this->exchanges[$name])) {
                $this->exchanges[$name] = $kind;
            }
        }
        foreach ((array) ($snapshot['queues'] ?? []) as $row) {
            if (!is_array($row)) {
                continue;
            }
            $name = (string) ($row['name'] ?? '');
            if ($name === '' || isset($this->queues[$name])) {
                continue;
            }
            $type = (string) ($row['type'] ?? $row['queue_type'] ?? 'classic');
            $this->declareQueue($name, $type === 'quorum' ? ['x-queue-type' => 'quorum'] : []);
        }
        foreach ((array) ($snapshot['bindings'] ?? []) as $row) {
            if (!is_array($row)) {
                continue;
            }
            $queue = (string) ($row['queue'] ?? '');
            $exchange = (string) ($row['exchange'] ?? '');
            if ($queue === '') {
                continue;
            }
            $this->bind($queue, $exchange, (string) ($row['key'] ?? $row['routing_key'] ?? ''), []);
        }
    }

    /**
     * Registers a consumer that lives on another node. The pump sends it a
     * cluster deliver frame instead of an AMQP frame.
     */
    public function addRemoteConsumer(string $queue, string $peer, int $session, bool $noAck, ?int $credit): void
    {
        $this->declareQueue($queue);
        foreach ($this->queues[$queue]['consumers'] as $existing) {
            if (($existing['peer'] ?? '') === $peer && ($existing['session'] ?? 0) === $session) {
                return;
            }
        }
        $this->queues[$queue]['consumers'][] = [
            'conn' => -1,
            'ch' => 0,
            'tag' => 'peer-' . $peer . '-' . $session,
            'credit' => $credit ?? 0,
            'peer' => $peer,
            'session' => $session,
            'noAck' => $noAck,
        ];
    }

    public function removeRemoteConsumer(string $queue, string $peer, int $session): void
    {
        if (!isset($this->queues[$queue])) {
            return;
        }
        $this->queues[$queue]['consumers'] = array_values(array_filter(
            $this->queues[$queue]['consumers'],
            static fn (array $c): bool => !(($c['peer'] ?? '') === $peer && ($c['session'] ?? 0) === $session),
        ));
    }

    /** Adds delivery credit for a remote subscriber. Null means unlimited. */
    public function setRemoteCredit(string $queue, string $peer, int $session, ?int $credit, bool $add): void
    {
        if (!isset($this->queues[$queue])) {
            return;
        }
        foreach ($this->queues[$queue]['consumers'] as $i => $c) {
            if (($c['peer'] ?? '') !== $peer || ($c['session'] ?? 0) !== $session) {
                continue;
            }
            if ($credit === null) {
                $this->queues[$queue]['consumers'][$i]['credit'] = 0;
                return;
            }
            $this->queues[$queue]['consumers'][$i]['credit'] = $add
                ? (int) $c['credit'] + $credit
                : $credit;
            return;
        }
    }

    /** Drops every remote consumer belonging to a peer that went away. */
    public function dropPeerConsumers(string $peer): void
    {
        foreach ($this->queues as $name => $queue) {
            $this->queues[$name]['consumers'] = array_values(array_filter(
                $queue['consumers'],
                static fn (array $c): bool => ($c['peer'] ?? '') !== $peer,
            ));
        }
    }
}
