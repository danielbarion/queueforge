<?php
declare(strict_types=1);

require_once __DIR__ . '/Raft.php';
require_once __DIR__ . '/RaftNode.php';

/** Shared v1 envelope, with negotiated v2 capability/consensus payloads. */
final class Cluster
{
    /** How long a request waits for its reply, matching Bun's 3000 ms. */
    public const REQUEST_TIMEOUT = 3.0;

    /**
     * Beyond the peers a majority needs, a peer is only sent an extra append
     * while it has fewer than this many requests in flight. Bun calls this
     * EXTRA_APPEND_CAP.
     */
    public const EXTRA_APPEND_CAP = 32;

    /** @var array<string, array{buf:string,write:callable}> */
    public array $peers = [];
    /**
     * In-flight requests by correlation id.
     *
     * @var array<int, array{op:string,qid:string,deadline:float,peer:string}>
     */
    private array $pending = [];
    private int $seq = 1;
    public ?RaftNode $raft = null;
    private array $peerFeatures = [];
    private bool $startingRaft = false;

    private function raftDir(): string { return $this->broker->dataDir() . '/raft'; }
    private function supported(): bool { return getenv('QUEUEFORGE_RAFT') !== '0'; }
    public function raftEnabled(): bool { return $this->raft !== null; }
    public function raftFeatures(): array
    {
        if (!$this->supported()) return [];
        $out = ['raft'];
        if ($this->raft !== null || is_file($this->raftDir() . '/auto')) $out[] = 'raft_auto';
        if ($this->raft !== null || is_file($this->raftDir() . '/enabled')) $out[] = 'raft_on';
        if (getenv('QUEUEFORGE_RAFT_QGROUPS') !== '0') $out[] = 'raft_qgroups';
        return $out;
    }
    public function markFreshRaft(): void
    {
        if ($this->supported()) RaftNode::atomic($this->raftDir(), 'auto', true);
    }
    private function voters(): array
    {
        $ids = array_map(static fn (array $m): string => (string) $m['id'], $this->broker->members);
        return $ids === [] ? [$this->nodeId] : $ids;
    }
    private function allHave(string $feature): bool
    {
        foreach ($this->voters() as $id) if ($id !== $this->nodeId && !in_array($feature, $this->peerFeatures[$id] ?? [], true)) return false;
        return true;
    }
    /** Explicit activation requires known support from every configured voter. */
    public function enableRaft(): bool
    {
        if ($this->raft !== null) return true;
        if (!$this->supported() || !$this->allHave('raft')) return false;
        $this->activateRaft();
        foreach ($this->peerIds() as $peer) $this->send($peer, ['v' => 1, 'op' => 'feature', 'id' => 0, 'from' => $this->nodeId, 'payload' => ['name' => 'raft', 'enabled' => true]]);
        return true;
    }
    private function activateRaft(): void
    {
        if ($this->raft !== null || $this->startingRaft || !$this->supported()) return;
        $this->startingRaft = true;
        try {
            RaftNode::atomic($this->raftDir(), 'enabled', true);
            $this->raft = new RaftNode($this->nodeId, $this->raftDir(), $this->voters(),
                fn (string $peer, array $msg) => $this->send($peer, ['v' => 1, 'op' => 'raft', 'id' => 0, 'from' => $this->nodeId, 'nodeId' => $this->nodeId, 'payload' => $msg]),
                function (string $group, array $entry): void {
                    $this->broker->applyRaft($group, $entry['kind'], $entry['data'], $entry['i']);
                    if ($group === 'meta' && $entry['kind'] === 'members') $this->reconfigureMembers($this->broker->members);
                },
                fn (string $group, mixed $state) => $this->broker->installRaftState($group, $state),
                fn (string $group) => $this->broker->raftState($group),
                function (string $group, ?string $leader): void {
                    if (method_exists($this->broker, 'raftLeaderChanged')) $this->broker->raftLeaderChanged($group, $leader);
                },
                function (string $group): bool {
                    foreach ($this->broker->allBrokers() as $broker) foreach ($broker->queues as $q) if (($q['raftGroup'] ?? $q['args']['raftGroup'] ?? null) === $group) return true;
                    return false;
                });
            foreach ($this->broker->allBrokers() as $broker) foreach ($broker->queues as $q) {
                $group = $q['raftGroup'] ?? $q['args']['raftGroup'] ?? null;
                if (is_string($group) && str_starts_with($group, 'q:')) $this->raft->addGroup($group);
            }
        } finally { $this->startingRaft = false; }
    }
    private function noteFeatures(string $peer, array $payload): void
    {
        $features = is_array($payload['features'] ?? null) ? $payload['features'] : [];
        $this->peerFeatures[$peer] = $features;
        if (!$this->supported() || !in_array($peer, $this->voters(), true)) return;
        if (in_array('raft_on', $features, true)) $this->activateRaft();
        $this->maybeEnable();
    }
    private function maybeEnable(): void
    {
        if (!$this->supported() || $this->raft !== null) return;
        if (is_file($this->raftDir() . '/enabled') || (is_file($this->raftDir() . '/auto') && $this->allHave('raft') && $this->allHave('raft_auto'))) $this->activateRaft();
    }
    public function propose(string $group, string $kind, mixed $data, callable $done): bool
    {
        if ($this->raft === null) { $done(false, 'Raft is not enabled'); return false; }
        return $this->raft->propose($group, $kind, $data, $done);
    }
    public function proposeMeta(string $kind, mixed $data, callable $done): bool { return $this->propose('meta', $kind, $data, $done); }
    public function queueGroup(string $vhost, string $name): ?string
    {
        return $this->raft !== null && getenv('QUEUEFORGE_RAFT_QGROUPS') !== '0' && $this->allHave('raft_qgroups') ? RaftNode::queueGroup($vhost, $name) : null;
    }
    public function quorumGroup(string $vhost, string $name): string
    {
        $q = $this->broker->forVhost($vhost)->queues[$name] ?? [];
        return is_string($q['raftGroup'] ?? $q['args']['raftGroup'] ?? null) ? ($q['raftGroup'] ?? $q['args']['raftGroup']) : 'quorum';
    }
    public function queueLeader(string $vhost, string $name): ?string { return $this->raft?->leader($this->quorumGroup($vhost, $name)); }
    public function registerQueueGroup(string $group, bool $lead = false): void { $this->raft?->addGroup($group, $lead); }
    public function dropQueueGroup(string $group): void { $this->raft?->dropGroup($group); }
    public function reconfigureMembers(array $members): void { $this->raft?->setVoters(array_map(static fn (array $m): string => (string) $m['id'], $members)); $this->maybeEnable(); }

    public string $buf = '';
    /**
     * Remote delivery ids this node handed out, keyed
     * "queue\0peer\0deliveryId", so a peer's ack can be mapped back to the
     * local message id.
     *
     * @var array<string, int>
     */
    private array $remoteDeliveries = [];
    private int $nextDelivery = 1;
    /** @var list<string> correlation ids whose request timed out */
    public array $timedOut = [];

    public function __construct(public Broker $broker, public string $nodeId)
    {
        $this->broker->cluster = $this;
        $this->broker->nodeId = $nodeId;
    }

    /** @return list<string> */
    public function peerIds(): array
    {
        return array_keys($this->peers);
    }

    public function hello(): array
    {
        return [
            'id' => 0,
            'op' => 'hello',
            'ok' => false,
            'error' => '',
            'v' => 1,
            'nodeId' => $this->nodeId,
            'from' => $this->nodeId,
            'kind' => '',
            'payload' => ['v' => 1, 'features' => $this->raftFeatures(), 'node' => $this->nodeId, 'snapshot' => $this->snapshot(), 'consumed' => $this->consumedList()],
        ];
    }

    /** @return array<string, mixed> */
    public function snapshot(): array
    {
        $queues = []; $exchanges = []; $bindings = [];
        foreach ($this->broker->allBrokers() as $broker) {
            foreach ($broker->queues as $name => $queue) {
                $queues[] = ['vhost' => $broker->vhost, 'name' => $name,
                    'durable' => (bool) ($queue['durable'] ?? true),
                    'exclusive' => (bool) ($queue['exclusive'] ?? false),
                    'autoDelete' => (bool) ($queue['autoDelete'] ?? false),
                    'type' => $queue['args']['queueType'] ?? 'classic',
                    'args' => $queue['declaredArgs'] ?? $queue['args'], 'home' => $broker->home($name),
                    'raftGroup' => $queue['raftGroup'] ?? $queue['args']['raftGroup'] ?? null];
            }
            foreach ($broker->exchanges as $name => $type) {
                $exchanges[] = ['vhost' => $broker->vhost, 'name' => $name, 'type' => $type] + ($broker->exchangeRows[$name] ?? []);
            }
            foreach ($broker->bindings as $row) $bindings[] = ['vhost' => $broker->vhost] + $row;
        }
        $users = []; $permissions = [];
        foreach ($this->broker->users as $name => $hash) $users[] = ['name' => $name, 'hash' => $hash, 'tags' => $this->broker->tags[$name] ?? []];
        foreach ($this->broker->permissions as $user => $hosts) foreach ($hosts as $vhost => $rules) $permissions[] = ['user' => $user, 'vhost' => $vhost] + $rules;
        return ['users' => $users, 'vhosts' => $this->broker->vhosts, 'permissions' => $permissions,
            'exchanges' => $exchanges, 'queues' => $queues, 'bindings' => $bindings, 'consumed' => $this->consumedList()];
    }
    private function consumedList(): array
    {
        $out = [];
        foreach ($this->broker->allBrokers() as $broker) foreach ($broker->consumedList() as $row) {
            if (is_array($row)) $out[] = ['vhost' => $broker->vhost, 'queue' => $row['queue'] ?? $row[0] ?? '', 'id' => $row['id'] ?? $row[1] ?? ''];
        }
        return $out;
    }
    private function applyConsumed(array $rows): void
    {
        foreach ($rows as $row) {
            if (!is_array($row)) continue;
            $vhost = (string) ($row['vhost'] ?? '/');
            $this->broker->forVhost($vhost)->applyConsumed([[(string) ($row['queue'] ?? $row[0] ?? ''), (string) ($row['id'] ?? $row[1] ?? '')]]);
        }
    }

    /** @param callable(string):void $write */
    public function attach(string $id, callable $write): void
    {
        $this->peers[$id] = ['buf' => '', 'write' => $write];
        $this->broker->refreshRole();
    }

    public function detach(string $id): void
    {
        unset($this->peers[$id]);
        foreach ($this->broker->allBrokers() as $broker) $broker->dropPeerConsumers($id);
        foreach ($this->pending as $correlation => $row) {
            if ($row['peer'] === $id) {
                unset($this->pending[$correlation]);
            }
        }
        $this->broker->refreshRole();
    }

    /**
     * Expires requests whose reply never came. Called from the select loop,
     * so a dead peer cannot leave a publish waiting forever.
     */
    public function tick(): void
    {
        $this->maybeEnable();
        $this->raft?->tick();
        $now = microtime(true);
        foreach ($this->pending as $correlation => $row) {
            if ($row['deadline'] > $now) {
                continue;
            }
            unset($this->pending[$correlation]);
            if ($row['qid'] !== '') {
                $this->timedOut[] = $row['qid'];
                $this->broker->forVhost($row['vhost'] ?? '/')->failQuorum($row['qid']);
            }
            // A waiting caller is told the request failed rather than being
            // left with a client that never gets an answer.
            if (($row['onReply'] ?? null) !== null) {
                ($row['onReply'])(null);
            }
        }
    }

    /**
     * Replicates a quorum append. Peers are ordered by in-flight requests
     * then by id, so the least busy are asked first. Everyone needed for a
     * majority is always asked; beyond that a peer is only asked while its
     * in-flight count is under the cap, which is Bun's EXTRA_APPEND_CAP of
     * 32 (bun/src/quorum-confirm.ts:8,21-40).
     *
     * @param array<string, mixed> $payload
     */
    public function replicate(array $payload): void
    {
        $qid = (string) ($payload['message_id'] ?? '');
        $peers = $this->peerIds();
        if ($peers === []) {
            return;
        }
        $inFlight = array_fill_keys($peers, 0);
        foreach ($this->pending as $row) {
            if (isset($inFlight[$row['peer']])) {
                $inFlight[$row['peer']]++;
            }
        }
        usort($peers, static function (string $x, string $y) use ($inFlight): int {
            return $inFlight[$x] <=> $inFlight[$y] ?: strcmp($x, $y);
        });
        // One of the majority is this node's own copy.
        $needed = max(0, Features::majority(max(1, count($this->broker->members))) - 1);
        foreach ($peers as $i => $peer) {
            if ($i >= $needed && $inFlight[$peer] >= self::EXTRA_APPEND_CAP) {
                continue;
            }
            $this->request($peer, 'quorum_append', $payload, $qid);
        }
    }

    /**
     * Sends a request to one peer and records it for reply correlation.
     * Returns the correlation id, or 0 when the peer is not connected.
     *
     * @param array<string, mixed> $payload
     */
    /**
     * Sends a request to a peer.
     *
     * An optional callback is invoked with the reply payload once it lands,
     * or with null if the request times out. That is what lets a forwarded
     * basic.get answer the client later without blocking the select loop.
     *
     * @param array<string, mixed> $payload
     * @param ?callable(?array<string, mixed>):void $onReply
     */
    public function request(string $peer, string $op, array $payload, string $qid = '', ?callable $onReply = null): int
    {
        if (!isset($this->peers[$peer])) {
            if ($onReply !== null) {
                $onReply(null);
            }
            return 0;
        }
        $correlation = $this->seq++;
        $this->pending[$correlation] = [
            'op' => $op,
            'vhost' => (string) ($payload['vhost'] ?? '/'),
            'qid' => $qid,
            'deadline' => microtime(true) + self::REQUEST_TIMEOUT,
            'peer' => $peer,
            'onReply' => $onReply,
        ];
        $this->send($peer, [
            'v' => 1,
            'op' => $op,
            'id' => $correlation,
            'from' => $this->nodeId,
            'nodeId' => $this->nodeId,
            'payload' => $payload,
        ]);
        return $correlation;
    }

    /** Sends one line to a peer, if it is connected. */
    private function send(string $peer, array $message): void
    {
        if (!isset($this->peers[$peer])) {
            return;
        }
        $line = json_encode($message);
        if (!is_string($line)) {
            return;
        }
        ($this->peers[$peer]['write'])($line . "\n");
    }

    /**
     * Pushes a message to a peer that subscribed with the sub op. The payload
     * carries both the Bun-native and Rust-shaped bodies, as Bun does
     * (bun/src/cluster.ts:321-349), so either implementation can read it.
     *
     * @param array<string, mixed> $msg
     */
    public function deliverTo(string $peer, string $queue, int $session, array $msg, int $localId, bool $settlesOnWrite, string $vhost = '/'): void
    {
        $delivery = $this->nextDelivery++;
        $this->remoteDeliveries[$vhost . "\0" . $queue . "\0" . $peer . "\0" . $delivery] = $localId;
        $body = base64_encode((string) ($msg['body'] ?? ''));
        $this->send($peer, [
            'v' => 1,
            'op' => 'deliver',
            'id' => 0,
            'from' => $this->nodeId,
            'nodeId' => $this->nodeId,
            'payload' => [
                'vhost' => $vhost,
                'queue' => $queue,
                'session' => $session,
                'delivery_id' => $delivery,
                'offset' => 0,
                'settles_on_write' => $settlesOnWrite,
                'message' => [
                    'exchange' => (string) ($msg['exchange'] ?? ''),
                    'routing_key' => (string) ($msg['key'] ?? $queue),
                    'body_b64' => $body,
                    'persistent' => ($msg['mode'] ?? 2) === 2,
                    'redelivered' => (bool) ($msg['redelivered'] ?? false),
                    'message_id' => (string) $localId,
                    'propRaw' => base64_encode((string) ($msg['propRaw'] ?? '')),
                    'headers' => $msg['headers'] ?? [],
                ],
                'msg' => [
                    'id' => (string) $localId,
                    'exchange' => (string) ($msg['exchange'] ?? ''),
                    'routingKey' => (string) ($msg['key'] ?? $queue),
                    'persistent' => ($msg['mode'] ?? 2) === 2,
                    'priority' => (int) ($msg['priority'] ?? 0),
                    'redelivered' => (bool) ($msg['redelivered'] ?? false),
                    'body' => $body,
                    'propRaw' => base64_encode((string) ($msg['propRaw'] ?? '')),
                ],
            ],
        ]);
    }

    /** @return list<string> reply lines, without the trailing newline */
    public function ingest(string $text): array
    {
        $this->buf .= $text;
        $replies = [];
        while (($nl = strpos($this->buf, "\n")) !== false) {
            $line = substr($this->buf, 0, $nl);
            $this->buf = substr($this->buf, $nl + 1);
            if (trim($line) === '') {
                continue;
            }
            $reply = $this->handleLine($line);
            if ($reply !== null) {
                $replies[] = $reply;
            }
        }
        return $replies;
    }

    public function handleLine(string $line): ?string
    {
        $msg = json_decode($line, true);
        if (!is_array($msg)) {
            return null;
        }
        $op = (string) ($msg['op'] ?? '');
        if ($op === 'hello') {
            $payload = is_array($msg['payload'] ?? null) ? $msg['payload'] : [];
            $id = (string) ($msg['nodeId'] ?? $payload['node'] ?? '');
            if ($id !== '') {
                $this->peers[$id] = $this->peers[$id] ?? ['buf' => '', 'write' => static function (string $ignored): void {
                }];
                $this->broker->refreshRole();
            }
            if ($this->raft === null && is_array($payload['snapshot'] ?? null)) {
                $this->broker->applySnapshot($payload['snapshot']);
            }
            $this->noteFeatures($id, $payload);
            // A peer's consumed set names quorum bodies it already handed
            // out, so anything we recovered for those ids is discarded
            // rather than delivered a second time.
            if (is_array($payload['consumed'] ?? null)) {
                $this->applyConsumed($payload['consumed']);
            }
            return $this->reply((int) ($msg['id'] ?? 0), true, [
                'v' => 1,
                'features' => $this->raftFeatures(),
                'node' => $this->nodeId,
                'snapshot' => $this->snapshot(),
                'consumed' => $this->consumedList(),
            ]);
        }
        if ($op === 'reply') {
            $id = (int) ($msg['id'] ?? 0);
            $row = $this->pending[$id] ?? null;
            unset($this->pending[$id]);
            $payload = is_array($msg['payload'] ?? null) ? $msg['payload'] : [];
            $peer = (string) ($msg['from'] ?? $msg['nodeId'] ?? ($row['peer'] ?? ''));
            if ($peer !== '' && array_key_exists('features', $payload)) $this->noteFeatures($peer, $payload);
            if ($this->raft === null && is_array($payload['snapshot'] ?? null)) {
                $this->broker->applySnapshot($payload['snapshot']);
            }
            if (is_array($payload['consumed'] ?? null)) {
                $this->applyConsumed($payload['consumed']);
            }
            if ($row !== null && ($msg['ok'] ?? false) === true && $row['qid'] !== '') {
                $this->broker->forVhost($row['vhost'] ?? '/')->noteCopy($row['qid']);
            }
            if ($row !== null && ($row['onReply'] ?? null) !== null) {
                ($row['onReply'])(($msg['ok'] ?? false) === true ? $payload : null);
            }
            return null;
        }
        $payload = is_array($msg['payload'] ?? null) ? $msg['payload'] : $msg;
        $from = (string) ($msg['from'] ?? $msg['nodeId'] ?? '');
        if ($op === 'raft') {
            if ($this->raft !== null) $this->raft->step($from, $payload);
            return null;
        }
        if ($op === 'feature') {
            if (($payload['name'] ?? '') === 'raft' && ($payload['enabled'] ?? true) === true && in_array($from, $this->voters(), true)) $this->activateRaft();
            return null;
        }
        try {
            $body = $this->handle($op, $payload, $from);
        } catch (RuntimeException $err) {
            return $this->reply((int) ($msg['id'] ?? 0), false, [], $err->getMessage());
        }
        if ($op === 'deliver') {
            return null;
        }
        return $this->reply((int) ($msg['id'] ?? 0), true, $body);
    }

    /** @param array<string, mixed> $payload */
    private function handle(string $op, array $payload, string $from = ''): mixed
    {
        $vhost = (string) ($payload['vhost'] ?? '/');
        $broker = $this->broker->forVhost($vhost);
        if ($this->raft !== null && in_array($op, ['quorum_append', 'declare_queue', 'declare', 'delete_queue', 'purge', 'quorum_drop', 'apply', 'join', 'forget'], true)) throw new RuntimeException('RAFT_REQUIRED');
        $queueOf = static fn (array $p): string => (string) ($p['queue'] ?? $p['name'] ?? '');
        if ($op === 'quorum_append' || $op === 'enqueue') {
            $decoded = Features::decodeQuorumAppend($payload);
            $ok = $broker->enqueueLocal(
                $decoded['queue'],
                $decoded['messageId'],
                $decoded['body'],
                $decoded['exchange'],
                $decoded['routingKey'],
                $decoded['persistent'],
                $this->rawProperties($payload),
                is_array($payload['headers'] ?? null) ? $payload['headers'] : [],
            );
            if ($op === 'quorum_append' && !$ok) {
                throw new RuntimeException('NOT_STORED');
            }
            return true;
        }
        if ($op === 'declare_queue' || $op === 'declare') {
            $name = $queueOf($payload);
            $args = is_array($payload['args'] ?? null) ? $payload['args'] : [];
            $broker->declareQueue($name, $args);
            return ['queue' => ['vhost' => $vhost, 'name' => $name, 'home' => $broker->home($name)]];
        }
        if ($op === 'delete_queue') {
            return $broker->deleteQueue($queueOf($payload)) >= 0;
        }
        if ($op === 'purge') {
            return $broker->purge($queueOf($payload));
        }
        if ($op === 'quorum_drop') {
            $queue = $queueOf($payload);
            $qid = (string) ($payload['id'] ?? $payload['message_id'] ?? $payload['qid'] ?? '');
            $broker->noteConsumed($queue, $qid);
            $broker->dropReplica($queue, $qid);
            return true;
        }
        if ($op === 'ack' || $op === 'nack') {
            $queue = $queueOf($payload);
            $delivery = (int) ($payload['delivery_id'] ?? $payload['id'] ?? 0);
            $key = $vhost . "\0" . $queue . "\0" . $from . "\0" . $delivery;
            $local = $this->remoteDeliveries[$key] ?? null;
            unset($this->remoteDeliveries[$key]);
            if ($local === null) {
                return true;
            }
            $requeue = $op === 'nack' && ($payload['requeue'] ?? true) !== false;
            if ($requeue) {
                $broker->requeue($local);
            } else {
                $broker->noteConsumed($queue, (string) ($broker->msgs[$local]['qid'] ?? ''));
                $broker->ack($local);
            }
            return true;
        }
        if ($op === 'get') {
            $queue = $queueOf($payload);
            $noAck = ($payload['noAck'] ?? $payload['no_ack'] ?? false) !== false;
            $id = $broker->getReady($queue);
            if ($id === null) {
                return ['empty' => true];
            }
            $msg = $broker->msgs[$id];
            if ($noAck) {
                $broker->ack($id);
            }
            return ['msg' => [
                'id' => (string) $id,
                'exchange' => (string) ($msg['exchange'] ?? ''),
                'routing_key' => (string) ($msg['key'] ?? $queue),
                'body_b64' => base64_encode($msg['body']),
                'body' => base64_encode($msg['body']),
                'routingKey' => (string) ($msg['key'] ?? $queue),
                'propRaw' => base64_encode((string) ($msg['propRaw'] ?? '')),
                'headers' => $msg['headers'] ?? [],
                'persistent' => ($msg['mode'] ?? 2) === 2,
                'redelivered' => (bool) ($msg['redelivered'] ?? false),
            ]];
        }
        if ($op === 'sub') {
            $credit = array_key_exists('credit', $payload) && $payload['credit'] !== null
                ? (int) $payload['credit']
                : null;
            $broker->addRemoteConsumer(
                $queueOf($payload),
                $from,
                (int) ($payload['session'] ?? 0),
                ($payload['noAck'] ?? $payload['no_ack'] ?? false) !== false,
                $credit,
            );
            return true;
        }
        if ($op === 'unsub') {
            $broker->removeRemoteConsumer($queueOf($payload), $from, (int) ($payload['session'] ?? 0));
            return true;
        }
        if ($op === 'credit' || $op === 'set_credit') {
            $credit = array_key_exists('credit', $payload) && $payload['credit'] !== null
                ? (int) $payload['credit']
                : null;
            $broker->setRemoteCredit(
                $queueOf($payload),
                $from,
                (int) ($payload['session'] ?? 0),
                $credit,
                $op === 'credit',
            );
            return true;
        }
        if ($op === 'apply') {
            $kind = (string) ($payload['kind'] ?? '');
            $body = $payload['body'] ?? null;
            if ($kind === 'members' && is_array($body)) {
                $members = [];
                foreach ($body as $row) {
                    if (is_array($row) && isset($row['id'], $row['addr'])) {
                        $members[] = ['id' => (string) $row['id'], 'addr' => (string) $row['addr']];
                    }
                }
                if ($members !== []) {
                    $this->broker->members = $members;
                    $broker->refreshRole();
                }
                return true;
            }
            if (is_array($body)) {
                $broker->applySnapshot($body);
            }
            return true;
        }
        if ($op === 'deliver') {
            $this->onDeliver($payload);
            return null;
        }
        if ($op === 'stats') {
            $name = $queueOf($payload);
            $q = $broker->queues[$name] ?? null;
            return [
                'messages_ready' => $q === null ? 0 : count($q['ready']),
                'consumer_count' => $q === null ? 0 : count($q['consumers']),
            ];
        }
        if ($op === 'join') {
            $id = (string) ($payload['id'] ?? '');
            $addr = (string) ($payload['addr'] ?? '');
            $members = array_values(array_filter($this->broker->members, static fn (array $row): bool => $row['id'] !== $id));
            $members[] = ['id' => $id, 'addr' => $addr];
            $this->broker->members = $members;
            return $members;
        }
        if ($op === 'forget') {
            $id = (string) ($payload['id'] ?? '');
            if ($id === $this->nodeId) {
                throw new RuntimeException('a node cannot forget itself');
            }
            $members = array_values(array_filter($this->broker->members, static fn (array $row): bool => $row['id'] !== $id));
            if ($members === []) {
                throw new RuntimeException('the member list cannot become empty');
            }
            $this->broker->members = $members;
            return $members;
        }
        return true;
    }

    /**
     * Stores a message pushed by the queue's home node. Prefers the
     * Rust-shaped payload when it carries a body, as Bun does.
     *
     * @param array<string, mixed> $payload
     */
    private function onDeliver(array $payload): void
    {
        $queue = (string) ($payload['queue'] ?? '');
        $rust = is_array($payload['message'] ?? null) ? $payload['message'] : [];
        $native = is_array($payload['msg'] ?? null) ? $payload['msg'] : [];
        $source = ($rust['body_b64'] ?? '') !== '' ? $rust : $native;
        $b64 = (string) ($source['body_b64'] ?? $source['body'] ?? '');
        $body = base64_decode($b64, true);
        if ($queue === '' || $body === false) {
            return;
        }
        $this->broker->forVhost((string) ($payload['vhost'] ?? '/'))->enqueueLocal(
            $queue,
            (string) ($source['message_id'] ?? $source['id'] ?? ''),
            $body,
            (string) ($source['exchange'] ?? ''),
            (string) ($source['routing_key'] ?? $source['routingKey'] ?? $queue),
            ($source['persistent'] ?? true) !== false,
            $this->rawProperties($payload),
            is_array($source['headers'] ?? null) ? $source['headers'] : [],
        );
    }

    private function rawProperties(array $payload): ?string
    {
        foreach ([$payload, $payload['message'] ?? null, $payload['msg'] ?? null] as $source) {
            if (!is_array($source) || !is_string($source['propRaw'] ?? null)) continue;
            $raw = base64_decode($source['propRaw'], true);
            if ($raw === false) throw new RuntimeException('invalid property encoding');
            return $raw;
        }
        return null;
    }

    /** @param mixed $payload */
    private function reply(int $id, bool $ok, mixed $payload, string $error = ''): string
    {
        $encoded = json_encode([
            'v' => 1,
            'op' => 'reply',
            'id' => $id,
            'ok' => $ok,
            'error' => $error,
            'from' => $this->nodeId,
            'nodeId' => $this->nodeId,
            'payload' => $payload,
        ]);
        return is_string($encoded) ? $encoded : '';
    }
}
