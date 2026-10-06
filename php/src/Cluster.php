<?php
declare(strict_types=1);

/** Newline JSON cluster protocol version 1, the same envelope Rust and Bun speak. */
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
            'payload' => ['v' => 1, 'node' => $this->nodeId, 'snapshot' => $this->snapshot(), 'consumed' => []],
        ];
    }

    /** @return array<string, mixed> */
    public function snapshot(): array
    {
        $queues = [];
        foreach ($this->broker->queues as $name => $queue) {
            $queues[] = ['vhost' => '/', 'name' => $name, 'type' => $queue['args']['queueType'] ?? 'classic'];
        }
        return [
            'users' => $this->broker->users,
            'vhosts' => ['/'],
            'exchanges' => $this->broker->exchanges,
            'queues' => $queues,
            'bindings' => $this->broker->bindings,
        ];
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
        $this->broker->dropPeerConsumers($id);
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
        $now = microtime(true);
        foreach ($this->pending as $correlation => $row) {
            if ($row['deadline'] > $now) {
                continue;
            }
            unset($this->pending[$correlation]);
            if ($row['qid'] !== '') {
                $this->timedOut[] = $row['qid'];
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
    public function request(string $peer, string $op, array $payload, string $qid = ''): int
    {
        if (!isset($this->peers[$peer])) {
            return 0;
        }
        $correlation = $this->seq++;
        $this->pending[$correlation] = [
            'op' => $op,
            'qid' => $qid,
            'deadline' => microtime(true) + self::REQUEST_TIMEOUT,
            'peer' => $peer,
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
    public function deliverTo(string $peer, string $queue, int $session, array $msg, int $localId, bool $settlesOnWrite): void
    {
        $delivery = $this->nextDelivery++;
        $this->remoteDeliveries[$queue . "\0" . $peer . "\0" . $delivery] = $localId;
        $body = base64_encode((string) ($msg['body'] ?? ''));
        $this->send($peer, [
            'v' => 1,
            'op' => 'deliver',
            'id' => 0,
            'from' => $this->nodeId,
            'nodeId' => $this->nodeId,
            'payload' => [
                'vhost' => '/',
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
            if (is_array($payload['snapshot'] ?? null)) {
                $this->broker->applySnapshot($payload['snapshot']);
            }
            return $this->reply((int) ($msg['id'] ?? 0), true, [
                'v' => 1,
                'node' => $this->nodeId,
                'snapshot' => $this->snapshot(),
                'consumed' => [],
            ]);
        }
        if ($op === 'reply') {
            $id = (int) ($msg['id'] ?? 0);
            $row = $this->pending[$id] ?? null;
            unset($this->pending[$id]);
            $payload = is_array($msg['payload'] ?? null) ? $msg['payload'] : [];
            if (is_array($payload['snapshot'] ?? null)) {
                $this->broker->applySnapshot($payload['snapshot']);
            }
            if ($row !== null && ($msg['ok'] ?? false) === true && $row['qid'] !== '') {
                $this->broker->noteCopy($row['qid']);
            }
            return null;
        }
        $payload = is_array($msg['payload'] ?? null) ? $msg['payload'] : $msg;
        $from = (string) ($msg['from'] ?? $msg['nodeId'] ?? '');
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
        $queueOf = static fn (array $p): string => (string) ($p['queue'] ?? $p['name'] ?? '');
        if ($op === 'quorum_append' || $op === 'enqueue') {
            $decoded = Features::decodeQuorumAppend($payload);
            $ok = $this->broker->enqueueLocal(
                $decoded['queue'],
                $decoded['messageId'],
                $decoded['body'],
                $decoded['exchange'],
                $decoded['routingKey'],
                $decoded['persistent'],
            );
            if ($op === 'quorum_append' && !$ok) {
                throw new RuntimeException('NOT_STORED');
            }
            return true;
        }
        if ($op === 'declare_queue' || $op === 'declare') {
            $name = $queueOf($payload);
            $args = is_array($payload['args'] ?? null) ? $payload['args'] : [];
            $this->broker->declareQueue($name, $args);
            return ['queue' => ['vhost' => '/', 'name' => $name, 'home' => $this->broker->home($name)]];
        }
        if ($op === 'delete_queue') {
            return $this->broker->deleteQueue($queueOf($payload)) >= 0;
        }
        if ($op === 'purge') {
            return $this->broker->purge($queueOf($payload));
        }
        if ($op === 'quorum_drop') {
            $this->broker->dropReplica($queueOf($payload), (string) ($payload['id'] ?? $payload['message_id'] ?? $payload['qid'] ?? ''));
            return true;
        }
        if ($op === 'ack' || $op === 'nack') {
            $queue = $queueOf($payload);
            $delivery = (int) ($payload['delivery_id'] ?? $payload['id'] ?? 0);
            $key = $queue . "\0" . $from . "\0" . $delivery;
            $local = $this->remoteDeliveries[$key] ?? null;
            unset($this->remoteDeliveries[$key]);
            if ($local === null) {
                return true;
            }
            $requeue = $op === 'nack' && ($payload['requeue'] ?? true) !== false;
            if ($requeue) {
                $this->broker->requeue($local);
            } else {
                $this->broker->ack($local);
            }
            return true;
        }
        if ($op === 'get') {
            $queue = $queueOf($payload);
            $noAck = ($payload['noAck'] ?? $payload['no_ack'] ?? false) !== false;
            $id = $this->broker->getReady($queue);
            if ($id === null) {
                return ['empty' => true];
            }
            $msg = $this->broker->msgs[$id];
            if ($noAck) {
                $this->broker->ack($id);
            }
            return ['msg' => [
                'id' => (string) $id,
                'exchange' => (string) ($msg['exchange'] ?? ''),
                'routing_key' => (string) ($msg['key'] ?? $queue),
                'body_b64' => base64_encode($msg['body']),
                'persistent' => ($msg['mode'] ?? 2) === 2,
                'redelivered' => (bool) ($msg['redelivered'] ?? false),
            ]];
        }
        if ($op === 'sub') {
            $credit = array_key_exists('credit', $payload) && $payload['credit'] !== null
                ? (int) $payload['credit']
                : null;
            $this->broker->addRemoteConsumer(
                $queueOf($payload),
                $from,
                (int) ($payload['session'] ?? 0),
                ($payload['noAck'] ?? $payload['no_ack'] ?? false) !== false,
                $credit,
            );
            return true;
        }
        if ($op === 'unsub') {
            $this->broker->removeRemoteConsumer($queueOf($payload), $from, (int) ($payload['session'] ?? 0));
            return true;
        }
        if ($op === 'credit' || $op === 'set_credit') {
            $credit = array_key_exists('credit', $payload) && $payload['credit'] !== null
                ? (int) $payload['credit']
                : null;
            $this->broker->setRemoteCredit(
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
                    $this->broker->refreshRole();
                }
                return true;
            }
            if (is_array($body)) {
                $this->broker->applySnapshot($body);
            }
            return true;
        }
        if ($op === 'deliver') {
            $this->onDeliver($payload);
            return null;
        }
        if ($op === 'stats') {
            $name = $queueOf($payload);
            $q = $this->broker->queues[$name] ?? null;
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
        $this->broker->enqueueLocal(
            $queue,
            (string) ($source['message_id'] ?? $source['id'] ?? ''),
            $body,
            (string) ($source['exchange'] ?? ''),
            (string) ($source['routing_key'] ?? $source['routingKey'] ?? $queue),
            ($source['persistent'] ?? true) !== false,
        );
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
