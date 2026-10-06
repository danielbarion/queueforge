<?php
declare(strict_types=1);

/** Newline JSON cluster protocol version 1, the same envelope Rust and Bun speak. */
final class Cluster
{
    /** @var array<string, array{buf:string,write:callable}> */
    public array $peers = [];
    /** @var array<int, string> */
    private array $pending = [];
    private int $seq = 1;
    public string $buf = '';

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
            'users' => array_keys($this->broker->users),
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
        $this->broker->refreshRole();
    }

    /** @param array<string, mixed> $payload */
    public function replicate(array $payload): void
    {
        $id = $this->seq++;
        $this->pending[$id] = (string) ($payload['message_id'] ?? '');
        $line = json_encode([
            'id' => $id,
            'op' => 'quorum_append',
            'v' => 1,
            'from' => $this->nodeId,
            'nodeId' => $this->nodeId,
            'payload' => $payload,
        ]);
        if (!is_string($line)) {
            return;
        }
        foreach ($this->peers as $peer) {
            ($peer['write'])($line . "\n");
        }
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
            return $this->reply((int) ($msg['id'] ?? 0), true, [
                'v' => 1,
                'node' => $this->nodeId,
                'snapshot' => $this->snapshot(),
                'consumed' => [],
            ]);
        }
        if ($op === 'reply') {
            $id = (int) ($msg['id'] ?? 0);
            $qid = $this->pending[$id] ?? '';
            unset($this->pending[$id]);
            if (($msg['ok'] ?? false) === true && $qid !== '') {
                $this->broker->noteCopy($qid);
            }
            return null;
        }
        $payload = is_array($msg['payload'] ?? null) ? $msg['payload'] : $msg;
        try {
            $body = $this->handle($op, $payload);
        } catch (RuntimeException $err) {
            return $this->reply((int) ($msg['id'] ?? 0), false, [], $err->getMessage());
        }
        if ($op === 'deliver') {
            return null;
        }
        return $this->reply((int) ($msg['id'] ?? 0), true, $body);
    }

    /** @param array<string, mixed> $payload */
    private function handle(string $op, array $payload): mixed
    {
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
            $name = (string) ($payload['name'] ?? $payload['queue'] ?? '');
            $args = is_array($payload['args'] ?? null) ? $payload['args'] : [];
            $this->broker->declareQueue($name, $args);
            return ['queue' => ['vhost' => '/', 'name' => $name, 'home' => $this->nodeId]];
        }
        if ($op === 'quorum_drop' || $op === 'ack') {
            return true;
        }
        if ($op === 'stats') {
            $name = (string) ($payload['queue'] ?? $payload['name'] ?? '');
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
