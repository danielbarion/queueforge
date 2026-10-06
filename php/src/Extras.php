<?php
declare(strict_types=1);

/** Management, metrics, cluster, MQTT, STOMP, and stream sockets on the broker's select loop. */
final class Extras
{
    /** @var array<int, array{kind:string,fp:mixed}> */
    public array $listens = [];
    /** @var array<int, array{kind:string,fp:mixed,buf:string,conn:int,peer:string,state:array<string, mixed>}> */
    public array $conns = [];
    public Cluster $cluster;
    public Http $http;
    public Protocols $protocols;
    private float $nextDial = 0.0;
    private string $membersFile = '';
    /**
     * Failed dial attempts per member. After this many a peer is treated as
     * unreachable rather than merely slow, so readiness is not held forever.
     *
     * @var array<string, int>
     */
    private array $strikes = [];
    private const UNREACHABLE_STRIKES = 5;

    /**
     * Holds readiness while a quorum queue exists and a peer has neither
     * been heard from nor refused enough dials to count as unreachable. A
     * node that answered /readyz immediately could take a quorum publish it
     * has no majority for. Bun gates the same way.
     */
    private function refreshReady(): void
    {
        $hasQuorum = false;
        foreach ($this->broker->queues as $queue) {
            if (($queue['args']['queueType'] ?? 'classic') === 'quorum') {
                $hasQuorum = true;
                break;
            }
        }
        if (!$hasQuorum || count($this->broker->members) < 2) {
            $this->broker->ready = true;
            return;
        }
        foreach ($this->broker->members as $member) {
            if ($member['id'] === $this->broker->nodeId) {
                continue;
            }
            $heard = isset($this->cluster->peers[$member['id']]);
            $unreachable = ($this->strikes[$member['id']] ?? 0) >= self::UNREACHABLE_STRIKES;
            if (!$heard && !$unreachable) {
                $this->broker->ready = false;
                return;
            }
        }
        $this->broker->ready = true;
    }

    /**
     * Reads the stored member list, falling back to the config. The file wins
     * so a membership change made at runtime survives a restart, which is how
     * Bun treats members.json (bun/src/cluster.ts:111-128).
     *
     * @param list<array{id:string,addr:string}> $fromConfig
     * @return list<array{id:string,addr:string}>
     */
    private function loadMembers(array $fromConfig): array
    {
        if (!is_file($this->membersFile)) {
            return $fromConfig;
        }
        $decoded = json_decode((string) file_get_contents($this->membersFile), true);
        if (!is_array($decoded)) {
            return $fromConfig;
        }
        $members = [];
        foreach ($decoded as $row) {
            if (is_array($row) && isset($row['id'], $row['addr'])) {
                $members[] = ['id' => (string) $row['id'], 'addr' => (string) $row['addr']];
            }
        }
        return $members === [] ? $fromConfig : $members;
    }

    /** Writes the member list so a runtime change outlives the process. */
    public function saveMembers(): void
    {
        if ($this->membersFile === '') {
            return;
        }
        $dir = dirname($this->membersFile);
        if (!is_dir($dir) && !mkdir($dir, 0777, true) && !is_dir($dir)) {
            return;
        }
        @file_put_contents($this->membersFile, json_encode($this->broker->members));
    }

    /** @param array<string, mixed> $cfg */
    public function __construct(public Broker $broker, array $cfg)
    {
        $this->cluster = new Cluster($broker, (string) ($cfg['node_id'] ?? 'queueforge'));
        $this->membersFile = rtrim((string) ($cfg['dir'] ?? '.'), '/') . '/members.json';
        $broker->members = $this->loadMembers(is_array($cfg['members'] ?? null) ? $cfg['members'] : []);
        $port = self::port((string) ($cfg['management'] ?? '127.0.0.1:15672'));
        $ui = dirname(__DIR__, 2) . '/rust/ui/dist';
        $this->http = new Http($broker, $ui, $port, !empty($cfg['tls']));
        // A membership change made through the management API is persisted and
        // broadcast as an apply op, which is how Bun propagates it.
        $this->http->onMembers = function (): void {
            $this->saveMembers();
            foreach ($this->cluster->peerIds() as $peer) {
                $this->cluster->request($peer, 'apply', [
                    'kind' => 'members',
                    'body' => $this->broker->members,
                ]);
            }
        };
        $this->protocols = new Protocols($broker);
        foreach (['management', 'metrics', 'cluster', 'mqtt', 'stomp', 'stream'] as $kind) {
            $addr = $cfg[$kind] ?? null;
            if (!is_string($addr) || $addr === '') {
                continue;
            }
            $scheme = ($kind !== 'metrics' && !empty($cfg['tls'])) ? 'tls' : 'tcp';
            $fp = stream_socket_server("$scheme://$addr", $errno, $errstr);
            if ($fp === false) {
                fwrite(STDERR, "listen $kind $addr: $errstr\n");
                continue;
            }
            stream_set_blocking($fp, false);
            $this->listens[(int) $fp] = ['kind' => $kind, 'fp' => $fp];
        }
    }

    /** @return list<mixed> */
    public function reads(): array
    {
        $out = [];
        foreach ($this->listens as $row) {
            $out[] = $row['fp'];
        }
        foreach ($this->conns as $row) {
            $out[] = $row['fp'];
        }
        return $out;
    }

    public function owns(mixed $fp): bool
    {
        $id = (int) $fp;
        return isset($this->listens[$id]) || isset($this->conns[$id]);
    }

    public function onRead(mixed $fp): void
    {
        $id = (int) $fp;
        if (isset($this->listens[$id])) {
            $client = @stream_socket_accept($fp, 0);
            if ($client === false) {
                return;
            }
            stream_set_blocking($client, false);
            $kind = $this->listens[$id]['kind'];
            $conn = $kind === 'mqtt' ? $this->protocols->nextMqtt() : ($kind === 'stomp' ? $this->protocols->nextStomp() : 0);
            $this->conns[(int) $client] = ['kind' => $kind, 'fp' => $client, 'buf' => '', 'conn' => $conn, 'peer' => '', 'state' => []];
            return;
        }
        $row = $this->conns[$id] ?? null;
        if ($row === null) {
            return;
        }
        $chunk = @fread($fp, 65536);
        if ($chunk === false || $chunk === '') {
            $meta = stream_get_meta_data($fp);
            if ($meta['eof'] ?? false) {
                $this->close($id);
            }
            return;
        }
        $this->conns[$id]['buf'] .= $chunk;
        $this->dispatch($id);
    }

    public function tick(): void
    {
        $this->cluster->tick();
        if (microtime(true) < $this->nextDial) {
            return;
        }
        $this->nextDial = microtime(true) + 0.2;
        $this->refreshReady();
        foreach ($this->broker->members as $member) {
            // Only the lower id dials, so a pair gets exactly one connection
            // instead of two. Bun uses the same rule.
            if (!Features::shouldDial($this->broker->nodeId, $member['id'])) {
                continue;
            }
            if (isset($this->cluster->peers[$member['id']])) {
                continue;
            }
            $fp = @stream_socket_client('tcp://' . $member['addr'], $errno, $errstr, 0.05);
            if ($fp === false) {
                $this->strikes[$member['id']] = ($this->strikes[$member['id']] ?? 0) + 1;
                continue;
            }
            $this->strikes[$member['id']] = 0;
            stream_set_blocking($fp, false);
            $this->cluster->attach($member['id'], static function (string $line) use ($fp): void {
                @fwrite($fp, $line);
            });
            $hello = json_encode($this->cluster->hello());
            if (is_string($hello)) {
                fwrite($fp, $hello . "\n");
            }
            $this->conns[(int) $fp] = ['kind' => 'cluster', 'fp' => $fp, 'buf' => '', 'conn' => 0, 'peer' => $member['id'], 'state' => []];
        }
    }

    /**
     * Drops a connection. A cluster peer is detached as well, so a partition
     * shrinks the peer set instead of leaving it to grow and skew the
     * majority that Broker::isLeader() derives from it.
     */
    private function close(int $id): void
    {
        $row = $this->conns[$id] ?? null;
        if ($row === null) {
            return;
        }
        unset($this->conns[$id]);
        if ($row['kind'] === 'cluster' && ($row['peer'] ?? '') !== '') {
            $this->cluster->detach($row['peer']);
        }
        if (is_resource($row['fp'])) {
            fclose($row['fp']);
        }
    }

    private function dispatch(int $id): void
    {
        $row = $this->conns[$id];
        $fp = $row['fp'];
        if ($row['kind'] === 'management' || $row['kind'] === 'metrics') {
            if (!str_contains($row['buf'], "\r\n\r\n")) {
                return;
            }
            $raw = $row['buf'];
            $head = strstr($raw, "\r\n\r\n", true);
            $length = 0;
            if (is_string($head) && preg_match('/Content-Length:\s*(\d+)/i', $head, $m) === 1) {
                $length = (int) $m[1];
            }
            if (strlen($raw) < strlen((string) $head) + 4 + $length) {
                return;
            }
            self::writeAll($fp, $this->http->handle($raw));
            $this->close($id);
            return;
        }
        if ($row['kind'] === 'cluster') {
            while (($nl = strpos($this->conns[$id]['buf'], "\n")) !== false) {
                $line = substr($this->conns[$id]['buf'], 0, $nl);
                $this->conns[$id]['buf'] = substr($this->conns[$id]['buf'], $nl + 1);
                $decoded = json_decode($line, true);
                if (is_array($decoded) && ($decoded['op'] ?? '') === 'hello') {
                    $payload = is_array($decoded['payload'] ?? null) ? $decoded['payload'] : [];
                    $peer = (string) ($decoded['nodeId'] ?? $payload['node'] ?? '');
                    if ($peer !== '') {
                        $this->conns[$id]['peer'] = $peer;
                        $this->cluster->attach($peer, static function (string $out) use ($fp): void {
                            @fwrite($fp, $out);
                        });
                    }
                }
                $reply = $this->cluster->handleLine($line);
                if ($reply !== null && $reply !== '') {
                    fwrite($fp, $reply . "\n");
                }
            }
            return;
        }
        if ($row['kind'] === 'mqtt') {
            // The buffer goes in by reference so a packet split across reads
            // keeps its tail for the next pass.
            $buf = $this->conns[$id]['buf'];
            $out = $this->protocols->mqtt($buf, $fp, $row['conn']);
            $this->conns[$id]['buf'] = $buf;
            if ($out !== '') {
                self::writeAll($fp, $out);
            }
            return;
        }
        if ($row['kind'] === 'stomp') {
            $buf = $this->conns[$id]['buf'];
            $out = $this->protocols->stomp($buf, $fp, $row['conn']);
            $this->conns[$id]['buf'] = $buf;
            if ($out !== '') {
                self::writeAll($fp, $out);
            }
            return;
        }
        if ($row['kind'] === 'stream') {
            // Protocols::stream() takes the buffer and the per-connection
            // state by reference and consumes whole frames only, so the
            // remainder stays buffered for the next read.
            $buf = $this->conns[$id]['buf'];
            $state = $this->conns[$id]['state'];
            $out = $this->protocols->stream($buf, $state);
            $this->conns[$id]['buf'] = $buf;
            $this->conns[$id]['state'] = $state;
            if ($out !== '') {
                self::writeAll($fp, $out);
            }
        }
    }

    /**
     * Writes every byte. A bare fwrite() can short-write on a full socket
     * buffer, which truncated larger management responses.
     *
     * @param resource $fp
     */
    private static function writeAll($fp, string $data): void
    {
        $at = 0;
        $total = strlen($data);
        while ($at < $total) {
            $n = @fwrite($fp, substr($data, $at));
            if ($n === false || $n === 0) {
                return;
            }
            $at += $n;
        }
    }

    private static function port(string $addr): int
    {
        $pos = strrpos($addr, ':');
        return $pos === false ? 80 : (int) substr($addr, $pos + 1);
    }
}
