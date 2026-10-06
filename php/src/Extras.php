<?php
declare(strict_types=1);

/** Management, metrics, cluster, MQTT, STOMP, and stream sockets on the broker's select loop. */
final class Extras
{
    /** @var array<int, array{kind:string,fp:mixed}> */
    public array $listens = [];
    /** @var array<int, array{kind:string,fp:mixed,buf:string,conn:int}> */
    public array $conns = [];
    public Cluster $cluster;
    public Http $http;
    public Protocols $protocols;
    private float $nextDial = 0.0;

    /** @param array<string, mixed> $cfg */
    public function __construct(public Broker $broker, array $cfg)
    {
        $this->cluster = new Cluster($broker, (string) ($cfg['node_id'] ?? 'queueforge'));
        $broker->members = $cfg['members'] ?? [];
        $port = self::port((string) ($cfg['management'] ?? '127.0.0.1:15672'));
        $ui = dirname(__DIR__, 2) . '/rust/ui/dist';
        $this->http = new Http($broker, $ui, $port, !empty($cfg['tls']));
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
            $this->conns[(int) $client] = ['kind' => $kind, 'fp' => $client, 'buf' => '', 'conn' => $conn];
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
                unset($this->conns[$id]);
                fclose($fp);
            }
            return;
        }
        $this->conns[$id]['buf'] .= $chunk;
        $this->dispatch($id);
    }

    public function tick(): void
    {
        if (microtime(true) < $this->nextDial) {
            return;
        }
        $this->nextDial = microtime(true) + 0.2;
        foreach ($this->broker->members as $member) {
            if ($member['id'] === $this->broker->nodeId || isset($this->cluster->peers[$member['id']])) {
                continue;
            }
            $fp = @stream_socket_client('tcp://' . $member['addr'], $errno, $errstr, 0.05);
            if ($fp === false) {
                continue;
            }
            stream_set_blocking($fp, false);
            $this->cluster->attach($member['id'], static function (string $line) use ($fp): void {
                @fwrite($fp, $line);
            });
            $hello = json_encode($this->cluster->hello());
            if (is_string($hello)) {
                fwrite($fp, $hello . "\n");
            }
            $this->conns[(int) $fp] = ['kind' => 'cluster', 'fp' => $fp, 'buf' => '', 'conn' => 0];
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
            fwrite($fp, $this->http->handle($raw));
            unset($this->conns[$id]);
            fclose($fp);
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
            $buf = $this->conns[$id]['buf'];
            $out = $this->protocols->mqtt($buf, $fp, $row['conn']);
            $this->conns[$id]['buf'] = '';
            if ($out !== '') {
                fwrite($fp, $out);
            }
            return;
        }
        if ($row['kind'] === 'stomp') {
            $out = $this->protocols->stomp($this->conns[$id]['buf'], $fp, $row['conn']);
            $this->conns[$id]['buf'] = '';
            if ($out !== '') {
                fwrite($fp, $out);
            }
        }
    }

    private static function port(string $addr): int
    {
        $pos = strrpos($addr, ':');
        return $pos === false ? 80 : (int) substr($addr, $pos + 1);
    }
}
