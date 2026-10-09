<?php
declare(strict_types=1);

/** Management, metrics, cluster, MQTT, STOMP, and stream sockets on the broker's select loop. */
require_once __DIR__ . "/WebSocket.php";
require_once __DIR__ . "/Integrations.php";

final class Extras
{
    /** @var array<int, array{kind:string,fp:mixed}> */
    public array $listens = [];
    private Integrations $integrations;
    /** @var array<int, array{kind:string,fp:mixed,buf:string,conn:int,peer:string,state:array<string, mixed>}> */
    public array $conns = [];
    public Cluster $cluster;
    public Http $http;
    public Protocols $protocols;
    public $onAmqp = null;
    private array $cfg = [];
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

    /** An attached outbound socket is not a completed peer negotiation. */
    public function peersReady(): bool
    {
        foreach($this->broker->members as$member){
            if($member['id']===$this->broker->nodeId)continue;
            $ready=false;foreach($this->conns as$row)if($row['kind']==='cluster'&&($row['peer']??'')===$member['id']&&($row['helloDone']??false)&&!($row['closing']??false)){$ready=true;break;}
            if(!$ready)return false;
        }
        return true;
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
        $this->cfg = $cfg;
        $this->integrations = new Integrations($broker);
        $this->cluster = new Cluster($broker, (string) ($cfg['node_id'] ?? 'queueforge'));
        $this->membersFile = rtrim((string) ($cfg['dir'] ?? '.'), '/') . '/members.json';
        $broker->members = $this->loadMembers(is_array($cfg['members'] ?? null) ? $cfg['members'] : []);
        if ($cfg['fresh'] ?? false) $this->cluster->markFreshRaft();
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
        // A user, permission or topology change made through this node's
        // management API is pushed to the peers as a snapshot apply.
        $this->http->onTopology = function (): void {
            foreach ($this->cluster->peerIds() as $peer) {
                $this->cluster->request($peer, 'apply', [
                    'kind' => 'snapshot',
                    'body' => $this->cluster->snapshot(),
                ]);
            }
        };
        $this->protocols = new Protocols($broker);
        $this->protocols->onWrite = function($fp,string $bytes):void { $this->send($fp,$bytes); };
        $this->protocols->onClose = function($fp):void { $this->close((int)$fp); };
        foreach (['management', 'metrics', 'cluster', 'mqtt', 'stomp', 'stream', 'amqps'] as $kind) {
            $addr = $cfg[$kind] ?? null;
            if (!is_string($addr) || $addr === '') {
                continue;
            }
            $secure = $kind === 'amqps' || ($kind !== 'metrics' && $kind !== 'cluster' && !empty($cfg['tls']));
            $scheme = 'tcp';
            $context=stream_context_create($secure?['ssl'=>['local_cert'=>$cfg['cert'],'local_pk'=>$cfg['key'],'cafile'=>$cfg['ca']?:null,'verify_peer'=>(bool)($cfg['ca']??''),'verify_peer_name'=>false,'allow_self_signed'=>false,'capture_peer_cert'=>true,'capture_peer_cert_chain'=>true,'crypto_method'=>STREAM_CRYPTO_METHOD_TLS_SERVER,'queueforge_tls'=>true,'fail_if_no_peer_cert'=>false]]:[]);
            $fp = stream_socket_server("$scheme://$addr", $errno, $errstr, STREAM_SERVER_BIND|STREAM_SERVER_LISTEN,$context);
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
            $ssl = stream_context_get_options($client)['ssl'] ?? [];
            if (($ssl['queueforge_tls'] ?? false) && !Server::acceptTls($client)) { fclose($client); return; }
            stream_set_blocking($client, false);
            $kind = $this->listens[$id]['kind'];
            $conn = $kind === 'mqtt' ? $this->protocols->nextMqtt() : ($kind === 'stomp' ? $this->protocols->nextStomp() : 0);
            if ($kind === 'amqps' && $this->onAmqp !== null) { ($this->onAmqp)($client); return; }
            $this->conns[(int) $client] = ['kind' => $kind, 'fp' => $client, 'buf' => '', 'conn' => $conn, 'peer' => '', 'state' => ['fp'=>$client,'advertised'=>['host'=>'127.0.0.1','port'=>self::port($this->cfg['stream']??'')]], 'out'=>'', 'closing'=>false];
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
                $this->conns[$id]['closing']=true;
            }
            return;
        }
        $this->conns[$id]['buf'] .= $chunk;
        $this->dispatch($id);
    }

    public function tick(): void
    {
        $this->cluster->tick();
        $this->integrations->tick();
        $this->protocols->tick();
        foreach (array_keys($this->conns) as $id) if (isset($this->conns[$id])) $this->flush($this->conns[$id]["fp"]);
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
            $id = (int) $fp;
            $this->conns[$id] = ['kind' => 'cluster', 'fp' => $fp, 'buf' => '', 'out' => '', 'closing' => false, 'conn' => 0, 'peer' => $member['id'], 'state' => []];
            $this->cluster->attach($member['id'], function (string $line) use ($id): void { $this->sendRaw($id, $line); });
            $hello = json_encode($this->cluster->hello());
            if (is_string($hello)) $this->sendRaw($id, $hello . "\n");
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
        // Subscriptions belong to the connection, so a dropped socket must
        // not leave entries pointing at a closed handle.
        if ($row['kind'] === 'mqtt') {
            $this->protocols->dropMqtt((int) ($row['conn'] ?? 0));
        }
        if ($row['kind'] === 'stream') $this->protocols->dropStream($row['state']);
        if ($row['kind'] === 'stomp') {
            $this->protocols->dropStomp((int) ($row['conn'] ?? 0));
        }
        if (is_resource($row['fp'])) {
            fclose($row['fp']);
        }
    }

    private function dispatch(int $id): void
    {
        $row = $this->conns[$id];
        $fp = $row['fp'];
        if (($row['ws'] ?? false) === true) {
            try { $decoded=WebSocket::decode($this->conns[$id]['buf'],$this->conns[$id]['wsState']); }
            catch(RuntimeException){$this->sendRaw($fp,WebSocket::frame(pack('n',1002),8));$this->conns[$id]['closing']=true;return;}
            if($decoded['reply']!=='')$this->sendRaw($fp,$decoded['reply']);
            if($decoded['closed']){$this->conns[$id]['closing']=true;return;}
            $buf=($this->conns[$id]['protocolBuf']??'').implode('',$decoded['messages']);
            $out=$row['kind']==='mqtt'?$this->protocols->mqtt($buf,$fp,$row['conn']):$this->protocols->stomp($buf,$fp,$row['conn']);
            $this->conns[$id]['protocolBuf']=$buf;if($out!=='')$this->send($fp,$out);
            if($row['kind']==='mqtt'&&$this->protocols->mqttClosing){$this->protocols->mqttClosing=false;$this->conns[$id]['closing']=true;}
            if($row['kind']==='stomp'&&$this->protocols->stompClosing){$this->protocols->stompClosing=false;$this->conns[$id]['closing']=true;}
            return;
        }
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
            if($row['kind']==='management'&&preg_match('#^GET /ws(?:[? ]|$)#',$head??'')) {
                $upgrade=WebSocket::upgrade((string)$head);
                if($upgrade===null){$this->sendRaw($fp,"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");$this->conns[$id]['closing']=true;return;}
                $this->sendRaw($fp,$upgrade['response']);$this->conns[$id]['kind']=$upgrade['kind'];$this->conns[$id]['ws']=true;$this->conns[$id]['wsState']=[];$this->conns[$id]['buf']=substr($raw,strlen((string)$head)+4);
                $this->conns[$id]['conn']=$upgrade['kind']==='mqtt'?$this->protocols->nextMqtt():$this->protocols->nextStomp();
                if($this->conns[$id]['buf']!=='')$this->dispatch($id);return;
            }
            if ($row['httpPending'] ?? false) return;
            $this->conns[$id]['httpPending'] = true;
            $response = $this->http->handle($raw, function (string $response) use ($id): void {
                if (!isset($this->conns[$id])) return;
                $this->sendRaw($id, $response); $this->conns[$id]['closing'] = true;
            });
            if ($response !== '') { $this->sendRaw($fp, $response); $this->conns[$id]['closing'] = true; }
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
                        $this->cluster->attach($peer, function (string $out) use ($id): void { $this->sendRaw($id, $out); });
                    }
                }
                $reply = $this->cluster->handleLine($line);
                if(is_array($decoded)&&(($decoded['op']??'')==='hello'||(($decoded['op']??'')==='reply'&&isset($decoded['payload']['features']))))$this->conns[$id]['helloDone']=true;
                if ($reply !== null && $reply !== '') {
                    $this->sendRaw($id, $reply . "\n");
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
                $this->send($fp, $out);
            }
            // A DISCONNECT closes the socket rather than leaving it open.
            if ($this->protocols->mqttClosing) {
                $this->protocols->mqttClosing = false;
                $this->conns[$id]['closing']=true;
            }
            return;
        }
        if ($row['kind'] === 'stomp') {
            $buf = $this->conns[$id]['buf'];
            $out = $this->protocols->stomp($buf, $fp, $row['conn']);
            $this->conns[$id]['buf'] = $buf;
            if ($out !== '') {
                $this->send($fp, $out);
            }
            if ($this->protocols->stompClosing) {
                $this->protocols->stompClosing = false;
                $this->conns[$id]['closing']=true;
            }
            return;
        }
        if ($row['kind'] === 'stream') {
            // Protocols::stream() takes the buffer and the per-connection
            // state by reference and consumes whole frames only, so the
            // remainder stays buffered for the next read.
            $buf = $this->conns[$id]['buf'];
            $state = $this->conns[$id]['state'];
            $out = $this->protocols->stream($buf, $state, $fp);
            $this->conns[$id]['buf'] = $buf;
            $this->conns[$id]['state'] = $state;
            if ($out !== '') {
                $this->send($fp, $out);
            }
            if ($this->protocols->streamClosing) {
                $this->protocols->streamClosing = false;
                $this->conns[$id]['closing'] = true;
            }
        }
    }

    public function writes(): array { return array_values(array_map(static fn($row)=>$row['fp'],array_filter($this->conns,static fn($row)=>($row['out']??'')!==''))); }
    private function sendRaw($fp,string $bytes):void { $id=(int)$fp;if(isset($this->conns[$id]))$this->conns[$id]['out']=($this->conns[$id]['out']??'').$bytes; }
    public function send($fp,string $bytes):void
    {
        $row=$this->conns[(int)$fp]??null;if($row===null)return;
        if($row['ws']??false)$bytes=WebSocket::frame($bytes,$row['kind']==='mqtt'?2:1);
        $this->sendRaw($fp,$bytes);
    }
    public function flush($fp):void
    {
        $id=(int)$fp;if(!isset($this->conns[$id]))return;$out=$this->conns[$id]['out']??'';
        if($out!==''){$n=@fwrite($fp,substr($out,0,65536));if($n===false){$this->close($id);return;}if($n>0)$this->conns[$id]['out']=substr($out,$n);}
        if(($this->conns[$id]['out']??'')===''&&($this->conns[$id]['closing']??false))$this->close($id);
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
