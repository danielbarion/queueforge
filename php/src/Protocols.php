<?php
declare(strict_types=1);

/** MQTT, STOMP and stream adapters share broker routing, permissions and durable storage. */
final class Protocols
{
    public array $mqttSubs = [];
    public array $stompSubs = [];
    public array $streams = [];
    public mixed $onWrite = null;
    public mixed $onClose = null;
    public bool $mqttClosing = false;
    public bool $stompClosing = false;
    public bool $streamClosing = false;
    private int $mqttConn = 1;
    private int $stompConn = 1;
    private array $mqttClients = [];
    private array $stompClients = [];
    private array $streamClients = [];
    private int $streamConn = 1;
    private array $retained = [];

    public function __construct(public Broker $broker)
    {
        $file = $this->broker->dataDir() . '/mqtt-retained.json';
        if (is_file($file)) {
            $this->retained = json_decode((string) file_get_contents($file), true) ?: [];
        }
    }

    public function nextMqtt(): int { return $this->mqttConn++; }
    public function nextStomp(): int { return $this->stompConn++; }

    private function write($fp, string $bytes): void
    {
        if ($bytes === '') return;
        if (is_callable($this->onWrite)) { ($this->onWrite)($fp, $bytes); return; }
        if (is_resource($fp)) @fwrite($fp, $bytes);
    }

    /** Invoked by the select loop so AMQP publications reach protocol subscribers. */
    public function tick(): void
    {
        foreach (array_keys($this->mqttClients) as $conn) {
            $s = $this->mqttClients[$conn];
            if ($s['keepalive'] > 0 && microtime(true) - $s['seen'] > $s['keepalive'] * 1.5) {
                $this->dropMqtt($conn);
                if (is_callable($this->onClose)) ($this->onClose)($s['fp']);
                continue;
            }
            $this->write($s['fp'], $this->pumpMqtt($conn));
        }
        foreach (array_keys($this->stompClients) as $conn) {
            $s = $this->stompClients[$conn]; $this->write($s['fp'], $this->pumpStomp($conn));
            if ($this->stompClients[$conn]['closing'] ?? false) { $this->dropStomp($conn); if (is_callable($this->onClose)) ($this->onClose)($s['fp']); }
        }
        foreach (array_keys($this->streamClients) as $id) {
            $s =& $this->streamClients[$id];
            if ($s['heartbeat'] > 0 && microtime(true) - $s['seen'] > $s['heartbeat'] * 2) {
                $fp = $s['fp'] ?? null; $state = ['protocolId' => $id];
                unset($s); $this->dropStream($state);
                if ($fp !== null && is_callable($this->onClose)) ($this->onClose)($fp);
                continue;
            }
            if (isset($s['fp'])) $this->write($s['fp'], $this->pumpStream($s));
            unset($s);
        }
    }

    private static function mqttLen(int $n): string
    {
        $out = '';
        do { $byte = $n % 128; $n = intdiv($n, 128); $out .= chr($n ? $byte | 128 : $byte); } while ($n);
        return $out;
    }
    private static function mqttPacket(int $first, string $body): string { return chr($first) . self::mqttLen(strlen($body)) . $body; }
    private static function str(string $s): string { return pack('n', strlen($s)) . $s; }
    private static function take(string $s, int &$at, int $n): string
    {
        if ($n < 0 || $at + $n > strlen($s)) throw new RuntimeException('short protocol frame');
        $v = substr($s, $at, $n); $at += $n; return $v;
    }
    private static function u8(string $s, int &$at): int { return ord(self::take($s, $at, 1)); }
    private static function u16(string $s, int &$at): int { return unpack('n', self::take($s, $at, 2))[1]; }
    private static function u32(string $s, int &$at): int { return unpack('N', self::take($s, $at, 4))[1]; }
    private static function u64(string $s, int &$at): int { return unpack('J', self::take($s, $at, 8))[1]; }
    private static function string(string $s, int &$at): string { $n = self::u16($s, $at); return $n === 65535 ? '' : self::take($s, $at, $n); }
    private static function varint(string $s, int &$at): int
    {
        $n = 0; $m = 1;
        for ($i = 0; $i < 4; $i++) { $b = self::u8($s, $at); $n += ($b & 127) * $m; if (!($b & 128)) return $n; $m *= 128; }
        throw new RuntimeException('invalid MQTT remaining length');
    }
    private static function mqttProps(string $s, int &$at): string { return self::take($s, $at, self::varint($s, $at)); }

    public function mqtt(string &$buf, $fp, int $conn): string
    {
        $out = '';
        try {
            while (strlen($buf) >= 2) {
                $at = 1;
                try { $size = self::varint($buf, $at); } catch (RuntimeException $e) { if (strlen($buf) < 5) break; throw $e; }
                if (strlen($buf) < $at + $size) break;
                $first = ord($buf[0]); $body = substr($buf, $at, $size); $buf = substr($buf, $at + $size);
                $type = $first >> 4;
                if ($type === 1) { $out .= $this->mqttConnect($body, $fp, $conn); if ($this->mqttClosing) break; continue; }
                if (!isset($this->mqttClients[$conn])) throw new RuntimeException('MQTT not connected');
                $this->mqttClients[$conn]['seen'] = microtime(true);
                $s =& $this->mqttClients[$conn]; $b=$this->broker->forVhost($s['vhost']); $at = 0;
                if ($type === 3) {
                    $topic = self::string($body, $at); $qos = ($first >> 1) & 3;
                    $pid = $qos ? self::u16($body, $at) : 0;
                    $props = $s['version'] === 5 ? self::mqttProps($body, $at) : '';
                    if ($topic === '' || strpbrk($topic, '+#') !== false || $qos > 2) throw new RuntimeException('invalid MQTT topic');
                    $done = function(bool $ok) use ($conn, $pid, $qos): void {
                        if (!isset($this->mqttClients[$conn])) return;
                        $client =& $this->mqttClients[$conn];
                        if ($ok && $qos > 0) $client['out'] .= self::mqttPacket($qos === 2 ? 0x50 : 0x40, pack('n', $pid));
                        elseif (!$ok && $client['version'] === 5 && $qos > 0) $client['out'] .= self::mqttPacket(0x40, pack('n', $pid) . "\x87\0");
                        elseif (!$ok) {
                            $socket = $client['fp']; unset($client); $this->dropMqtt($conn);
                            if (is_callable($this->onClose)) ($this->onClose)($socket); else $this->mqttClosing = true;
                        }
                    };
                    if (!$this->mqttSend($s, $topic, substr($body, $at), min(1, $qos), (bool) ($first & 1), $props, $done)) $done(false);
                } elseif ($type === 4) {
                    $pid = self::u16($body, $at);
                    if (isset($s['inflight'][$pid])) { $b->ack($s['inflight'][$pid]); unset($s['inflight'][$pid]); }
                } elseif ($type === 5 || $type === 6) {
                    $out .= self::mqttPacket($type === 5 ? 0x62 : 0x70, self::take($body, $at, 2));
                } elseif ($type === 8 || $type === 10) {
                    if (($first & 15) !== 2) throw new RuntimeException('invalid MQTT flags');
                    $pid = self::u16($body, $at); if ($s['version'] === 5) self::mqttProps($body, $at);
                    $codes = ''; $added = [];
                    while ($at < strlen($body)) {
                        $filter = self::string($body, $at); $key = str_replace(['/', '+'], ['.', '*'], $filter);
                        if ($type === 10) {
                            $had = isset($s['subs'][$filter]); unset($s['subs'][$filter]);
                            $b->unbind($s['queue'], 'amq.topic', $key); $codes .= chr($had ? 0 : 0x11);
                            if (!$s['subs'] && $s['consumer']) { $b->unregisterProtocolConsumer($s['queue'], $s['owner'], 'mqtt-' . $conn); $s['consumer'] = false; }
                            continue;
                        }
                        $qos = min(1, self::u8($body, $at) & 3);
                        $allowed = !str_starts_with($filter, '$share/') && $this->broker->resourceAllowed($s['user'], $s['vhost'], 'read', 'amq.topic')
                            && $this->broker->resourceAllowed($s['user'], $s['vhost'], 'configure', $s['queue'])
                            && $this->broker->topicReadAllowed($s['user'], $s['vhost'], 'amq.topic', $key);
                        if (!$allowed) { $codes .= chr($s['version'] === 5 ? 0x87 : 0x80); continue; }
                        $this->ensureMqttQueue($s);
                        $b->bind($s['queue'], 'amq.topic', $key);
                        $s['subs'][$filter] = $qos; $added[$filter] = $qos; $codes .= chr($qos);
                    }
                    $out .= self::mqttPacket($type === 8 ? 0x90 : 0xb0, pack('n', $pid) . ($s['version'] === 5 ? "\0" . $codes : ($type === 8 ? $codes : '')));
                    foreach ($added as $filter => $qos) foreach ($this->retained[$s['vhost']] ?? [] as $topic => $m) {
                        if (Features::mqttMatch($filter, $topic)) $out .= $this->mqttDelivery($s, $topic, base64_decode($m['body']), 0, true, false, 0, base64_decode($m['props']));
                    }
                } elseif ($type === 12) $out .= "\xd0\0";
                elseif ($type === 14) { $this->dropMqtt($conn, false); $this->mqttClosing = true; return $out; }
                else throw new RuntimeException('unsupported MQTT packet');
                unset($s);
                $out .= $this->pumpMqtt($conn);
            }
        } catch (Throwable $e) { $this->dropMqtt($conn); $this->mqttClosing = true; }
        return $out;
    }

    private function mqttConnect(string $body, $fp, int $conn): string
    {
        if (isset($this->mqttClients[$conn])) throw new RuntimeException('duplicate CONNECT');
        $at = 0; $name = self::string($body, $at); $version = self::u8($body, $at); $flags = self::u8($body, $at); $keepalive = self::u16($body, $at);
        if (!in_array($version, [3,4,5], true) || !in_array($name, ['MQTT','MQIsdp'], true) || ($flags & 1)) throw new RuntimeException('invalid CONNECT');
        if ($version === 5) self::mqttProps($body, $at);
        $client = self::string($body, $at); $clean = (bool) ($flags & 2); $will = null;
        if ($flags & 4) {
            $props = $version === 5 ? self::mqttProps($body, $at) : '';
            $topic = self::string($body, $at); $payload = self::string($body, $at);
            $will = ['topic'=>$topic,'body'=>$payload,'qos'=>min(1,($flags>>3)&3),'retain'=>(bool)($flags&32),'props'=>$props];
        }
        $user = $flags & 128 ? self::string($body, $at) : ''; $pass = $flags & 64 ? self::string($body, $at) : ''; $vhost = '/';
        if (str_contains($user, ':')) { [$vhost, $user] = explode(':', $user, 2); $vhost = $vhost ?: '/'; }
        $identity = $this->broker->authenticate($user, $pass);
        $code = $identity === null ? ($version === 5 ? 0x86 : 4) : (!$this->broker->hasVhostAccess($identity, $vhost) ? ($version === 5 ? 0x87 : 5) : 0);
        if ($identity !== null) $user = $identity;
        if ($code) { $this->mqttClosing = true; return self::mqttPacket(0x20, "\0" . chr($code) . ($version === 5 ? "\0" : '')); }
        if ($client === '') { if (!$clean && $version !== 5) throw new RuntimeException('missing client id'); $client = 'mqtt-' . bin2hex(random_bytes(12)); }
        foreach ($this->mqttClients as $id => $old) if ($old['client'] === $client && $old['vhost'] === $vhost) {
            $this->dropMqtt($id, false); if (is_callable($this->onClose)) ($this->onClose)($old['fp']);
        }
        $b=$this->broker->forVhost($vhost);
        $queue = 'mqtt-subscription-' . $client . 'qos1'; $exists = isset($b->queues[$queue]);
        if ($clean && $exists) $b->deleteQueue($queue);
        $subs = [];
        if (!$clean && $exists) foreach ($b->bindings as $binding) if ($binding['queue'] === $queue && $binding['exchange'] === 'amq.topic') $subs[str_replace(['.','*'], ['/','+'], $binding['key'])] = 1;
        $owner = -(1000000 + $conn); $this->broker->userByConn[$owner] = $user;
        $this->mqttClients[$conn] = ['owner'=>$owner,'fp'=>$fp,'version'=>$version,'client'=>$client,'user'=>$user,'vhost'=>$vhost,'queue'=>$queue,'clean'=>$clean,'will'=>$will,'subs'=>$subs,'inflight'=>[],'pid'=>1,'keepalive'=>$keepalive,'seen'=>microtime(true),'out'=>'','consumer'=>false];
        return self::mqttPacket(0x20, chr(!$clean && $exists ? 1 : 0) . "\0" . ($version === 5 ? "\0" : '')) . $this->pumpMqtt($conn);
    }

    private function ensureMqttQueue(array $s): void
    {
        $b=$this->broker->forVhost($s['vhost']);
        if (!isset($b->queues[$s['queue']])) $b->declareQueue($s['queue'], [], !$s['clean'], $s['clean']);
    }
    private function mqttSend(array $s, string $topic, string $body, int $qos, bool $retain, string $props, ?callable $done = null): bool
    {
        $b=$this->broker->forVhost($s['vhost']);
        $key = str_replace('/', '.', $topic);
        if (!$b->resourceAllowed($s['user'], $s['vhost'], 'write', 'amq.topic') || !$b->topicWriteAllowed($s['user'], $s['vhost'], 'amq.topic', $key)) return false;
        if ($retain) {
            if ($body === '') unset($this->retained[$s['vhost']][$topic]);
            else $this->retained[$s['vhost']][$topic] = ['body'=>base64_encode($body),'props'=>base64_encode($props),'qos'=>$qos];
            $file = $this->broker->dataDir() . '/mqtt-retained.json';
            Broker::writeDurable($file, json_encode($this->retained, JSON_THROW_ON_ERROR));
            if ($body === '') { if ($done) $done(true); return true; }
        }
        $headers = [['x-mqtt-publish-qos', $qos]]; if ($props !== '') $headers[] = ['x-mqtt-props', $props];
        $propRaw = Amqp10::writeProps(['deliveryMode' => $qos ? 2 : 1], $headers);
        $b->publishAsync($s['owner'], 0, 'amq.topic', $key, $body, $qos ? 2 : 1, 0, $headers, null, $propRaw, $done ?? static function(bool $ok): void {});
        return true;
    }
    private function mqttDelivery(array $s, string $topic, string $body, int $qos, bool $retain, bool $dup, int $pid, string $props = ''): string
    {
        return self::mqttPacket(0x30 | ($qos << 1) | ($retain ? 1 : 0) | ($dup ? 8 : 0), self::str($topic) . ($qos ? pack('n',$pid) : '') . ($s['version'] === 5 ? self::mqttLen(strlen($props)) . $props : '') . $body);
    }
    private function pumpMqtt(int $connection): string
    {
        if (!isset($this->mqttClients[$connection])) return '';
        $session =& $this->mqttClients[$connection];
        $broker = $this->broker->forVhost($session['vhost']);
        if ($session['subs'] && !$session['consumer']) {
            $this->ensureMqttQueue($session);
            $session['consumer'] = true;
            $broker->registerProtocolConsumer($session['queue'], $session['owner'], 'mqtt-' . $connection,
                fn(): bool => isset($this->mqttClients[$connection]) && count($this->mqttClients[$connection]['inflight']) < 128,
                function(array $message, int $id) use ($connection): void { $this->deliverMqtt($connection, $message, $id); }, false);
        }
        $broker->pumpConsumers();
        $out = $session['out']; $session['out'] = '';
        return $out;
    }

    private function deliverMqtt(int $connection, array $message, int $id): void
    {
        if (!isset($this->mqttClients[$connection])) return;
        $session =& $this->mqttClients[$connection];
        $topic = str_replace('.', '/', $message['key']);
        $subscriptionQos = 0;
        foreach ($session['subs'] as $filter => $qos) if (Features::mqttMatch($filter, $topic)) $subscriptionQos = max($subscriptionQos, $qos);
        $qos = $message['mode'] === 2 ? 1 : 0; $properties = '';
        foreach ($message['headers'] ?? [] as [$key, $value]) {
            if ($key === 'x-mqtt-publish-qos') $qos = (int) $value;
            if ($key === 'x-mqtt-props') $properties = (string) $value;
        }
        $qos = min($qos, $subscriptionQos); $packetId = 0;
        if ($qos) {
            do { $packetId = $session['pid']; $session['pid'] = $packetId === 65535 ? 1 : $packetId + 1; } while (isset($session['inflight'][$packetId]));
            $session['inflight'][$packetId] = $id;
        }
        $session['out'] .= $this->mqttDelivery($session, $topic, $message['body'], $qos, false, (bool) $message['redelivered'], $packetId, $properties);
        if (!$qos) $this->broker->forVhost($session['vhost'])->ack($id);
    }
    public function dropMqtt(int $conn, bool $abnormal = true): void
    {
        $s = $this->mqttClients[$conn] ?? null; if (!$s) return; $b=$this->broker->forVhost($s['vhost']); unset($this->mqttClients[$conn]);
        if ($s['consumer']) $b->unregisterProtocolConsumer($s['queue'], $s['owner'], 'mqtt-' . $conn);
        foreach (array_reverse($s['inflight']) as $id) $b->requeue($id);
        if ($s['clean'] && isset($b->queues[$s['queue']])) $b->deleteQueue($s['queue']);
        if ($abnormal && $s['will']) { $w = $s['will']; $this->mqttSend($s,$w['topic'],$w['body'],$w['qos'],$w['retain'],$w['props']); }
        unset($this->broker->userByConn[$s['owner']]);
    }

    private static function stompEscape(string $s): string { return str_replace(['\\',"\r","\n",':'], ['\\\\','\\r','\\n','\\c'], $s); }
    private static function stompUnescape(string $s): string { return preg_replace_callback('/\\\\([nrc\\\\])/', static fn($m) => ['n'=>"\n",'r'=>"\r",'c'=>':','\\'=>'\\'][$m[1]], $s); }
    private function stompFrame(string $cmd, array $headers = [], string $body = '', string $version = '1.2'): string
    {
        $out = $cmd . "\n"; foreach ($headers as $k=>$v) $out .= ($version === '1.0' ? $k : self::stompEscape($k)) . ':' . ($version === '1.0' ? (string)$v : self::stompEscape((string)$v)) . "\n";
        return $out . "\n" . $body . "\0";
    }
    private function stompParse(string &$buf, string $version): ?array
    {
        $buf = ltrim($buf, "\r\n"); if (!preg_match('/\r?\n\r?\n/', $buf, $match, PREG_OFFSET_CAPTURE)) return null;
        $end = $match[0][1]; $start = $end + strlen($match[0][0]); $lines = explode("\n", substr($buf,0,$end)); $cmd = rtrim(array_shift($lines),"\r"); $headers=[];
        foreach ($lines as $line) { $line = rtrim($line,"\r"); $sep = strpos($line,':'); if ($sep === false || $sep === 0) continue; $k=substr($line,0,$sep);$v=substr($line,$sep+1); if ($version !== '1.0' && !in_array($cmd,['CONNECT','STOMP'],true)) {$k=self::stompUnescape($k);$v=self::stompUnescape($v);} if (!array_key_exists($k,$headers)) $headers[$k]=$v; }
        if (isset($headers['content-length'])) {
            if (!ctype_digit($headers['content-length'])) throw new RuntimeException('invalid content-length');
            $bodyEnd = $start + (int)$headers['content-length']; if (strlen($buf) <= $bodyEnd) return null;
            if ($buf[$bodyEnd] !== "\0") throw new RuntimeException('missing frame terminator');
        } else { $bodyEnd = strpos($buf,"\0",$start); if ($bodyEnd === false) return null; }
        $body=substr($buf,$start,$bodyEnd-$start);$buf=substr($buf,$bodyEnd+1);return [$cmd,$headers,$body];
    }
    public function stomp(string &$buf, $fp, int $conn): string
    {
        $out = '';
        try {
            while (($f = $this->stompParse($buf,$this->stompClients[$conn]['version'] ?? '1.2')) !== null) {
                [$cmd,$h,$body]=$f;
                if ($cmd === 'CONNECT' || $cmd === 'STOMP') {
                    if (isset($this->stompClients[$conn])) throw new RuntimeException('already connected');
                    $versions=explode(',',$h['accept-version']??'1.0');$version=in_array('1.2',$versions,true)?'1.2':(in_array('1.1',$versions,true)?'1.1':'1.0');
                    $user=$h['login']??'';$vhost=$h['host']??'/';
                    $identity = $this->broker->authenticate($user, $h['passcode'] ?? '');
                    if ($identity === null || !$this->broker->hasVhostAccess($identity, $vhost)) throw new RuntimeException('Access refused');
                    $user = $identity;
                    $owner=-(2000000+$conn);$this->broker->userByConn[$owner]=$user;
                    $this->stompClients[$conn]=['connection'=>$conn,'owner'=>$owner,'fp'=>$fp,'user'=>$user,'vhost'=>$vhost,'version'=>$version,'subs'=>[],'tx'=>[],'next'=>1,'out'=>'','closing'=>false];
                    $out.=$this->stompFrame('CONNECTED',['version'=>$version,'heart-beat'=>'0,0']);continue;
                }
                if (!isset($this->stompClients[$conn])) throw new RuntimeException('not connected');
                $s =& $this->stompClients[$conn]; $deferred = false;
                if (in_array($cmd,['SEND','ACK','NACK'],true) && isset($h['transaction'])) {
                    if (!isset($s['tx'][$h['transaction']])) throw new RuntimeException('transaction is not active');
                    $s['tx'][$h['transaction']][]=$f;
                } elseif ($cmd === 'BEGIN') {
                    $tx=$h['transaction']??'';if (!$tx || isset($s['tx'][$tx])) throw new RuntimeException('invalid transaction');$s['tx'][$tx]=[];
                } elseif ($cmd === 'COMMIT' || $cmd === 'ABORT') {
                    $tx=$h['transaction']??'';if (!isset($s['tx'][$tx])) throw new RuntimeException('transaction is not active');$ops=$s['tx'][$tx];unset($s['tx'][$tx]);
                    if ($cmd === 'COMMIT' && $ops) {
                        $deferred = true; $pending = count($ops); $accepted = true;
                        $done = function(bool $ok) use (&$pending, &$accepted, $conn, $h): void {
                            $accepted = $accepted && $ok;
                            if (--$pending === 0) $this->stompComplete($conn, $h['receipt'] ?? null, $accepted);
                        };
                        foreach ($ops as $op) $this->stompOp($s, $op, $done);
                    }
                } elseif ($cmd === 'DISCONNECT') {
                    if (isset($h['receipt'])) $out.=$this->stompFrame('RECEIPT',['receipt-id'=>$h['receipt']]);
                    unset($s);$this->dropStomp($conn);$this->stompClosing=true;return $out;
                } elseif ($cmd === 'SEND') {
                    $deferred = true;
                    $this->stompOp($s, $f, fn(bool $ok) => $this->stompComplete($conn, $h['receipt'] ?? null, $ok));
                } else $this->stompOp($s,$f);
                if (!$deferred && isset($h['receipt'])) $out.=$this->stompFrame('RECEIPT',['receipt-id'=>$h['receipt']], '', $s['version']);
                unset($s); $out.=$this->pumpStomp($conn);
                if ($this->stompClients[$conn]['closing'] ?? false) { $this->dropStomp($conn); $this->stompClosing = true; break; }
            }
        } catch (Throwable $e) { $out.=$this->stompFrame('ERROR',['message'=>$e->getMessage()],$e->getMessage());$this->dropStomp($conn);$this->stompClosing=true; }
        return $out;
    }
    private function stompTarget(string $dest): array
    {
        if (str_starts_with($dest,'/queue/')) return ['',substr($dest,7),true];
        if (str_starts_with($dest,'/amq/queue/')) return ['',substr($dest,11),false];
        if (str_starts_with($dest,'/topic/')) return ['amq.topic',substr($dest,7),false];
        if (str_starts_with($dest,'/exchange/')) { $parts=explode('/',substr($dest,10),2);return [$parts[0],$parts[1]??'',false]; }
        throw new RuntimeException('invalid destination');
    }
    private function stompOp(array &$s, array $f, ?callable $done = null): void
    {
        $b=$this->broker->forVhost($s['vhost']);
        [$cmd,$h,$body]=$f;
        if ($cmd === 'SEND') {
            [$ex,$key,$declare]=$this->stompTarget($h['destination']??'');
            if (!$b->resourceAllowed($s['user'],$s['vhost'],'write',$ex?:'amq.default') || !$b->topicWriteAllowed($s['user'],$s['vhost'],$ex,$key)) throw new RuntimeException('write access refused');
            if ($declare) { if (!$b->resourceAllowed($s['user'],$s['vhost'],'configure',$key)) throw new RuntimeException('configure access refused');$b->declareQueue($key); }
            $reserved=['destination','receipt','transaction','content-length','content-type','persistent','priority','expiration','reply-to','correlation-id','message-id','ack','id','subscription','prefetch-count','amqp-message-id'];$app=[];
            foreach ($h as $k=>$v) if (!in_array($k,$reserved,true)) $app[]=[$k,$v];
            $persistent=($h['persistent']??'')==='true';$prop=$this->stompProperties($h,$app,$persistent);
            $b->publishAsync($s['owner'],0,$ex,$key,$body,$persistent?2:1,(int)($h['priority']??0),$app,isset($h['expiration'])?(int)$h['expiration']:null,$prop,$done ?? static function(bool $ok): void {});
            return;
        } elseif ($cmd === 'SUBSCRIBE') {
            $id=$h['id']??($s['version']==='1.0'?($h['destination']??''):'');if($id===''||isset($s['subs'][$id]))throw new RuntimeException('invalid subscription id');
            $dest=$h['destination']??'';[$ex,$queue,$declare]=$this->stompTarget($dest);$temp=$ex!=='';
            if ($temp) {
                if (!$b->resourceAllowed($s['user'],$s['vhost'],'read',$ex) || !$b->topicReadAllowed($s['user'],$s['vhost'],$ex,$queue))throw new RuntimeException('topic read refused');
                $key=$queue;$queue='stomp-subscription-'.bin2hex(random_bytes(12));
                if (!$b->resourceAllowed($s['user'],$s['vhost'],'configure',$queue))throw new RuntimeException('configure refused');
                $b->declareQueue($queue,[],false,true,false,true);$b->bind($queue,$ex,$key);
            } elseif ($declare) {if (!$b->resourceAllowed($s['user'],$s['vhost'],'configure',$queue))throw new RuntimeException('configure refused');$b->declareQueue($queue);}
            elseif (!isset($b->queues[$queue])) throw new RuntimeException('queue does not exist');
            if (!$b->resourceAllowed($s['user'],$s['vhost'],'read',$queue))throw new RuntimeException('read access refused');
            $s['subs'][$id]=['queue'=>$queue,'destination'=>$dest,'ack'=>$h['ack']??'auto','prefetch'=>max(0,(int)($h['prefetch-count']??0)),'pending'=>[],'temporary'=>$temp,'vhost'=>$s['vhost'],'owner'=>$s['owner'],'tag'=>'stomp-'.$s['connection'].'-'.$id];
            $connection = $s['connection']; $tag = $s['subs'][$id]['tag'];
            $b->registerProtocolConsumer($queue, $s['owner'], $tag,
                function() use ($connection, $id): bool {
                    $sub = $this->stompClients[$connection]['subs'][$id] ?? null;
                    return $sub !== null && !($this->stompClients[$connection]['closing'] ?? false) && ($sub['prefetch'] === 0 || count($sub['pending']) < $sub['prefetch']);
                }, function(array $message, int $messageId) use ($connection, $id): void { $this->deliverStomp($connection, $id, $message, $messageId); },
                $s['subs'][$id]['ack'] === 'auto', (int)($h['x-priority'] ?? 0));
        } elseif ($cmd === 'UNSUBSCRIBE') { $id=$h['id']??$h['destination']??'';if(isset($s['subs'][$id])){$this->stopStompSub($s['subs'][$id]);unset($s['subs'][$id]);} }
        elseif ($cmd === 'ACK' || $cmd === 'NACK') {
            $key=$h['id']??$h['message-id']??'';
            foreach ($s['subs'] as &$sub) {
                if (!isset($sub['pending'][$key]))continue;
                $ids=[];foreach($sub['pending']as$ack=>$msg){if($sub['ack']==='client'||$ack===$key){$ids[]=$msg;unset($sub['pending'][$ack]);}if($ack===$key)break;}
                foreach($ids as $msg){if($cmd==='ACK')$b->ack($msg);elseif(($h['requeue']??'true')!=='false')$b->requeue($msg);else $b->deadLetter($msg,'rejected');}break;
            }unset($sub);
        } else throw new RuntimeException('unknown command '.$cmd);
        if ($done) $done(true);
    }
    private function stompProperties(array $headers, array $app, bool $persistent): string
    {
        $props = ['deliveryMode' => $persistent ? 2 : 1];
        foreach (['content-type' => 'contentType', 'correlation-id' => 'correlationId', 'reply-to' => 'replyTo', 'expiration' => 'expiration', 'amqp-message-id' => 'messageId'] as $key => $property) {
            if (isset($headers[$key])) $props[$property] = $headers[$key];
        }
        return Amqp10::writeProps($props, $app);
    }
    private function stompComplete(int $connection, ?string $receipt, bool $ok): void
    {
        if (!isset($this->stompClients[$connection])) return;
        $session =& $this->stompClients[$connection];
        if (!$ok) {
            $session['out'] .= $this->stompFrame('ERROR', ['message' => 'message refused'], 'message was not durably accepted', $session['version']);
            $session['closing'] = true;
        } elseif ($receipt !== null) $session['out'] .= $this->stompFrame('RECEIPT', ['receipt-id' => $receipt], '', $session['version']);
    }

    private function pumpStomp(int $connection): string
    {
        if (!isset($this->stompClients[$connection])) return '';
        $session =& $this->stompClients[$connection];
        $this->broker->forVhost($session['vhost'])->pumpConsumers();
        $out = $session['out']; $session['out'] = '';
        return $out;
    }

    private function deliverStomp(int $connection, string $subscription, array $message, int $messageId): void
    {
        if (!isset($this->stompClients[$connection]['subs'][$subscription])) return;
        $session =& $this->stompClients[$connection];
        $sub =& $session['subs'][$subscription];
        $ack = 'T_' . $subscription . '@@' . $connection . '@@' . $session['next']++;
        $headers = ['subscription' => $subscription, 'destination' => $sub['destination'], 'message-id' => $ack, 'redelivered' => $message['redelivered'] ? 'true' : 'false'];
        if ($sub['ack'] !== 'auto') { $sub['pending'][$ack] = $messageId; $headers['ack'] = $ack; }
        foreach ($message['headers'] ?? [] as [$key, $value]) {
            if (!isset($headers[$key]) && !str_starts_with($key, 'x-mqtt') && is_scalar($value)) $headers[$key] = (string) $value;
        }
        $props = Amqp10::readProps($message['propRaw'] ?? null);
        if (isset($props['contentType'])) $headers['content-type'] = $props['contentType'];
        $headers['content-length'] = strlen($message['body']);
        $session['out'] .= $this->stompFrame('MESSAGE', $headers, $message['body'], $session['version']);
    }
    private function stopStompSub(array $sub):void{$b=$this->broker->forVhost($sub['vhost']);$b->unregisterProtocolConsumer($sub['queue'],$sub['owner'],$sub['tag']);foreach(array_reverse($sub['pending'])as$msg)$b->requeue($msg);if($sub['temporary'])$b->deleteQueue($sub['queue']);}
    public function dropStomp(int $conn):void{if(!isset($this->stompClients[$conn]))return;foreach($this->stompClients[$conn]['subs']as$sub)$this->stopStompSub($sub);unset($this->broker->userByConn[$this->stompClients[$conn]['owner']]);unset($this->stompClients[$conn]);}

    private static function streamFrame(int $key,string $payload,int $version=1):string{$b=pack('nn',$key,$version).$payload;return pack('N',strlen($b)).$b;}
    private static function streamResp(int $key,int $corr,int $code=1,string $extra=''):string{return self::streamFrame($key|0x8000,pack('Nn',$corr,$code).$extra);}
    private static function streamMap(string $buf,int &$at):array{$count=self::u32($buf,$at);$m=[];for($i=0;$i<$count;$i++){$k=self::string($buf,$at);$m[$k]=self::string($buf,$at);}return$m;}
    private static function streamStrings(string $buf,int &$at):array{$count=self::u32($buf,$at);$m=[];for($i=0;$i<$count;$i++)$m[]=self::string($buf,$at);return$m;}
    private static function encodeStrings(array $s):string{$b=pack('N',count($s));foreach($s as$v)$b.=self::str($v);return$b;}
    private static function encodeMap(array $s):string{$b=pack('N',count($s));foreach($s as$k=>$v)$b.=self::str($k).self::str((string)$v);return$b;}
    private function isStream(string $name,string $vhost='/'):bool{$b=$this->broker->forVhost($vhost);return isset($b->queues[$name])&&($b->queues[$name]['args']['queueType']??'')==='stream';}
    private function streamAllowed(array $s,string $operation,string $name):bool{return$this->broker->resourceAllowed($s['user'],$s['vhost'],$operation,$name);}

    public function stream(string &$buf,array &$state,$fp=null):string
    {
        if (isset($state['protocolId']) && !isset($this->streamClients[$state['protocolId']])) { $this->streamClosing = true; return ''; }
        if(!isset($state['protocolId'])){$state['protocolId']=$this->streamConn++;$this->streamClients[$state['protocolId']]=['id'=>$state['protocolId'],'user'=>'','vhost'=>'/','authed'=>false,'opened'=>false,'publishers'=>[],'subs'=>[],'out'=>'','corr'=>1,'pending'=>[],'heartbeat'=>0,'seen'=>microtime(true),'beat'=>microtime(true)];}
        $s =& $this->streamClients[$state['protocolId']];
        if (isset($state['advertised'])) $s['advertised'] = $state['advertised'];
        if ($fp !== null) $s['fp'] = $fp;
        elseif (isset($state['fp'])) $s['fp'] = $state['fp'];
        if (!isset($s['advertised']) && isset($s['fp']) && is_resource($s['fp'])) {
            $local = @stream_socket_get_name($s['fp'], false);
            if (is_string($local) && ($colon = strrpos($local, ':')) !== false) {
                $host = trim(substr($local, 0, $colon), '[]');
                $s['advertised'] = ['host' => $host === '0.0.0.0' || $host === '::' ? 'localhost' : $host, 'port' => (int) substr($local, $colon + 1)];
            }
        }
        $out = '';
        try{
            while(strlen($buf)>=4){$size=unpack('N',substr($buf,0,4))[1];if($size<4||$size>1048576)throw new RuntimeException('invalid stream frame length');if(strlen($buf)<$size+4)break;
                $frame=substr($buf,4,$size);$buf=substr($buf,4+$size);$at=0;$key=self::u16($frame,$at);$version=self::u16($frame,$at);$r=substr($frame,$at);$s['seen']=microtime(true);
                if(!$s['opened']&&!in_array($key,[17,18,19,20,21,22,23],true))throw new RuntimeException('stream not open');
                $out.=$this->streamCommand($key,$version,$r,$s);
                if($this->streamClosing){$this->dropStream($state);break;}
            }
            if(!$this->streamClosing)$out.=$this->pumpStream($s);
        }catch(Throwable $e){$this->streamClosing=true;$this->dropStream($state);}
        return$out;
    }
    private function streamCommand(int $key,int $version,string $r,array &$s):string
    {
        $b=$this->broker->forVhost($s['vhost']);
        $at=0;
        if($key===23)return'';
        if($key===20){self::u32($r,$at);$s['heartbeat']=self::u32($r,$at);return'';}
        if($key===(26|0x8000)){
            $corr=self::u32($r,$at);$code=self::u16($r,$at);$id=$s['pending'][$corr]??null;unset($s['pending'][$corr]);
            if($id!==null&&isset($s['subs'][$id])&&$code===1){$s['subs'][$id]['offset']=$this->readStreamOffset($r,$at,$s['subs'][$id]['stream'],$s['vhost']);$s['subs'][$id]['active']=$s['subs'][$id]['selected'];}return'';
        }
        if($key===2)return$this->streamPublish($r,$s,$version);
        if($key===9){$id=self::u8($r,$at);$credit=self::u16($r,$at);if(!isset($s['subs'][$id]))return self::streamFrame(0x8009,pack('nC',4,$id));$s['subs'][$id]['credit']+=$credit;return'';}
        if($key===10){$ref=self::string($r,$at);$name=self::string($r,$at);$offset=self::u64($r,$at);if($this->isStream($name,$s['vhost'])&&$this->streamAllowed($s,'read',$name)){$b->streamStoreOffset($name,$ref,$offset);$b->flushDurable();}return'';}
        $corr=self::u32($r,$at);
        if($key===17)return self::streamResp($key,$corr,1,self::encodeMap(['product'=>'QueueForge','platform'=>'PHP','version'=>'4.3.0']));
        if($key===18)return self::streamResp($key,$corr,1,self::encodeStrings(['PLAIN']));
        if($key===19){$mech=self::string($r,$at);$bytes=self::take($r,$at,self::u32($r,$at));if($mech!=='PLAIN')return self::streamResp($key,$corr,7);$parts=explode("\0",$bytes);$user=$parts[1]??'';$pass=$parts[2]??'';
            $identity=$b->authenticate($user,$pass);if($identity===null){$this->streamClosing=true;return self::streamResp($key,$corr,8);}$s['user']=$identity;$s['authed']=true;return self::streamResp($key,$corr).self::streamFrame(20,pack('NN',1048576,60));}
        if($key===21){$vhost=self::string($r,$at);if(!$s['authed']||!$b->hasVhostAccess($s['user'],$vhost)){$this->streamClosing=true;return self::streamResp($key,$corr,12);}$s['vhost']=$vhost;$s['opened']=true;
            $advertised=$s['advertised']??['host'=>'localhost','port'=>'5552'];return self::streamResp($key,$corr,1,self::encodeMap(['advertised_host'=>$advertised['host'],'advertised_port'=>$advertised['port']]));}
        if($key===22){$this->streamClosing=true;return self::streamResp($key,$corr);}
        if($key===27){$commands=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,27,28,29,30];$extra=pack('N',count($commands));foreach($commands as$k)$extra.=pack('nnn',$k,1,$k===2?2:1);return self::streamResp($key,$corr,1,$extra);}
        if($key===13){
            $name=self::string($r,$at);$args=self::streamMap($r,$at);
            if($b->cluster?->raftEnabled()) {
                if(!$name||str_starts_with($name,'amq.'))return self::streamResp($key,$corr,17);
                if(!$this->streamAllowed($s,'configure',$name))return self::streamResp($key,$corr,16);
                if(isset($b->queues[$name]))return self::streamResp($key,$corr,5);
                $a=['x-queue-type'=>'stream'];foreach($args as$k=>$v){$k=str_starts_with($k,'x-')?$k:'x-'.$k;$a[$k]=ctype_digit($v)?(int)$v:$v;}
                return $this->streamMetadata($s,$key,$corr,'PUT','/api/queues/'.rawurlencode($s['vhost']).'/'.rawurlencode($name),['durable'=>true,'arguments'=>$a]);
            }
            return self::streamResp($key,$corr,$this->createStream($s,$name,$args));
        }
        if($key===14){$name=self::string($r,$at);
            if($b->cluster?->raftEnabled()) {
                if(!$this->isStream($name,$s['vhost']))return self::streamResp($key,$corr,2);
                if(!$this->streamAllowed($s,'configure',$name))return self::streamResp($key,$corr,16);
                return $this->streamMetadata($s,$key,$corr,'DELETE','/api/queues/'.rawurlencode($s['vhost']).'/'.rawurlencode($name),[]);
            }
            return self::streamResp($key,$corr,$this->deleteStream($s,$name));
        }
        if($key===15){$names=self::streamStrings($r,$at);$adv=$s['advertised']??['host'=>'localhost','port'=>'5552'];$extra=pack('N',1).pack('n',0).self::str($adv['host']).pack('N',(int)$adv['port']).pack('N',count($names));
            foreach($names as$name){$exists=$this->isStream($name,$s['vhost']);$extra.=self::str($name).pack('nnN',$exists?1:2,$exists?0:65535,0);}return self::streamFrame(0x800f,pack('N',$corr).$extra);}
        if($key===1){$id=self::u8($r,$at);$ref=self::string($r,$at);$name=self::string($r,$at);$code=isset($s['publishers'][$id])?17:(!$this->isStream($name,$s['vhost'])?2:(!$this->streamAllowed($s,'write',$name)?16:1));
            if($code===1)$s['publishers'][$id]=['stream'=>$name,'ref'=>$ref?:null];return self::streamResp($key,$corr,$code);}
        if($key===6){$id=self::u8($r,$at);$code=isset($s['publishers'][$id])?1:18;unset($s['publishers'][$id]);return self::streamResp($key,$corr,$code);}
        if($key===5){$ref=self::string($r,$at);$name=self::string($r,$at);return self::streamResp($key,$corr,1,pack('J',$b->streamPublisherSequence($name,$ref)??0));}
        if($key===7){$id=self::u8($r,$at);$name=self::string($r,$at);$offset=$this->readStreamOffset($r,$at,$name,$s['vhost']);$credit=self::u16($r,$at);$props=$at<strlen($r)?self::streamMap($r,$at):[];
            $code=isset($s['subs'][$id])?3:(!$this->isStream($name,$s['vhost'])?2:(!$this->streamAllowed($s,'read',$name)?16:1));$single=($props['single-active-consumer']??'')==='true';if($single&&empty($props['name']))$code=17;
            if($code!==1)return self::streamResp($key,$corr,$code);
            $group=$single?$s['vhost']."\0".$name."\0".$props['name']:null;
            $s['subs'][$id]=['stream'=>$name,'offset'=>$offset,'credit'=>$credit,'group'=>$group,'super'=>$props['super-stream']??null,'active'=>!$single,'selected'=>!$single];
            if($single)$this->rebalanceStreams($group);return self::streamResp($key,$corr);}
        if($key===12){$id=self::u8($r,$at);if(!isset($s['subs'][$id]))return self::streamResp($key,$corr,4);$group=$s['subs'][$id]['group'];unset($s['subs'][$id]);if($group)$this->rebalanceStreams($group);return self::streamResp($key,$corr);}
        if($key===11){$ref=self::string($r,$at);$name=self::string($r,$at);if(!$this->isStream($name,$s['vhost']))return self::streamResp($key,$corr,2,pack('J',0));$offset=$b->streamStoredOffset($name,$ref);return self::streamResp($key,$corr,$offset===null?19:1,pack('J',$offset??0));}
        if($key===28){$name=self::string($r,$at);if(!$this->isStream($name,$s['vhost']))return self::streamResp($key,$corr,2,pack('N',0));$first=$b->streamFirst($name);$last=$b->streamNext($name)-1;$extra=pack('N',4);foreach(['first_chunk_id'=>$first,'last_chunk_id'=>$last,'committed_chunk_id'=>$last,'committed_offset'=>$last]as$k=>$v)$extra.=self::str($k).pack('J',$v);return self::streamResp($key,$corr,1,$extra);}
        if($key===24||$key===25){$routing=$key===24?self::string($r,$at):null;$name=self::string($r,$at);if(!isset($b->exchanges[$name]))return self::streamResp($key,$corr,2,self::encodeStrings([]));$parts=$this->streamPartitions($name,$routing,$s['vhost']);return self::streamResp($key,$corr,1,self::encodeStrings($parts));}
        if($key===29){$name=self::string($r,$at);$parts=self::streamStrings($r,$at);$keys=self::streamStrings($r,$at);$args=self::streamMap($r,$at);
            if(!$name||str_starts_with($name,'amq.')||!$parts||count($parts)!==count($keys)||count(array_unique($parts))!==count($parts))return self::streamResp($key,$corr,17);
            if(!$this->streamAllowed($s,'configure',$name)||!$this->streamAllowed($s,'read',$name))return self::streamResp($key,$corr,16);
            if(isset($b->exchanges[$name]))return self::streamResp($key,$corr,5);
            foreach($parts as$i=>$p){if(!$p||str_starts_with($p,'amq.'))return self::streamResp($key,$corr,17);if(isset($b->queues[$p]))return self::streamResp($key,$corr,5);if(!$this->streamAllowed($s,'configure',$p)||!$this->streamAllowed($s,'write',$p)||!$b->topicReadAllowed($s['user'],$s['vhost'],$name,$keys[$i]))return self::streamResp($key,$corr,16);}
            $a=['x-queue-type'=>'stream'];foreach($args as$k=>$v){$k=str_starts_with($k,'x-')?$k:'x-'.$k;$a[$k]=ctype_digit($v)?(int)$v:$v;}
            $definitions=['exchanges'=>[['name'=>$name,'type'=>'direct','durable'=>true]],'queues'=>[],'bindings'=>[]];
            foreach($parts as$i=>$p){$definitions['queues'][]=['name'=>$p,'durable'=>true,'arguments'=>$a];$definitions['bindings'][]=['source'=>$name,'destination'=>$p,'routing_key'=>$keys[$i],'arguments'=>['x-stream-partition-order'=>(string)$i]];}
            // Validate every declaration and binding before mutating any live topology.
            if($b->cluster?->raftEnabled())return $this->streamMetadata($s,$key,$corr,'POST','/api/definitions/'.rawurlencode($s['vhost']),$definitions);
            require_once __DIR__.'/Metadata.php';require_once __DIR__.'/HttpMetadata.php';
            try{HttpMetadata::stage($this->broker->root(),'POST','/api/definitions/'.rawurlencode($s['vhost']),$definitions);$b->declareExchange($name,'direct');foreach($parts as$i=>$p){$this->createStream($s,$p,$args);$b->bind($p,$name,$keys[$i],[['x-stream-partition-order',(string)$i]]);}$b->flushDurable();}catch(Throwable){return self::streamResp($key,$corr,17);}
            return self::streamResp($key,$corr);}
        if($key===30){$name=self::string($r,$at);if(!isset($b->exchanges[$name]))return self::streamResp($key,$corr,2);if(!$this->streamAllowed($s,'configure',$name))return self::streamResp($key,$corr,16);
            $parts=array_values(array_unique($this->streamPartitions($name,null,$s['vhost'])));
            foreach($parts as$p){if(!$this->isStream($p,$s['vhost']))return self::streamResp($key,$corr,2);if(!$this->streamAllowed($s,'configure',$p))return self::streamResp($key,$corr,16);}
            if($b->cluster?->raftEnabled()){
                require_once __DIR__.'/Metadata.php';require_once __DIR__.'/HttpMetadata.php';$commands=[];$host=rawurlencode($s['vhost']);
                try{foreach($parts as$p){$stage=HttpMetadata::stage($this->broker->root(),'DELETE','/api/queues/'.$host.'/'.rawurlencode($p),[]);array_push($commands,...$stage['commands']);}$stage=HttpMetadata::stage($this->broker->root(),'DELETE','/api/exchanges/'.$host.'/'.rawurlencode($name),[]);array_push($commands,...$stage['commands']);}catch(Throwable){return self::streamResp($key,$corr,17);}
                return $this->streamMetadataCommands($s,$key,$corr,$commands);
            }
            foreach($parts as$p)$this->deleteStream($s,$p);$b->deleteExchange($name);return self::streamResp($key,$corr);}
        return self::streamResp($key,$corr,17);
    }
    /** A native-stream topology reply follows the same committed metadata path as HTTP. */
    private function streamMetadata(array &$s,int $key,int $corr,string $method,string $path,array $json):string
    {
        require_once __DIR__.'/Metadata.php';require_once __DIR__.'/HttpMetadata.php';
        try { $stage=HttpMetadata::stage($this->broker->root(),$method,$path,$json); }
        catch(Throwable) { return self::streamResp($key,$corr,17); }
        if($stage===null)return self::streamResp($key,$corr,17);
        return $this->streamMetadataCommands($s,$key,$corr,$stage['commands']);
    }
    private function streamMetadataCommands(array $s,int $key,int $corr,array $commands):string
    {
        $id=$s['id'];
        // Continue an accepted sequence after disconnect, but never retain or reply to a dead session.
        $submit=function(int $at)use(&$submit,$commands,$id,$key,$corr):void {
            if($at===count($commands)) { if(isset($this->streamClients[$id]))$this->streamClients[$id]['out'].=self::streamResp($key,$corr,1);$submit=null;return; }
            $command=$commands[$at];
            $this->broker->cluster->proposeMeta($command['kind'],$command['data'],function(bool $ok,?string $error)use(&$submit,$at,$command,$id,$key,$corr):void {
                if($ok){if($command['kind']==='delete_queue')$this->streamDeleted($command['data']['vhost']??'/',$command['data']['name']);$submit($at+1);}
                else{if(isset($this->streamClients[$id]))$this->streamClients[$id]['out'].=self::streamResp($key,$corr,17);$submit=null;}
            });
        };
        $submit(0);
        return '';
    }
    private function streamDeleted(string $vhost,string $name):void
    {
        $groups=[];
        foreach($this->streamClients as&$client)foreach($client['subs']as$id=>$sub)if($client['vhost']===$vhost&&$sub['stream']===$name){if($sub['group'])$groups[]=$sub['group'];unset($client['subs'][$id]);$client['out'].=self::streamFrame(16,pack('n',2).self::str($name));}unset($client);
        foreach(array_unique($groups)as$group)$this->rebalanceStreams($group);
    }
    private function createStream(array $s,string $name,array $args):int
    {
        $b=$this->broker->forVhost($s['vhost']);
        if(!$name||str_starts_with($name,'amq.'))return 17;if(!$this->streamAllowed($s,'configure',$name))return 16;if(isset($b->queues[$name]))return 5;
        $a=['x-queue-type'=>'stream'];foreach($args as$k=>$v){$k=str_starts_with($k,'x-')?$k:'x-'.$k;$a[$k]=ctype_digit($v)?(int)$v:$v;}
        try{$b->declareQueue($name,$a);$b->flushDurable();return 1;}catch(Throwable $e){return 17;}
    }
    private function deleteStream(array $s,string $name):int
    {
        $b=$this->broker->forVhost($s['vhost']);
        if(!$this->isStream($name,$s['vhost']))return 2;if(!$this->streamAllowed($s,'configure',$name))return 16;$b->deleteQueue($name);
        $this->streamDeleted($s['vhost'],$name);return 1;
    }
    private function streamPartitions(string $name,?string $routing=null,string $vhost="/"):array
    {
        $bindings=array_values(array_filter($this->broker->forVhost($vhost)->bindings,static fn($b)=>$b['exchange']===$name&&($routing===null||$b['key']===$routing)));
        $order=static function($b){foreach($b['args']??[]as[$k,$v])if($k==='x-stream-partition-order')return(int)$v;return 0;};usort($bindings,static fn($a,$b)=>$order($a)<=>$order($b));return array_column($bindings,'queue');
    }
    private function readStreamOffset(string $r,int &$at,string $name,string $vhost):int
    {
        $b=$this->broker->forVhost($vhost);
        $type=self::u16($r,$at);$first=$this->isStream($name,$vhost)?$b->streamFirst($name):0;$next=$this->isStream($name,$vhost)?$b->streamNext($name):0;
        if($type===1)return$first;if($type===2)return max($first,$next-1);if($type===3)return$next;if($type===4)return self::u64($r,$at);
        if($type===5){$time=self::u64($r,$at);$offset=$first;while($offset<$next){$records=$b->streamRead($name,$offset,128);if(!$records)break;foreach($records as$m){if($m['time']>=$time)return$m['offset'];$offset=$m['offset']+1;}}return$next;}throw new RuntimeException('invalid offset specification');
    }
    private function streamPublish(string $bytes, array &$session, int $version): string
    {
        $broker = $this->broker->forVhost($session['vhost']);
        $at = 0;
        $publisherId = self::u8($bytes, $at);
        $count = self::u32($bytes, $at);
        $publisher = $session['publishers'][$publisherId] ?? null;
        $jobs = [];
        for ($i = 0; $i < $count; $i++) {
            $rawId = self::take($bytes, $at, 8);
            $sequence = unpack('J', $rawId)[1];
            if ($version >= 2) self::string($bytes, $at);
            $payload = self::take($bytes, $at, self::u32($bytes, $at));
            $code = !$publisher ? 18 : (!$this->isStream($publisher['stream'], $session['vhost']) ? 2 : (!$this->streamAllowed($session, 'write', $publisher['stream']) ? 16 : 1));
            $message = null;
            if ($code === 1) {
                try { $message = $this->streamInbound($payload); }
                catch (Throwable $error) { $code = 15; }
            }
            $jobs[] = [$rawId, $sequence, $message, $code];
        }
        if (!$jobs) return '';
        $pending = count($jobs);
        $confirmed = [];
        $failed = [];
        $finish = function(string $rawId, int $code) use (&$session, &$pending, &$confirmed, &$failed, $publisherId): void {
            if ($code === 1) $confirmed[] = $rawId;
            else $failed[] = $rawId . pack('n', $code);
            if (--$pending !== 0) return;
            if ($confirmed) $session['out'] .= self::streamFrame(3, chr($publisherId) . pack('N', count($confirmed)) . implode('', $confirmed));
            if ($failed) $session['out'] .= self::streamFrame(4, chr($publisherId) . pack('N', count($failed)) . implode('', $failed));
        };
        foreach ($jobs as [$rawId, $sequence, $message, $code]) {
            if ($code !== 1) { $finish($rawId, $code); continue; }
            try {
                $broker->streamAppendAsync($publisher['stream'], $message['body'], $message['headers'], $message['propRaw'], $publisher['ref'], $sequence,
                    static function(bool $ok, ?int $offset = null) use ($finish, $rawId): void { $finish($rawId, $ok ? 1 : 15); });
            } catch (Throwable $error) { $finish($rawId, 15); }
        }
        return '';
    }
    private function pumpStream(array &$s):string
    {
        $b=$this->broker->forVhost($s['vhost']);
        foreach(array_unique(array_column($s['subs'],'stream'))as$name)if(!$this->isStream($name,$s['vhost']))$this->streamDeleted($s['vhost'],$name);
        $out=$s['out'];$s['out']='';if(!$s['opened'])return$out;
        if($s['heartbeat']>0&&microtime(true)-$s['beat']>=$s['heartbeat']){$out.=self::streamFrame(23,'');$s['beat']=microtime(true);}
        foreach($s['subs']as$id=>&$sub){if(!$sub['active']||$sub['credit']<=0||!$this->isStream($sub['stream'],$s['vhost']))continue;
            $messages=$b->streamRead($sub['stream'],$sub['offset'],min(128,$sub['credit']));foreach($messages as$m){$entry=$this->streamOutbound($m);$data=pack('N',strlen($entry)).$entry;
                $chunk="\x50\0".pack('nN',1,1).pack('J',$m['time']).pack('J',0).pack('J',$m['offset']).pack('N',crc32($data)).pack('NNN',strlen($data),0,0).$data;
                $out.=self::streamFrame(8,chr($id).$chunk);$sub['credit']--;$sub['offset']=$m['offset']+1;
            }
        }unset($sub);return$out;
    }
    private function rebalanceStreams(string $group):void
    {
        $members=[];foreach($this->streamClients as$cid=>$s)foreach($s['subs']as$id=>$sub)if($sub['group']===$group)$members[]=[$cid,$id,$sub];if(!$members)return;
        $index=0;if($members[0][2]['super'])$index=max(0,(int)array_search($members[0][2]['stream'],$this->streamPartitions($members[0][2]['super'],null,explode("\0",$group)[0]),true));$want=$index%count($members);
        foreach($members as$i=>[$cid,$id,$sub]){$client=&$this->streamClients[$cid];$selected=$i===$want;if($sub['selected']===$selected)continue;$client['subs'][$id]['selected']=$selected;$client['subs'][$id]['active']=false;$corr=$client['corr']++;$client['pending'][$corr]=$id;$client['out'].=self::streamFrame(26,pack('NCC',$corr,$id,$selected?1:0));unset($client);}
    }
    public function dropStream(array &$state):void
    {
        $id=$state['protocolId']??null;if($id===null||!isset($this->streamClients[$id]))return;$groups=[];foreach($this->streamClients[$id]['subs']as$sub)if($sub['group'])$groups[]=$sub['group'];unset($this->streamClients[$id]);foreach(array_unique($groups)as$g)$this->rebalanceStreams($g);
    }

    private function streamInbound(string $payload): array
    {
        $message = Amqp10::inbound($payload);
        return [
            'body' => $message['body'],
            'headers' => $message['headers'],
            'propRaw' => Amqp10::writeProps($message['props'], $message['headers']),
        ];
    }

    private function streamOutbound(array $message): string
    {
        $props = Amqp10::readProps($message['propRaw'] ?? null);
        unset($props['headers']['x-stream-offset']);
        $headers = array_values(array_filter($message['headers'] ?? [], static fn(array $h): bool => $h[0] !== 'x-stream-offset'));
        $pairs = [];
        foreach ($props['headers'] as $key => $value) $pairs[] = [$key, $value];
        $message['headers'] = $headers;
        $message['propRaw'] = Amqp10::writeProps($props, $pairs);
        $message['mode'] = 2;
        return Amqp10::outbound($message);
    }
}
