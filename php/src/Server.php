<?php
declare(strict_types=1);

require_once __DIR__ . "/Metadata.php";

final class Chan
{
    public bool $confirm = false;
    public int $nextPub = 1;
    public int $nextDel = 1;
    public int $prefetch = 0;
    public ?string $replyAddress = null;
    public ?string $replyTag = null;
    public bool $metaPending = false;
    public array $metaFrames = [];
    /** @var array<int, int> */
    public array $unacked = [];
    public ?string $consumer = null;
    public ?string $queue = null;
    /** @var array<string, bool> no-ack flag per consumer tag */
    public array $noAck = [];
    /**
     * Consumer tags served by another node, with the peer and session the
     * home assigned, so a cancel can be forwarded.
     *
     * @var array<string, array{peer:string,session:int}>
     */
    public array $remote = [];
    /** @var array{key:string,mode:int,need:int,got:string}|null */
    public ?array $pub = null;
    /** @var array<int, string> consumer tag of each unacked delivery tag */
    public array $tagOf = [];
    /** @var array<string, int> unacked deliveries held per consumer tag */
    public array $held = [];
    /** tx.select was sent: publishes and acks wait for tx.commit. */
    public bool $tx = false;
    /** @var list<array<string,mixed>> publishes held by the transaction */
    public array $txPub = [];
    /** @var list<array{0:string,1:int,2:bool}> acks and nacks held by the transaction */
    public array $txAcks = [];

    /** Forget one unacked delivery. Returns its message id, or null. */
    public function settle(int $dtag): ?int
    {
        if (!isset($this->unacked[$dtag])) {
            return null;
        }
        $msgId = $this->unacked[$dtag];
        unset($this->unacked[$dtag]);
        $tag = $this->tagOf[$dtag] ?? null;
        unset($this->tagOf[$dtag]);
        if ($tag !== null && isset($this->held[$tag])) {
            $this->held[$tag]--;
        }
        return $msgId;
    }
}

final class Sock
{
    /** @param resource $fp */
    public function __construct(public $fp)
    {
    }

    public string $in = '';
    /** Index of the first unparsed byte in `in`. Compacting every frame copied the tail. */
    public int $inAt = 0;
    public string $out = '';
    public int $off = 0;
    public string $stage = 'header';
    public bool $gone = false;
    /** The authenticated user, set at connection.start-ok. */
    public string $user = '';
    /** Verified certificate identity, populated only from an established TLS stream. */
    public ?string $peerCN = null;
    public bool $tls = false;
    /** The vhost from connection.open. */
    public string $vhost = '/';
    /** True once a frame has named a queue. The connection is not moved again. */
    public bool $settled = false;
    public ?string $metadataQueue = null;
    public float $metadataDeadline = 0;
    /** @var array<int, Chan> */
    public array $channels = [];
    /** @var array<string, mixed> AMQP 1.0 phase and link state */
    public array $amqp10 = [];

    public function send(string $bytes): void
    {
        $this->out .= $bytes;
    }

    public function flush(): void
    {
        $len = strlen($this->out);
        while ($this->off < $len) {
            $n = @fwrite($this->fp, substr($this->out, $this->off, 65536));
            if ($n === false || $n === 0) {
                return;
            }
            $this->off += $n;
        }
        $this->out = '';
        $this->off = 0;
    }
}

/** One-process AMQP 0-9-1 listener for classic durable queues. */
final class Server
{
    /** @var array<int, Sock> */
    public array $conns = [];
    /** @var array<int, int> stream resource id to connection id */
    private array $byFp = [];
    private int $nextConn = 1;
    private int $rr = 0;

    private float $nextSync = 0;
    private float $nextBeat = 0;
    /** Seconds between server heartbeats; half the 60 the tune frame offers. */
    private const BEAT_SECONDS = 30.0;
    /**
     * How often expiry is swept. Separate from the heartbeat, which is far
     * too coarse: a message with a one-second TTL would otherwise keep
     * counting toward queue depth for another half minute.
     */
    private const SWEEP_SECONDS = 0.25;
    private float $nextSweep = 0;
    /** True while tx.commit applies what a transaction held. */
    private bool $replaying = false;
    /** @var list<array{0:mixed,1:float}> streams of moved connections, closed after a delay */
    private array $retired = [];
    /**
     * At or below this many outstanding confirms, flush at once rather than
     * waiting for the interval. Batching needs a queue to batch; a handful of
     * blocked publishers have none.
     */
    private const FLUSH_SMALL = 8;

    /** @param resource|null $listen */
    public function __construct(private $listen, private Broker $broker, private int $fsyncMs, public ?Extras $extras = null, public ?Handoff $handoff = null)
    {
        $this->nextSync = microtime(true) + $this->fsyncMs / 1000;
        $this->nextBeat = microtime(true) + self::BEAT_SECONDS;
        $this->bootAt = microtime(true);
        $this->amqp10 = new Amqp10($broker);
        $broker->onDeleteQueue = function (string $vhost, string $name, array $consumers): void {
            foreach ($consumers as $consumer) {
                $client = $this->conns[$consumer['conn']] ?? null;
                if ($client === null || $client->gone || $client->vhost !== $vhost || $consumer['ch'] < 0) continue;
                $client->send(Codec::method($consumer['ch'], 60, 30, Codec::shortstr($consumer['tag']) . chr(1)));
                $client->flush();
            }
        };
        $broker->onDeleteVhost = function (string $vhost): void {
            foreach ($this->conns as $id => $sock) if ($sock->vhost === $vhost && !$sock->gone) {
                $sock->send(Codec::connectionClose(320, 'CONNECTION_FORCED - vhost deleted', 0, 0));
                $sock->flush(); $this->drop($id);
            }
        };
        if ($extras !== null) $extras->onAmqp = fn($fp) => $this->take($fp);
        if ($extras !== null) {
            $extras->http->onConnections = function (): array {
                $rows = [];
                foreach ($this->conns as $id => $sock) if (!$sock->gone && $sock->user !== '') $rows[] = [
                    'name' => (string) $id, 'user' => $this->broker->identityName($sock->user), 'vhost' => $sock->vhost,
                    'channels' => count($sock->channels), 'state' => 'running', 'protocol' => $sock->stage === 'amqp10' ? 'AMQP 1.0' : 'AMQP 0-9-1',
                ];
                return $rows;
            };
            $extras->http->onChannels = function (): array {
                $rows = [];
                foreach ($this->conns as $id => $sock) foreach ($sock->channels as $channel => $ch) $rows[] = [
                    'name' => "$id:$channel", 'number' => $channel, 'user' => $this->broker->identityName($sock->user), 'vhost' => $sock->vhost,
                    'prefetch_count' => $ch->prefetch, 'messages_unacknowledged' => count($ch->unacked), 'state' => 'running',
                ];
                return $rows;
            };
        }
    }

    private Amqp10 $amqp10;
    private float $bootAt = 0;
    private bool $announced = false;

    public function run(): void
    {
        while (true) {
            $this->pullHandoff();
            $read = [];
            if (is_resource($this->listen)) {
                $read[] = $this->listen;
            }
            if ($this->extras !== null) {
                foreach ($this->extras->reads() as $fp) {
                    $read[] = $fp;
                }
            }
            $write = $this->extras?->writes() ?? [];
            foreach ($this->conns as $sock) {
                if ($sock->gone) {
                    continue;
                }
                $read[] = $sock->fp;
                if ($sock->off < strlen($sock->out)) {
                    $write[] = $sock->fp;
                }
            }
            $except = [];
            $left = $this->nextSync - microtime(true);
            $wait = $this->broker->waiting === [] ? 1.0 : max(0.001, $left);
            $wait = min($wait, max(0.001, $this->nextSweep - microtime(true)));
            // Consensus and chained metadata callbacks need ticks even without socket activity.
            if ($this->extras?->cluster->raftEnabled()) $wait = min($wait, 0.01);
            if ($this->handoff !== null) {
                $wait = min($wait, 0.002);
                $this->closeRetired();
            }
            $sec = (int) $wait;
            $usec = (int) (($wait - $sec) * 1000000);
            $selected = ($read === [] && $write === []) ? 0 : @stream_select($read, $write, $except, $sec, $usec);
            if ($selected === false) {
                continue;
            }
            foreach ($read as $fp) {
                if ($this->extras !== null && $this->extras->owns($fp)) {
                    $this->extras->onRead($fp);
                    continue;
                }
                if ($fp === $this->listen) {
                    $listenerMeta = stream_get_meta_data($this->listen);
                    $secureListener = str_contains($listenerMeta['stream_type'] ?? '', 'ssl');
                    if ($secureListener) { stream_set_blocking($this->listen, true); stream_set_timeout($this->listen, 5); }
                    $client = @stream_socket_accept($this->listen, $secureListener ? 1 : 0);
                    if ($secureListener) stream_set_blocking($this->listen, false);
                    if ($client !== false) {
                        $this->take($client);
                    }
                    continue;
                }
                $id = $this->idOf($fp);
                if ($id === null) {
                    continue;
                }
                $chunk = @fread($fp, 65536);
                if ($chunk === false || $chunk === '') {
                    $meta = stream_get_meta_data($fp);
                    if (($chunk === '' || $chunk === false) && ($meta['eof'] ?? false)) {
                        $this->drop($id);
                    }
                    continue;
                }
                $this->onData($id, $chunk);
            }
            foreach ($write as $fp) {
                if ($this->extras !== null && $this->extras->owns($fp)) { $this->extras->flush($fp); continue; }
                $id = $this->idOf($fp);
                if ($id !== null) {
                    $this->conns[$id]->flush();
                }
            }
            // Nothing readable means the publishers are blocked rather than
            // streaming.
            $this->commit($selected === 0);
            $this->pullHandoff();
            $this->announce();
            $this->beat();
            foreach ($this->broker->allBrokers() as $scope) $scope->maybeCompact();
            foreach ($this->conns as $sock) if ($sock->stage === 'amqp10' && !$sock->gone) { $sock->send($this->amqp10->tick($sock->amqp10)); $sock->flush(); }
            if ($this->extras !== null) {
                $this->extras->tick();
            }
            foreach(array_keys($this->conns)as$id){
                $sock=$this->conns[$id]??null;if($sock===null||$sock->gone)continue;
                if($sock->metadataQueue!==null)$this->onData($id,'');
                elseif($this->handoff!==null&&$sock->stage==='header'){
                    // Probe newly adopted sockets without blocking until their real header arrives.
                    $chunk=@fread($sock->fp,65536);
                    if($chunk!==false&&$chunk!=='')$this->onData($id,$chunk);
                    elseif(feof($sock->fp))$this->drop($id);
                }
            }
        }
    }

    /** @param resource $fp */
    private function take($fp, ?array $state = null, string $bytes = ''): void
    {
        if (!self::acceptTls($fp)) { fclose($fp); return; }
        stream_set_blocking($fp, false);
        $initial = stream_get_meta_data($fp);
        if (!isset($initial['crypto'])) $this->tune($fp);
        $id = $this->nextConn++;
        $sock = new Sock($fp);
        $meta = stream_get_meta_data($fp);
        $sock->tls = isset($meta['crypto']);
        $sock->peerCN = $this->verifiedPeerCN($fp);

        if ($state !== null) {
            $sock->stage = 'frames';
            if(is_string($state['metadataQueue']??null)){$sock->metadataQueue=$state['metadataQueue'];$sock->metadataDeadline=microtime(true)+2;}
            $sock->user = (string) ($state['user'] ?? '');
            $sock->vhost = (string) ($state['vhost'] ?? '/');
            foreach ($state['channels'] ?? [] as $item) {
                if (!is_array($item)) {
                    continue;
                }
                $ch = new Chan();
                $ch->confirm = ($item['confirm'] ?? false) === true;
                $ch->prefetch = (int) ($item['prefetch'] ?? 0);
                $sock->channels[(int) $item['id']] = $ch;
            }
            $sock->in = $bytes;
        }
        $this->conns[$id] = $sock;
        $this->byFp[(int) $fp] = $id;
        $this->broker->prom['connections']++;
        $this->broker->prom['connectionsOpened']++;
        if ($state !== null) {
            $this->onData($id, '');
        } elseif ($this->handoff !== null) {
            // Consume any header bytes already waiting on the newly adopted socket.
            $chunk=@fread($fp,65536);
            if($chunk!==false&&$chunk!=='')$this->onData($id,$chunk);
            elseif(feof($fp))$this->drop($id);
        }
    }

    /** Complete explicitly configured TLS endpoints; ordinary TCP is never upgraded. */
    public static function acceptTls($fp): bool
    {
        $ssl = stream_context_get_options($fp)['ssl'] ?? [];
        if (($ssl['queueforge_tls'] ?? false) !== true || isset(stream_get_meta_data($fp)['crypto'])) return true;
        stream_set_blocking($fp, false);
        $deadline = microtime(true) + 5;
        do {
            $result = @stream_socket_enable_crypto($fp, true, STREAM_CRYPTO_METHOD_TLS_SERVER);
            if ($result !== 0) return $result === true && isset(stream_get_meta_data($fp)['crypto']);
            $read = [$fp]; $write = []; $except = [];
            if (@stream_select($read, $write, $except, 0, 10000) === false) return false;
        } while (microtime(true) < $deadline);
        return false;
    }

    /** Context verification happened during accept; a captured certificate alone proves nothing. */
    private function verifiedPeerCN($fp): ?string
    {
        $meta = stream_get_meta_data($fp);
        $ssl = stream_context_get_options($fp)['ssl'] ?? [];
        if ((!isset($meta['crypto']) && !str_contains($meta['stream_type'] ?? '', 'ssl')) || ($ssl['verify_peer'] ?? false) !== true || ($ssl['allow_self_signed'] ?? false) === true
            || !is_string($ssl['cafile'] ?? null) || $ssl['cafile'] === '' || !isset($ssl['peer_certificate'])) return null;
        $cert = openssl_x509_parse($ssl['peer_certificate']);
        $cn = $cert['subject']['CN'] ?? null;
        return is_string($cn) && $cn !== '' && !str_contains($cn, "\0") ? $cn : null;
    }

    private function pullHandoff(): void
    {
        if ($this->handoff === null) {
            return;
        }
        while ($packet = $this->handoff->recv()) {
            $msg = $packet['msg'];
            if (($msg['type'] ?? '') !== 'conn' || !is_resource($packet['fp'])) {
                if (is_resource($packet['fp'])) {
                    fclose($packet['fp']);
                }
                continue;
            }
            if (isset($msg['state']) && is_array($msg['state'])) {
                $raw = base64_decode((string) ($msg['bytes'] ?? ''), true);
                $this->take($packet['fp'], $msg['state'], is_string($raw) ? $raw : '');
            } else {
                $this->take($packet['fp']);
            }
        }
    }

    /** Tells the parent this child can take connections, once peers are up. */
    private function announce(): void
    {
        if ($this->announced || $this->handoff === null) {
            return;
        }
        $need = max(0, count($this->broker->members) - 1);
        $have = $this->extras === null ? 0 : count($this->extras->cluster->peerIds());
        if (($have < $need || !($this->extras?->peersReady() ?? true)) && microtime(true) < $this->bootAt + 8) {
            return;
        }
        $this->announced = $this->handoff->send(['type' => 'ready']);
    }

    /**
     * Moves this connection to the queue's home before the naming frame is handled.
     * One move. A publish stays here until its header and body are buffered, so
     * closing this process's socket does not discard unread bytes.
     *
     * @return 'no'|'pending'|'moved'
     */
    private function maybeMigrate(int $id): string
    {
        $broker = $this->brokerFor($id);
        if ($this->handoff === null || !isset($this->conns[$id])) {
            return 'no';
        }
        $sock = $this->conns[$id];
        if ($sock->tls || $sock->settled || $sock->stage !== 'frames') {
            return 'no';
        }
        // Bytes still in the kernel buffer travel with the passed socket, so
        // nothing more is read here. Draining it made a deep publish window a
        // datagram larger than the receiver's buffer, which cut the
        // connection.
        $queue = Handoff::namedQueue($sock->in, $sock->inAt);
        if ($queue === null) {
            return 'no';
        }
        if (($broker->queues[$queue]['args']['queueType'] ?? 'classic') === 'quorum') {
            $sock->settled = true;
            return 'no';
        }
        $home = $broker->home($queue);
        if ($home === '' || $home === $broker->nodeId) {
            $sock->settled = true;
            return 'no';
        }
        if (!$this->namingFrameReady($sock->in, $sock->inAt)) {
            return 'pending';
        }
        $channels = [];
        foreach ($sock->channels as $cid => $ch) {
            $channels[] = ['id' => $cid, 'confirm' => $ch->confirm, 'prefetch' => $ch->prefetch];
        }
        $payload = [
            'type' => 'migrate',
            'home' => $home,
            'state' => [
                'user' => $sock->user,
                'vhost' => $sock->vhost,
                'metadataQueue' => $broker->cluster?->raftEnabled() && isset($broker->queues[$queue]) ? $queue : null,
                'channels' => $channels,
            ],
            'bytes' => base64_encode(substr($sock->in, $sock->inAt)),
        ];
        // The relay reads one datagram into a 256 KiB buffer. A connection whose
        // unread bytes would not fit stays here and is forwarded instead.
        if (strlen((string) $payload['bytes']) > Handoff::MAX_BYTES) {
            $sock->settled = true;
            return 'no';
        }
        $ok = false;
        for ($try = 0; $try < 100 && !$ok; $try++) {
            $ok = $this->handoff->send($payload, $sock->fp);
            if (!$ok) {
                usleep(1000);
            }
        }
        if (!$ok) {
            $sock->settled = true;
            return 'no';
        }
        $this->forget($id);
        return 'moved';
    }

    /**
     * Stop serving a connection that moved to another process. Nothing is
     * requeued or deleted: the home process owns the session now. The local
     * stream is closed a little later, once the home has the socket.
     */
    private function forget(int $id): void
    {
        $broker = $this->brokerFor($id);
        $sock = $this->conns[$id] ?? null;
        if ($sock === null) {
            return;
        }
        unset($this->byFp[(int) $sock->fp], $this->conns[$id]);
        $broker->prom['connections']--;
        $this->retired[] = [$sock->fp, microtime(true) + 1.0];
    }

    /** Close the streams of connections that moved away a while ago. */
    private function closeRetired(): void
    {
        $now = microtime(true);
        $keep = [];
        foreach ($this->retired as [$fp, $at]) {
            if ($at <= $now) {
                @fclose($fp);
            } else {
                $keep[] = [$fp, $at];
            }
        }
        $this->retired = $keep;
    }

    /** A consume is ready at its method frame. A publish waits for header and body. */
    private function namingFrameReady(string $buf, int $at): bool
    {
        if (strlen($buf) - $at < 11 || ($buf[$at] ?? '') !== "\x01") {
            return false;
        }
        $len = unpack('N', substr($buf, $at + 3, 4))[1];
        if (strlen($buf) - $at < 8 + $len) {
            return false;
        }
        $class = unpack('n', substr($buf, $at + 7, 2))[1];
        $method = unpack('n', substr($buf, $at + 9, 2))[1];
        if (!($class === 60 && $method === 40)) {
            return true;
        }
        $pos = $at + 8 + $len;
        if (strlen($buf) - $pos < 8 || ($buf[$pos] ?? '') !== "\x02") {
            return false;
        }
        $hlen = unpack('N', substr($buf, $pos + 3, 4))[1];
        if (strlen($buf) - $pos < 8 + $hlen || $hlen < 12) {
            return false;
        }
        $need = Codec::readU64($buf, $pos + 11);
        $cursor = $pos + 8 + $hlen;
        $got = 0;
        while ($got < $need) {
            if (strlen($buf) - $cursor < 8 || ($buf[$cursor] ?? '') !== "\x03") {
                return false;
            }
            $blen = unpack('N', substr($buf, $cursor + 3, 4))[1];
            if (strlen($buf) - $cursor < 8 + $blen) {
                return false;
            }
            $got += $blen;
            $cursor += 8 + $blen;
        }
        return true;
    }

    /** @param resource $fp */
    private function tune($fp): void
    {
        if (!function_exists("socket_import_stream")) {
            return;
        }
        $socket = socket_import_stream($fp);
        if ($socket !== false) {
            @socket_set_option($socket, SOL_TCP, TCP_NODELAY, 1);
        }
    }

    /**
     * Looks up a connection by stream. This was a linear scan over every
     * connection, run once per readable stream, which made the select loop
     * quadratic in connection count.
     *
     * @param resource $fp
     */
    private function idOf($fp): ?int
    {
        return $this->byFp[(int) $fp] ?? null;
    }

    /**
     * Emits a heartbeat frame on idle connections. The tune frame advertises
     * 60 seconds, so a client that enforces it used to time out against a
     * server that never sent one.
     */
    private function beat(): void
    {
        $now = microtime(true);
        // Expire messages past their TTL and drop queues idle past x-expires.
        // Without this an expired message keeps counting toward queue depth
        // and max-length until something happens to dequeue it.
        if ($now >= $this->nextSweep) {
            $this->nextSweep = $now + self::SWEEP_SECONDS;
            foreach ($this->broker->allBrokers() as $scope) $scope->sweep();
            $this->pump();
        }
        if ($now < $this->nextBeat) {
            return;
        }
        $this->nextBeat = $now + self::BEAT_SECONDS;
        foreach ($this->conns as $sock) {
            if ($sock->gone || $sock->stage !== 'frames') {
                continue;
            }
            if ($sock->out !== '') {
                continue;
            }
            $sock->send(Codec::heartbeat());
            $sock->flush();
        }
    }

    private function drop(int $id): void
    {
        $broker = $this->brokerFor($id);
        if (!isset($this->conns[$id])) {
            return;
        }
        $sock = $this->conns[$id];
        if ($sock->stage === 'amqp10') $this->amqp10->closed($sock->amqp10);
        $sock->gone = true;
        unset($this->byFp[(int) $sock->fp]);
        @fclose($sock->fp);
        $broker->prom['connections']--;
        $broker->prom['connectionsClosed']++;
        $broker->prom['channels'] -= count($sock->channels);
        $broker->prom['channelsClosed'] += count($sock->channels);
        foreach (array_keys($sock->channels) as $channel) {
            $this->releaseChannel($id, $channel);
        }
        foreach ($broker->queues as $name => $q) {
            $before = count($q['consumers']);
            $broker->queues[$name]['consumers'] = array_values(array_filter(
                $q['consumers'],
                static fn (array $c): bool => $c['conn'] !== $id,
            ));
            $broker->prom['consumers'] -= $before - count($broker->queues[$name]['consumers']);
        }
        // An exclusive queue goes with the connection that declared it.
        foreach ($broker->queues as $name => $q) {
            if (($q['owner'] ?? null) === $id) {
                $broker->deleteQueue($name);
            }
        }
        unset($this->conns[$id]);
        $this->pump();
    }

    /**
     * A closing channel gives back its unacked messages, in delivery order,
     * and its consumers go. An auto-delete queue that lost its last consumer
     * is deleted.
     */
    private function releaseChannel(int $id, int $channel): void
    {
        $broker = $this->brokerFor($id);
        $ch = $this->conns[$id]->channels[$channel] ?? null;
        if ($ch === null) {
            return;
        }
        foreach (array_reverse(array_keys($ch->unacked)) as $dtag) {
            $msgId = $ch->settle($dtag);
            if ($msgId !== null) {
                $broker->requeue($msgId);
            }
        }
        $ch->txPub = [];
        $ch->txAcks = [];
        $touched = [];
        foreach ($broker->queues as $name => $q) {
            $before = count($q['consumers']);
            $broker->queues[$name]['consumers'] = array_values(array_filter(
                $q['consumers'],
                static fn (array $c): bool => !($c['conn'] === $id && $c['ch'] === $channel),
            ));
            $removed = $before - count($broker->queues[$name]['consumers']);
            if ($removed > 0) {
                $broker->prom['consumers'] -= $removed;
                $touched[] = $name;
            }
        }
        foreach ($touched as $name) {
            $this->autoDelete($name, $broker);
        }
    }

    /** Delete an auto-delete queue once its last consumer is gone. */
    private function autoDelete(string $name, ?Broker $scope = null): void
    {
        $broker = $scope ?? $this->broker;
        $q = $broker->queues[$name] ?? null;
        if ($q !== null && ($q['autoDelete'] ?? false) && ($q['hadConsumer'] ?? false) && $q['consumers'] === []) {
            $broker->deleteQueue($name);
        }
    }

    /**
     * An exclusive queue belongs to the connection that declared it. Any
     * other connection gets 405 RESOURCE_LOCKED, and its channel closes.
     */
    private function locked(int $id, int $channel, string $queue, int $class, int $method): bool
    {
        $broker = $this->brokerFor($id);
        $owner = $broker->queues[$queue]['owner'] ?? null;
        if ($owner === null || $owner === $id) {
            return false;
        }
        $this->conns[$id]->send(Codec::channelClose($channel, 405, "RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '$queue' in vhost '/'", $class, $method));
        return true;
    }

    private function onData(int $id, string $data): void
    {
        $broker = $this->brokerFor($id);
        $sock = $this->conns[$id];
        $sock->in .= $data;
        if($sock->metadataQueue!==null){
            if(!isset($broker->queues[$sock->metadataQueue])){
                if(microtime(true)>=$sock->metadataDeadline){$sock->send(Codec::connectionClose(541,'INTERNAL_ERROR - queue metadata unavailable',0,0));$sock->flush();$this->drop($id);}
                return;
            }
            $sock->metadataQueue=null;
        }
        if ($sock->stage === 'header') {
            if (strlen($sock->in) < 8) {
                return;
            }
            $head = substr($sock->in, 0, 8);
            // An AMQP 1.0 client sends AMQP\x00\x01\x00\x00, or
            // AMQP\x03\x01\x00\x00 for the SASL layer. Both are handed to the
            // 1.0 shim rather than dropped.
            if ($head === "AMQP\x00\x01\x00\x00" || $head === "AMQP\x03\x01\x00\x00") {
                $sock->stage = 'amqp10';
                $sock->amqp10['conn'] = $id;
                $sock->send($this->amqp10->drive($sock->in, $sock->amqp10));
                $sock->flush();
                if (($sock->amqp10['closing'] ?? false) === true) {
                    $this->drop($id);
                }
                return;
            }
            if ($head !== "AMQP\x00\x00\x09\x01") {
                $this->drop($id);
                return;
            }
            $sock->inAt = 8;
            $sock->stage = 'frames';
            $sock->send(Codec::connectionStart($sock->peerCN !== null ? 'PLAIN EXTERNAL' : 'PLAIN'));
        }
        if ($sock->stage === 'amqp10') {
            $sock->send($this->amqp10->drive($sock->in, $sock->amqp10));
            $sock->flush();
            if (($sock->amqp10['closing'] ?? false) === true) {
                $this->drop($id);
            }
            return;
        }
        while (isset($this->conns[$id])) {
            $rest = strlen($sock->in) - $sock->inAt;
            if ($rest < 7) {
                break;
            }
            $i = $sock->inAt;
            $type = ord($sock->in[$i]);
            $channel = unpack('n', substr($sock->in, $i + 1, 2))[1];
            $len = unpack('N', substr($sock->in, $i + 3, 4))[1];
            if ($rest < 8 + $len) {
                break;
            }
            if ($sock->in[$i + 7 + $len] !== "\xce") {
                $this->drop($id);
                return;
            }
            if ($type === 1) {
                $move = $this->maybeMigrate($id);
                if ($move === 'moved') {
                    return;
                }
                if ($move === 'pending') {
                    break;
                }
            }
            $payload = substr($sock->in, $i + 7, $len);
            $sock->inAt = $i + 8 + $len;
            $this->onFrame($id, $type, $channel, $payload);
            if (!isset($this->conns[$id])) {
                return;
            }
            $sock = $this->conns[$id];
        }
        if ($sock->inAt >= 16384) {
            $sock->in = substr($sock->in, $sock->inAt);
            $sock->inAt = 0;
        }
        $sock->flush();
    }

    private function onFrame(int $id, int $type, int $channel, string $payload): void
    {
        $broker = $this->brokerFor($id);
        $sock = $this->conns[$id];
        if ($type === 8) {
            return;
        }
        $ch = $sock->channels[$channel] ?? null;
        if ($ch?->metaPending) { $ch->metaFrames[] = [$type, $channel, $payload]; return; }
        if ($type === 2 && $ch !== null && $ch->pub !== null && strlen($payload) >= 12) {
            $ch->pub['need'] = Codec::readU64($payload, 4);
            // Keep the property bytes from the flag word on so they can be
            // replayed to consumers verbatim, the way Bun preserves propRaw.
            $ch->pub['propRaw'] = substr($payload, 12);
            $flags = unpack('n', substr($payload, 12, 2))[1];
            $at = 14;
            if (($flags & 0x8000) !== 0) {
                Codec::readShortstr($payload, $at);
            }
            if (($flags & 0x4000) !== 0) {
                Codec::readShortstr($payload, $at);
            }
            if (($flags & 0x2000) !== 0) {
                $ch->pub['headers'] = [];
                foreach (Codec::readTable($payload, $at) as $name => $value) {
                    // The value keeps its decoded type, so an integer 5 and
                    // the string "5" are distinct for a headers-exchange
                    // binding, as they are in Bun.
                    $ch->pub['headers'][] = [(string) $name, $value];
                }
            }
            if (($flags & 0x1000) !== 0 && isset($payload[$at])) {
                $ch->pub['mode'] = ord($payload[$at]);
                $at++;
            }
            if (($flags & 0x0800) !== 0 && isset($payload[$at])) {
                $ch->pub['priority'] = ord($payload[$at]);
                $at++;
            }
            if (($flags & 0x0400) !== 0) {
                Codec::readShortstr($payload, $at);
            }
            if (($flags & 0x0200) !== 0) {
                Codec::readShortstr($payload, $at);
            }
            if (($flags & 0x0100) !== 0) {
                $ch->pub['expiration'] = (int) Codec::readShortstr($payload, $at);
            }
            if (($flags & 0x0080) !== 0) {
                Codec::readShortstr($payload, $at);
            }
            if (($flags & 0x0040) !== 0) {
                $at += 8;
            }
            if (($flags & 0x0020) !== 0) {
                Codec::readShortstr($payload, $at);
            }
            if (($flags & 0x0010) !== 0) {
                $ch->pub['userId'] = Codec::readShortstr($payload, $at);
            }
            // The low bit of a flag word says another word follows. Properties
            // in those later words are not read, but the words are drained so
            // the offset does not drift.
            while (($flags & 1) !== 0 && $at + 2 <= strlen($payload)) {
                $flags = unpack('n', substr($payload, $at, 2))[1];
                $at += 2;
            }
            if ($ch->pub['need'] === 0) {
                $this->finishPublish($id, $channel);
            }
            return;
        }
        if ($type === 3 && $ch !== null && $ch->pub !== null) {
            $ch->pub['got'] .= $payload;
            if (strlen($ch->pub['got']) >= $ch->pub['need']) {
                $ch->pub['got'] = substr($ch->pub['got'], 0, $ch->pub['need']);
                $this->finishPublish($id, $channel);
            }
            return;
        }
        if ($type !== 1 || strlen($payload) < 4) {
            return;
        }
        $class = unpack('n', substr($payload, 0, 2))[1];
        $method = unpack('n', substr($payload, 2, 2))[1];
        $o = 4;
        if ($ch?->metaPending) { $ch->metaFrames[] = [$type, $channel, $payload]; return; }
        if ($ch !== null && $broker->cluster?->raftEnabled()) {
            try { $stage = Metadata::amqp($broker, $id, $channel, $class, $method, $payload, $sock->user); }
            catch (RuntimeException $error) { $this->fail($id, $channel, $error, $class, $method); return; }
            if ($stage !== null) {
                $commands = $stage['commands'];
                $ch->metaPending = true;
                $respond = function (bool $ok) use ($id, $channel, $class, $method, $stage): void {
                    $client = $this->conns[$id] ?? null;
                    if ($client === null || $client->gone || !isset($client->channels[$channel])) return;
                    $client->send($ok ? $stage['response'] : Codec::channelClose($channel, 541, 'INTERNAL_ERROR - metadata commit failed', $class, $method));
                    $client->flush();
                    $state = $client->channels[$channel];
                    $state->metaPending = false;
                    if (!$ok) { $state->metaFrames = []; return; }
                    while (!$state->metaPending && $state->metaFrames !== []) {
                        [$frameType, $frameChannel, $framePayload] = array_shift($state->metaFrames);
                        $this->onFrame($id, $frameType, $frameChannel, $framePayload);
                    }
                };
                $submit = function (int $at) use (&$submit, $commands, $broker, $respond): void {
                    if ($at === count($commands)) { $respond(true); return; }
                    $command = $commands[$at];
                    $broker->cluster->proposeMeta($command['kind'], $command['data'], function (bool $ok, ?string $error) use ($at, &$submit, $respond): void {
                        if ($ok) $submit($at + 1); else $respond(false);
                    });
                };
                $submit(0);
                return;
            }
        }
        if ($class === 10 && $method === 11) {
            $this->startOk($id, $payload, $o);
        } elseif ($class === 10 && $method === 31) {
            return;
        } elseif ($class === 10 && $method === 40) {
            // connection.open names the vhost. Access is checked here, so a
            // user with no permission on it is refused rather than silently
            // landing on the default namespace.
            $vhost = Codec::readShortstr($payload, $o);
            $vhost = $vhost === '' ? '/' : $vhost;
            if (!$broker->hasVhostAccess($sock->user ?? '', $vhost)) {
                $sock->send(Codec::connectionClose(403, "ACCESS_REFUSED - vhost '$vhost'", 10, 40));
                $sock->flush();
                $this->drop($id);
                return;
            }
            if (!$broker->connectionAllowed($sock->user ?? '', $vhost, count(array_filter($this->conns, static fn($c) => $c->user === $sock->user && !$c->gone)))) {
                $sock->send(Codec::connectionClose(403, 'ACCESS_REFUSED - connection limit', 10, 40));
                $sock->flush();
                $this->drop($id);
                return;
            }
            $sock->vhost = $vhost;
            $sock->send(Codec::connectionOpenOk());
        } elseif ($class === 10 && $method === 50) {
            $sock->send(Codec::connectionCloseOk());
            $sock->flush();
            $this->drop($id);
        } elseif ($class === 20 && $method === 10) {
            if (!$broker->channelAllowed($sock->user ?? '', array_sum(array_map(static fn($c) => $c->user === $sock->user ? count($c->channels) : 0, $this->conns)))) {
                $sock->send(Codec::connectionClose(403, 'ACCESS_REFUSED - channel limit', 20, 10));
                $sock->flush();
                $this->drop($id);
                return;
            }
            $sock->channels[$channel] = new Chan();
            $broker->prom['channels']++;
            $broker->prom['channelsOpened']++;
            $sock->send(Codec::channelOpenOk($channel));
        } elseif ($class === 20 && $method === 40) {
            if (isset($sock->channels[$channel])) {
                $broker->prom['channels']--;
                $broker->prom['channelsClosed']++;
                $this->releaseChannel($id, $channel);
            }
            unset($sock->channels[$channel]);
            $sock->send(Codec::channelCloseOk($channel));
        } elseif ($class === 50 && $method === 10 && $ch !== null) {
            $this->declare($id, $channel, $payload, $o);
        } elseif ($class === 85 && $method === 10 && $ch !== null) {
            $ch->confirm = true;
            $nowait = isset($payload[$o]) ? (ord($payload[$o]) & 1) : 0;
            if ($nowait === 0) {
                $sock->send(Codec::confirmSelectOk($channel));
            }
        } elseif ($class === 60 && $method === 10 && $ch !== null) {
            $ch->prefetch = unpack('n', substr($payload, $o + 4, 2))[1];
            $sock->send(Codec::qosOk($channel));
        } elseif ($class === 60 && $method === 20 && $ch !== null) {
            $this->consume($id, $channel, $payload, $o);
        } elseif ($class === 60 && $method === 40 && $ch !== null) {
            if (!$this->beginPublish($ch, $payload, $o)) {
                $sock->send(Codec::channelClose($channel, 540, 'NOT_IMPLEMENTED - immediate=true', 60, 40));
            }
        } elseif ($class === 60 && $method === 80 && $ch !== null) {
            $this->ack($id, $channel, $payload, $o, false);
        } elseif ($class === 60 && $method === 120 && $ch !== null) {
            $this->ack($id, $channel, $payload, $o, true);
        } elseif ($class === 40 && $method === 10 && $ch !== null) {
            $this->exchangeDeclare($id, $channel, $payload, $o);
        } elseif ($class === 50 && $method === 20 && $ch !== null) {
            $this->queueBind($id, $channel, $payload, $o);
        } elseif ($class === 20 && $method === 20 && $ch !== null) {
            // channel.flow: this broker never stops a publisher, so it only
            // echoes the requested state back.
            $active = isset($payload[$o]) && (ord($payload[$o]) & 1) === 1;
            $sock->send(Codec::flowOk($channel, $active));
        } elseif ($class === 20 && $method === 41) {
            return;
        } elseif ($class === 60 && $method === 30 && $ch !== null) {
            $this->cancel($id, $channel, $payload, $o);
        } elseif ($class === 60 && $method === 70 && $ch !== null) {
            $this->get($id, $channel, $payload, $o);
        } elseif ($class === 60 && $method === 90 && $ch !== null) {
            $this->reject($id, $channel, $payload, $o);
        } elseif ($class === 60 && ($method === 100 || $method === 110) && $ch !== null) {
            $this->recover($id, $channel, $payload, $o, $method);
        } elseif ($class === 50 && $method === 30 && $ch !== null) {
            $this->queuePurge($id, $channel, $payload, $o);
        } elseif ($class === 50 && $method === 40 && $ch !== null) {
            $this->queueDelete($id, $channel, $payload, $o);
        } elseif ($class === 50 && $method === 50 && $ch !== null) {
            $this->queueUnbind($id, $channel, $payload, $o);
        } elseif ($class === 40 && $method === 20 && $ch !== null) {
            $this->exchangeDelete($id, $channel, $payload, $o);
        } elseif ($class === 40 && ($method === 30 || $method === 40) && $ch !== null) {
            $this->exchangeBind($id, $channel, $payload, $o, $method === 30);
        } elseif ($class === 90 && $ch !== null && ($method === 10 || $method === 20 || $method === 30)) {
            $this->transaction($id, $channel, $method);
        } else {
            if ($ch === null && $class !== 10 && $class !== 20) {
                // A channel-scoped method on a channel that was never opened
                // is a connection error in RabbitMQ, not a channel error.
                $sock->send(Codec::connectionClose(504, "CHANNEL_ERROR - channel $channel is not open", $class, $method));
                $sock->flush();
                $this->drop($id);
                return;
            }
            // RabbitMQ answers an unsupported method with a channel error.
            // Falling through silently left the client waiting forever.
            $sock->send(Codec::channelClose($channel, 540, "NOT_IMPLEMENTED - $class.$method", $class, $method));
        }
    }

    private function cancel(int $id, int $channel, string $payload, int $o): void
    {
        $broker = $this->brokerFor($id);
        $tag = Codec::readShortstr($payload, $o);
        $nowait = isset($payload[$o]) && (ord($payload[$o]) & 1) === 1;
        $ch = $this->conns[$id]->channels[$channel];
        // A consumer served by another node is unsubscribed there.
        if (isset($ch->remote[$tag]) && $this->extras !== null) {
            $this->extras->cluster->request($ch->remote[$tag]['peer'], 'unsub', [
                'vhost' => $broker->vhost,
                'session' => $ch->remote[$tag]['session'],
            ]);
            unset($ch->remote[$tag]);
        }
        $touched = [];
        foreach ($broker->queues as $name => $queue) {
            $before = count($queue['consumers']);
            $broker->queues[$name]['consumers'] = array_values(array_filter(
                $queue['consumers'],
                static fn (array $c): bool => !($c['conn'] === $id && $c['ch'] === $channel && $c['tag'] === $tag),
            ));
            if (count($broker->queues[$name]['consumers']) !== $before) {
                $touched[] = $name;
            }
        }
        if ($ch->consumer === $tag) {
            $ch->consumer = null;
            $ch->queue = null;
        }
        if (!$nowait) {
            $this->conns[$id]->send(Codec::cancelOk($channel, $tag));
        }
        foreach ($touched as $name) {
            $this->autoDelete((string) $name, $broker);
        }
    }

    private function get(int $id, int $channel, string $payload, int $o): void
    {
        $broker = $this->brokerFor($id);
        $o += 2;
        $queue = Codec::readShortstr($payload, $o);
        if (!$this->allowed($id, $channel, "read", $queue, 60, 70)) return;
        if (($broker->queues[$queue]["args"]["queueType"] ?? "") === "stream") { $this->conns[$id]->send(Codec::channelClose($channel,540,"NOT_IMPLEMENTED - basic.get on stream queue",60,70));return; }
        $noAck = isset($payload[$o]) && (ord($payload[$o]) & 1) === 1;
        if ($this->locked($id, $channel, $queue, 60, 70)) {
            return;
        }
        $sock = $this->conns[$id];
        $ch = $sock->channels[$channel];
        // A classic queue homed elsewhere is asked over the cluster link. The
        // reply comes back through the select loop, so the client's answer is
        // written from the callback rather than blocking here.
        $home = $this->remoteHome($queue, $broker);
        if ($home !== null && $this->extras !== null) {
            $this->extras->cluster->request(
                $home,
                'get',
                ['vhost' => $broker->vhost, 'queue' => $queue, 'noAck' => $noAck],
                '',
                function (?array $reply) use ($id, $channel, $queue, $noAck, $broker): void {
                    $sock = $this->conns[$id] ?? null;
                    $ch = $sock?->channels[$channel] ?? null;
                    if ($sock === null || $ch === null) {
                        return;
                    }
                    $msg = is_array($reply['msg'] ?? null) ? $reply['msg'] : null;
                    if ($reply === null || $msg === null) {
                        // A timeout and an empty queue are both reported as
                        // empty; the alternative is leaving the client hanging.
                        $broker->prom['getEmpty']++;
                        $sock->send(Codec::getEmpty($channel));
                        $sock->flush();
                        return;
                    }
                    $body = base64_decode((string) ($msg['body_b64'] ?? $msg['body'] ?? ''), true);
                    $dtag = $ch->nextDel++;
                    $sock->send(Codec::getOk(
                        $channel,
                        $dtag,
                        false,
                        (string) ($msg['exchange'] ?? ''),
                        (string) ($msg['routing_key'] ?? $msg['routingKey'] ?? $queue),
                        0,
                        ($msg['durable'] ?? false) ? 2 : 1,
                        $body === false ? '' : $body,
                        null,
                    ));
                    $sock->flush();
                    $broker->prom['delivered']++;
                    $broker->prom[$noAck ? 'deliveredGetAuto' : 'deliveredGetManual']++;
                },
            );
            return;
        }
        // basic.get counts as use for x-expires, as on RabbitMQ.
        if (isset($broker->queues[$queue])) {
            $broker->queues[$queue]['lastUsed'] = microtime(true);
        }
        $msgId = $broker->getReady($queue);
        if ($msgId === null) {
            $broker->prom['getEmpty']++;
            $sock->send(Codec::getEmpty($channel));
            return;
        }
        $msg = $broker->msgs[$msgId];
        $dtag = $ch->nextDel++;
        if (!$noAck) {
            $ch->unacked[$dtag] = $msgId;
        }
        $sock->send(Codec::getOk(
            $channel,
            $dtag,
            $msg['redelivered'] ?? false,
            $msg['exchange'] ?? '',
            $msg['key'] ?? $queue,
            $broker->readyCount($queue),
            $msg['mode'],
            $msg['body'],
            $msg['propRaw'] ?? null,
            is_array($msg['headers'] ?? null) ? $msg['headers'] : [],
        ));
        $broker->prom['delivered']++;
        $broker->prom[$noAck ? 'deliveredGetAuto' : 'deliveredGetManual']++;
        if (($msg['redelivered'] ?? false) === true) {
            $broker->prom['redelivered']++;
        }
        if ($noAck) {
            $broker->drop($msgId);
        }
    }

    private function reject(int $id, int $channel, string $payload, int $o): void
    {
        $broker = $this->brokerFor($id);
        if (strlen($payload) < $o + 9) {
            return;
        }
        $tag = Codec::readU64($payload, $o);
        $requeue = (ord($payload[$o + 8]) & 1) === 1;
        $ch = $this->conns[$id]->channels[$channel];
        if (!isset($ch->unacked[$tag])) {
            return;
        }
        $msgId = $ch->settle($tag);
        if ($requeue) {
            $broker->requeue($msgId);
        } else {
            $broker->deadLetter($msgId);
        }
        $this->pump();
    }

    private function recover(int $id, int $channel, string $payload, int $o, int $method): void
    {
        $broker = $this->brokerFor($id);
        $requeue = isset($payload[$o]) && (ord($payload[$o]) & 1) === 1;
        $sock = $this->conns[$id];
        if (!$requeue) {
            // Matching Bun, which rejects recover without requeue rather than
            // dropping the unacked messages on the floor.
            $sock->send(Codec::channelClose($channel, 540, 'NOT_IMPLEMENTED - recover requeue=false', 60, $method));
            return;
        }
        $ch = $sock->channels[$channel];
        foreach (array_reverse(array_keys($ch->unacked)) as $tag) {
            $msgId = $ch->settle($tag);
            if ($msgId !== null) {
                $broker->requeue($msgId);
            }
        }
        if ($method === 110) {
            $sock->send(Codec::recoverOk($channel));
        }
        $this->pump();
    }

    private function queuePurge(int $id, int $channel, string $payload, int $o): void
    {
        $broker = $this->brokerFor($id);
        $o += 2;
        $queue = Codec::readShortstr($payload, $o);
        if (!$this->allowed($id, $channel, "read", $queue, 50, 30)) return;
        $nowait = isset($payload[$o]) && (ord($payload[$o]) & 1) === 1;
        if ($this->locked($id, $channel, $queue, 50, 30)) {
            return;
        }
        // Purging a queue homed elsewhere has to reach the node holding it.
        $home = $this->remoteHome($queue, $broker);
        if ($home !== null && $this->extras !== null) {
            $this->extras->cluster->request($home, 'purge', ['vhost' => $broker->vhost, 'queue' => $queue]);
        }
        $n = $broker->purge($queue);
        if (!$nowait) {
            $this->conns[$id]->send(Codec::purgeOk($channel, $n));
        }
    }

    private function queueDelete(int $id, int $channel, string $payload, int $o): void
    {
        $broker = $this->brokerFor($id);
        $o += 2;
        $queue = Codec::readShortstr($payload, $o);
        if (!$this->allowed($id, $channel, "configure", $queue, 50, 40)) return;
        $bits = isset($payload[$o]) ? ord($payload[$o]) : 0;
        $ifUnused = ($bits & 1) === 1;
        $ifEmpty = ($bits & 2) === 2;
        $nowait = ($bits & 4) === 4;
        if ($this->locked($id, $channel, $queue, 50, 40)) {
            return;
        }
        $q = $broker->queues[$queue] ?? null;
        if ($q !== null && $ifUnused && $q['consumers'] !== []) {
            $this->conns[$id]->send(Codec::channelClose($channel, 406, "PRECONDITION_FAILED - queue '$queue' in vhost '/' in use", 50, 40));
            return;
        }
        if ($q !== null && $ifEmpty && $broker->readyCount($queue) > 0) {
            $this->conns[$id]->send(Codec::channelClose($channel, 406, "PRECONDITION_FAILED - queue '$queue' in vhost '/' not empty", 50, 40));
            return;
        }
        $home = $this->remoteHome($queue, $broker);
        if ($home !== null && $this->extras !== null) {
            $this->extras->cluster->request($home, 'delete_queue', ['vhost' => $broker->vhost, 'queue' => $queue]);
        }
        // Consumers of a deleted queue are told with basic.cancel, as RabbitMQ
        // does for clients that advertise consumer_cancel_notify.
        foreach ($q['consumers'] ?? [] as $c) {
            $sock = $this->conns[$c['conn']] ?? null;
            if ($sock === null) {
                continue;
            }
            $sock->send(Codec::method($c['ch'], 60, 30, Codec::shortstr($c['tag']) . chr(0)));
            $peer = $sock->channels[$c['ch']] ?? null;
            if ($peer !== null && $peer->consumer === $c['tag']) {
                $peer->consumer = null;
                $peer->queue = null;
            }
        }
        $n = $broker->deleteQueue($queue);
        if (!$nowait) {
            $this->conns[$id]->send(Codec::queueDeleteOk($channel, $n));
        }
    }

    private function queueUnbind(int $id, int $channel, string $payload, int $o): void
    {
        $broker = $this->brokerFor($id);
        $o += 2;
        $queue = Codec::readShortstr($payload, $o);
        $exchange = Codec::readShortstr($payload, $o);
        $key = Codec::readShortstr($payload, $o);
        if (!$this->allowed($id, $channel, "write", $queue, 50, 20) || !$this->allowed($id, $channel, "read", $exchange, 50, 20)) return;
        $broker->unbind($queue, $exchange, $key);
        $this->conns[$id]->send(Codec::unbindOk($channel));
    }

    private function exchangeDelete(int $id, int $channel, string $payload, int $o): void
    {
        $broker = $this->brokerFor($id);
        $o += 2;
        $name = Codec::readShortstr($payload, $o);
        if (!$this->allowed($id, $channel, "configure", $name, 40, 20)) return;
        $bits = isset($payload[$o]) ? ord($payload[$o]) : 0;
        $nowait = ($bits & 2) === 2;
        $broker->deleteExchange($name);
        if (!$nowait) {
            $this->conns[$id]->send(Codec::method($channel, 40, 21));
        }
    }

    private function exchangeBind(int $id, int $channel, string $payload, int $o, bool $bind): void
    {
        $broker = $this->brokerFor($id);
        $o += 2;
        $destination = Codec::readShortstr($payload, $o);
        $source = Codec::readShortstr($payload, $o);
        $key = Codec::readShortstr($payload, $o);
        if ($bind) {
            $broker->bindExchange($destination, $source, $key);
        } else {
            $broker->unbindExchange($destination, $source, $key);
        }
        $this->conns[$id]->send(Codec::method($channel, 40, $bind ? 31 : 51));
    }

    /**
     * tx.select, tx.commit and tx.rollback. In a transaction, publishes and
     * acks are held on the channel. Commit applies them in order; rollback
     * drops them, so the publishes never happen and the acked messages stay
     * unacked, as on RabbitMQ.
     */
    private function transaction(int $id, int $channel, int $method): void
    {
        $broker = $this->brokerFor($id);
        $sock = $this->conns[$id];
        $ch = $sock->channels[$channel];
        if ($method === 10) {
            $ch->tx = true;
        } elseif ($method === 20) {
            if (!$ch->tx) {
                $sock->send(Codec::channelClose($channel, 406, 'PRECONDITION_FAILED - channel is not transactional', 90, 20));
                return;
            }
            $this->replaying = true;
            try {
                foreach ($ch->txPub as $pub) {
                    $ch->pub = $pub;
                    $this->finishPublish($id, $channel);
                }
                foreach ($ch->txAcks as [$payload, $o, $negative]) {
                    $this->ack($id, $channel, $payload, $o, $negative);
                }
            } finally {
                $this->replaying = false;
            }
            $ch->txPub = [];
            $ch->txAcks = [];
        } elseif ($method === 30) {
            if (!$ch->tx) {
                $sock->send(Codec::channelClose($channel, 406, 'PRECONDITION_FAILED - channel is not transactional', 90, 30));
                return;
            }
            $ch->txPub = [];
            $ch->txAcks = [];
        }
        $sock->send(Codec::method($channel, 90, $method + 1));
    }

    private function brokerFor(int $id): Broker { return $this->broker->forVhost($this->conns[$id]->vhost ?? '/'); }
    private function allowed(int $id, int $channel, string $operation, string $name, int $class, int $method): bool
    {
        $sock = $this->conns[$id];
        if ($this->broker->resourceAllowed($sock->user, $sock->vhost, $operation, $name)) return true;
        $sock->send(Codec::channelClose($channel, 403, "ACCESS_REFUSED - $operation access to '$name'", $class, $method));
        $this->releaseChannel($id, $channel);
        return false;
    }

    private function startOk(int $id, string $payload, int $o): void
    {
        $broker = $this->brokerFor($id);
        $table = unpack('N', substr($payload, $o, 4))[1];
        $o += 4 + $table;
        $mech = Codec::readShortstr($payload, $o);
        $resp = Codec::readLongstr($payload, $o);
        if ($mech === 'EXTERNAL') {
            $user = $this->conns[$id]->peerCN;
            // The authorization identity follows the trusted common name; no password fallback.
            if ($user === null || !array_key_exists($user, $broker->users)) {
                $this->conns[$id]->send(Codec::connectionClose(403, 'ACCESS_REFUSED - EXTERNAL login refused', 10, 11));
                $this->conns[$id]->flush();
                $this->drop($id);
                return;
            }
        } elseif ($mech === 'PLAIN') {
            $parts = explode("\0", $resp);
            $user = count($parts) >= 3 ? $parts[1] : ($parts[0] ?? '');
            $pass = count($parts) >= 3 ? $parts[2] : ($parts[1] ?? '');
            $user = $broker->authenticate($user, $pass);
            if ($user === null) { $this->drop($id); return; }
        } else {
            $this->conns[$id]->send(Codec::connectionClose(403, 'ACCESS_REFUSED - mechanism', 10, 11));
            $this->conns[$id]->flush();
            $this->drop($id);
            return;
        }
        $this->conns[$id]->user = $user;
        // Register the user with broker for topic permissions.
        $broker->userByConn[$id] = $user;
        $this->conns[$id]->send(Codec::connectionTune());
    }

    private function declare(int $id, int $channel, string $payload, int $o): void
    {
        $broker = $this->brokerFor($id);
        $o += 2;
        $name = Codec::readShortstr($payload, $o);
        if (!$this->allowed($id, $channel, "configure", $name, 50, 10)) return;
        $bits = isset($payload[$o]) ? ord($payload[$o]) : 0;
        $passive = ($bits & 1) === 1;
        $durable = ($bits & 2) === 2;
        $exclusive = ($bits & 4) === 4;
        $autoDelete = ($bits & 8) === 8;
        if ($name === '') {
            // A server-generated name has to be unique; the old fixed
            // 'amq.gen' made every anonymous queue collide.
            $name = 'amq.gen-' . bin2hex(random_bytes(8));
        }
        $args = [];
        if (isset($payload[$o])) {
            $o++;
            $args = Codec::readTable($payload, $o);
        }
        if ($this->locked($id, $channel, $name, 50, 10)) {
            return;
        }
        $locator = $args['x-queue-leader-locator'] ?? null;
        if ($locator !== null && $locator !== 'client-local' && $locator !== 'balanced') {
            $this->conns[$id]->send(Codec::channelClose($channel, 406, "PRECONDITION_FAILED - invalid arg 'x-queue-leader-locator' for queue '$name' in vhost '/': \"$locator\" is not one of [client-local, balanced]", 50, 10));
            return;
        }
        if (!$passive && !isset($broker->queues[$name])
            && !$broker->queueAllowed($this->conns[$id]->vhost)) {
            $this->conns[$id]->send(Codec::channelClose($channel, 403, 'ACCESS_REFUSED - queue limit', 50, 10));
            return;
        }
        $isNew = !isset($broker->queues[$name]);
        try {
            $state = $broker->declareQueue($name, $args, $durable, $exclusive, $passive, $autoDelete);
        } catch (RuntimeException $err) {
            $this->fail($id, $channel, $err, 50, 10);
            return;
        }
        if ($exclusive && !isset($broker->queues[$name]['owner'])) {
            $broker->queues[$name]['owner'] = $id;
        }
        if (!$passive) {
            try{$this->replicateQueue($broker,$name,$args);}catch(RuntimeException $error){$this->fail($id,$channel,$error,50,10);return;}
        }
        $this->conns[$id]->send(Codec::queueDeclareOk($channel, $name, $state['messages'], $state['consumers']));
    }

    /** Creates the queue on every peer before the client is told the declare finished. */
    private function replicateQueue(Broker $broker,string $name,array $args): void
    {
        if ($this->extras === null || count($this->broker->members) < 2) {
            return;
        }
        $pending=0;$done=0;$failed=false;
        foreach($this->extras->cluster->peerIds()as$peer){
            $id = $this->extras->cluster->request($peer, 'declare_queue', [
                'vhost'=>$broker->vhost,'queue'=>$name,'args'=>$args,
            ], '', static function ($reply) use (&$done,&$failed): void {
                $done++;if($reply===null)$failed=true;
            });
            if($id!==0)$pending++;else $failed=true;
        }
        $deadline = microtime(true) + 0.5;
        while ($done < $pending && microtime(true) < $deadline) {
            $read = $this->extras->reads();
            $write = $this->extras->writes();
            $except = [];
            if ($read === [] || @stream_select($read, $write, $except, 0, 20000) === false) {
                break;
            }
            foreach ($write as $fp) $this->extras->flush($fp);
            foreach ($read as $fp) {
                if ($this->extras->owns($fp)) {
                    $this->extras->onRead($fp);
                }
            }
        }
        if($failed||$done!==$pending)throw new RuntimeException('INTERNAL_ERROR - metadata replication failed',541);
    }

    /**
     * Answers a broker error. A 5xx reply code closes the connection, as
     * RabbitMQ does for the transient-queue deprecation; anything else closes
     * just the channel.
     */
    private function fail(int $id, int $channel, RuntimeException $err, int $class, int $method): void
    {
        $broker = $this->brokerFor($id);
        $code = $err->getCode();
        $code = $code >= 300 && $code < 600 ? $code : 406;
        $sock = $this->conns[$id] ?? null;
        if ($sock === null) {
            return;
        }
        if ($code >= 500) {
            $sock->send(Codec::connectionClose($code, $err->getMessage(), $class, $method));
            $sock->flush();
            $this->drop($id);
            return;
        }
        $sock->send(Codec::channelClose($channel, $code, $err->getMessage(), $class, $method));
    }

    private function consume(int $id, int $channel, string $payload, int $o): void
    {
        $broker = $this->brokerFor($id);
        $o += 2;
        $queue = Codec::readShortstr($payload, $o);
        if (!$this->allowed($id, $channel, "read", $queue, 60, 20)) return;
        $tag = Codec::readShortstr($payload, $o);
        $bits = isset($payload[$o]) ? ord($payload[$o]) : 0;
        $noAck = ($bits & 2) === 2;
        $exclusive = ($bits & 4) === 4;
        $nowait = ($bits & 8) === 8;
        if ($tag === '') {
            // Unique per consumer, as RabbitMQ's amq.ctag-...: two consumers on
            // one channel must not share a tag.
            $tag = 'amq.ctag-' . bin2hex(random_bytes(8));
        }
        $priority = 0;
        $args = [];
        if (isset($payload[$o])) {
            $o++;
            $args = Codec::readTable($payload, $o);
            $priority = (int) ($args['x-priority'] ?? 0);
        }
        if ($queue === 'amq.rabbitmq.reply-to') {
            $ch = $this->conns[$id]->channels[$channel];
            if (!$noAck || $ch->replyAddress !== null) {
                $this->conns[$id]->send(Codec::channelClose($channel, 406, 'PRECONDITION_FAILED - reply-to requires no-ack and one consumer', 60, 20));
                return;
            }
            $ch->replyAddress = 'amq.rabbitmq.reply-to.' . bin2hex(random_bytes(16));
            $ch->replyTag = $tag;
            if (!$nowait) $this->conns[$id]->send(Codec::consumeOk($channel, $tag));
            return;
        }
        if ($this->locked($id, $channel, $queue, 60, 20)) {
            return;
        }
        $ch = $this->conns[$id]->channels[$channel];
        // A classic queue homed elsewhere is subscribed to over the cluster
        // link; the home node then pushes deliver frames back.
        $home = $this->remoteHome($queue, $broker);
        if ($home !== null && $this->extras !== null) {
            $session = $broker->nextSession();
            $this->extras->cluster->request($home, 'sub', [
                'vhost' => $broker->vhost,
                'queue' => $queue,
                'session' => $session,
                'noAck' => $noAck,
                'credit' => 0,
            ]);
            $ch->consumer = $tag;
            $ch->queue = $queue;
            $ch->noAck[$tag] = $noAck;
            $ch->remote[$tag] = ['peer' => $home, 'session' => $session];
            if (!$nowait) {
                $this->conns[$id]->send(Codec::consumeOk($channel, $tag));
            }
            return;
        }
        try {
            $stream = ($broker->queues[$queue]['args']['queueType'] ?? '') === 'stream';
            if ($stream && ($ch->prefetch === 0 || $noAck)) throw new RuntimeException('PRECONDITION_FAILED - stream consume requires prefetch and manual acknowledgements',406);
            $broker->addConsumer($queue, $id, $channel, $tag, $noAck, $exclusive, $priority);
            $consumer = array_key_last($broker->queues[$queue]['consumers']);
            $broker->queues[$queue]['consumers'][$consumer]['readyFn'] = function()use($id,$channel,$tag):bool { $sock=$this->conns[$id]??null;$ch=$sock?->channels[$channel]??null;return $ch!==null&&!$sock->gone&&($ch->prefetch===0||($ch->held[$tag]??0)<$ch->prefetch); };
            $broker->queues[$queue]['consumers'][$consumer]['deliverFn'] = function(array $msg,int $msgId)use($id,$channel,$tag,$noAck,$broker):void { $this->deliverMessage($broker,$id,$channel,$tag,$noAck,$msg,$msgId); };
            if($stream){$offset=$args['x-stream-offset']??'next';$cursor=is_int($offset)?max($broker->streamFirst($queue),$offset):match($offset){'first'=>$broker->streamFirst($queue),'last'=>max($broker->streamFirst($queue),$broker->streamNext($queue)-1),default=>$broker->streamNext($queue)};$broker->queues[$queue]['consumers'][$consumer]['streamOffset']=$cursor;}

        } catch (RuntimeException $err) {
            $this->fail($id, $channel, $err, 60, 20);
            return;
        }
        $ch->consumer = $tag;
        $ch->queue = $queue;
        $ch->noAck[$tag] = $noAck;
        $broker->queues[$queue]['hadConsumer'] = true;
        if (!$nowait) {
            $this->conns[$id]->send(Codec::consumeOk($channel, $tag));
        }
        $this->pump();
    }

    /**
     * Starts a publish. Returns false when the client asked for
     * immediate delivery, which this broker rejects the way Bun does.
     */
    private function beginPublish(Chan $ch, string $payload, int $o): bool
    {
        $o += 2;
        $exchange = Codec::readShortstr($payload, $o);
        $key = Codec::readShortstr($payload, $o);
        $bits = isset($payload[$o]) ? ord($payload[$o]) : 0;
        if (($bits & 2) === 2) {
            return false;
        }
        $ch->pub = [
            'exchange' => $exchange,
            'key' => $key,
            'mandatory' => ($bits & 1) === 1,
            'mode' => 1,
            'priority' => 0,
            'headers' => [],
            'expiration' => null,
            'propRaw' => null,
            'need' => 0,
            'got' => '',
        ];
        return true;
    }

    private function finishPublish(int $id, int $channel): void
    {
        $broker = $this->brokerFor($id);
        $ch = $this->conns[$id]->channels[$channel];
        if ($ch->pub === null) {
            return;
        }
        $pub = $ch->pub;
        $ch->pub = null;
        $sock = $this->conns[$id];
        // RabbitMQ refuses a user-id property that is not the login.
        $identity = $broker->identityName($sock->user ?? '');
        if (isset($pub['userId']) && $pub['userId'] !== $identity) {
            $sock->send(Codec::channelClose($channel, 406, "PRECONDITION_FAILED - user_id property set to '{$pub['userId']}' but authenticated user was '$identity'", 60, 40));
            return;
        }
        if ($ch->tx && !$this->replaying) {
            $ch->txPub[] = $pub;
            return;
        }
        if (!$this->allowed($id, $channel, "write", $pub["exchange"], 60, 40)) return;
        $tag = $ch->confirm ? $ch->nextPub++ : 0;
        
        // Enforce topic write permissions for non-empty exchanges.
        $user = $broker->userByConn[$id] ?? $sock->user;
        if ($pub['exchange'] !== '') {
            if (!$broker->topicWriteAllowed($user, $sock->vhost, $pub['exchange'], $pub['key'])) {
                $sock->send(Codec::channelClose($channel, 403, 'ACCESS_REFUSED - write access to topic refused', 60, 40));
                return;
            }
        }
        
        if (($pub['propRaw'] ?? null) !== null) {
            $props = Amqp10::readProps($pub['propRaw']);
            if (($props['replyTo'] ?? null) === 'amq.rabbitmq.reply-to' && $ch->replyAddress !== null) {
                $props['replyTo'] = $ch->replyAddress;
                $pub['propRaw'] = Amqp10::writeProps($props, array_map(static fn($key, $value) => [$key, $value], array_keys($props['headers']), array_values($props['headers'])));
            }
        }
        if ($pub['exchange'] === '' && str_starts_with($pub['key'], 'amq.rabbitmq.reply-to.')) {
            foreach ($this->conns as $recipient) if (!$recipient->gone && $recipient->vhost === $sock->vhost) foreach ($recipient->channels as $target => $reply) {
                if ($reply->replyAddress !== $pub['key']) continue;
                $recipient->send(Codec::deliver($target, $reply->replyTag, $reply->nextDel++, $pub['key'], $pub['mode'], $pub['got'], false, '', $pub['propRaw']));
                $recipient->flush();
            }
            if ($tag > 0) $sock->send(Codec::basicAck($channel, $tag));
            return;
        }
        try {
            $result = $broker->publish(
                $id,
                $channel,
                $tag,
                $pub['exchange'],
                $pub['key'],
                $pub['got'],
                $pub['mode'],
                $pub['priority'] ?? 0,
                $pub['headers'] ?? [],
                $pub['expiration'] ?? null,
                $pub['propRaw'] ?? null,
            );
        } catch (RuntimeException $err) {
            // A missing queue behind the default exchange, or an internal
            // exchange, is a channel error rather than a silent drop.
            $this->fail($id, $channel, $err, 60, 40);
            return;
        }
        if ($result === 'return') {
            if ($pub['mandatory']) {
                $broker->prom['unroutableReturned']++;
                $ret = Codec::method($channel, 60, 50, pack('n', 312) . Codec::shortstr('NO_ROUTE') . Codec::shortstr($pub['exchange']) . Codec::shortstr($pub['key']));
                $header = Codec::contentHeader(strlen($pub['got']), $pub['mode'], $pub['propRaw'] ?? null);
                $this->conns[$id]->send($ret . Codec::frame(2, $channel, $header) . Codec::frame(3, $channel, $pub['got']));
            } else {
                $broker->prom['unroutableDropped']++;
            }
        }
        if ($result === 'return' && $tag > 0) {
            $this->conns[$id]->send(Codec::basicAck($channel, $tag));
        }
        if ($result === 'nack' && $tag > 0) {
            $this->conns[$id]->send(Codec::basicNack($channel, $tag));
        }
        $this->pump();
    }

    private function ack(int $id, int $channel, string $payload, int $o, bool $negative = false): void
    {
        $broker = $this->brokerFor($id);
        if (strlen($payload) < $o + 9) {
            return;
        }
        $tag = Codec::readU64($payload, $o);
        $bits = ord($payload[$o + 8]);
        $multiple = ($bits & 1) === 1;
        $requeue = $negative && ($bits & 2) === 2;
        $ch = $this->conns[$id]->channels[$channel];
        if ($ch->tx && !$this->replaying) {
            $ch->txAcks[] = [$payload, $o, $negative];
            return;
        }
        foreach (array_keys($ch->unacked) as $have) {
            if ($multiple ? $have <= $tag : $have === $tag) {
                $msg = $ch->settle($have);
                if ($msg === null) {
                    continue;
                }
                if ($negative && $requeue) {
                    $broker->requeue($msg);
                } elseif ($negative) {
                    $broker->prom['dlxRejected']++;
                    $broker->deadLetter($msg, 'rejected');
                } else {
                    $broker->ack($msg);
                }
            }
        }
        $this->pump();
    }

    private function exchangeDeclare(int $id, int $channel, string $payload, int $o): void
    {
        $broker = $this->brokerFor($id);
        $o += 2;
        $name = Codec::readShortstr($payload, $o);
        if (!$this->allowed($id, $channel, "configure", $name, 40, 10)) return;
        $kind = Codec::readShortstr($payload, $o);
        $bits = isset($payload[$o]) ? ord($payload[$o]) : 0;
        $passive = ($bits & 1) === 1;
        $durable = ($bits & 2) === 2;
        $autoDelete = ($bits & 4) === 4;
        $internal = ($bits & 8) === 8;
        $alternate = null;
        if (isset($payload[$o])) {
            $o++;
            $args = Codec::readTable($payload, $o);
            $value = $args['alternate-exchange'] ?? null;
            $alternate = is_scalar($value) ? (string) $value : null;
        }
        try {
            $broker->declareExchange($name, $kind, $durable, $autoDelete, $internal, $alternate, $passive, $args ?? []);
        } catch (RuntimeException $err) {
            $this->fail($id, $channel, $err, 40, 10);
            return;
        }
        $this->conns[$id]->send(Codec::method($channel, 40, 11));
    }

    private function queueBind(int $id, int $channel, string $payload, int $o): void
    {
        $broker = $this->brokerFor($id);
        $o += 2;
        $queue = Codec::readShortstr($payload, $o);
        $exchange = Codec::readShortstr($payload, $o);
        $key = Codec::readShortstr($payload, $o);
        if (!$this->allowed($id, $channel, "write", $queue, 50, 20) || !$this->allowed($id, $channel, "read", $exchange, 50, 20)) return;
        $args = [];
        if (isset($payload[$o])) {
            $o++;
            foreach (Codec::readTable($payload, $o) as $name => $value) {
                $args[] = [(string) $name, $value];
            }
        }
        try {
            $sock = $this->conns[$id];
            $broker->bind($queue, $exchange, $key, $args, $sock->user, $sock->vhost);
        } catch (RuntimeException $err) {
            $this->fail($id, $channel, $err, 50, 20);
            return;
        }
        $this->conns[$id]->send(Codec::method($channel, 50, 21));
    }

    /**
     * Releases confirms whose records are on disk.
     *
     * Waiting out the fsync interval only pays off when enough publishes are
     * in flight to batch. Below FLUSH_SMALL the publishers are effectively
     * blocked on their confirms, so the interval is pure added latency: at
     * one confirm in flight it caps the achievable rate at one message per
     * interval, which measured 77 messages a second against a 10 ms tick.
     * Flushing small batches at once lifts that to the paced offer. Bun gets
     * there with its lone-flush path (bun/src/store.ts:69-82).
     *
     * The durability rule is unchanged: a confirm still goes out only after
     * the fsync that covers its append, which test/roundtrip.php asserts by
     * SIGKILLing the broker.
     */
    private function commit(bool $idle = false): void
    {
        // A quorum publish whose replication timed out is rolled back before
        // the confirms are considered, so it leaves as a nack.
        if ($this->extras !== null && $this->extras->cluster->timedOut !== []) {
            $this->extras->cluster->timedOut = [];
        }
        foreach ($this->broker->allBrokers() as $broker) {
        if ($broker->waiting === []) {
            continue;
        }
        $small = count($broker->waiting) <= self::FLUSH_SMALL;
        if (!$idle && !$small && microtime(true) < $this->nextSync) {
            continue;
        }
        $dirty = [];
        foreach ($broker->flush() as $ack) {
            if ($ack['tag'] <= 0 || !isset($this->conns[$ack['conn']])) {
                continue;
            }
            $sock = $this->conns[$ack['conn']];
            if ($sock->stage === 'amqp10') {
                $sock->send($this->amqp10->confirmed($sock->amqp10, $ack['tag'], ($ack['nack'] ?? false) === true));
            } elseif (($ack['nack'] ?? false) === true) {
                $sock->send(Codec::basicNack($ack['ch'], $ack['tag']));
            } else {
                $sock->send(Codec::basicAck($ack['ch'], $ack['tag']));
                $broker->prom['confirmed']++;
            }
            $dirty[$ack['conn']] = $sock;
        }
        foreach ($dirty as $sock) {
            $sock->flush();
        }
        $this->nextSync = microtime(true) + $this->fsyncMs / 1000;
        $this->pump();
        }
    }

    /**
     * Hands ready messages to consumers.
     *
     * Consumer selection lives in the broker so priority and
     * x-single-active-consumer are applied consistently; the per-queue
     * round-robin cursor replaces the single server-wide one, which used to
     * let traffic on one queue skew the rotation on another.
     */
    /**
     * The peer that owns a classic queue, or null when this node does.
     *
     * A classic queue lives on exactly one node, chosen by hash, so a get,
     * subscribe, purge or delete that arrives anywhere else has to be
     * forwarded. Publishing forwards per destination inside the broker,
     * since one publish can fan out to queues with different homes.
     */
    private function remoteHome(string $queue, ?Broker $scope = null): ?string
    {
        return $this->extras === null ? null : ($scope ?? $this->broker)->remoteHomeOf($queue);
    }

    private function deliverMessage(Broker $broker,int $id,int $channel,string $tag,bool $noAck,array $msg,int $msgId):void
    {
        $sock=$this->conns[$id]??null;$ch=$sock?->channels[$channel]??null;if($ch===null){if($msgId>=0)$broker->requeue($msgId);return;}
        $dtag=$ch->nextDel++;if(!$noAck){$ch->unacked[$dtag]=$msgId;$ch->tagOf[$dtag]=$tag;$ch->held[$tag]=($ch->held[$tag]??0)+1;}
        $sock->send(Codec::deliver($channel,$tag,$dtag,$msg['key']??$msg['queue']??'',$msg['mode']??2,$msg['body'],$msg['redelivered']??false,$msg['exchange']??'',$msg['propRaw']??null,$msg['headers']??[]));$sock->flush();
        $broker->prom['delivered']++;$broker->prom[$noAck?'deliveredConsumeAuto':'deliveredConsumeManual']++;if($msg['redelivered']??false)$broker->prom['redelivered']++;
    }
    private function pumpStream(Broker $broker,string $name):void
    {
        $remaining=128;
        foreach($broker->queues[$name]['consumers'] as $i=>$consumer) {
            if(!isset($consumer['streamOffset'],$consumer['readyFn'])||!isset($this->conns[$consumer['conn']]))continue;
            while($remaining>0 && ($consumer['readyFn'])()) {
                $offset=$broker->queues[$name]['consumers'][$i]['streamOffset'];$messages=$broker->streamRead($name,$offset,1);if($messages===[])break;$record=$messages[0];
                $props=Amqp10::readProps($record['propRaw']);$headers=$props['headers'];$headers['x-stream-offset']=$record['offset'];$headers=array_map(static fn($key,$value)=>[$key,$value],array_keys($headers),array_values($headers));
                $msg=['queue'=>$name,'key'=>$name,'body'=>$record['body'],'mode'=>2,'propRaw'=>Amqp10::writeProps($props,$headers),'headers'=>$headers];
                $broker->queues[$name]['consumers'][$i]['streamOffset']=$record['offset']+1;$remaining--;
                $this->deliverMessage($broker,$consumer['conn'],$consumer['ch'],$consumer['tag'],false,$msg,-1);
            }
        }
    }

    private function pump(): void
    {
        foreach ($this->broker->allBrokers() as $broker) foreach ($broker->queues as $name => $q) {
            if (($q["args"]["queueType"] ?? "") === "stream") { $this->pumpStream($broker,$name); continue; }
            while (($broker->queues[$name]['ready'] ?? []) !== []
                && ($broker->queues[$name]['consumers'] ?? []) !== []) {
                $pick = $broker->pickConsumer($name, function (array $cons): bool {
                    if(isset($cons['readyFn']))return ($cons['readyFn'])();
                    if (($cons['peer'] ?? '') !== '') {
                        // A consumer on another node. Credit of zero means
                        // unlimited, matching the prefetch convention.
                        return (int) ($cons['credit'] ?? 0) >= 0;
                    }
                    if (!isset($this->conns[$cons['conn']])) {
                        return false;
                    }
                    $ch = $this->conns[$cons['conn']]->channels[$cons['ch']] ?? null;
                    if ($ch === null) {
                        return false;
                    }
                    return $ch->prefetch === 0 || ($ch->held[$cons['tag']] ?? 0) < $ch->prefetch;
                });
                if ($pick === null) {
                    break;
                }
                $head = $broker->queues[$name]['ready'][0] ?? null;
                // A gated quorum body is not deliverable yet, and the queue
                // is ordered, so nothing behind it is either.
                if (is_int($head) && $broker->isGated($head)) {
                    break;
                }
                $msgId = $broker->getReady($name);
                if ($msgId === null) break;
                if (!isset($broker->msgs[$msgId])) {
                    continue;
                }
                $broker->noteConsumed($name, (string) ($broker->msgs[$msgId]['qid'] ?? ''));
                if ($broker->expired($msgId)) {
                    $broker->prom['dlxExpired']++;
                    $broker->deadLetter($msgId, 'expired');
                    continue;
                }
                $cons = $broker->queues[$name]['consumers'][$pick];
                $noAck = ($cons['noAck'] ?? false) === true;
                if(isset($cons['deliverFn'])) { ($cons['deliverFn'])($broker->msgs[$msgId],$msgId);if($noAck&&isset($broker->msgs[$msgId]))$broker->ack($msgId);continue; }
                if (($cons['peer'] ?? '') !== '' && $this->extras !== null) {
                    $msg = $broker->msgs[$msgId];
                    $this->extras->cluster->deliverTo(
                        (string) $cons['peer'],
                        $name,
                        (int) ($cons['session'] ?? 0),
                        $msg,
                        $msgId,
                        $noAck,
                    );
                    if ($noAck) {
                        $broker->drop($msgId);
                    }
                    $broker->prom['delivered']++;
                    continue;
                }
                $sock = $this->conns[$cons['conn']];
                $ch = $sock->channels[$cons['ch']];
                $dtag = $ch->nextDel++;
                if (!$noAck) {
                    $ch->unacked[$dtag] = $msgId;
                    $ch->tagOf[$dtag] = $cons['tag'];
                    $ch->held[$cons['tag']] = ($ch->held[$cons['tag']] ?? 0) + 1;
                }
                $msg = $broker->msgs[$msgId];
                $sock->send(Codec::deliver(
                    $cons['ch'],
                    $cons['tag'],
                    $dtag,
                    $msg['key'] ?? $msg['queue'],
                    $msg['mode'],
                    $msg['body'],
                    $msg['redelivered'] ?? false,
                    $msg['exchange'] ?? '',
                    $msg['propRaw'] ?? null,
                    is_array($msg['headers'] ?? null) ? $msg['headers'] : [],
                ));
                $sock->flush();
                $broker->prom['delivered']++;
                $broker->prom[$noAck ? 'deliveredConsumeAuto' : 'deliveredConsumeManual']++;
                if (($msg['redelivered'] ?? false) === true) {
                    $broker->prom['redelivered']++;
                }
                if ($noAck) {
                    $broker->drop($msgId);
                }
            }
        }
    }
}
