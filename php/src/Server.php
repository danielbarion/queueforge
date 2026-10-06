<?php
declare(strict_types=1);

final class Chan
{
    public bool $confirm = false;
    public int $nextPub = 1;
    public int $nextDel = 1;
    public int $prefetch = 0;
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
}

final class Sock
{
    /** @param resource $fp */
    public function __construct(public $fp)
    {
    }

    public string $in = '';
    public string $out = '';
    public int $off = 0;
    public string $stage = 'header';
    public bool $gone = false;
    /** The authenticated user, set at connection.start-ok. */
    public string $user = '';
    /** The vhost from connection.open. */
    public string $vhost = '/';
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
    /**
     * At or below this many outstanding confirms, flush at once rather than
     * waiting for the interval. Batching needs a queue to batch; a handful of
     * blocked publishers have none.
     */
    private const FLUSH_SMALL = 8;

    /** @param resource $listen */
    public function __construct(private $listen, private Broker $broker, private int $fsyncMs, public ?Extras $extras = null)
    {
        $this->nextSync = microtime(true) + $this->fsyncMs / 1000;
        $this->nextBeat = microtime(true) + self::BEAT_SECONDS;
        $this->amqp10 = new Amqp10($broker);
    }

    private Amqp10 $amqp10;

    public function run(): void
    {
        while (true) {
            $read = [$this->listen];
            if ($this->extras !== null) {
                foreach ($this->extras->reads() as $fp) {
                    $read[] = $fp;
                }
            }
            $write = [];
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
            // Never sleep past the next expiry sweep, or a message with a
            // short TTL would keep counting toward queue depth for up to a
            // second after it expired.
            $wait = min($wait, max(0.001, $this->nextSweep - microtime(true)));
            $sec = (int) $wait;
            $usec = (int) (($wait - $sec) * 1000000);
            $selected = @stream_select($read, $write, $except, $sec, $usec);
            if ($selected === false) {
                continue;
            }
            foreach ($read as $fp) {
                if ($this->extras !== null && $this->extras->owns($fp)) {
                    $this->extras->onRead($fp);
                    continue;
                }
                if ($fp === $this->listen) {
                    $client = @stream_socket_accept($this->listen, 0);
                    if ($client !== false) {
                        stream_set_blocking($client, false);
                        $this->tune($client);
                        $id = $this->nextConn++;
                        $this->conns[$id] = new Sock($client);
                        $this->byFp[(int) $client] = $id;
                        $this->broker->prom['connections']++;
                        $this->broker->prom['connectionsOpened']++;
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
                $id = $this->idOf($fp);
                if ($id !== null) {
                    $this->conns[$id]->flush();
                }
            }
            // Nothing readable means the publishers are blocked rather than
            // streaming.
            $this->commit($selected === 0);
            $this->beat();
            $this->broker->maybeCompact();
            if ($this->extras !== null) {
                $this->extras->tick();
            }
        }
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
            $this->broker->sweep();
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
        if (!isset($this->conns[$id])) {
            return;
        }
        $sock = $this->conns[$id];
        $sock->gone = true;
        unset($this->byFp[(int) $sock->fp]);
        @fclose($sock->fp);
        $this->broker->prom['connections']--;
        $this->broker->prom['connectionsClosed']++;
        $this->broker->prom['channels'] -= count($sock->channels);
        $this->broker->prom['channelsClosed'] += count($sock->channels);
        foreach ($this->broker->queues as $name => $q) {
            $before = count($q['consumers']);
            $this->broker->queues[$name]['consumers'] = array_values(array_filter(
                $q['consumers'],
                static fn (array $c): bool => $c['conn'] !== $id,
            ));
            $this->broker->prom['consumers'] -= $before - count($this->broker->queues[$name]['consumers']);
        }
        unset($this->conns[$id]);
    }

    private function onData(int $id, string $data): void
    {
        $sock = $this->conns[$id];
        $sock->in .= $data;
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
            $sock->in = substr($sock->in, 8);
            $sock->stage = 'frames';
            $sock->send(Codec::connectionStart());
        }
        if ($sock->stage === 'amqp10') {
            $sock->send($this->amqp10->drive($sock->in, $sock->amqp10));
            $sock->flush();
            if (($sock->amqp10['closing'] ?? false) === true) {
                $this->drop($id);
            }
            return;
        }
        while (isset($this->conns[$id]) && strlen($sock->in) >= 7) {
            $type = ord($sock->in[0]);
            $channel = unpack('n', substr($sock->in, 1, 2))[1];
            $len = unpack('N', substr($sock->in, 3, 4))[1];
            if (strlen($sock->in) < 8 + $len) {
                break;
            }
            if ($sock->in[7 + $len] !== "\xce") {
                $this->drop($id);
                return;
            }
            $payload = substr($sock->in, 7, $len);
            $sock->in = substr($sock->in, 8 + $len);
            $this->onFrame($id, $type, $channel, $payload);
            if (!isset($this->conns[$id])) {
                return;
            }
            $sock = $this->conns[$id];
        }
        $sock->flush();
    }

    private function onFrame(int $id, int $type, int $channel, string $payload): void
    {
        $sock = $this->conns[$id];
        if ($type === 8) {
            return;
        }
        $ch = $sock->channels[$channel] ?? null;
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
        if ($class === 10 && $method === 11) {
            $this->startOk($id, $payload, $o);
        } elseif ($class === 10 && $method === 31) {
            return;
        } elseif ($class === 10 && $method === 40) {
            // connection.open names the vhost. Access is checked here, so a
            // user with no permission on it is refused rather than silently
            // landing on the default namespace.
            $o += 2;
            $vhost = Codec::readShortstr($payload, $o);
            $vhost = $vhost === '' ? '/' : $vhost;
            if (!$this->broker->hasVhostAccess($sock->user ?? '', $vhost)) {
                $sock->send(Codec::connectionClose(403, "ACCESS_REFUSED - vhost '$vhost'", 10, 40));
                $sock->flush();
                $this->drop($id);
                return;
            }
            if (!$this->broker->connectionAllowed($sock->user ?? '', $vhost, $this->broker->prom['connections'])) {
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
            if (!$this->broker->channelAllowed($sock->user ?? '', $this->broker->prom['channels'])) {
                $sock->send(Codec::connectionClose(403, 'ACCESS_REFUSED - channel limit', 20, 10));
                $sock->flush();
                $this->drop($id);
                return;
            }
            $sock->channels[$channel] = new Chan();
            $this->broker->prom['channels']++;
            $this->broker->prom['channelsOpened']++;
            $sock->send(Codec::channelOpenOk($channel));
        } elseif ($class === 20 && $method === 40) {
            if (isset($sock->channels[$channel])) {
                $this->broker->prom['channels']--;
                $this->broker->prom['channelsClosed']++;
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
        $tag = Codec::readShortstr($payload, $o);
        $nowait = isset($payload[$o]) && (ord($payload[$o]) & 1) === 1;
        $ch = $this->conns[$id]->channels[$channel];
        // A consumer served by another node is unsubscribed there.
        if (isset($ch->remote[$tag]) && $this->extras !== null) {
            $this->extras->cluster->request($ch->remote[$tag]['peer'], 'unsub', [
                'vhost' => '/',
                'session' => $ch->remote[$tag]['session'],
            ]);
            unset($ch->remote[$tag]);
        }
        foreach ($this->broker->queues as $name => $queue) {
            $this->broker->queues[$name]['consumers'] = array_values(array_filter(
                $queue['consumers'],
                static fn (array $c): bool => !($c['conn'] === $id && $c['ch'] === $channel && $c['tag'] === $tag),
            ));
        }
        if ($ch->consumer === $tag) {
            $ch->consumer = null;
            $ch->queue = null;
        }
        if (!$nowait) {
            $this->conns[$id]->send(Codec::cancelOk($channel, $tag));
        }
    }

    private function get(int $id, int $channel, string $payload, int $o): void
    {
        $o += 2;
        $queue = Codec::readShortstr($payload, $o);
        $noAck = isset($payload[$o]) && (ord($payload[$o]) & 1) === 1;
        $sock = $this->conns[$id];
        $ch = $sock->channels[$channel];
        // A classic queue homed elsewhere is asked over the cluster link. The
        // reply comes back through the select loop, so the client's answer is
        // written from the callback rather than blocking here.
        $home = $this->remoteHome($queue);
        if ($home !== null && $this->extras !== null) {
            $this->extras->cluster->request(
                $home,
                'get',
                ['vhost' => '/', 'queue' => $queue, 'noAck' => $noAck],
                '',
                function (?array $reply) use ($id, $channel, $queue, $noAck): void {
                    $sock = $this->conns[$id] ?? null;
                    $ch = $sock?->channels[$channel] ?? null;
                    if ($sock === null || $ch === null) {
                        return;
                    }
                    $msg = is_array($reply['msg'] ?? null) ? $reply['msg'] : null;
                    if ($reply === null || $msg === null) {
                        // A timeout and an empty queue are both reported as
                        // empty; the alternative is leaving the client hanging.
                        $this->broker->prom['getEmpty']++;
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
                    $this->broker->prom['delivered']++;
                    $this->broker->prom[$noAck ? 'deliveredGetAuto' : 'deliveredGetManual']++;
                },
            );
            return;
        }
        $msgId = $this->broker->getReady($queue);
        if ($msgId === null) {
            $this->broker->prom['getEmpty']++;
            $sock->send(Codec::getEmpty($channel));
            return;
        }
        $msg = $this->broker->msgs[$msgId];
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
            $this->broker->readyCount($queue),
            $msg['mode'],
            $msg['body'],
            $msg['propRaw'] ?? null,
            is_array($msg['headers'] ?? null) ? $msg['headers'] : [],
        ));
        $this->broker->prom['delivered']++;
        $this->broker->prom[$noAck ? 'deliveredGetAuto' : 'deliveredGetManual']++;
        if (($msg['redelivered'] ?? false) === true) {
            $this->broker->prom['redelivered']++;
        }
        if ($noAck) {
            $this->broker->drop($msgId);
        }
    }

    private function reject(int $id, int $channel, string $payload, int $o): void
    {
        if (strlen($payload) < $o + 9) {
            return;
        }
        $tag = Codec::readU64($payload, $o);
        $requeue = (ord($payload[$o + 8]) & 1) === 1;
        $ch = $this->conns[$id]->channels[$channel];
        if (!isset($ch->unacked[$tag])) {
            return;
        }
        $msgId = $ch->unacked[$tag];
        unset($ch->unacked[$tag]);
        if ($requeue) {
            $this->broker->requeue($msgId);
        } else {
            $this->broker->deadLetter($msgId);
        }
        $this->pump();
    }

    private function recover(int $id, int $channel, string $payload, int $o, int $method): void
    {
        $requeue = isset($payload[$o]) && (ord($payload[$o]) & 1) === 1;
        $sock = $this->conns[$id];
        if (!$requeue) {
            // Matching Bun, which rejects recover without requeue rather than
            // dropping the unacked messages on the floor.
            $sock->send(Codec::channelClose($channel, 540, 'NOT_IMPLEMENTED - recover requeue=false', 60, $method));
            return;
        }
        $ch = $sock->channels[$channel];
        foreach (array_reverse($ch->unacked, true) as $tag => $msgId) {
            $this->broker->requeue($msgId);
            unset($ch->unacked[$tag]);
        }
        if ($method === 110) {
            $sock->send(Codec::recoverOk($channel));
        }
        $this->pump();
    }

    private function queuePurge(int $id, int $channel, string $payload, int $o): void
    {
        $o += 2;
        $queue = Codec::readShortstr($payload, $o);
        $nowait = isset($payload[$o]) && (ord($payload[$o]) & 1) === 1;
        // Purging a queue homed elsewhere has to reach the node holding it.
        $home = $this->remoteHome($queue);
        if ($home !== null && $this->extras !== null) {
            $this->extras->cluster->request($home, 'purge', ['vhost' => '/', 'queue' => $queue]);
        }
        $n = $this->broker->purge($queue);
        if (!$nowait) {
            $this->conns[$id]->send(Codec::purgeOk($channel, $n));
        }
    }

    private function queueDelete(int $id, int $channel, string $payload, int $o): void
    {
        $o += 2;
        $queue = Codec::readShortstr($payload, $o);
        $bits = isset($payload[$o]) ? ord($payload[$o]) : 0;
        $nowait = ($bits & 4) === 4;
        $home = $this->remoteHome($queue);
        if ($home !== null && $this->extras !== null) {
            $this->extras->cluster->request($home, 'delete_queue', ['vhost' => '/', 'queue' => $queue]);
        }
        $n = $this->broker->deleteQueue($queue);
        if (!$nowait) {
            $this->conns[$id]->send(Codec::queueDeleteOk($channel, $n));
        }
    }

    private function queueUnbind(int $id, int $channel, string $payload, int $o): void
    {
        $o += 2;
        $queue = Codec::readShortstr($payload, $o);
        $exchange = Codec::readShortstr($payload, $o);
        $key = Codec::readShortstr($payload, $o);
        $this->broker->unbind($queue, $exchange, $key);
        $this->conns[$id]->send(Codec::unbindOk($channel));
    }

    private function exchangeDelete(int $id, int $channel, string $payload, int $o): void
    {
        $o += 2;
        $name = Codec::readShortstr($payload, $o);
        $bits = isset($payload[$o]) ? ord($payload[$o]) : 0;
        $nowait = ($bits & 2) === 2;
        $this->broker->deleteExchange($name);
        if (!$nowait) {
            $this->conns[$id]->send(Codec::method($channel, 40, 21));
        }
    }

    private function exchangeBind(int $id, int $channel, string $payload, int $o, bool $bind): void
    {
        $o += 2;
        $destination = Codec::readShortstr($payload, $o);
        $source = Codec::readShortstr($payload, $o);
        $key = Codec::readShortstr($payload, $o);
        if ($bind) {
            $this->broker->bindExchange($destination, $source, $key);
        } else {
            $this->broker->unbindExchange($destination, $source, $key);
        }
        $this->conns[$id]->send(Codec::method($channel, 40, $bind ? 31 : 41));
    }

    /**
     * tx.select, tx.commit and tx.rollback. Publishes are applied as they
     * arrive rather than buffered, so commit is an acknowledgement and
     * rollback cannot undo anything. A client that needs real atomicity
     * should use publisher confirms instead.
     */
    private function transaction(int $id, int $channel, int $method): void
    {
        $sock = $this->conns[$id];
        if ($method === 30) {
            $sock->send(Codec::channelClose($channel, 540, 'NOT_IMPLEMENTED - tx.rollback cannot undo an applied publish', 90, 30));
            return;
        }
        $sock->send(Codec::method($channel, 90, $method + 1));
    }

    private function startOk(int $id, string $payload, int $o): void
    {
        $table = unpack('N', substr($payload, $o, 4))[1];
        $o += 4 + $table;
        $mech = Codec::readShortstr($payload, $o);
        $resp = Codec::readLongstr($payload, $o);
        if ($mech !== 'PLAIN') {
            $this->drop($id);
            return;
        }
        $parts = explode("\0", $resp);
        $user = count($parts) >= 3 ? $parts[1] : ($parts[0] ?? '');
        $pass = count($parts) >= 3 ? $parts[2] : ($parts[1] ?? '');
        if (!$this->broker->verify($user, $pass)) {
            $this->drop($id);
            return;
        }
        $this->conns[$id]->user = $user;
        $this->conns[$id]->send(Codec::connectionTune());
    }

    private function declare(int $id, int $channel, string $payload, int $o): void
    {
        $o += 2;
        $name = Codec::readShortstr($payload, $o);
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
        if (!$passive && !isset($this->broker->queues[$name])
            && !$this->broker->queueAllowed($this->conns[$id]->vhost)) {
            $this->conns[$id]->send(Codec::channelClose($channel, 403, 'ACCESS_REFUSED - queue limit', 50, 10));
            return;
        }
        try {
            $state = $this->broker->declareQueue($name, $args, $durable, $exclusive, $passive, $autoDelete);
        } catch (RuntimeException $err) {
            $this->fail($id, $channel, $err, 50, 10);
            return;
        }
        $this->conns[$id]->send(Codec::queueDeclareOk($channel, $name, $state['messages'], $state['consumers']));
    }

    /**
     * Answers a broker error. A 5xx reply code closes the connection, as
     * RabbitMQ does for the transient-queue deprecation; anything else closes
     * just the channel.
     */
    private function fail(int $id, int $channel, RuntimeException $err, int $class, int $method): void
    {
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
        $o += 2;
        $queue = Codec::readShortstr($payload, $o);
        $tag = Codec::readShortstr($payload, $o);
        $bits = isset($payload[$o]) ? ord($payload[$o]) : 0;
        $noAck = ($bits & 2) === 2;
        $exclusive = ($bits & 4) === 4;
        $nowait = ($bits & 8) === 8;
        if ($tag === '') {
            $tag = 'ctag-' . $id . '-' . $channel;
        }
        $priority = 0;
        if (isset($payload[$o])) {
            $o++;
            $args = Codec::readTable($payload, $o);
            $priority = (int) ($args['x-priority'] ?? 0);
        }
        $ch = $this->conns[$id]->channels[$channel];
        // A classic queue homed elsewhere is subscribed to over the cluster
        // link; the home node then pushes deliver frames back.
        $home = $this->remoteHome($queue);
        if ($home !== null && $this->extras !== null) {
            $session = $this->broker->nextSession();
            $this->extras->cluster->request($home, 'sub', [
                'vhost' => '/',
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
            $this->broker->addConsumer($queue, $id, $channel, $tag, $noAck, $exclusive, $priority);
        } catch (RuntimeException $err) {
            $this->fail($id, $channel, $err, 60, 20);
            return;
        }
        $ch->consumer = $tag;
        $ch->queue = $queue;
        $ch->noAck[$tag] = $noAck;
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
        $ch = $this->conns[$id]->channels[$channel];
        if ($ch->pub === null) {
            return;
        }
        $pub = $ch->pub;
        $tag = $ch->confirm ? $ch->nextPub++ : 0;
        $ch->pub = null;
        $sock = $this->conns[$id];
        if (!$this->broker->topicWriteAllowed($sock->user, $sock->vhost, $pub['exchange'], $pub['key'])) {
            $sock->send(Codec::channelClose($channel, 403, 'ACCESS_REFUSED - write access to topic refused', 60, 40));
            return;
        }
        try {
            $result = $this->broker->publish(
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
                $this->broker->prom['unroutableReturned']++;
                $ret = Codec::method($channel, 60, 50, pack('n', 312) . Codec::shortstr('NO_ROUTE') . Codec::shortstr($pub['exchange']) . Codec::shortstr($pub['key']));
                $header = Codec::contentHeader(strlen($pub['got']), $pub['mode'], $pub['propRaw'] ?? null);
                $this->conns[$id]->send($ret . Codec::frame(2, $channel, $header) . Codec::frame(3, $channel, $pub['got']));
            } else {
                $this->broker->prom['unroutableDropped']++;
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
        if (strlen($payload) < $o + 9) {
            return;
        }
        $tag = Codec::readU64($payload, $o);
        $bits = ord($payload[$o + 8]);
        $multiple = ($bits & 1) === 1;
        $requeue = $negative && ($bits & 2) === 2;
        $ch = $this->conns[$id]->channels[$channel];
        foreach ($ch->unacked as $have => $msg) {
            if ($multiple ? $have <= $tag : $have === $tag) {
                if ($negative && $requeue) {
                    $this->broker->requeue($msg);
                } elseif ($negative) {
                    $this->broker->prom['dlxRejected']++;
                    $this->broker->deadLetter($msg, 'rejected');
                } else {
                    $this->broker->ack($msg);
                }
                unset($ch->unacked[$have]);
            }
        }
        $this->pump();
    }

    private function exchangeDeclare(int $id, int $channel, string $payload, int $o): void
    {
        $o += 2;
        $name = Codec::readShortstr($payload, $o);
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
            $this->broker->declareExchange($name, $kind, $durable, $autoDelete, $internal, $alternate, $passive);
        } catch (RuntimeException $err) {
            $this->fail($id, $channel, $err, 40, 10);
            return;
        }
        $this->conns[$id]->send(Codec::method($channel, 40, 11));
    }

    private function queueBind(int $id, int $channel, string $payload, int $o): void
    {
        $o += 2;
        $queue = Codec::readShortstr($payload, $o);
        $exchange = Codec::readShortstr($payload, $o);
        $key = Codec::readShortstr($payload, $o);
        $args = [];
        if (isset($payload[$o])) {
            $o++;
            foreach (Codec::readTable($payload, $o) as $name => $value) {
                $args[] = [(string) $name, $value];
            }
        }
        try {
            $this->broker->bind($queue, $exchange, $key, $args);
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
            foreach ($this->extras->cluster->timedOut as $qid) {
                $this->broker->failQuorum($qid);
            }
            $this->extras->cluster->timedOut = [];
        }
        if ($this->broker->waiting === []) {
            return;
        }
        $small = count($this->broker->waiting) <= self::FLUSH_SMALL;
        if (!$idle && !$small && microtime(true) < $this->nextSync) {
            return;
        }
        foreach ($this->broker->flush() as $ack) {
            if ($ack['tag'] <= 0 || !isset($this->conns[$ack['conn']])) {
                continue;
            }
            $sock = $this->conns[$ack['conn']];
            if (($ack['nack'] ?? false) === true) {
                $sock->send(Codec::basicNack($ack['ch'], $ack['tag']));
                $sock->flush();
                continue;
            }
            $sock->send(Codec::basicAck($ack['ch'], $ack['tag']));
            $sock->flush();
            $this->broker->prom['confirmed']++;
        }
        $this->nextSync = microtime(true) + $this->fsyncMs / 1000;
        $this->pump();
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
    private function remoteHome(string $queue): ?string
    {
        return $this->extras === null ? null : $this->broker->remoteHomeOf($queue);
    }

    private function pump(): void
    {
        foreach ($this->broker->queues as $name => $q) {
            while (($this->broker->queues[$name]['ready'] ?? []) !== []
                && ($this->broker->queues[$name]['consumers'] ?? []) !== []) {
                $pick = $this->broker->pickConsumer($name, function (array $cons): bool {
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
                    return $ch->prefetch === 0 || count($ch->unacked) < $ch->prefetch;
                });
                if ($pick === null) {
                    break;
                }
                $head = $this->broker->queues[$name]['ready'][0] ?? null;
                // A gated quorum body is not deliverable yet, and the queue
                // is ordered, so nothing behind it is either.
                if (is_int($head) && $this->broker->isGated($head)) {
                    break;
                }
                $msgId = array_shift($this->broker->queues[$name]['ready']);
                if ($msgId === null || !isset($this->broker->msgs[$msgId])) {
                    continue;
                }
                $this->broker->noteConsumed($name, (string) ($this->broker->msgs[$msgId]['qid'] ?? ''));
                if ($this->broker->expired($msgId)) {
                    $this->broker->prom['dlxExpired']++;
                    $this->broker->deadLetter($msgId, 'expired');
                    continue;
                }
                $cons = $this->broker->queues[$name]['consumers'][$pick];
                $noAck = ($cons['noAck'] ?? false) === true;
                if (($cons['peer'] ?? '') !== '' && $this->extras !== null) {
                    $msg = $this->broker->msgs[$msgId];
                    $this->extras->cluster->deliverTo(
                        (string) $cons['peer'],
                        $name,
                        (int) ($cons['session'] ?? 0),
                        $msg,
                        $msgId,
                        $noAck,
                    );
                    if ($noAck) {
                        $this->broker->drop($msgId);
                    }
                    $this->broker->prom['delivered']++;
                    continue;
                }
                $sock = $this->conns[$cons['conn']];
                $ch = $sock->channels[$cons['ch']];
                $dtag = $ch->nextDel++;
                if (!$noAck) {
                    $ch->unacked[$dtag] = $msgId;
                }
                $msg = $this->broker->msgs[$msgId];
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
                $this->broker->prom['delivered']++;
                $this->broker->prom[$noAck ? 'deliveredConsumeAuto' : 'deliveredConsumeManual']++;
                if (($msg['redelivered'] ?? false) === true) {
                    $this->broker->prom['redelivered']++;
                }
                if ($noAck) {
                    $this->broker->drop($msgId);
                }
            }
        }
    }
}
