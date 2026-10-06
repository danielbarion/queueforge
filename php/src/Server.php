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
    /** @var array<int, Chan> */
    public array $channels = [];

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
    }

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
        foreach ($this->broker->queues as $name => $q) {
            $this->broker->queues[$name]['consumers'] = array_values(array_filter(
                $q['consumers'],
                static fn (array $c): bool => $c['conn'] !== $id,
            ));
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
            if ($head !== "AMQP\x00\x00\x09\x01") {
                $this->drop($id);
                return;
            }
            $sock->in = substr($sock->in, 8);
            $sock->stage = 'frames';
            $sock->send(Codec::connectionStart());
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
                    $ch->pub['headers'][] = [(string) $name, (string) $value];
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
            $sock->send(Codec::connectionOpenOk());
        } elseif ($class === 10 && $method === 50) {
            $sock->send(Codec::connectionCloseOk());
            $sock->flush();
            $this->drop($id);
        } elseif ($class === 20 && $method === 10) {
            $sock->channels[$channel] = new Chan();
            $sock->send(Codec::channelOpenOk($channel));
        } elseif ($class === 20 && $method === 40) {
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
        $msgId = $this->broker->getReady($queue);
        if ($msgId === null) {
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
        ));
        $this->broker->prom['delivered']++;
        if ($noAck) {
            $this->broker->ack($msgId);
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
        $this->conns[$id]->send(Codec::connectionTune());
    }

    private function declare(int $id, int $channel, string $payload, int $o): void
    {
        $o += 2;
        $name = Codec::readShortstr($payload, $o);
        if ($name === '') {
            // A server-generated name has to be unique; the old fixed
            // 'amq.gen' made every anonymous queue collide.
            $name = 'amq.gen-' . bin2hex(random_bytes(8));
        }
        $bits = isset($payload[$o]) ? ord($payload[$o]) : 0;
        $durable = ($bits & 2) === 2;
        $exclusive = ($bits & 4) === 4;
        $args = [];
        if (isset($payload[$o])) {
            $o++;
            $args = Codec::readTable($payload, $o);
        }
        try {
            $this->broker->declareQueue($name, $args, $durable, $exclusive);
        } catch (RuntimeException $err) {
            $this->conns[$id]->send(Codec::channelClose($channel, 406, $err->getMessage(), 50, 10));
            return;
        }
        $this->conns[$id]->send(Codec::queueDeclareOk(
            $channel,
            $name,
            $this->broker->readyCount($name),
            $this->broker->consumerCount($name),
        ));
    }

    private function consume(int $id, int $channel, string $payload, int $o): void
    {
        $o += 2;
        $queue = Codec::readShortstr($payload, $o);
        $tag = Codec::readShortstr($payload, $o);
        if ($tag === '') {
            $tag = 'ctag-' . $id . '-' . $channel;
        }
        $ch = $this->conns[$id]->channels[$channel];
        $ch->consumer = $tag;
        $ch->queue = $queue;
        $this->broker->addConsumer($queue, $id, $channel, $tag);
        $this->conns[$id]->send(Codec::consumeOk($channel, $tag));
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
        $ch->pub = null;
        if ($result === 'return' && $pub['mandatory']) {
            $ret = Codec::method($channel, 60, 50, pack('n', 312) . Codec::shortstr('NO_ROUTE') . Codec::shortstr($pub['exchange']) . Codec::shortstr($pub['key']));
            $header = Codec::contentHeader(strlen($pub['got']), $pub['mode'], $pub['propRaw'] ?? null);
            $this->conns[$id]->send($ret . Codec::frame(2, $channel, $header) . Codec::frame(3, $channel, $pub['got']));
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
                    $this->broker->deadLetter($msg);
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
        $this->broker->declareExchange($name, $kind);
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
                $args[] = [(string) $name, (string) $value];
            }
        }
        $this->broker->bind($queue, $exchange, $key, $args);
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

    private function pump(): void
    {
        foreach ($this->broker->queues as $name => $q) {
            while ($this->broker->queues[$name]['ready'] !== [] && $this->broker->queues[$name]['consumers'] !== []) {
                $consumers = $this->broker->queues[$name]['consumers'];
                $n = count($consumers);
                $pick = null;
                for ($k = 0; $k < $n; $k++) {
                    $i = ($this->rr + $k) % $n;
                    $cons = $consumers[$i];
                    if (($cons['peer'] ?? '') !== '') {
                        // A consumer on another node. Credit of zero means
                        // unlimited, matching the AMQP prefetch convention.
                        if (($cons['credit'] ?? 0) < 0) {
                            continue;
                        }
                        $pick = $i;
                        break;
                    }
                    if (!isset($this->conns[$cons['conn']])) {
                        continue;
                    }
                    $ch = $this->conns[$cons['conn']]->channels[$cons['ch']] ?? null;
                    if ($ch === null) {
                        continue;
                    }
                    if ($ch->prefetch !== 0 && count($ch->unacked) >= $ch->prefetch) {
                        continue;
                    }
                    $pick = $i;
                    break;
                }
                if ($pick === null) {
                    break;
                }
                $this->rr++;
                $msgId = array_shift($this->broker->queues[$name]['ready']);
                if ($msgId === null || !isset($this->broker->msgs[$msgId])) {
                    continue;
                }
                if ($this->broker->expired($msgId)) {
                    $this->broker->deadLetter($msgId);
                    continue;
                }
                $cons = $this->broker->queues[$name]['consumers'][$pick];
                if (($cons['peer'] ?? '') !== '' && $this->extras !== null) {
                    $msg = $this->broker->msgs[$msgId];
                    $this->extras->cluster->deliverTo(
                        (string) $cons['peer'],
                        $name,
                        (int) ($cons['session'] ?? 0),
                        $msg,
                        $msgId,
                        (bool) ($cons['noAck'] ?? false),
                    );
                    if (($cons['noAck'] ?? false) === true) {
                        $this->broker->ack($msgId);
                    }
                    $this->broker->prom['delivered']++;
                    continue;
                }
                $sock = $this->conns[$cons['conn']];
                $ch = $sock->channels[$cons['ch']];
                $dtag = $ch->nextDel++;
                $ch->unacked[$dtag] = $msgId;
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
                ));
                $sock->flush();
                $this->broker->prom['delivered']++;
            }
        }
    }
}
