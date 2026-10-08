<?php
declare(strict_types=1);

require_once dirname(__DIR__, 2) . '/src/Codec.php';

/**
 * Minimal AMQP 0-9-1 client for the test suite. It speaks enough of the
 * protocol to drive every method the broker implements, so the checks do not
 * need an external client library and can run inside the bench container.
 */
final class Amqp
{
    /** @var resource */
    private $fp;
    private string $buf = '';

    public function __construct(
        string $host = '127.0.0.1',
        int $port = 5672,
        private string $user = 'admin',
        private string $pass = 'devpassword12',
        bool $open = true,
        private float $timeout = 5.0,
    ) {
        $fp = @stream_socket_client("tcp://$host:$port", $errno, $errstr, $this->timeout);
        if ($fp === false) {
            throw new RuntimeException("connect $host:$port failed: $errstr");
        }
        $this->fp = $fp;
        stream_set_blocking($fp, false);
        if ($open) {
            $this->handshake();
        }
    }

    /**
     * Sends only the protocol header and returns the raw connection.start
     * arguments, so a test can inspect the server-properties table.
     */
    public function rawStart(): string
    {
        $this->write("AMQP\x00\x00\x09\x01");
        return $this->expect(10, 10)['args'];
    }

    private function handshake(): void
    {
        $this->write("AMQP\x00\x00\x09\x01");
        $this->expect(10, 10);
        $response = "\0" . $this->user . "\0" . $this->pass;
        $this->method(0, 10, 11, pack('N', 0) . Codec::shortstr('PLAIN') . Codec::longstr($response) . Codec::shortstr('en_US'));
        $this->expect(10, 30);
        $this->method(0, 10, 31, pack('nNn', 0, 131072, 0));
        $this->method(0, 10, 40, Codec::shortstr('/') . Codec::shortstr('') . chr(0));
        $this->expect(10, 41);
    }

    public function channel(int $ch = 1): void
    {
        $this->method($ch, 20, 10, Codec::shortstr(''));
        $this->expect(20, 11);
    }

    public function confirmSelect(int $ch = 1): void
    {
        $this->method($ch, 85, 10, chr(0));
        $this->expect(85, 11);
    }

    public function qos(int $count, int $ch = 1): void
    {
        $this->method($ch, 60, 10, pack('N', 0) . pack('n', $count) . chr(0));
        $this->expect(60, 11);
    }

    /** @param array<string, string|int> $args */
    public function declareQueue(string $name, bool $durable = true, array $args = [], int $ch = 1, bool $passive = false, bool $exclusive = false, bool $autoDelete = false): array
    {
        $bits = ($passive ? 1 : 0) | ($durable ? 2 : 0) | ($exclusive ? 4 : 0) | ($autoDelete ? 8 : 0);
        $payload = pack('n', 0) . Codec::shortstr($name) . chr($bits) . self::table($args);
        $this->method($ch, 50, 10, $payload);
        $frame = $this->expect(50, 11);
        $o = 0;
        $queue = Codec::readShortstr($frame['args'], $o);
        $messages = unpack('N', substr($frame['args'], $o, 4))[1];
        $consumers = unpack('N', substr($frame['args'], $o + 4, 4))[1];
        return ['queue' => $queue, 'messages' => $messages, 'consumers' => $consumers];
    }

    public function declareExchange(string $name, string $kind, int $ch = 1, array $args = [], bool $internal = false, bool $passive = false): void
    {
        $bits = ($passive ? 1 : 0) | 2 | ($internal ? 8 : 0);
        $this->method($ch, 40, 10, pack('n', 0) . Codec::shortstr($name) . Codec::shortstr($kind) . chr($bits) . self::table($args));
        $this->expect(40, 11);
    }

    public function deleteExchange(string $name, int $ch = 1): void
    {
        $this->method($ch, 40, 20, pack('n', 0) . Codec::shortstr($name) . chr(0));
        $this->expect(40, 21);
    }

    public function bindExchange(string $destination, string $source, string $key = '', int $ch = 1): void
    {
        $this->method($ch, 40, 30, pack('n', 0) . Codec::shortstr($destination) . Codec::shortstr($source) . Codec::shortstr($key) . chr(0) . self::table([]));
        $this->expect(40, 31);
    }

    public function unbindExchange(string $destination, string $source, string $key = '', int $ch = 1): void
    {
        $this->method($ch, 40, 40, pack('n', 0) . Codec::shortstr($destination) . Codec::shortstr($source) . Codec::shortstr($key) . chr(0) . self::table([]));
        // exchange.unbind-ok is 40.51 in AMQP 0-9-1.
        $this->expect(40, 51);
    }

    /** @param array<string, string|int> $args */
    public function bind(string $queue, string $exchange, string $key = '', array $args = [], int $ch = 1): void
    {
        $this->method($ch, 50, 20, pack('n', 0) . Codec::shortstr($queue) . Codec::shortstr($exchange) . Codec::shortstr($key) . chr(0) . self::table($args));
        $this->expect(50, 21);
    }

    public function unbind(string $queue, string $exchange, string $key = '', int $ch = 1): void
    {
        $this->method($ch, 50, 50, pack('n', 0) . Codec::shortstr($queue) . Codec::shortstr($exchange) . Codec::shortstr($key) . self::table([]));
        $this->expect(50, 51);
    }

    public function purge(string $queue, int $ch = 1): int
    {
        $this->method($ch, 50, 30, pack('n', 0) . Codec::shortstr($queue) . chr(0));
        $frame = $this->expect(50, 31);
        return unpack('N', substr($frame['args'], 0, 4))[1];
    }

    public function deleteQueue(string $queue, int $ch = 1): int
    {
        $this->method($ch, 50, 40, pack('n', 0) . Codec::shortstr($queue) . chr(0));
        $frame = $this->expect(50, 41);
        return unpack('N', substr($frame['args'], 0, 4))[1];
    }

    public function consume(string $queue, string $tag = '', bool $noAck = false, int $ch = 1, bool $exclusive = false, ?int $priority = null): string
    {
        $bits = ($noAck ? 2 : 0) | ($exclusive ? 4 : 0);
        $args = $priority === null ? [] : ['x-priority' => $priority];
        $this->method($ch, 60, 20, pack('n', 0) . Codec::shortstr($queue) . Codec::shortstr($tag) . chr($bits) . self::table($args));
        $frame = $this->expect(60, 21);
        $o = 0;
        return Codec::readShortstr($frame['args'], $o);
    }

    public function cancel(string $tag, int $ch = 1): string
    {
        $this->method($ch, 60, 30, Codec::shortstr($tag) . chr(0));
        $frame = $this->expect(60, 31);
        $o = 0;
        return Codec::readShortstr($frame['args'], $o);
    }

    /**
     * Publishes one message. Properties use the same flag order the broker
     * parses, so the raw property bytes exercise the preserved propRaw path.
     *
     * @param array<string, string|int> $properties
     */
    public function publish(string $exchange, string $key, string $body, array $properties = [], bool $mandatory = false, bool $immediate = false, int $ch = 1): void
    {
        $this->write($this->publishBytes($exchange, $key, $body, $properties, $mandatory, $immediate, $ch));
    }

    /**
     * The bytes {@link publish} would send, so a test can write several
     * publishes in one socket write.
     *
     * @param array<string, string|int> $properties
     */
    public function publishBytes(string $exchange, string $key, string $body, array $properties = [], bool $mandatory = false, bool $immediate = false, int $ch = 1): string
    {
        $bits = ($mandatory ? 1 : 0) | ($immediate ? 2 : 0);
        $out = Codec::method($ch, 60, 40, pack('n', 0) . Codec::shortstr($exchange) . Codec::shortstr($key) . chr($bits));
        $out .= Codec::frame(2, $ch, pack('nn', 60, 0) . Codec::u64(strlen($body)) . self::properties($properties));
        if ($body !== '') {
            $out .= Codec::frame(3, $ch, $body);
        }
        return $out;
    }

    /** Writes bytes that were built earlier, as one socket write. */
    public function sendRaw(string $bytes): void
    {
        $this->write($bytes);
    }

    /** @return array{delivered:bool,body:?string,messages:int} */
    public function get(string $queue, bool $noAck = false, int $ch = 1): array
    {
        $this->method($ch, 60, 70, pack('n', 0) . Codec::shortstr($queue) . chr($noAck ? 1 : 0));
        $frame = $this->read();
        if ($frame === null) {
            throw new RuntimeException('basic.get: no reply');
        }
        if ($frame['class'] === 60 && $frame['method'] === 72) {
            return ['delivered' => false, 'body' => null, 'messages' => 0, 'tag' => 0, 'propRaw' => null];
        }
        if ($frame['class'] !== 60 || $frame['method'] !== 71) {
            throw new RuntimeException("basic.get: unexpected {$frame['class']}.{$frame['method']}");
        }
        $tag = Codec::readU64($frame['args'], 0);
        $o = 8 + 1;
        Codec::readShortstr($frame['args'], $o);
        Codec::readShortstr($frame['args'], $o);
        $messages = unpack('N', substr($frame['args'], $o, 4))[1];
        return [
            'delivered' => true,
            'body' => $frame['body'],
            'messages' => $messages,
            'tag' => $tag,
            'propRaw' => $frame['propRaw'],
        ];
    }

    public function ack(int $tag, int $ch = 1): void
    {
        $this->method($ch, 60, 80, Codec::u64($tag) . chr(0));
    }

    public function nack(int $tag, bool $requeue = true, int $ch = 1): void
    {
        $this->method($ch, 60, 120, Codec::u64($tag) . chr($requeue ? 2 : 0));
    }

    public function reject(int $tag, bool $requeue = true, int $ch = 1): void
    {
        $this->method($ch, 60, 90, Codec::u64($tag) . chr($requeue ? 1 : 0));
    }

    public function recover(bool $requeue = true, int $ch = 1): void
    {
        $this->method($ch, 60, 110, chr($requeue ? 1 : 0));
    }

    public function flow(bool $active, int $ch = 1): bool
    {
        $this->method($ch, 20, 20, chr($active ? 1 : 0));
        $frame = $this->expect(20, 21);
        return isset($frame['args'][0]) && (ord($frame['args'][0]) & 1) === 1;
    }

    public function tx(int $method, int $ch = 1): void
    {
        $this->method($ch, 90, $method, '');
    }

    public function method(int $ch, int $class, int $method, string $args = ''): void
    {
        $this->write(Codec::method($ch, $class, $method, $args));
    }

    /**
     * Reads the next method frame, assembling the content header and body
     * when the method carries one.
     *
     * @return array{class:int,method:int,ch:int,args:string,body:?string,propRaw:?string}|null
     */
    public function read(float $seconds = 5.0): ?array
    {
        $deadline = microtime(true) + $seconds;
        while (true) {
            $frame = $this->frame($deadline);
            if ($frame === null) {
                return null;
            }
            if ($frame['type'] !== 1 || strlen($frame['payload']) < 4) {
                continue;
            }
            $class = unpack('n', substr($frame['payload'], 0, 2))[1];
            $method = unpack('n', substr($frame['payload'], 2, 2))[1];
            $args = substr($frame['payload'], 4);
            $carriesContent = ($class === 60 && in_array($method, [60, 71, 50], true));
            $body = null;
            $propRaw = null;
            if ($carriesContent) {
                $header = $this->frame($deadline);
                if ($header === null || $header['type'] !== 2) {
                    return null;
                }
                $need = Codec::readU64($header['payload'], 4);
                $propRaw = substr($header['payload'], 12);
                $body = '';
                while (strlen($body) < $need) {
                    $chunk = $this->frame($deadline);
                    if ($chunk === null) {
                        return null;
                    }
                    if ($chunk['type'] !== 3) {
                        continue;
                    }
                    $body .= $chunk['payload'];
                }
            }
            return ['class' => $class, 'method' => $method, 'ch' => $frame['ch'], 'args' => $args, 'body' => $body, 'propRaw' => $propRaw];
        }
    }

    /** Reads until the given method arrives, failing on a close or timeout. */
    public function expect(int $class, int $method, float $seconds = 5.0): array
    {
        $deadline = microtime(true) + $seconds;
        while (microtime(true) < $deadline) {
            $frame = $this->read(max(0.05, $deadline - microtime(true)));
            if ($frame === null) {
                break;
            }
            if ($frame['class'] === $class && $frame['method'] === $method) {
                return $frame;
            }
            if ($frame['class'] === 20 && $frame['method'] === 40) {
                throw new RuntimeException('channel.close: ' . self::closeText($frame['args']));
            }
            if ($frame['class'] === 10 && $frame['method'] === 50) {
                throw new RuntimeException('connection.close: ' . self::closeText($frame['args']));
            }
        }
        throw new RuntimeException("timeout waiting for $class.$method");
    }

    /**
     * Reads frames until one matches, returning null instead of throwing when
     * a close arrives. Used by checks that expect a channel error.
     *
     * @return array{code:int,text:string}|null
     */
    public function expectClose(float $seconds = 5.0): ?array
    {
        $deadline = microtime(true) + $seconds;
        while (microtime(true) < $deadline) {
            $frame = $this->read(max(0.05, $deadline - microtime(true)));
            if ($frame === null) {
                return null;
            }
            $isChannelClose = $frame['class'] === 20 && $frame['method'] === 40;
            $isConnectionClose = $frame['class'] === 10 && $frame['method'] === 50;
            if ($isChannelClose || $isConnectionClose) {
                return [
                    'code' => unpack('n', substr($frame['args'], 0, 2))[1],
                    'text' => self::closeText($frame['args']),
                    // 10 for a connection close, 20 for a channel close, so a
                    // test can assert which scope was torn down.
                    'class' => $frame['class'],
                ];
            }
        }
        return null;
    }

    private static function closeText(string $args): string
    {
        $o = 2;
        return Codec::readShortstr($args, $o);
    }

    /** @return array{type:int,ch:int,payload:string}|null */
    private function frame(float $deadline): ?array
    {
        while (true) {
            if (strlen($this->buf) >= 7) {
                $len = unpack('N', substr($this->buf, 3, 4))[1];
                if (strlen($this->buf) >= 8 + $len) {
                    $type = ord($this->buf[0]);
                    $ch = unpack('n', substr($this->buf, 1, 2))[1];
                    $payload = substr($this->buf, 7, $len);
                    $this->buf = substr($this->buf, 8 + $len);
                    return ['type' => $type, 'ch' => $ch, 'payload' => $payload];
                }
            }
            $left = $deadline - microtime(true);
            if ($left <= 0) {
                return null;
            }
            $read = [$this->fp];
            $write = [];
            $except = [];
            $sec = (int) $left;
            if (@stream_select($read, $write, $except, $sec, (int) (($left - $sec) * 1000000)) < 1) {
                continue;
            }
            $chunk = @fread($this->fp, 65536);
            if ($chunk === false || $chunk === '') {
                $meta = stream_get_meta_data($this->fp);
                if ($meta['eof'] ?? false) {
                    return null;
                }
                continue;
            }
            $this->buf .= $chunk;
        }
    }

    private function write(string $bytes): void
    {
        $at = 0;
        $total = strlen($bytes);
        while ($at < $total) {
            $n = @fwrite($this->fp, substr($bytes, $at));
            if ($n === false || $n === 0) {
                throw new RuntimeException('write failed');
            }
            $at += $n;
        }
    }

    /**
     * Encodes a content header property block. Flag bits follow the AMQP
     * order: content-type, content-encoding, headers, delivery-mode,
     * priority, correlation-id, reply-to, expiration.
     *
     * @param array<string, string|int> $p
     */
    private static function properties(array $p): string
    {
        $flags = 0;
        $out = '';
        if (isset($p['contentType'])) {
            $flags |= 0x8000;
            $out .= Codec::shortstr((string) $p['contentType']);
        }
        if (isset($p['contentEncoding'])) {
            $flags |= 0x4000;
            $out .= Codec::shortstr((string) $p['contentEncoding']);
        }
        if (isset($p['headers']) && is_array($p['headers'])) {
            $flags |= 0x2000;
            $out .= self::table($p['headers']);
        }
        $flags |= 0x1000;
        $out .= chr((int) ($p['deliveryMode'] ?? 2));
        if (isset($p['priority'])) {
            $flags |= 0x0800;
            $out .= chr((int) $p['priority']);
        }
        if (isset($p['correlationId'])) {
            $flags |= 0x0400;
            $out .= Codec::shortstr((string) $p['correlationId']);
        }
        if (isset($p['replyTo'])) {
            $flags |= 0x0200;
            $out .= Codec::shortstr((string) $p['replyTo']);
        }
        if (isset($p['expiration'])) {
            $flags |= 0x0100;
            $out .= Codec::shortstr((string) $p['expiration']);
        }
        return pack('n', $flags) . $out;
    }

    /** @param array<string, string|int> $fields */
    public static function table(array $fields): string
    {
        $inner = '';
        foreach ($fields as $name => $value) {
            $inner .= Codec::shortstr((string) $name);
            if (is_int($value)) {
                $inner .= 'I' . pack('N', $value & 0xffffffff);
            } elseif (is_array($value)) {
                // A field array, which is how a CC or BCC header carries
                // more than one routing key.
                $items = '';
                foreach ($value as $item) {
                    $items .= 'S' . Codec::longstr((string) $item);
                }
                $inner .= 'A' . pack('N', strlen($items)) . $items;
            } else {
                $inner .= 'S' . Codec::longstr((string) $value);
            }
        }
        return pack('N', strlen($inner)) . $inner;
    }

    /**
     * Decodes the headers table out of a raw property block, so a test can
     * assert on headers the broker attached, such as x-death.
     *
     * @return array<string, mixed>
     */
    public static function headersOf(?string $propRaw): array
    {
        if ($propRaw === null || strlen($propRaw) < 2) {
            return [];
        }
        $flags = unpack('n', substr($propRaw, 0, 2))[1];
        $at = 2;
        if (($flags & 0x8000) !== 0) {
            Codec::readShortstr($propRaw, $at);
        }
        if (($flags & 0x4000) !== 0) {
            Codec::readShortstr($propRaw, $at);
        }
        if (($flags & 0x2000) === 0) {
            return [];
        }
        return Codec::readTable($propRaw, $at);
    }

    public function close(): void
    {
        if (is_resource($this->fp)) {
            @fclose($this->fp);
        }
    }
}
